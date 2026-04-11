# Velaria 分析型存储格式 v1 设计

## 当前角色

这份文档是 Velaria 内部优先分析型本地存储格式的 v1 设计说明。

它的目标不是把仓库重新定义成单文件数据库，也不是替代 Parquet / Arrow IPC 作为外部交换格式，而是为后续本地持久表、过滤/聚合裁剪和流式小批 flush 提供一个可直接实现的目录式列存格式方案。

当前主状态板仍然是 [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md)。本文聚焦一个更窄的问题：在不改动现有 public contract 的前提下，Velaria 的内部分析表该如何落盘。

## 1. 当前仓库现状与缺口

当前仓库已经具备列式内存表示、批流统一执行核心和本地 checkpoint / state 机制，但还没有内核级表存储格式。

- `DataflowSession` 当前公开的稳定 batch 文件读取入口仍然是 `read(...)`、`read_csv(...)`、`read_line_file(...)`、`read_json(...)`，以及 SQL 上的 `CREATE TABLE ... USING csv|line|json` 与基于 probe 的 `CREATE TABLE ... OPTIONS(path: '...')`。
  - 参考：[docs/runtime-contract.md](../docs/runtime-contract.md)
  - 代码位置：[session.cc](../src/dataflow/core/contract/api/session.cc)
- `CREATE TABLE` 当前只是把 schema 注册到内存 catalog，`INSERT` 也是把数据 append 到内存 `Table`，没有持久 catalog/table 文件。
  - 参考：[session.cc](../src/dataflow/core/contract/api/session.cc)
  - 参考：[catalog.cc](../src/dataflow/core/contract/catalog/catalog.cc)
- 核心表模型已经有 `Table + columnar_cache`，并且 `ColumnarTable`、`ValueColumnBuffer`、`ArrowColumnBacking` 已经表达出列式缓存和 Arrow-like backing 的方向。
  - 参考：[table.h](../src/dataflow/core/execution/table.h)
  - 参考：[columnar_batch.h](../src/dataflow/core/execution/columnar_batch.h)
- streaming 路径已有本地 `StateStore`、本地 checkpoint 文件，以及流式聚合 state 的序列化逻辑，但这些能力仍然服务 query-local state 和 checkpoint，不是通用分析表文件格式。
  - 参考：[stream.h](../src/dataflow/core/execution/stream/stream.h)
  - 参考：[stream.cc](../src/dataflow/core/execution/stream/stream.cc)
  - 参考：[docs/runtime-contract.md](../docs/runtime-contract.md)
- 分层边界已经明确：public session 入口仍然是 `DataflowSession`；Python 生态层不能发明第二套执行语义；experimental runtime 不能重定义 batch/stream contract。
  - 参考：[docs/core-boundary.md](../docs/core-boundary.md)

结论：

- 当前缺的不是“再加一种外部交换格式”，而是“给 Velaria 自己的列式运行时增加一个内部优先的分析表落盘格式”。
- 这个格式必须服从现有内核边界，而不是反过来驱动新的 public contract。

## 2. 目标与非目标

### 2.1 v1 目标

- 为单表提供目录式本地 bundle 存储。
- 保持列式 block 组织，服务 OLAP 扫描、过滤、聚合。
- 支持 batch append。
- 支持 stream micro-batch flush，每次 flush 形成新的 append-only segment。
- 支持 block 级 schema / stats 读取、列裁剪、基础 stats 裁剪。
- 保持与现有 `Table` / `ColumnarTable` / `ValueColumnBuffer` 的映射关系清晰。
- 不改变 `DataflowSession` 作为唯一 public session 入口。
- 不改变现有 `explain / progress / checkpoint` 语义。

### 2.2 明确非目标

v1 不做以下内容：

- WAL
- 原地 `update/delete`
- 多表 catalog 容器
- 分布式副本与远端存储协议
- 对外稳定文件标准承诺
- Parquet / Arrow IPC 替代
- 内嵌向量 ANN 索引
- segment compaction
- MVCC / snapshot isolation
- 从 block stats 直接回答完整聚合结果

## 3. 总体结论

v1 采用以下固定方向：

- 形态：`directory bundle`
- 定位：`internal format first`
- 组织：`append-only segments + columnar blocks + sidecar stats`
- 写入：`batch append + mixed stream flush`
- 读取：`schema load + manifest enumerate + column prune + stats prune + block scan`
- 恢复：`segment 完成标记 + manifest 原子替换`

这不是 SQLite 风格的单文件 `.db`。原因很简单：

- 现阶段更需要 append 和 flush 简单性，而不是单文件封装感。
- 目录式布局更适合 segment 级提交、孤儿清理、后续 compaction 和多进程共享读取。
- 当前 streaming 已经有 query-local checkpoint / state；分析表格式不应在 v1 混入统一 DB 容器语义。

## 4. v1 逻辑模型

v1 只定义四层逻辑对象：

- `TableBundle`
  - 单表的目录根
  - 持有 manifest、schema 和全部 committed segments
- `Segment`
  - 一次 append 或一次 stream flush 的提交单元
  - append-only，提交后不可修改
- `Block`
  - segment 内的列式数据块
  - scan / prune 的基本数据单元
- `BlockStats`
  - 每个 block 的侧边统计信息
  - 用于列裁剪外的粗粒度跳过与成本估计

固定规则：

- 一个 table bundle 只承载一个表。
- 一个 segment 可以包含一个或多个 block。
- 一次 batch append 或一次 stream flush 只生成一个新 segment。
- 一个 block 内的所有列行数必须一致。
- manifest 只列出 committed segments；未进入 manifest 的目录视为未提交数据。

## 5. 推荐目录结构

推荐目录结构固定为：

```text
<table>.velaria/
  table.manifest
  schema.json
  segments/
    seg-000000000001/
      segment.meta.json
      segment.complete
      block-000000/
        col-000.bin
        col-001.bin
        ...
        block.stats.json
      block-000001/
        ...
    seg-000000000002/
      ...
```

各文件职责固定如下：

- `table.manifest`
  - 当前 table bundle 的唯一提交视图
  - JSON 文本，使用临时文件写完后原子替换
- `schema.json`
  - 当前表 schema
  - JSON 文本，phase-1 固定为单 schema 版本，不支持在线 schema evolution
- `segments/seg-*/segment.meta.json`
  - segment 级元数据
  - 包含 `segment_id`、`created_at`、`flush_reason`、`row_count`、`block_count`
- `segments/seg-*/segment.complete`
  - segment 物理写入完成标记
  - 先写 block 和 meta，最后写这个标记
- `segments/seg-*/block-*/col-*.bin`
  - 单列二进制数据文件
- `segments/seg-*/block-*/block.stats.json`
  - block 级 sidecar stats

v1 不单独建立根级 `stats/` 目录，统一使用 block 侧边 `block.stats.json`。

## 6. Manifest 与元数据结构

### 6.1 `table.manifest`

`table.manifest` 固定包含以下字段：

- `format = "velaria-analytic-v1"`
- `layout = "directory-bundle"`
- `table_id`
- `schema_file = "schema.json"`
- `next_segment_sequence`
- `total_rows`
- `segments`

`segments` 每项固定包含：

- `segment_id`
- `path`
- `row_count`
- `block_count`
- `flush_reason`
- `committed_at`

约束：

- reader 只信任 manifest 列出的 segments。
- 目录扫描不能替代 manifest。
- manifest 更新方式必须与当前 checkpoint 的原子替换思路保持一致：先写临时文件，再 rename 替换。

### 6.2 `schema.json`

`schema.json` 固定保存：

- `table_id`
- `fields`

每个 field 固定保存：

- `name`
- `logical_type`
- `nullable`
- `fixed_vector_dim`，仅 `FixedVector` 使用

v1 支持的逻辑类型与当前 `Value` 对齐：

- `Nil`
- `Bool`
- `Int64`
- `Double`
- `String`
- `FixedVector`

参考：[value.h](../src/dataflow/core/execution/value.h)

## 7. 列文件与类型编码

v1 列文件统一使用 `col-XXX.bin`，格式原则固定为：

- 小端序
- 每个列文件只存一个 block 中的一列
- 列文件内部顺序固定为：
  - column header
  - null bitmap
  - payload

### 7.1 固定宽类型

`Bool`、`Int64`、`Double` 使用固定宽 payload：

- `Bool`
  - payload 使用 `uint8_t` 数组，`0/1` 表示值
  - null 仍由独立 null bitmap 表达
- `Int64`
  - payload 使用连续 `int64_t`
- `Double`
  - payload 使用连续 `double`

### 7.2 `String`

`String` 使用：

- null bitmap
- `uint64_t` offsets 数组，长度为 `row_count + 1`
- UTF-8 bytes payload

不做字典编码，不做 page 内压缩，不做 Bloom filter。

### 7.3 `FixedVector`

`FixedVector` 使用：

- null bitmap
- 连续 `float32` payload
- 维度由 schema 和 column header 双重记录

payload 行布局固定为：

- 第 `i` 行向量占据 `[i * dim, (i + 1) * dim)` 范围

v1 不在向量列上构建 ANN 索引，也不定义向量专属 block stats。

### 7.4 与内存模型的映射

落盘设计要尽量贴近当前列式内存模型：

- `Table` 是公开/兼容边界上的表对象。
- `columnar_cache` 已经说明运行时倾向于保留列式 sidecar。
- `ColumnarTable.columns` + `arrow_formats` 已经足够作为持久格式 writer/reader 的内存起点。

参考：

- [table.h](../src/dataflow/core/execution/table.h)
- [columnar_batch.h](../src/dataflow/core/execution/columnar_batch.h)

## 8. Block 与 stats 规则

### 8.1 block 形成规则

v1 默认 block 目标行数固定为 `65,536`。

规则：

- 一次 append / flush 先生成一个 segment。
- segment 内按 `65,536` 行切分 block。
- 如果本次输入不足 `65,536` 行，仍然生成一个 block，不做跨 segment 拼接。
- stream micro-batch flush 默认一个 flush 一个 segment；即使只有很少行，也直接提交，不等待未来 flush 合并。

这样做的代价是 small segments 会变多，但它换来了更简单的提交边界和恢复逻辑。segment compaction 明确保留到 phase-2。

### 8.2 `block.stats.json`

每个 block 固定记录：

- `block_id`
- `row_count`
- `columns`

每列固定记录：

- `name`
- `logical_type`
- `row_count`
- `null_count`

此外：

- `Bool`
  - 记录 `min_bool` / `max_bool`
- `Int64`
  - 记录 `min_i64` / `max_i64`
  - 记录 `sum_f64`
  - 记录 `non_null_count`
- `Double`
  - 记录 `min_f64` / `max_f64`
  - 记录 `sum_f64`
  - 记录 `non_null_count`
- `String`
  - 记录 `min_string` / `max_string`
- `FixedVector`
  - 只记录 `dimension`
  - 可选记录 `non_null_count`

约束：

- `sum_f64` 只用于 `Int64/Double`。
- `String` 不做字典统计，不做 distinct 估计，不做前缀索引。
- `FixedVector` stats 不用于相似度裁剪。

## 9. 读取模型

v1 读取流程固定为：

1. 打开 `table.manifest`
2. 读取 `schema.json`
3. 枚举 committed segments
4. 对每个 segment 枚举 blocks
5. 先读取 `block.stats.json`
6. 基于需要的列和谓词做列裁剪与 stats 裁剪
7. 只打开 surviving blocks 的目标列文件
8. 读入列文件并构造 `ColumnarTable`
9. 只有在兼容边界才 materialize rows

### 9.1 phase-1 支持的 stats 裁剪

phase-1 只支持以下 block 级裁剪判定：

- 单列比较谓词：
  - `=`
  - `!=`
  - `<`
  - `<=`
  - `>`
  - `>=`
- 作用类型：
  - `Bool`
  - `Int64`
  - `Double`
  - `String`

复杂表达式规则：

- `AND`
  - 两边都可静态判定时，合并裁剪结果
- `OR`
  - phase-1 不做 aggressive skip，默认保守保留 block
- 函数表达式、cast、向量谓词
  - 不使用 stats 裁剪

### 9.2 聚合与 stats 的关系

phase-1 中，block stats 只用于：

- 跳过不可能命中的 block
- 给 scan / planner 提供粗粒度成本信息

phase-1 不允许：

- 仅靠 stats 回答 `SUM/MIN/MAX/COUNT`
- 把 stats 当成独立二级索引层

## 10. 写入模型

### 10.1 batch append

batch append 固定流程：

1. 输入 `Table` 或 `ColumnarTable`
2. 若只有 rows，则先构造 `columnar_cache`
3. 分配新 `segment_id`
4. 写 `segment.meta.json.tmp`
5. 按 block 切分并写每列 `col-*.bin`
6. 写每个 `block.stats.json`
7. 落 `segment.complete`
8. 重写 `table.manifest.tmp`
9. 原子替换 `table.manifest`

### 10.2 stream micro-batch flush

stream flush 固定沿用 batch append 路径，不单独发明第二套文件格式。

规则：

- 每次 flush 形成一个独立 segment
- `flush_reason` 固定记录为 `stream-flush`
- 现有 `checkpoint_path` 和 `StateStore` 继续保持原语义
- 分析表 segment 提交不能替代 stream checkpoint 成功语义

也就是说：

- stream flush 写入分析表，只是数据落盘
- checkpoint 仍由现有 streaming contract 管理
- 这两个动作在 phase-1 不合并成一个统一事务

参考：

- [stream.h](../src/dataflow/core/execution/stream/stream.h)
- [stream.cc](../src/dataflow/core/execution/stream/stream.cc)
- [docs/runtime-contract.md](../docs/runtime-contract.md)

## 11. 一致性、恢复与孤儿清理

v1 一致性策略固定为“manifest 提交，segment 完成标记辅助恢复”。

### 11.1 提交准则

- `segment.complete` 存在但 manifest 未引用：
  - 视为未提交孤儿 segment
  - reader 不可见
- manifest 已引用某 segment：
  - reader 视为 committed
- manifest 永远是读取视图的最终来源

### 11.2 恢复准则

打开 bundle 时：

- 如果 segment 目录存在但没有 `segment.complete`
  - 直接忽略
- 如果有 `segment.complete` 但 manifest 未引用
  - 视为 orphan
  - phase-1 默认忽略，可记录 warning
- 如果 manifest 引用了不存在的 segment
  - 视为 bundle 损坏
  - 读取失败，不做自动修复

### 11.3 原子替换

manifest 写法必须沿用当前 checkpoint 的成熟做法：

- 先写临时文件
- 关闭并 flush
- rename 替换目标文件

参考当前 checkpoint 逻辑：[stream.cc](../src/dataflow/core/execution/stream/stream.cc)

## 12. 与现有接口的接入边界

### 12.1 文件格式层

文件格式层负责：

- segment / block / column file 编解码
- stats 生成与读取
- manifest 与 schema 元数据读写

它不负责：

- SQL 解析
- public session 命名
- checkpoint / progress contract

### 12.2 执行接入层

执行接入层负责：

- scan reader
- column prune
- stats prune
- `ColumnarTable` materialization

它不负责：

- catalog 命名持久化
- stream checkpoint 事务

### 12.3 SQL / Session 暴露层

v1 只定义候选暴露方式，不在本文中把它们写成既定 public API：

- 候选 1：`session.read(path)` 通过 probe 识别 bundle
- 候选 2：增加内部 helper，例如 `session.read_velaria(path)`，后续再决定是否公开
- 候选 3：SQL 候选 `CREATE TABLE ... USING velaria OPTIONS(path: '...')`

固定约束：

- 不引入新的 public session 类型
- 仍然以 `DataflowSession` 作为唯一对外 session 入口
- 不修改现有 batch file reader contract
- 不修改现有 `explain / progress / checkpoint` 语义

参考：

- [docs/core-boundary.md](../docs/core-boundary.md)
- [docs/runtime-contract.md](../docs/runtime-contract.md)

## 13. phase-1 实现切片

phase-1 实现范围固定为：

- 单表 bundle
- append-only segments
- block sidecar stats
- 基础 scan 读取
- 列裁剪
- 基础 stats 裁剪

phase-1 任务拆分固定为：

1. 定义 `schema.json` / `table.manifest` / `segment.meta.json` / `block.stats.json` 结构。
2. 实现 `Table` / `ColumnarTable` 到 `segment/block` 的 writer。
3. 实现列文件 reader，并恢复为 `ColumnarTable`。
4. 实现基于 `block.stats.json` 的基础 prune evaluator。
5. 实现 bundle scan 的内部入口。
6. 在不改 public contract 的前提下，为后续 `session.read(...)` 或 SQL `USING velaria` 预留接入点。

phase-1 明确不触碰：

- stream checkpoint 文件格式
- `StateStore` 后端与 state key 语义
- distributed runtime
- root README
- `sql_demo / df_demo / stream_demo`

## 14. 验收标准

实现者完成 phase-1 时，至少要满足以下验收点：

- 能把一个内存 `Table` 写成单表 bundle。
- 能从 bundle 扫描出 `ColumnarTable`，并保持 schema 与行数一致。
- 针对 `Int64/Double/String` 的简单比较谓词，block stats 裁剪能正确跳过不命中的 block。
- batch append 与 stream flush 都走同一套 segment writer。
- 流式 checkpoint 路径仍保持现有本地文件 contract，不被 bundle 写入逻辑替换。
- `sql_demo / df_demo / stream_demo` 行为不变。

## 15. phase-2 留白

phase-2 可以继续讨论，但不属于 v1 承诺：

- segment compaction
- schema evolution
- 多表 catalog 元数据
- 压缩编码
- Bloom filter / 更细粒度索引
- 向量列专用索引
- 统一表写入与 stream checkpoint 的恢复编排
- 多进程共享映射与只读并发扫描优化

## 16. 最终结论

Velaria 需要的不是一个泛化的单文件 `.db` 容器，而是一套内部优先、目录式、列式 block 组织的分析表格式。

v1 的边界已经固定：

- 单表 bundle
- append-only segments
- sidecar block stats
- batch append + stream flush 共用写入路径
- `DataflowSession` 入口不变
- `checkpoint / progress / explain` contract 不变

在这个边界内推进，能够补上当前仓库最真实的缺口，同时不把系统过早推向一个职责过重的“统一数据库容器”。
