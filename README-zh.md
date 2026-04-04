# Velaria：纯 C++17 本地数据流内核

`README-zh.md` 是中文镜像文档，对应英文主文档位于 [README.md](./README.md)。两份文档必须保持同步。

Velaria 是一个本地优先的 C++17 数据流引擎研究项目。当前目标保持收敛：

- 保持一个 native kernel 作为执行真相来源
- 稳住单机链路
- 通过正式支持的 Python 生态层向外暴露能力
- 把同机 actor/rpc 路径放在实验通道，而不是做成第二套内核

## 执行方向

核心 runtime 的方向明确是 column-first。

这意味着：

- 内部执行应优先采用列式 ownership 和算子链路
- row materialization 是 sink、调试和遗留边界的兼容层，不是主执行形态
- 新的热路径工作应减少 `row -> column -> row` 转换，而不是把它常态化
- 只要公开 contract 不变，性能收益优先于保留 row-heavy 内部实现

实际规则：

- batch 路径如果能够一直保持列式直到最终边界，就不应提前回退到 rows
- 仍然存在的 row fallback 必须是显式的、lazy 的，并且局限在确实需要它的边界

## 分层模型

### Core Kernel

负责：

- 本地 batch 与 streaming 执行
- column-first 的执行策略与内部数据布局方向
- logical planning 与最小 SQL 映射
- source/sink ABI
- explain / progress / checkpoint contract
- 本地 vector search

仓库入口：

- 文档：
  - [docs/core-boundary.md](./docs/core-boundary.md)
  - [docs/runtime-contract.md](./docs/runtime-contract.md)
  - [docs/streaming_runtime_design.md](./docs/streaming_runtime_design.md)
- source group：
  - `//:velaria_core_logical_sources`
  - `//:velaria_core_execution_sources`
  - `//:velaria_core_contract_sources`
- regression：
  - `//:core_regression`

### Python Ecosystem

负责：

- `python_api` 里的 native binding
- Arrow 输入与输出
- `uv` 工作流
- wheel / native wheel / CLI 打包
- Excel / Bitable / custom stream adapter
- 本地 run 与 artifact 的 workspace 跟踪

不负责：

- 执行热路径语义
- 独立的 explain / progress / checkpoint 语义
- 替代内核 checkpoint 存储
- 把 SQLite 当成大结果输出引擎

仓库入口：

- 文档：
  - [python_api/README.md](./python_api/README.md)
- source group：
  - `//:velaria_python_ecosystem_sources`
  - `//python_api:velaria_python_supported_sources`
  - `//python_api:velaria_python_example_sources`
  - `//python_api:velaria_python_experimental_sources`
- regression：
  - `//:python_ecosystem_regression`
  - `//python_api:velaria_python_supported_regression`
  - `./scripts/run_python_ecosystem_regression.sh`

### Experimental Runtime

负责：

- 同机 `actor/rpc/jobmaster` 实验
- transport / codec / scheduler 观测
- 同机 smoke 与 benchmark 工具

不代表：

- 已完成 distributed scheduling
- 已完成 distributed fault recovery
- 已完成 cluster resource governance
- 已支持 production 级 distributed execution

仓库入口：

- source group：
  - `//:velaria_experimental_sources`
- regression：
  - `//:experimental_regression`
  - `./scripts/run_experimental_regression.sh`

## 黄金路径

```text
Arrow / CSV / Python ingress
  -> DataflowSession / DataFrame / StreamingDataFrame
  -> local runtime kernel
  -> sink
  -> explain / progress / checkpoint
```

公开 session 入口：

- `DataflowSession`

核心对外对象：

- `DataFrame`
- `StreamingDataFrame`
- `StreamingQuery`

## 稳定 Runtime Contract

主要 stream 入口：

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.explainStreamSql(sql, options)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`

稳定 contract surface：

- `StreamingQueryProgress`
- `snapshotJson()`
- `explainStreamSql(...)`
- `execution_mode / execution_reason / transport_mode`
- `checkpoint_delivery_mode`
- source/sink lifecycle：`open -> nextBatch -> checkpoint -> ack -> close`

`explainStreamSql(...)` 固定返回：

- `logical`
- `physical`
- `strategy`

`strategy` 统一解释 mode 选择、fallback reason、transport、backpressure 与 checkpoint delivery mode。

当 stream SQL 以 sink 为目标时，`logical` 与 `physical` 也会显式保留 source/sink 绑定信息，保证 explain 输出与真实运行路径一致。

workspace 落盘会保留内核 contract，不会重定义它们：

- `explain.json` 保存 `logical / physical / strategy`
- `progress.jsonl` 逐行追加原生 `snapshotJson()` 输出
- 大结果保留为文件；SQLite 只保存索引元数据和小 preview

rows 不是内核 contract 的优化目标。
它们保留为部分边界的兼容表示；内部执行应尽可能保持 column-first。

## 当前范围

当前已具备：

- 一个 native kernel 同时支持 batch + streaming
- 明确朝 column-first batch 执行推进，并在兼容边界上采用 lazy row materialization
- `read_csv`, `readStream(...)`, `readStreamCsvDir(...)`
- query-local 反压、有界 backlog、progress snapshot、checkpoint path
- 执行模式：`single-process`、`local-workers`
- 文件 source/sink
- core SQL v1 的 batch 路径：
  - `CREATE TABLE`、`CREATE SOURCE TABLE`、`CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - 带列投影/别名、`WHERE`、`GROUP BY`、`ORDER BY`、`LIMIT`、当前最小 `JOIN` 的 `SELECT`
- batch SQL 内建函数：
  - `LOWER`、`UPPER`、`TRIM`、`LTRIM`、`RTRIM`
  - `LENGTH`、`LEN`、`CHAR_LENGTH`、`CHARACTER_LENGTH`、`REVERSE`
  - `CONCAT`、`CONCAT_WS`、`LEFT`、`RIGHT`、`SUBSTR` / `SUBSTRING`、`POSITION`、`REPLACE`
  - `ABS`、`CEIL`、`FLOOR`、`ROUND`、`YEAR`、`MONTH`、`DAY`
- 基础 streaming operators：`select / filter / withColumn / drop / limit / window / orderBy`
- stateful streaming 聚合：`sum / count / min / max / avg`
- 与当前内核对齐的 stream SQL 子集：
  - `session.streamSql(...)` 只接收 `SELECT`
  - `session.explainStreamSql(...)` 接收 `SELECT` 或 `INSERT INTO <sink> SELECT ...`
  - `session.startStreamSql(...)` 只接收 `INSERT INTO <sink> SELECT ...`
  - stream source 必须来自 source table，stream target 必须是 sink table
  - stream `ORDER BY` 当前只接受 bounded source
  - window / stateful aggregate 的 explain 继续保持 `logical / physical / strategy`
- 固定维度 float vector 的本地检索
- Python Arrow 输入/输出
- 本地 tracked run、run 目录落盘与 artifact 索引
- 同机 actor/rpc/jobmaster smoke 链路

当前不做：

- 宣称已完成 distributed runtime
- 把 Python callback 或 Python UDF 拉进热路径
- 更宽泛的 SQL 扩展，例如更复杂的 `JOIN`、`CTE`、`subquery`、`UNION`
- ANN / 独立 vector DB / 分布式 vector 执行

当前 SQL v1 约束：

- `CREATE SOURCE TABLE` 是只读表，拒绝 `INSERT`
- `CREATE SINK TABLE` 可写但不能作为查询输入
- `ORDER BY` 当前只会解析 `SELECT` 输出中可见的列
- unbounded stream 上的 `ORDER BY` 会被显式拒绝；当前 runtime 只对 bounded source 定义全局排序语义
- stream SQL 遇到 batch-only 形态时会优先返回明确的 `not supported in SQL v1` 或 table-kind 错误，而不是模糊运行时失败

## Plan

当前有效计划位于：

- [plans/core-runtime-columnar-plan.md](./plans/core-runtime-columnar-plan.md)

这份文档集中维护：

- 已实现项
- 明确不做项
- 下一阶段

本仓库的性能准则：

- 优先选择更少复制的列式执行，而不是 row-heavy 的便利实现
- Python / Arrow / CLI 导出成本属于边界成本，不能作为把内部重新做回 row-first 的理由
- 修改热路径时应补 benchmark，让 column-first 收益保持可测量

## Python Ecosystem

当前支持的 Python surface：

- `Session.read_csv(...)`
- `Session.sql(...)`
- `Session.create_dataframe_from_arrow(...)`
- `Session.create_stream_from_arrow(...)`
- `Session.create_temp_view(...)`
- `Session.read_stream_csv_dir(...)`
- `Session.stream_sql(...)`
- `Session.explain_stream_sql(...)`
- `Session.start_stream_sql(...)`
- `Session.vector_search(...)`
- `Session.explain_vector_search(...)`
- `read_excel(...)`
- custom source / custom sink adapter

### Workspace 模型

- `Workspace`
  - 根目录位于 `VELARIA_HOME` 或 `~/.velaria`
- `RunStore`
  - 每次执行对应一个 run 目录
  - 持久化 `run.json`、`inputs.json`、`explain.json`、`progress.jsonl`、日志和 `artifacts/`
- `ArtifactIndex`
  - 默认使用 SQLite 做元数据索引
  - SQLite 不可用时退化到 JSONL
  - 只缓存小结果 preview

这一层主要服务于 agent / skill 调用、本地可追踪性和机器可读 CLI 集成；它不是第二套执行引擎。

### CLI 真实入口

仓库内真实可见的 CLI 入口是：

- 源码目录：
  - `uv run --project python_api python python_api/velaria_cli.py ...`
- 安装 wheel 或本地 package 后：
  - `velaria-cli ...`
  - `velaria_cli ...`
- 打包产物：
  - `./dist/velaria-cli ...`

全局命令只应在你已经安装 wheel 或 package 之后使用。

### Python 工作流

最小本地启动方式：

```bash
uv sync --project python_api --python python3.13
uv run --project python_api python python_api/examples/demo_batch_sql_arrow.py
```

完整的开发、构建、smoke 和回归命令见：

- [docs/development-zh.md](./docs/development-zh.md)

## Local Vector Search

vector search 是本地内核能力，不是独立子系统。

当前范围：

- fixed-dimension `float32`
- 指标：`cosine`、`dot`、`l2`
- `top-k`
- exact scan only
- Python `Session.vector_search(...)`
- Arrow `FixedSizeList<float32>`
- explain 输出

推荐的本地 CSV vector 文本格式：

- `[1 2 3]`
- `[1,2,3]`

设计文档：

- [docs/local_vector_search_v01.md](./docs/local_vector_search_v01.md)

CLI 示例：

```bash
uv run --project python_api python python_api/velaria_cli.py csv-sql \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

./dist/velaria-cli vector-search \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --metric cosine \
  --top-k 5
```

vector explain 属于稳定 contract，当前字段包括：

- `mode=exact-scan`
- `metric=<cosine|dot|l2>`
- `dimension=<N>`
- `top_k=<K>`
- `candidate_rows=<M>`
- `filter_pushdown=false`
- `acceleration=flat-buffer+heap-topk`

benchmark 基线：

```bash
./scripts/run_vector_search_benchmark.sh
```

## 开发说明

仓库内的构建、smoke 和回归入口统一放在：

- [docs/development-zh.md](./docs/development-zh.md)

## 仓库规则

- 语言基线：`C++17`
- 构建系统：`Bazel`
- 对外 session 入口保持为 `DataflowSession`
- 不要破坏 `sql_demo / df_demo / stream_demo`
- 示例源码统一使用 `.cc`
- 本仓库中的 Python 命令统一使用 `uv`
- `README.md` 与 `README-zh.md` 必须保持同步
