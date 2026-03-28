# DataFrame 优先：语义对齐 C++ 引擎 v1 兼容计划

## 结论（直接执行）
你明确了方向：**DataFrame 优先 + 流式优先**。

接下来我们采用统一 `数据计划层（DataFrame/Streaming共享）` 策略：
- 批处理 DataFrame 保持 v0.1 能力不变
- 流处理 API（`readStream/writeStream`）进入 v0.2，并在同一执行核心上复用算子
- RDD API 作为 `兼容扩展层` 后置

---

## 1) v1 目标（先行交付，8~12 周）

### A. 核心能力（必须）
1. **Session/Context 与作业入口**
- `createSession()/DataflowSession` 兼容入口
   - `session.read()`、`session.createDataFrame()`
   - `read.csv/parquet/json/orc`（v1）
   - `write`：`overwrite/append` + `parquet/csv`

2. **DataFrame API（核心 60%）**
   - Projection：`select`, `withColumn`, `withColumnRenamed`, `drop`
   - Filter：`filter/where`
   - 聚合：`groupBy`, `agg`, `sum/count/avg/min/max`, `countDistinct`
   - Join：`join`（inner, left, right）
   - 核心转换：`selectExpr`, `orderBy/sort`, `limit`, `distinct`, `dropDuplicates`
   - 操作控制：`cache/persist`, `repartition`, `coalesce`
   - Actions：`show`, `collect`, `count`, `take`, `first`, `foreachPartition`

3. **SQL 支持**
   - `session.sql(sqlText)`
   - SQL 方言：先支持标准 ANSI + 常用函数
   - CTAS：`CREATE TABLE AS` / `INSERT INTO`（可简化）
   - Catalog：临时视图 `createTempView`, `createOrReplaceTempView`

4. **分布式执行（MVP）**
   - DAG 构建 -> 执行计划 -> stage/task 下发
   - 分区器（hash/range）
   - Shuffle（落盘+网络，支持 join/agg/repartition）
   - 失败重试与 task 级重算（lineage 恢复思路）
   - 基础监控（job/stage/task 状态、耗时、失败重试次数）

5. **兼容层语义与边界**
- 在语义对齐调用下，返回行为一致的“默认语义”
   - 标注“兼容差异”（如 catalog/cache 一些默认值）
- 统一异常模型，给出 `外部接口映射文档`

### B. 明确先不支持（v1）
- Structured Streaming（可后续）
- UDAF 全套（先支持部分内建聚合）
- 高级 MLlib / GraphX
- 动态分区裁剪 / 全套资源队列

---

## 2) 版本化兼容映射（Reference->当前引擎）

### DataFrame -> C++ DSL
| 参考 API | v1 对应 | 备注 |
|---|---|---|
| `session.read.parquet` | `read.parquet` | 一致 |
| `df.select` | `DataFrame.select` | 一致 |
| `df.filter` / `df.where` | `DataFrame.filter` | 一致 |
| `df.groupBy(...).agg(...)` | `DataFrame.groupBy().agg()` | 算子树统一
| `df.join(other, on, how)` | `DataFrame.join` | 先支持 inner/left/right/full 外连接后续加 |
| `df.persist(StorageLevel)` | `DataFrame.persist` | 支持 MEMORY/DISK 简版 |
| `session.sql("...")` | `session.sql` | SQL 解析器先支持基础 SELECT/CTE |
| `show/collect/count` | Action | 保留惰性计划语义 |

### 术语映射（内部实现）
- DataFrame / Dataset：统一为 `LogicalPlan + TypedRow`，避免提前拆成 Row/对象层面兼容复杂性
- RDD：在 v1 标注为 `Compatibility Layer`，内部不优先展开 API 全量实现
- SQL：使用统一 `logical->physical`，并将函数通过 expression registry 映射到底层算子

---

## 3) 分层设计（DataFrame 优先）

### Phase 1：算子与计划（单机）
- LogicalPlan/PhysicalPlan + Rule-based Optimizer（列裁剪/谓词下推）
- Arrow/Acero 执行算子适配
- 执行引擎输出迭代器 + 批处理（batch）

### Phase 2：分布式化
- Controller（master）+ Worker（executor）
- Stage/Task 调度 + 心跳 + 资源注册
- Shuffle Manager：目录化分片文件 + checksum/重试标记

### Phase 3：运行时增强
- 任务优先级 / 队列 / 资源上限
- 异常恢复策略：`retry`, `speculative`（可选）
- 基础 Web/CLI 监控 API

---

## 4) 立即可执行清单（本周）

1. 建立 API 程序接口定义：`IDataFrame`, `IDataSet`, `DataflowSessionCompatible`
2. 落地 30 个最小 DataFrame 单测（含 select/filter/groupBy/join/action）
3. 先跑一个端到端 demo：
   - 本地 CSV -> filter + groupBy + join -> write parquet
4. 再把同一套计划提交到 3 节点分布式 mock 环境跑通

---

## 5) 你要确认的两点（快速决策）

- 是否把 **SQL 方言**先限定到 **ANSI 子集**（推荐）？
- 是否要求第一版支持 **Hive Metastore/Glue catalog 对接**（否则先用内置 catalog）？
