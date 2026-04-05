# 流式优先（Streaming-first）版本路线图

## 当前角色

这份文档记录的是流式优先阶段的早期路线图。
它现在主要作为历史演进背景保留，不是当前流式能力的状态板。

## 当前实现吸收情况（2026-04）

后续实现已经吸收的内容主要包括：

- `readStream` / `writeStream` 主链路
- `StreamingQuery`、checkpoint、progress 和 explain 合同
- `csv-dir` / local source 的最小流式入口
- `console/file` 风格的本地 sink 路径
- stream SQL 最小子集
- 最小 window SQL 与 stateful grouped aggregate
- `SingleProcess` / `LocalWorkers` 的本地执行模式

当前未按这份路线图完整实现或不再作为当前主线推进的内容包括：

- “micro-batch / continuous 两路并行”的完整抽象
- 通用 `StreamingPlanner` 扩展为更宽广计划系统
- 完整事件时间 / watermark 体系
- exactly-once 或更强幂等输出语义
- 更广泛的 source/sink 和更复杂流式 SQL

## 与当前主 plan 的关系

当前流式 contract 与实现形态应以根 `README`、`docs/runtime-contract.md`、`docs/streaming_runtime_design.md` 和 `plans/core-runtime-columnar-plan.md` 为准。
这份文档用于保留“为什么先做流式入口与最小状态语义”的历史背景。

## 历史部分说明

下文保留原始路线图。
若其中出现更大的未来能力面，应理解为早期路线判断，而不是当前状态承诺。

## 结论（立即采纳）
你这边要求「优先支持流式」，我会把后续版本切换为：

1. **先把 DataFrame 扩展到流式执行语义**（micro-batch / continuous 两路并行）
2. **再补齐批处理到流的统一算子层**
3. **在批式 DataFrame API 上复用同一套计划与优化器**

---

## v0.2 目标（4~8 周）

### 第一优先：Streaming 抽象
- `StreamSource`：`socket/csv-dir/kafka-like mock`（先从文件夹增量 polling + local source 开始）
- `StreamQuery`：`readStream` / `writeStream` / `trigger` / `awaitTermination`
- `StreamingPlanner`：
  - 先复用 `PlanNode`，再加 `StreamScan/Watermark/WindowState` 节点
  - 输出为 “有界批次（micro-batch id）” + “无界时间边界（无界模型）” 两模式

### 第二优先：状态与窗口
- 支持 `event_time` 概念（先支持处理时间）
- 实现基础 `tumble window` 聚合（count、sum）
- 支持 `map/filter/withColumn` 的流式兼容
- 状态存储先用内存状态（stateful map）

### 第三优先：可靠性
- 每个 micro-batch 形成 checkpoint id + offset（文件）
- 故障恢复：重放最后成功 checkpoint 后的分片
- 幂等输出（idempotent sink semantics）

### 第四优先：流式 I/O
- sink：`console`、`csv`（append）先行
- 输出语义：`append` / `complete`（先 append）

---

## 与 DataFrame 批处理的关系
- 不推翻原有 DataFrame v0.1 代码；只在其上加流接口层：
- `StreamingSession`（可复用同名 `DataflowSession`）
  - `DataFrame.readStream`（返回 `StreamingDataFrame`）
  - `StreamingDataFrame.writeStream`（输出接入）
- 复用已有算子：`select/filter/withColumn/drop/groupBy/sum`
- SQL v1 当前已接入最小流式子集：
  - `streamSql("SELECT ...")`
  - `startStreamSql("INSERT INTO sink_table SELECT ...")`
  - `CREATE SOURCE TABLE ... USING csv`
  - `CREATE SINK TABLE ... USING csv`
- 当前流式 SQL 仍只覆盖 `SELECT/WHERE/GROUP BY/HAVING/LIMIT + SUM/COUNT(*)` 的单表路径；`from_json / watermark / JOIN` 仍在后续范围，最小 `window SQL` 已落地。

## 当前已落地的最小流式 SQL 设计

### 设计目标

- 不重写现有 parser/planner 主链路
- 直接把可支持的 SQL 子集映射到 `StreamingDataFrame`
- 先打通 `csv source -> stream sql select -> csv sink`

### 当前入口

- `DataflowSession::streamSql(const std::string&)`
- `DataflowSession::explainStreamSql(const std::string&, StreamingQueryOptions)`
- `DataflowSession::startStreamSql(const std::string&, StreamingQueryOptions)`

### 当前映射关系

- `CREATE SOURCE TABLE ... USING csv OPTIONS(path ...)`
  - 注册 `DirectoryCsvStreamSource`
- `CREATE SINK TABLE ... USING csv OPTIONS(path ...)`
  - 注册 `FileAppendStreamSink`
- `SELECT ... FROM stream_source`
  - 读取已注册流式 view
- `INSERT INTO sink_table SELECT ...`
  - 把查询结果挂到已注册 sink，并返回 `StreamingQuery`

### 当前约束

- 只支持 `USING csv`
- 只支持单表查询
- 只支持 `SUM` 与 `COUNT(*)`
- 只支持最小 `WINDOW BY <time_col> EVERY <window_ms> AS <output_col>`
- `HAVING` 只作用于当前已支持的聚合输出
- `LIMIT` 沿用现有 streaming API 的 batch 级实现
- 不支持 `JOIN / AVG / MIN / MAX / INSERT ... VALUES`

---

## 实施步骤（本周）
1. 新增 `src/dataflow/stream/` 接口与 `micro_batch.h/.cc`
2. 增加 `readStream(StreamSource)` + `StreamingQuery.start()/stop()` 流程
3. 加 2 个最小 Demo：
   - `stream_scan_demo`：模拟追加文件目录 -> 2s micro-batch
   - `stream_count_demo`：滚动计数并输出到 console
4. 在 `README` 的里程碑里标注：**Streaming 为新 M1A**


## v0.4 / v0.5（2026-03-29 更新）

### v0.4 已完成（当前仓库）
- logical/physical 保持统一主链路，执行模式决策以 query 级 strategy decision 形式暴露。
- source/sink ABI 已接入 streaming 主执行链：`RuntimeSourceAdapter / RuntimeSinkAdapter`。
- checkpoint/progress/backpressure 合同继续保持 query-local 语义。

### v0.5 已完成（当前仓库）
- 新增 ABI 适配层回归测试，覆盖 open/next/checkpoint/ack/close。
- 新增 runtime contract 测试，覆盖 backpressure、checkpoint delivery mode、execution-mode consistency。
- 新增 `explainStreamSql(...)` 与 strategy explain，把 mode/fallback reason 收口为统一决策出口。
- 新增 Python 自动化用例，覆盖 Arrow batch、`RecordBatchReader`、`__arrow_c_stream__`、stream SQL 与 progress 字段语义。
- 新增同机 observability regression 脚本，覆盖 `stream_benchmark / stream_actor_benchmark / actor_rpc_smoke`。
- 最小 `window SQL` 已落地，并映射回现有 streaming operators。
- README 与 runtime 设计文档补齐版本边界、contract 与测试矩阵说明。

### 仍在后续版本范围
- Parquet/S3 source 的生产级实现。
- checkpoint 与外部 sink 的更强语义保证（例如重放幂等与恢复策略细化）。
- Python UDF 与 callback sink 仍暂不纳入当前版本；custom stream source 通过 Arrow 适配已纳入 v0.5。
