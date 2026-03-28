# 流式优先（Streaming-first）版本路线图

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
  - `StreamingSession`（可复用同名 `SparkSession`）
  - `DataFrame.readStream`（返回 `StreamingDataFrame`）
  - `StreamingDataFrame.writeStream`（输出接入）
- 复用已有算子：`select/filter/withColumn/drop/groupBy/sum`
- SQL v1 仍然以 `SELECT` 子集为主，流式 SQL 后续接入（`from_json`、`watermark`、`window`）

---

## 实施步骤（本周）
1. 新增 `src/dataflow/stream/` 接口与 `micro_batch.h/.cc`
2. 增加 `readStream(StreamSource)` + `StreamingQuery.start()/stop()` 流程
3. 加 2 个最小 Demo：
   - `stream_scan_demo`：模拟追加文件目录 -> 2s micro-batch
   - `stream_count_demo`：滚动计数并输出到 console
4. 在 `README` 的里程碑里标注：**Streaming 为新 M1A**
