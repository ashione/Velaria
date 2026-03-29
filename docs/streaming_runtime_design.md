# Streaming Runtime Design

## Scope

当前文档只描述 Velaria 单机 `StreamingQuery` 路径，以及其与本机 actor-stream 运行时的接合点。

当前不覆盖：

- 跨 host 运行
- 真正分布式调度
- 通用 SQL 自动下推
- 任意 streaming plan 的 actor 并行化

## Query Execution Modes

`StreamingQueryOptions.execution_mode` 当前支持：

- `SingleProcess`
- `LocalWorkers`
- `ActorCredit`
- `Auto`

语义：

- `SingleProcess`：所有 batch 在本进程内执行。
- `LocalWorkers`：partition-local 阶段使用本进程线程并行，barrier 阶段仍在本地汇总。
- `ActorCredit`：仅对明确支持的热路径下推到本地 actor-stream runtime。
- `Auto`：先采样，再在 `SingleProcess` 和 `ActorCredit` 之间做一次 query-local 选择。

## Strategy Decision and Explain

当前 planner / runtime 共享同一份 strategy decision 语义，避免“planner 解释一套、runtime 实际执行另一套”。

当前暴露出口：

- `StreamingQueryProgress.execution_mode`
- `StreamingQueryProgress.execution_reason`
- `StreamingQueryProgress.transport_mode`
- `DataflowSession::explainStreamSql(...)`

`explainStreamSql(...)` 当前输出三段：

- `logical`
- `physical`
- `strategy`

其中 `strategy` 至少包含：

- selected mode
- fallback / downgrade reason
- actor hot-path eligibility
- estimated state size
- estimated batch cost
- transport/shared-memory 相关参数快照
- checkpoint delivery mode
- backpressure 相关阈值

## Current Actor Pushdown Boundary

当前只有一类 query 会被 `ActorCredit` / `Auto` 接住：

- 前置变换全部为 `PartitionLocal`
- 最后一个 barrier 是 `groupBy({"window_start", "key"}).sum("value", ...)`

也就是说，当前 actor runtime 只服务于：

- `window_start`
- `key`
- `value`

这条窗口分组求和热路径。

如果 query 不满足这个形状，`StreamingQuery` 会回退到普通执行链，并把原因写入：

- `StreamingQueryProgress.execution_mode`
- `StreamingQueryProgress.execution_reason`

同一条 query 当前要求在以下模式下结果一致：

- `SingleProcess`
- `LocalWorkers`
- `ActorCredit`
- `Auto`

其中 `Auto` 若未命中热路径，必须稳定回退到 `SingleProcess` 并给出明确 reason。

## Auto Selection Rule

`Auto` 当前采用“前若干批采样”的启发式规则。

采样输出：

- `rows_per_batch`
- `average_projected_payload_bytes`
- `single_rows_per_s`
- `actor_rows_per_s`
- `actor_speedup`
- `compute_to_overhead_ratio`

默认阈值：

- `sample_batches = 2`
- `min_rows_per_batch = 64K`
- `min_projected_payload_bytes = 256KB`
- `min_compute_to_overhead_ratio = 1.5`
- `min_actor_speedup = 1.05`
- `strong_actor_speedup = 1.25`

选择规则：

- 常规情况下，要求 `rows`、`payload`、`speedup`、`compute/overhead` 都满足阈值。
- 若 `actor_speedup` 已经足够强，则允许越过 `compute/overhead` 的软阈值。

## Shared Memory Transport

本机 actor-stream 路径在确认上下游都在同一 host 时，可启用 shared memory transport。

当前行为：

- 大 input payload：coordinator 直接编码进 `shm_open + mmap` 区域，再把共享内存引用发给 worker。
- worker input decode：直接在 `mmap` 视图上反序列化，不再先拷贝成 `std::vector<uint8_t>`。
- worker output：大 payload 时也可写回 shared memory。
- coordinator output decode：同样直接在 `mmap` 视图上反序列化。

这意味着当前已经去掉了“shared memory -> vector copy -> decode”这层明显多余的内存移动。

## Backpressure and Checkpoint Contract

当前 contract 重点不是强事务语义，而是“边界明确且可诊断”。

### Backpressure

- `backlog`：pull 后、consumer drain 前的队列 batch 数
- `blocked_count`：producer 因 backlog / partition 压力进入 wait 的事件数
- `max_backlog_batches`：enqueue 后观测到的最大 backlog
- `inflight_batches / inflight_partitions`：当前仍在队列里待消费的工作量

延迟口径：

- `last_batch_latency_ms`：单批从执行开始到 sink flush 完成
- `last_sink_latency_ms`：sink write/flush
- `last_state_latency_ms`：state/window finalize；stateless batch 为 `0`

### Checkpoint

- checkpoint 使用本地文件并采用原子替换写入
- 默认 delivery mode 为 `at-least-once`
- `best-effort` 仅在 source 实现 `restoreOffsetToken(...)` 时恢复 offset
- 两种模式都不宣称 exactly-once sink delivery

## Binary Batch Layout

actor-stream payload 当前使用 typed binary batch：

- 列投影：只序列化需要的列
- 字符串低基数字典编码
- `Int64` varint/zigzag
- cache-friendly 热点列布局
- buffer pool 复用发送缓冲

编码器当前支持：

- `prepareRange(...)`
- `serializePreparedRange(...)`
- `serializePreparedRangeToBuffer(...)`
- `deserializeFromBuffer(...)`
- `deserializeWindowKeyValueFromBuffer(...)`

其中 `prepareRange(...)` 会一次性构建：

- projected columns
- inferred types
- dictionary metadata
- estimated size

然后复用到实际编码，避免同一分区先 `estimate` 再 `serialize` 的双重扫描。

## Known Limits

- `ActorCredit` 还不是通用执行器，只是定向热路径。
- 最终状态回并仍在 coordinator 本地完成。
- query 级 `Auto` 当前阈值更接近“保守正确”，还不是最终调优状态。
- `split_ms / merge_ms` 仍是毫秒级指标，对极短阶段不够敏感。
- SQL 路径仍未自动下推到 actor runtime；当前优化主要落在 streaming 执行内核。
- 同机 observability 仍是 experiment profile，不是完整 dashboard / distributed telemetry 体系。

## Recommended Next Steps

1. 扩展 actor pushdown 到 `count` 和更多 group aggregate。
2. 调整 query 级 `Auto` 阈值，使其更贴近真实 `StreamingQuery` workload。
3. 给 query 级 progress 增加更细粒度的 actor 指标拆分。
4. 继续把 source 到执行内核的中间 `Table/Row/Value` 转换收紧到更早的列式表示。


## Source/Sink ABI Bridge (v0.4)

为了把 ABI 变成可运行路径而不仅是头文件契约，stream runtime 现在提供：

- `RuntimeSourceAdapter`：把 `RuntimeSource` 映射到 `StreamSource`。
- `RuntimeSinkAdapter`：把 `RuntimeSink` 映射到 `StreamSink`。
- `makeRuntimeSourceAdapter(...) / makeRuntimeSinkAdapter(...)`：统一构造入口。

桥接规则：

- query 启动时，`query_id / trigger / checkpoint` 从 `Stream*Context` 下传到 `Runtime*Context`。
- 每批完成后，`source_offset / batches_processed` 通过 `RuntimeCheckpointMarker` 传递给 source/sink。
- source 侧在 checkpoint 后收到 `ack(token)`，用于 credit/checkpoint 相关外部实现。
- 反压仍由 `StreamingQuery` 的 query-local backlog 控制，ABI 不改变核心反压语义。

## Test Matrix Additions (v0.5)

新增测试覆盖：

- `source_sink_abi_test`：验证 runtime ABI 适配层上下文、checkpoint、ack、close 调用链。
- `python_api/tests/test_streaming_v05.py`：验证 Python Arrow batch SQL 与 stream SQL progress 合同。
- `stream_strategy_explain_test`：验证 explain 输出、execution-mode consistency、auto fallback reason。
- `python_api/tests/test_arrow_stream_ingestion.py`：验证 `RecordBatchReader`、`__arrow_c_stream__` 与 stream Arrow ingestion。

配套 regression 脚本：

- `scripts/run_stream_observability_regression.sh`

这些测试与现有 `stream_runtime_test / stream_actor_credit_test / planner_v03_test / sql_regression_test` 共同构成 v0.5 的最小回归面。


## Checkpoint Delivery Mode

`StreamingQueryOptions.checkpoint_delivery_mode` 当前支持：

- `at-least-once`（默认）：恢复时重放 source，避免漏处理，允许重复。
- `best-effort`：按 checkpoint offset 尝试从已完成位置继续，减少重复但不保证严格语义。
