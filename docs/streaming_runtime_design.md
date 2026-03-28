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

## Recommended Next Steps

1. 扩展 actor pushdown 到 `count` 和更多 group aggregate。
2. 调整 query 级 `Auto` 阈值，使其更贴近真实 `StreamingQuery` workload。
3. 给 query 级 progress 增加更细粒度的 actor 指标拆分。
4. 继续把 source 到执行内核的中间 `Table/Row/Value` 转换收紧到更早的列式表示。
