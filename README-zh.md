# Velaria：纯 C++17 Streaming-First 数据流内核

`README-zh.md` 是中文镜像文档，对应的英文主文档位于 [`README.md`](./README.md)。后续所有 README 变更必须同时更新这两份文件，保持结构和语义同步。

Velaria 是一个面向可演进运行时的轻量数据流引擎研究项目。当前目标刻意收敛为：单机本地优先、Streaming-first，以及先把单机状态流吞吐和稳定性做好，再逐步扩展到本地多进程和后续分布式实验。

本仓库并不声称自己已经是完整分布式系统。当前工作重点是 `DataflowSession`、`StreamingDataFrame`、本地状态管理、流控与反压、以及本机多进程执行路径。batch 语义依然保留，但统一放进同一个 bounded stream 执行模型里。

## 里程碑状态

### v0.5 快照（2026-03-29）

当前执行路线是：

`micro-batch in-proc + query-local backpressure + local worker scale-up`

同时仓库保留 `actor + rpc + jobmaster` 作为本机多进程实验路径。

当前进展：

- 单机流式主链路：`readStream -> StreamingDataFrame -> writeStream` 已支持 query 级配置、checkpoint/progress、本地文件 source/sink、窗口列和基础状态聚合。
- 稳定性路径：`StreamingQuery` 提供 query-local 流控与端到端反压，采用有界队列，避免 sink 或状态处理变慢时 source 无界积压。
- query 级执行模式：支持 `single-process`、`local-workers`、`actor-credit`、`auto`，并在 progress snapshot 中记录最终决策及原因。
- logical / physical 规划：统一了 `WindowAssign`、`Sink` 等逻辑节点，并在 progress 中结构化记录执行模式、transport、水位和 batch 成本估算。
- source/sink ABI：`RuntimeSource` 和 `RuntimeSink` 显式携带 `query_id / checkpoint / backpressure` 上下文，作为后续跨进程 source/sink 扩展的稳定边界。
- source/sink 适配层：`RuntimeSourceAdapter` 和 `RuntimeSinkAdapter` 把 runtime ABI 对象桥接到 `readStream(...)` 与 `writeStream(...)`，同时保持 query-local 反压和状态语义不变。
- 单进程示例：`sql_demo`、`df_demo`、`stream_demo` 继续作为本地语义基线。
- 本机多进程实验：`actor + rpc + jobmaster` 仍然只是同机任务分发最小闭环，不应描述为已完成的分布式运行时。

### v0.4 / v0.5 补充

这一轮按“先语义边界，再扩展入口”的顺序推进：

- `v0.4`：补齐 source/sink ABI 到 streaming 主链路的接入，完成 `makeRuntimeSourceAdapter` 与 `makeRuntimeSinkAdapter`。
- `v0.5`：补齐 query progress 合同、checkpoint/ack 透传、以及 Python Arrow + stream SQL 端到端用例的测试覆盖。

当前仓库边界不变：

- 本地多进程路径仍是实验，不是完整分布式调度器。
- Python 继续坚持 Arrow 数据交换。当前支持 custom stream source 先转 Arrow micro-batch，再接入流式执行；仍不支持 Python callback/UDF。custom sink 采用 Arrow batch 回调适配，而不是 native sink ABI 回调。

## 当前本机多进程架构

```text
client
  -> scheduler(jobmaster)
      -> worker
          -> in-proc operator chain
      -> result back to client
```

角色说明：

- `in-proc operator chain`：worker 进程内部仍使用本地算子链执行，不拆成跨节点执行图。
- `cross-process RPC`：client、scheduler、worker 通过 RPC 和 codec 消息通信。
- `simple jobmaster`：`actor_rpc_scheduler` 只负责接收、派发、转发和状态观测，不实现复杂容错、重试策略和资源隔离。
- `single-node fallback`：保留 `--single-node` 路径，保证本地验证不依赖多进程实验链路。

## 术语统一

为了避免外部框架命名进入仓库接口，统一使用以下术语：

- `Session` 风格命名：`DataflowSession`
- SQL 入口：`session.sql(...)`
- 数据读取入口：`session.read(...)`
- 不使用 `compatibility`，改用“语义对齐”“接口映射”或“外部行为参考”

不要把外部框架专名直接写进主接口、注释或文档。

## 当前能力边界

当前已具备：

- `DataflowSession`、`DataFrame`、`StreamingDataFrame`
- `read_csv`、`readStream(...)`、`readStreamCsvDir(...)`
- Streaming query 配置：trigger interval、max in-flight batches、max queued partitions、checkpoint path，以及 `single-process / local-workers / actor-credit / auto`
- 流控与反压：query-local backlog 水位、blocked 计数、有界 backlog、progress snapshot
- Streaming source/sink：memory source、目录 CSV 增量 source、console sink、append 文件 sink
- Python 数据交换路径：CSV 输入、进程内 Arrow batch/stream 输入、Arrow 输出
- 基础算子：`select / filter / withColumn / drop / limit / window`
- 聚合：`groupBy + sum/count`，以及 batch `SUM / COUNT / AVG / MIN / MAX`
- 基础状态聚合：stateful `sum/count`
- 本地 SQL 主链路：`SqlParser -> SqlPlanner -> DataFrame`
- 最小流式 SQL 闭环：`streamSql(SELECT ...)` 和 `startStreamSql(INSERT INTO ... SELECT ...)`
- 流式 SQL CSV 表：`CREATE SOURCE TABLE ... USING csv`、`CREATE SINK TABLE ... USING csv`
- Python wrapper v1：`Session / DataFrame / StreamingDataFrame / StreamingQuery`
- 临时视图：`createTempView / resolve`
- actor/rpc 最小闭环：`scheduler / worker / client / smoke`

当前限制：

- 执行模型目前固定为 `micro-batch only`，不做 continuous。
- window 仅支持固定 tumble 风格窗口列和基础状态聚合。
- 当前版本不扩展 CTE。
- 本轮不扩展子查询。
- 本轮不扩展 `UNION`。
- `JOIN` 仍刻意保持最小能力。
- 流式 SQL 不支持 `JOIN / AVG / MIN / MAX / window SQL / INSERT ... VALUES`。
- Python 绑定会通过仓库规则自动探测本机 CPython 头文件，支持 custom stream source 经 Arrow 转换接入，但仍不支持 Python callback/UDF 或 Python sink 直接下沉到 native sink ABI。
- 反压当前是 query-local，不处理多 query 全局公平调度。
- 多进程路径仍是同机实验，不包含真正的分布式调度、故障恢复、状态迁移和资源治理。

## Streaming 运行时

当前 streaming 入口包括：

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`
- `StreamingQuery.start() / awaitTermination() / stop()`

`StreamingQueryOptions` 当前包含：

- `trigger_interval_ms`
- `max_inflight_batches`
- `max_queued_partitions`
- `backpressure_high_watermark / backpressure_low_watermark`
- `checkpoint_path`
- `execution_mode = single-process / local-workers / actor-credit / auto`
- `local_workers`
- `actor_workers / actor_max_inflight_partitions`
- `actor_shared_memory_transport / actor_shared_memory_min_payload_bytes`
- `actor_auto_options`
- `max_retained_windows`

`StreamingQueryProgress` 和 `snapshotJson()` 额外暴露：

- `execution_mode`
- `execution_reason`
- `estimated_state_size_bytes / estimated_batch_cost`
- `backpressure_max_queue_batches / backpressure_high_watermark / backpressure_low_watermark`

这些字段用于说明 query 最终选择了哪条本地执行路径，以及为什么选择或回退。

### query 级 actor / auto 选择

- `actor-credit` 和 `auto` 只对明确支持的热路径生效：前置变换必须全部是 partition-local，最终 barrier 必须是按 `window_start + key` 分组的 `sum(value)`。
- 这条热路径会下推到本地 actor-stream runtime，对 `window_start / key / value` 做 partition-local partial aggregate，再把结果回并到 `StreamingQuery`。
- 不满足条件时会自动回退到 `single-process`，并在 `execution_reason` 中记录原因。
- 这意味着 actor/auto 不是泛化的“任意计划自动并行化”，而是对可识别窗口分组求和热路径的定向加速。

### 反压语义

- 反压按 `batch / partition` 水位控制。
- backlog 超过阈值时，source pull 会被阻塞，而不是继续无界积压。
- sink 写慢、状态更新慢、或者局部并行分区过多，都会反馈到 query-local backlog 计数。
- 最少可观察指标包括 `blocked_count / max_backlog_batches / inflight_batches / inflight_partitions / last_batch_latency_ms / last_sink_latency_ms / last_state_latency_ms`。

### checkpoint / progress

- 每批完成后，运行时会把 `batches_pulled / batches_processed / blocked_count / max_backlog_batches / last_source_offset` 落盘到本地 checkpoint 文件。
- `MemoryStreamSource` 按 batch offset 恢复。
- `DirectoryCsvStreamSource` 按最后完成的文件名恢复。
- `snapshotJson()` 还会携带 `execution_mode / execution_reason`，用于诊断 query 是否进入 actor-credit 或发生 auto 回退。

### 状态索引与窗口淘汰

- stateful 聚合以 `group key -> aggregate state` 作为主索引。
- 如果分组键包含 `window_start` 这类窗口列，运行时会额外维护窗口成员索引，便于按窗口淘汰旧状态。
- 当 `max_retained_windows > 0` 时，只保留最近 N 个窗口的状态，避免状态无界增长。

## 流式 SQL

Velaria 基于现有 `StreamingDataFrame` API 提供一个最小流式 SQL 子集。

### API 入口

- `session.streamSql("SELECT ...") -> StreamingDataFrame`
- `session.startStreamSql("INSERT INTO sink_table SELECT ...", options) -> StreamingQuery`

### 当前支持的查询子集

- 单表 `SELECT`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `LIMIT`
- `SUM(col)`
- `COUNT(*)`

当前实现会把 SQL 重新映射回已有流式算子：

- `FROM source` -> 已注册的流式 view
- `WHERE` -> `filter`
- `GROUP BY + SUM/COUNT(*)` -> `groupBy + sum/count`
- `HAVING` -> 聚合后的 `filter`
- `LIMIT` -> 现有 `StreamingDataFrame::limit`

### 当前支持的 DDL / DML

- `CREATE SOURCE TABLE ... USING csv OPTIONS(path 'dir', delimiter ',')`
- `CREATE SINK TABLE ... USING csv OPTIONS(path '/tmp/out.csv', delimiter ',')`
- `INSERT INTO sink_table SELECT ...`

绑定关系：

- `SOURCE TABLE USING csv` 映射到 `DirectoryCsvStreamSource`
- `SINK TABLE USING csv` 映射到 `FileAppendStreamSink`
- `INSERT INTO ... SELECT ...` 需要通过 `session.startStreamSql(...)` 启动

示例：

```sql
CREATE SOURCE TABLE stream_events (key STRING, value INT)
USING csv OPTIONS(path '/tmp/stream-input', delimiter ',');

CREATE SINK TABLE stream_summary (key STRING, value_sum INT)
USING csv OPTIONS(path '/tmp/stream-output.csv', delimiter ',');

INSERT INTO stream_summary
SELECT key, SUM(value) AS value_sum
FROM stream_events
WHERE value > 6
GROUP BY key
HAVING value_sum > 15
LIMIT 10;
```

### 当前 SQL 限制

- 只支持 `USING csv`
- 只支持单表查询
- 不支持 `JOIN`
- 不支持 `AVG / MIN / MAX`
- 不支持窗口 SQL 语法
- 不支持流式 `INSERT INTO ... VALUES`
- `LIMIT` 沿用当前 streaming API 语义，不应解读为完整无限流全局 SQL limit

## SQL 规划与执行

`session.sql()` 当前遵循 `Parser -> Planner -> DataFrame`：

- `SqlParser` 把 SQL 解析成 `SqlQuery` AST，支持 `ON (...)`、`HAVING`、`LIMIT` 和简单的括号谓词，例如 `WHERE a.score > (5)`。
- `SqlPlanner::buildLogicalPlan(...)` 生成 `Scan -> (Join?) -> (Filter?) -> (Aggregate?) -> (Having?) -> Project -> (Limit?)` 这样的逻辑步骤。
- `SqlPlanner::buildPhysicalPlan(...)` 把逻辑步骤映射为物理步骤：
  - `Join` 和 `Aggregate` 是 barrier 阶段。
  - 连续的 `Filter/Project/Limit/Having` 可以融合成 `FusedUnary`。
  - 连续 `Project` 会去重，连续 `Limit` 取更小值。
- `materializeFromPhysical(...)` 会把物理计划回放成现有 `DataFrame` 算子链，复用本地执行器。

## SQL DDL 语义

Velaria 当前在本地内存 catalog 中支持三类表：

- `CREATE TABLE`：普通表，可读可写
- `CREATE SOURCE TABLE`：源表，当前只读
- `CREATE SINK TABLE`：结果表，当前只写，不可查询

`SOURCE` 和 `SINK` 当前只表达表语义并执行约束校验，还没有引入外部 connector。

## SQL DDL / DML 支持

### DDL

- `CREATE TABLE`：创建普通内存表，支持读写
- `CREATE SOURCE TABLE`：创建源表，只可读
- `CREATE SINK TABLE`：创建结果表，只可写

示例：

```sql
CREATE TABLE users (id INT, name STRING, score INT);
CREATE SOURCE TABLE source_users (name STRING, score INT);
CREATE SINK TABLE summary_by_region (region STRING, score_sum INT);
```

### DML

- `INSERT INTO ... VALUES`：支持全列插入和显式列名
- `INSERT INTO ... SELECT`：支持将查询结果写入目标表

示例：

```sql
INSERT INTO users (id, name, score) VALUES (1, 'alice', 10), (2, 'bob', 20);

INSERT INTO summary_by_region
SELECT name AS region, SUM(score) AS score_sum
FROM users
GROUP BY name;
```

### 语义约束

- 任何写入 `SOURCE` 表的 `INSERT` 都会报语义错误。
- 对 `SINK` 表做 `SELECT`，包括作为 `JOIN` 输入，都会报语义错误。
- `SELECT` 仍必须满足当前版本规划器约束，例如 `HAVING` 的聚合语义和 `GROUP BY` 投影一致性。

### scheduler 模型

- scheduler 只做编排和分发，不在本地执行 SQL。
- 包括 dashboard 在内的所有提交入口都必须走 worker 执行路径。
- 如果使用 `--no-auto-worker` 禁掉自动 worker，并且没有可用 worker，提交会返回 `no worker available`，而不是在 scheduler 内部偷偷本地执行。

### 当前调度策略

- 提交路径：dashboard 和 client 先构建计划，再通过 `submitRemote` 创建 `job -> chain -> task`。
- 排队语义：远端可执行任务进入 `JobMaster` 的 `pending_remote_tasks_` 队列。只有 chain 变为 runnable 且 scheduler 观察到空闲 worker 时才会下发。
- 派发规则：dispatcher 从队列头按到达顺序把任务派给空闲 worker。
- worker 绑定：每个 task 与 worker 连接绑定。发送成功后状态更新为 `TASK_SUBMITTED / CHAIN_SUBMITTED`；发送失败则回灌 `TASK_FAILED / JOB_FAILED`。
- 并发与并行：chain parallelism 受 `chain_parallelism` 和空闲 worker 数限制。每个 task 支持重试和 heartbeat 观测。
- 结果确认：worker 返回 `Result` 后，scheduler 会更新 `job / chain / task` snapshot，再回推给提交端。
- 运维建议：通过日志事件和 `/api/jobs` snapshot 观察 `JOB_* / CHAIN_* / TASK_*` 的状态变化。
- 默认 worker 策略：scheduler 会通过 `--local-workers` 自动启动本地 worker，默认值是 `2`。需要纯手工模式时再使用 `--no-auto-worker`。

## Python Wrapper

仓库内已经提供最小 Python 绑定层，允许 Python 代码直接调用 `DataflowSession`、stream API 和 streaming SQL。

### 绑定策略

- 不引入额外第三方绑定框架
- 采用手写 CPython extension
- 对外暴露四类 Python 对象：
  - `Session`
  - `DataFrame`
  - `StreamingDataFrame`
  - `StreamingQuery`

### GIL 策略

以下操作在 native 执行期间会显式释放 GIL：

- `Session.read_csv(...)`
- `Session.sql(...)`
- `Session.stream_sql(...)`
- `Session.start_stream_sql(...)`
- `DataFrame.count()`
- `DataFrame.to_rows()`
- `DataFrame.show()`
- `StreamingQuery.start()`
- `StreamingQuery.await_termination(...)`
- `StreamingQuery.stop()`

第一版仍不支持：

- Python callback 执行
- Python UDF
- Python sink 直接下沉到 native sink ABI

### 当前 Python API

- `Session.read_csv(path, delimiter=',')`
- `Session.sql(sql_text)`
- `Session.create_dataframe_from_arrow(pyarrow_table)`
- `Session.create_stream_from_arrow(pyarrow_table_or_batches)`
- `Session.create_temp_view(name, df_or_stream_df)`
- `Session.read_stream_csv_dir(path, delimiter=',')`
- `Session.stream_sql(sql_text)`
- `Session.start_stream_sql(sql_text, trigger_interval_ms=1000, checkpoint_path='')`
- `DataFrame.to_rows() / to_arrow() / count() / show()`
- `StreamingDataFrame.select(...) / filter(...) / with_column(...) / drop(...) / limit(...)`
- `StreamingDataFrame.window(...) / group_sum(...) / group_count(...)`
- `StreamingDataFrame.write_stream_csv(...) / write_stream_console(...)`
- `StreamingQuery.start() / await_termination(...) / stop() / progress()`

说明：

- `create_dataframe_from_arrow(...)` 支持 `pyarrow.Table` 以及可被 `pyarrow.table(...)` 归一化的对象
- `create_stream_from_arrow(...)` 支持单个 `pyarrow.Table` 或 `RecordBatch`，也支持由多个 `Table` 或 `RecordBatch` 组成的 Python 序列
- Python 既可以继续使用 CSV，也可以完全跳过文件落盘，直接把 Arrow 数据送进 batch 或 stream 执行链

### 构建与运行

Python wrapper 在 Bazel workspace 初始化时会自动探测可用的 CPython 解释器和开发头文件。

如果需要显式覆盖仓库初始化用的 Python，可设置：

```bash
export VELARIA_PYTHON_BIN=/path/to/pythonX.Y
```

Python 依赖管理和 demo 运行统一使用 `uv`。

`python_api/` 目录包含：

- `python_api/pyproject.toml`
- `python_api/requirements.lock`
- `python_api/BUILD.bazel`

关键打包目标：

- `//python_api:velaria_py_pkg`：Bazel 运行时包，会把 Python 源码和 `_velaria.so` 放进同一包目录，供测试和 binary 直接使用
- `//python_api:velaria_whl`：pure Python wheel
- `//python_api:velaria_native_whl`：注入 `_velaria.so` 的 native wheel
- `python_api/pyproject.toml` 已声明 `velaria/_velaria.so` 为 package data，因此当该文件出现在包目录里时，setuptools/uv 打包也会自动带上 native 模块
- 安装 native wheel 后：`import velaria` 会直接加载包内 `_velaria.so`
- 源码树开发态：如果已经构建过 `//:velaria_pyext`，`velaria` 会自动发现 `bazel-bin/_velaria.so`，不需要额外环境变量或 `PYTHONPATH`
- Python 包版本号统一收敛在 `python_api/version.bzl` 和 `python_api/velaria/_version.py`。后续 bump 版本请使用 `./scripts/bump_velaria_version.sh <version>`，并同步刷新 `uv.lock`。

构建扩展产物：

```bash
bazel build //:velaria_pyext
bazel build //python_api:velaria_whl
bazel build //python_api:velaria_native_whl
```

安装 Python 运行时依赖：

```bash
uv sync --project python_api --python /opt/homebrew/bin/python3.13
```

运行 demo：

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python /opt/homebrew/bin/python3.13
uv run --project python_api python python_api/demo_stream_sql.py
uv run --project python_api python python_api/demo_batch_sql_arrow.py
```

运行 Python 测试：

```bash
bazel test //python_api:custom_stream_source_test //python_api:streaming_v05_test
uv run --project python_api python -m unittest discover -s python_api/tests
```

这些 demo 会展示：

- 在 Python 侧创建 `Session`
- 直接用 `pyarrow.Table` 构建 batch 和 stream 输入
- 通过 streaming SQL 启动 `INSERT INTO ... SELECT ...`
- 读取 sink CSV 输出并打印 query progress
- `pyarrow.Table -> DataFrame` roundtrip
- 基于 Arrow-backed 临时视图运行 batch SQL

## 构建与运行命令

### 单机示例

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
bazel run //:stream_sql_demo
bazel run //:stream_stateful_demo
bazel test //:stream_runtime_test
bazel run //:stream_benchmark
bazel run //:stream_actor_benchmark
bazel test //:stream_actor_credit_test
bazel build //:velaria_pyext
uv sync --project python_api --python python3.12
uv run --project python_api python python_api/demo_batch_sql_arrow.py
uv run --project python_api python python_api/demo_stream_sql.py
```

### 本机 actor-stream 实验

- `stream_actor_benchmark`：验证本地 worker 进程、partition credit 和 actor RPC 下的窗口 + key partial aggregate 路径。
- `stream_benchmark`：从 `StreamingQuery` 入口验证 `single-process / local-workers / actor-credit / auto` 四种执行模式。
- `stream_actor_credit_test`：验证：
  - credit 上限不会被突破
  - blocked 计数会触发
  - 多进程结果与单进程基线一致

这条路径仍然只是独立实验运行时：

- 用来验证 `partition credit + local worker process + result merge` 的最小闭环。
- 关注重点是语义和流控，不保证性能一定优于单进程。
- worker 当前只做 partition 级 partial aggregate，最终状态回并仍由 coordinator 完成。
- 大 payload 在同机路径上会走 `shared memory + mmap`。worker 和 coordinator 都可以直接基于 buffer view 解码，而不是先拷贝到 `std::vector<uint8_t>`。
- 轻量聚合时 actor-stream 仍可能慢于单进程。CPU-heavy 场景可用下面命令验证：

```bash
bazel run //:stream_actor_benchmark -- 16 65536 4 4 0 400
```

参数顺序：

- `batch_count`
- `rows_per_batch`
- `worker_count`
- `max_inflight_partitions`
- `worker_delay_ms`
- `cpu_spin_per_row`
- 可选第 7 个参数：`mode = single / actor / auto / all`

`stream_benchmark` 的一组参考结果：

- `stateful-single`: `89k rows/s`
- `stateful-local-workers`: `88k rows/s`
- `stateful-actor-credit`: `108k rows/s`
- `stateful-auto`: 默认阈值下仍可能保守回退到 `single-process`

这说明 query 级 actor 下推已经接入 `StreamingQuery`，但 `auto` 阈值仍需要结合真实 workload 继续调优。

### 构建 actor/rpc/jobmaster 目标

```bash
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

## CI 与打包

GitHub Actions CI 当前刻意只在 `pull_request` 事件上运行，普通分支 `push` 不会触发 CI。

当前 CI job 包括：

- `native-and-python`：native build、C++ 回归测试和 Python smoke
- `wheel-manylinux`：构建 `//python_api:velaria_native_whl`，再通过 `auditwheel` 修复并上传 manylinux wheel artifact
- `wheel-macos`：在 macOS 上构建 `//python_api:velaria_native_whl`，并上传 macOS wheel artifact

这样可以降低日常分支开发成本，同时在 PR 阶段完成验证，并产出可供评审下载的 native wheel artifact。

正式发布与 PR CI 分离：

- 创建形如 `vX.Y.Z` 的 Git tag
- release workflow 会校验该 tag 是否与 `velaria.__version__` 一致
- 校验通过后，会构建 manylinux wheel 和 macOS wheel，并将两者发布到 GitHub release

### smoke 自检

```bash
bazel run //:actor_rpc_smoke
```

预期输出：

```text
[smoke] actor rpc codec roundtrip ok
```

### 三进程本地联调

终端 A：

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

scheduler 默认会自启动一个本地 worker。若要强制手工 worker，保留 `--no-auto-worker`，再启动：

终端 B：

```bash
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
```

终端 C：

```bash
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

预期日志：

```text
[scheduler] listen 127.0.0.1:61000
[worker] connected 127.0.0.1:61000
[client] job accepted: job_1
[client] job result: rows=..., cols=..., schema=..., first_row=...
```

### Dashboard（TypeScript + React）

- 地址：`http://127.0.0.1:8080`
- 前端源码位于 `src/dataflow/runner/dashboard/app.ts`
- Bazel 目标 `//:dashboard_app_js` 会生成 `src/dataflow/runner/dashboard/app.js`
- 页面能力：
  - 通过原始 payload 或 SQL 提交任务
  - 查看 `/api/jobs` 和 `/api/jobs/{id}`
  - 每秒自动刷新，也支持手动刷新

启动示例：

```bash
./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

如果前端资源已经编译好，可以跳过 TypeScript 重编译：

```bash
BUILD_DASHBOARD=0 ./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

### 一键端到端

```bash
./scripts/run_actor_rpc_e2e.sh
```

示例：

```bash
./scripts/run_actor_rpc_e2e.sh --payload "hello world"
./scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
DO_BUILD=1 ./scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
```

脚本环境变量：

- `ADDRESS`：RPC 地址，默认 `127.0.0.1:61000`
- `WORKER_ID`：worker 名称，默认 `worker-1`
- `PAYLOAD`：默认 payload，默认 `demo payload`
- `TIMEOUT_SECONDS`：超时时间，默认 `20`
- `DO_BUILD`：是否先构建，`1` 或 `0`，默认 `0`
- `BUILD_DASHBOARD`：是否先构建 dashboard，`1` 或 `0`，默认 `1`

如果提交时没有可用 worker，dashboard API 会返回 `503 Service Unavailable`，并带 `message: "scheduler reject: no worker available"`，说明任务尚未进入 runnable 调度队列。

### 单节点回退路径

```bash
bazel run //:actor_rpc_client -- --single-node --payload "local-only payload"
```

## 调试与验收

### 1. build

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

### 2. smoke

```bash
bazel run //:actor_rpc_smoke
```

验收标准：

- 进程返回码为 `0`
- 输出包含 `[smoke] actor rpc codec roundtrip ok`
- 输出中不应出现 `codec roundtrip failed` 或 `codec mismatch`

### 3. 多进程联动

按顺序启动：`scheduler -> worker -> client`。

验收标准：

- scheduler 输出 `[scheduler] listen 127.0.0.1:61000`
- worker 输出 `[worker] connected 127.0.0.1:61000`
- client 先收到 `job accepted`，再收到 `job result`
- 日志中不应出现 `no-worker-available`
- 日志中不应出现 `cannot connect`
- 日志中不应出现 `scheduler closed connection`

### 4. 常见错误模式

- `scheduler reject: no-worker-available`：通常是 worker 未连接成功，或者 client 启动过早。
- `[worker] cannot connect ...` 或 `[client] cannot connect ...`：通常是 scheduler 未启动、地址错误或端口不一致。
- `scheduler failed to listen on ...`：通常是端口冲突或绑定失败。
- `Invalid --listen endpoint` 或 `Invalid --connect endpoint`：参数必须使用 `host:port`。

## 示例源码扩展名检查

示例源码统一保持为 `.cc`：

- `src/dataflow/examples/wordcount.cc`
- `src/dataflow/examples/dataframe_demo.cc`
- `src/dataflow/examples/stream_demo.cc`
- `src/dataflow/examples/stream_stateful_demo.cc`
- `src/dataflow/examples/stream_state_container_demo.cc`
- `src/dataflow/examples/sql_demo.cc`
- `src/dataflow/examples/actor_rpc_scheduler.cc`
- `src/dataflow/examples/actor_rpc_worker.cc`
- `src/dataflow/examples/actor_rpc_client.cc`
- `src/dataflow/examples/actor_rpc_smoke.cc`

核对命令：

```bash
find src/dataflow/examples -maxdepth 1 -type f ! -name '*.cc'
```

如果该命令没有输出，说明示例目录里仍然只有 `.cc` 文件。

## 一行 build/smoke 摘要

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke \
  && bazel run //:actor_rpc_smoke \
  && echo '[summary] build+smoke ok'
```
