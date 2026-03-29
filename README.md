# Velaria：纯 C++17 单机 Streaming-First Dataflow 内核

Velaria 是一个面向可演进运行时的轻量数据流引擎研究项目，当前目标收敛为：**单机本地优先、Streaming-first、稳定性与状态流吞吐优先**。

本仓库当前不以真正分布式系统为目标，而是围绕 `DataflowSession`、`StreamingDataFrame`、本地状态、流控/反压、本机多进程并行执行做内核化实现。batch 语义继续保留，但当前按照“bounded stream”视角纳入同一执行模型。

## 本轮里程碑（v0.3，2026-03-28）

当前已对齐的执行路线是：`micro-batch in-proc + query-local backpressure + local worker scale-up`，并保留 `actor + rpc + jobmaster` 作为本机多进程实验路径。

- 单机流式路径：`readStream -> StreamingDataFrame -> writeStream` 已具备 query 级配置、checkpoint/progress、本地文件 source/sink、窗口列、基础状态聚合。
- 稳定性路径：`StreamingQuery` 负责 query-local 流控与端到端反压，默认采用“有界排队后阻塞”，避免 source 在 sink/state 过慢时无限积压。
- query 级执行模式：`StreamingQuery` 当前支持 `single-process / local-workers / actor-credit / auto`，并会在 progress/snapshot 中记录最终选择与原因。
- logical/physical 规划：logical plan 新增统一节点 `WindowAssign / Sink`；physical strategy 在 progress 中结构化记录执行模式、transport、水位与批成本估算。
- source/sink ABI：新增 C++ 运行时 ABI（`RuntimeSource / RuntimeSink`），接口显式带 `query_id / checkpoint / backpressure` 上下文，作为后续跨进程 source/sink 扩展的稳定边界。
- 单进程路径：`sql_demo`、`df_demo`、`stream_demo` 继续保留，用于验证本地执行语义。
- 本机多进程路径：保留 `actor + rpc + jobmaster` 最小闭环，用于验证同机任务分发与运行时边界，不作为真正分布式完成态。

## 当前本地多进程架构

```text
client
  -> scheduler(jobmaster)
      -> worker
          -> in-proc operator chain
      -> result back to client
```

角色说明：

- `in-proc operator chain`：单个 worker 进程内部仍使用本地算子链执行，不把算子拆成跨节点执行图。
- `cross-process RPC`：client、scheduler、worker 之间通过 RPC/codec 传输任务与结果。
- `simple jobmaster`：当前由 `actor_rpc_scheduler` 承担最小 jobmaster 职责，只负责接收、分配、转发与状态观测，不做复杂容错、重试、资源隔离。
- `single-node fallback`：保留 `--single-node` 本地回退路径，避免影响已有单机闭环。

这意味着当前版本已经具备“本地多进程协作执行”的最小形态，但还不是完整分布式运行时。

## 术语统一

为避免外部生态专有命名侵入仓库，统一使用以下术语：

- `Session` 风格命名 -> `DataflowSession`
- SQL 入口 -> `session.sql(...)`
- 读取入口 -> `session.read(...)`
- `compatibility` 表达 -> `语义对齐` 或 `接口语义参考`

新增文档、注释、接口说明请避免引入外部框架专名，使用仓库内统一术语。

## 当前能力边界

已具备：

- `DataflowSession` / `DataFrame` / `StreamingDataFrame`
- `read_csv` / `readStream(...)` / `readStreamCsvDir(...)`
- Streaming query 配置：`trigger interval`、`max in-flight batches`、`max queued partitions`、`checkpoint path`、`single-process/local-workers/actor-credit/auto`
- 流控与反压：query-local backlog 水位、阻塞计数、bounded backlog、progress snapshot
- Streaming source/sink：`memory source`、`目录 CSV 增量 source`、`console sink`、`append 文件 sink`
- Python 数据交换：`CSV` 文件输入 + `Arrow` 进程内 batch/stream 输入 + `Arrow` 输出
- 基础算子：`select / filter / withColumn / drop / limit / window`
- 聚合：`groupBy + sum/count`，以及批处理 `SUM / COUNT / AVG / MIN / MAX`
- 基础状态聚合：stateful `sum/count`
- 本地 SQL 主链路：`SqlParser -> SqlPlanner -> DataFrame`
- 流式 SQL 最小闭环：`streamSql(SELECT ...)`、`startStreamSql(INSERT INTO ... SELECT ...)`
- 流式 SQL CSV 表：`CREATE SOURCE TABLE ... USING csv`、`CREATE SINK TABLE ... USING csv`
- Python wrapper（v1）：`Session / DataFrame / StreamingDataFrame / StreamingQuery`
- 临时视图：`createTempView / resolve`
- actor/rpc 最小闭环：`scheduler / worker / client / smoke`

当前限制：

- 运行模型当前固定为 `micro-batch only`，不做 continuous。
- window 当前仅提供固定窗口列（tumble）与基础状态聚合，不扩展更复杂窗口语义。
- 当前版本不扩展 CTE / 子查询。
- 当前版本不扩展 `UNION` 相关能力。
- `JOIN` 仅保留现有最小能力，不在本轮继续做更复杂 join 语义扩展。
- 流式 SQL 当前不支持 `JOIN / AVG / MIN / MAX / window SQL / INSERT ... VALUES`。
- Python wrapper 当前通过仓库内 Python 配置规则自动探测本机可用的 CPython 开发头，不支持 Python callback / UDF / 自定义 source/sink。
- 当前反压是 query-local，一版不做多 query 全局公平调度。
- 当前多进程链路仍是本地多进程验证，不包含真正分布式调度、容错恢复、状态迁移、资源治理。

## Streaming 运行时（当前版本）

当前 streaming 主接口为：

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`
- `StreamingQuery.start() / awaitTermination() / stop()`

`StreamingQueryOptions` 当前承载：

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

`StreamingQueryProgress` / `snapshotJson()` 当前额外暴露：

- `execution_mode`
- `execution_reason`
- `estimated_state_size_bytes / estimated_batch_cost`
- `backpressure_max_queue_batches / backpressure_high_watermark / backpressure_low_watermark`

用于说明当前 query 最终走的是哪条本地执行路径，以及为何选择或回退。

### query 级 actor / auto 选择

- `actor-credit` 和 `auto` 当前只会对**明确支持的热路径**生效：前置变换全部是 partition-local，且最终 barrier 为 `window_start + key` 上的 `sum(value)`。
- 当前这条热路径会下推到本地 actor-stream runtime，执行 `window_start/key/value` 的 partition partial aggregate，再把结果回并回 `StreamingQuery`。
- 不满足这条条件时，query 会自动回退到现有 `single-process` 路径，并在 `execution_reason` 中说明原因。
- 这意味着当前的 actor/auto 不是“任意 streaming plan 自动并行化”，而是**对明确可识别的窗口分组求和热路径做定向加速**。

### 反压语义

- 反压按 `batch / partition` 水位控制。
- 当 backlog 超过阈值时，source 拉取会被阻塞，而不是继续无限积压。
- sink 写出慢、状态更新慢、或局部并行分区过多，都会回灌到 query-local backlog 指标。
- 当前最少可观察指标：`blocked_count / max_backlog_batches / inflight_batches / inflight_partitions / last_batch_latency_ms / last_sink_latency_ms / last_state_latency_ms`。

### checkpoint / progress

- 每批完成后会把 `batches_pulled / batches_processed / blocked_count / max_backlog_batches / last_source_offset` 落到本地 checkpoint 文件。
- `MemoryStreamSource` 会按 batch offset 恢复。
- `DirectoryCsvStreamSource` 会按最后完成文件名恢复。
- `snapshotJson()` 当前还会携带 `execution_mode / execution_reason`，用于排查 query 是否进入 actor-credit 或 auto 回退。

### 状态索引与窗口淘汰

- 当前 stateful 聚合会把 `group key -> aggregate state` 作为状态主索引。
- 若分组键包含窗口列（如 `window_start`），运行时会额外维护窗口成员索引，用于按窗口淘汰旧状态。
- `max_retained_windows > 0` 时，仅保留最近 N 个窗口对应的状态键，避免状态无限增长。

## 流式 SQL（当前版本）

当前已补上基于现有 `StreamingDataFrame` API 的最小流式 SQL 子集。

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

当前实现仍然是把 SQL 映射回已有流式算子：

- `FROM source` -> 已注册的流式 view
- `WHERE` -> `filter`
- `GROUP BY + SUM/COUNT(*)` -> `groupBy + sum/count`
- `HAVING` -> 聚合结果上的 `filter`
- `LIMIT` -> 现有 `StreamingDataFrame::limit`

### 当前支持的流式 SQL DDL / DML

- `CREATE SOURCE TABLE ... USING csv OPTIONS(path 'dir', delimiter ',')`
- `CREATE SINK TABLE ... USING csv OPTIONS(path '/tmp/out.csv', delimiter ',')`
- `INSERT INTO sink_table SELECT ...`

其中：

- `SOURCE TABLE USING csv` 绑定到 `DirectoryCsvStreamSource`
- `SINK TABLE USING csv` 绑定到 `FileAppendStreamSink`
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

### 当前限制

- 只支持 `USING csv`
- 只支持单表查询
- 不支持 `JOIN`
- 不支持 `AVG / MIN / MAX`
- 不支持窗口 SQL 语法
- 不支持 `INSERT INTO ... VALUES` 的流式路径
- `LIMIT` 沿用现有 streaming API 语义，不应解读为完整无限流全局 SQL limit

## Python wrapper（v1）

当前仓库已补上最小 Python 对象绑定，目标是让 Python 直接调用现有 `DataflowSession`、stream API 和 stream SQL。

### 绑定方式

- 不引入额外第三方绑定框架
- 采用手写 CPython extension
- 对外暴露四类 Python 对象：
  - `Session`
  - `DataFrame`
  - `StreamingDataFrame`
  - `StreamingQuery`

### GIL 策略

为了避免 Python 线程被长时间 native 调用阻塞，以下操作会在 native 执行期间显式释放 GIL：

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

第一版明确不支持：

- Python callback
- Python UDF
- Python 自定义 source / sink

这样可以避免 native 线程反向进入 Python 时的额外 GIL 管理复杂度。

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

其中：

- `create_dataframe_from_arrow(...)` 支持 `pyarrow.Table` 和可被 `pyarrow.table(...)` 归一化的对象
- `create_stream_from_arrow(...)` 支持单个 `pyarrow.Table` / `RecordBatch`，也支持由多批 `Table` / `RecordBatch` 组成的 Python 序列
- 因此 Python 侧既可以继续走 `CSV`，也可以完全跳过文件落盘，直接把内存中的 Arrow 数据交给 batch / stream 执行链

### 构建与运行

当前 Python wrapper 会在 Bazel workspace 初始化时自动探测本机可用的 CPython 解释器与开发头。
如需显式指定，可设置 `VELARIA_PYTHON_BIN=/path/to/pythonX.Y`。
Python 侧依赖管理与 demo 运行默认使用 `uv`。

仓库中同时提供了 `python_api/` 目录：

- `python_api/pyproject.toml`
- `python_api/requirements.lock`
- `python_api/BUILD.bazel`

用于统一 Python API 元数据、`uv` 依赖管理和 Bazel wheel 构建入口。

需要注意：

- `//python_api:velaria_whl` 负责打包 pure-Python API 层
- `//python_api:velaria_native_whl` 会在 pure wheel 基础上注入 `_velaria.so`，产出本地平台 wheel
- 已安装 native wheel 时，`import velaria` 会直接从包内加载 `_velaria.so`
- 在源码树开发态，如果仓库里已经构建过 `//:velaria_pyext`，`velaria` 会自动从 `bazel-bin/_velaria.so` 发现 native 扩展
- `VELARIA_PYTHON_EXT` 仍保留为显式覆盖入口，但不是主路径

构建扩展：

```bash
bazel build //:velaria_pyext
bazel build //python_api:velaria_whl
bazel build //python_api:velaria_native_whl
```

Python 运行时依赖由 `uv sync --project python_api ...` 统一安装，其中包含 `pyarrow`，因为 `DataFrame.to_arrow()` 会返回 `pyarrow.Table`。
当前 Python API 面向 `Python 3.12` 和 `Python 3.13`。

运行 demo：

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python /opt/homebrew/bin/python3.13
uv run --project python_api python python_api/demo_stream_sql.py
uv run --project python_api python python_api/demo_batch_sql_arrow.py
```

demo 会展示：

- Python 侧创建 `Session`
- Python 侧直接用 `pyarrow.Table` 建 batch / stream 输入
- Python 侧调用 stream SQL 启动 `INSERT INTO ... SELECT ...`
- Python 侧读取 sink CSV 并打印结果与 query progress
- Python 侧 `pyarrow.Table -> DataFrame` roundtrip
- Python 侧 batch SQL 可直接基于 Arrow 输入视图执行并返回 Arrow 结果

## SQL 规划执行模型（v1）

`session.sql()` 已按 `Parser -> Planner -> DataFrame` 的三层推进：

- `SqlParser` 将 SQL 解析成 `SqlQuery` AST。当前已支持 `ON (...)`、`HAVING`、`LIMIT`，并可解析简单的括号谓词写法（如 `WHERE a.score > (5)`）。
- `SqlPlanner::buildLogicalPlan(...)` 生成逻辑步骤：`Scan -> (Join?) -> (Filter?) -> (Aggregate?) -> (Having?) -> Project -> (Limit?)`。
- `SqlPlanner::buildPhysicalPlan(...)` 将逻辑步骤映射到物理步骤，当前策略：
  - `Join` 与 `Aggregate` 作为 barrier 阶段；
  - 连续 `Filter/Project/Limit/Having` 可尝试融合为 `FusedUnary`；
  - 连续 `Project` 去重、连续 `Limit` 取小值（优化边界保留可验证）。
- `materializeFromPhysical(...)` 按物理步骤回放为现有 `DataFrame` 算子链，复用本地执行器。

## SQL DDL（v1）语义

参考主流数据处理引擎的 `Catalog` 语义，当前支持以下建表类型：

- `CREATE TABLE`：普通表，既可被 `SELECT`，也可被 `INSERT`。
- `CREATE SOURCE TABLE`：源表，偏向上游输入，当前版本限制为只读，禁止 `INSERT`。
- `CREATE SINK TABLE`：结果表，偏向下游输出，当前版本限制为只写，禁止 `SELECT`（含 `JOIN` 输入）。

当前版本仍是本地内存 catalog，`SOURCE`/`SINK` 只记录表语义并做执行约束校验，不直接引入外部连接器。

## SQL DDL / DML 支持（v1）

### DDL

- `CREATE TABLE`: 创建普通内存表，支持读写。
- `CREATE SOURCE TABLE`: 创建源表，仅可读。
- `CREATE SINK TABLE`: 创建汇聚/结果表，仅可用于写入。

示例：

```sql
CREATE TABLE users (id INT, name STRING, score INT);
CREATE SOURCE TABLE source_users (name STRING, score INT);
CREATE SINK TABLE summary_by_region (region STRING, score_sum INT);
```

### DML

- `INSERT INTO ... VALUES`：支持按列顺序插入或显式列名。
- `INSERT INTO ... SELECT`：支持把子查询结果写入目标表（当前解析器仅允许 `SELECT` 子查询语法）。

示例：

```sql
INSERT INTO users (id, name, score) VALUES (1, 'alice', 10), (2, 'bob', 20);

INSERT INTO summary_by_region
SELECT name AS region, SUM(score) AS score_sum
FROM users
GROUP BY name;
```

### 语义限制

- 任何 `INSERT` 到 `SOURCE` 表都会返回语义错误。
- 对 `SINK` 表执行 `SELECT`（或 `JOIN` 中作为右表）会返回语义错误。
- `SELECT` 中必须满足本版本约束（例如 `HAVING` 依赖聚合语义、`GROUP BY` 与投影一致性等）。

### 执行模型

- scheduler 只负责编排与分发，不直接执行 SQL。
- 包含 `dashboard` 在内的提交入口都走 worker 路径（`JobMaster + worker`）。
- 当未禁用自启动 worker（`--no-auto-worker`）且 worker 不可用时，调度返回 `no worker available`，不会在 scheduler 内本地“伪执行”。

### 调度策略（当前版本）

- 提交入口：`dashboard/client` 先构建计划并通过 `submitRemote` 创建 `job -> chain -> task`。
- 排队语义：远端可执行任务进入 `JobMaster` 的 `pending_remote_tasks_` 队列；只有 `chain` 变为可运行且调度器发现空闲 worker 时才会派发。
- 派发规则：`dispatcher` 按 `idle_workers` 空闲 worker 数从队列头取任务，按到达顺序依次下发（先到先服务）。
- worker 绑定：每个任务与 worker 连接绑定；发送成功后任务状态变更为 `TASK_SUBMITTED/CHAIN_SUBMITTED`，失败则回灌 `TASK_FAILED/JOB_FAILED`。
- 并发与并行：`chain` 并发受 `chain_parallelism` 和可用 worker 约束；每条 task 支持重试与 heartbeat 观测。
- 结果确认：worker 完成后返回 `Result`，scheduler 由 worker 响应更新 `job/chain/task` 快照并回推给提交端。
- 管理建议：用日志事件与 `/api/jobs` 快照观察 `JOB_* / CHAIN_* / TASK_*` 的状态变化判断是否在排队与是否正在运行。
- 当前默认 worker 策略：scheduler 默认自启动本地 worker（`--local-workers`，默认 2）。如需人工模式可使用 `--no-auto-worker`，此时才会出现 `no-worker-available` 回退路径。

## 构建与启动命令

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

### 本机多进程 actor-stream 实验

- `stream_actor_benchmark`：使用本地 worker 进程、分区级 credit 和 actor rpc 消息执行窗口 + key 的 partial aggregate。
- `stream_benchmark`：从 `StreamingQuery` 入口直接验证 `single-process / local-workers / actor-credit / auto` 四种 query 级执行模式。
- `stream_actor_credit_test`：验证本机多进程路径下：
  - credit 上限不被突破
  - 阻塞计数会触发
  - 多进程结果与单进程基线一致

这条路径当前是独立实验运行时：

- 目标是验证 `partition credit + local worker process + result merge` 的最小闭环。
- 重点在语义与流控，不代表当前实现已经优于单进程。
- 当前 worker 做的是 partition 级 partial aggregate，最终状态回并仍在 coordinator 本地完成。
- 当前输入大 payload 会在同 host 本地 worker 路径上走 `shared memory + mmap`；worker 和 coordinator 都已经支持基于 buffer 视图的解码，不再先把整块 payload 拷进 `std::vector<uint8_t>` 再反序列化。
- 对轻量 aggregation，actor-stream 仍可能慢于单进程；在 CPU-heavy 聚合负载下可用 `stream_actor_benchmark` 验证本机多进程 scale-up，例如：

```bash
bazel run //:stream_actor_benchmark -- 16 65536 4 4 0 400
```

参数含义依次为：
- `batch_count`
- `rows_per_batch`
- `worker_count`
- `max_inflight_partitions`
- `worker_delay_ms`
- `cpu_spin_per_row`
- 可选第 7 个参数：`mode = single / actor / auto / all`

当前 query 级 `stream_benchmark` 的一个参考结果：

- `stateful-single`: `89k rows/s`
- `stateful-local-workers`: `88k rows/s`
- `stateful-actor-credit`: `108k rows/s`
- `stateful-auto`: 默认阈值下仍可能保守回退到 `single-process`

这说明 query 级 actor 下推已经接入 `StreamingQuery`，但 `auto` 阈值仍需要针对真实 query workload 继续调优。

### 构建 actor/rpc/jobmaster 路线

```bash
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

### smoke 自检

```bash
bazel run //:actor_rpc_smoke
```

预期输出：

```text
[smoke] actor rpc codec roundtrip ok
```

### 三进程联调

终端 A：

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

默认 scheduler 会自启动一个本地 worker（`--no-auto-worker` 可关闭），如果你希望强制手工 worker，可再按需启动：

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

- 访问地址：`http://127.0.0.1:8080`
- 前端代码只维护 `src/dataflow/runner/dashboard/app.ts`，通过 Bazel 目标
  `//:dashboard_app_js` 自动编译并输出 `src/dataflow/runner/dashboard/app.js`，避免重复维护两份代码。
- 页面能力：  
  - 通过 `payload` / `SQL` 触发任务提交
  - 查看 `/api/jobs` 与 `/api/jobs/{id}` 的任务历史/详情
  - 每秒自动刷新并可手动 `刷新`
- 启动示例（保留 RPC + 打开 Dashboard）：

```bash
./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

如果你已经编译过且希望跳过 TS 编译：

```bash
BUILD_DASHBOARD=0 ./scripts/run_actor_rpc_scheduler.sh -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
```

### 一键联调（推荐）

```bash
./scripts/run_actor_rpc_e2e.sh
```

参数示例：

```bash
./scripts/run_actor_rpc_e2e.sh --payload "hello world"
./scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
DO_BUILD=1 ./scripts/run_actor_rpc_e2e.sh --sql "SELECT 1 AS x"
```

脚本配置环境变量：

- `ADDRESS`：RPC 地址（默认 `127.0.0.1:61000`）
- `WORKER_ID`：worker 名（默认 `worker-1`）
- `PAYLOAD`：默认 payload（默认 `demo payload`）
- `TIMEOUT_SECONDS`：超时（默认 `20`）
- `DO_BUILD`：是否先构建（`1`/`0`，默认 `0`）
- `BUILD_DASHBOARD`：是否先构建 dashboard TS（`1`/`0`，默认 `1`，底层调用 `bazel build //:dashboard_app_js`）

提交时若未发现可用 worker，Dashboard 接口会返回 `503 Service Unavailable`
并带有 `message: "scheduler reject: no worker available"`，说明任务尚未进入可执行调度队列。

### 单节点回退路径

```bash
bazel run //:actor_rpc_client -- --single-node --payload "local-only payload"
```

## 调试与验收步骤

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
- 不得出现 `codec roundtrip failed` 或 `codec mismatch`

### 3. 多进程联动

按顺序启动 `scheduler -> worker -> client`。

验收标准：

- scheduler 先打印 `[scheduler] listen 127.0.0.1:61000`
- worker 打印 `[worker] connected 127.0.0.1:61000`
- client 先收到 `job accepted`，再收到 `job result`
- 不得出现 `no-worker-available`
- 不得出现 `cannot connect`
- 不得出现 `scheduler closed connection`

### 4. 常见错误规避

- `scheduler reject: no-worker-available`：通常是 worker 未连接成功，或 client 启动顺序过早。
- `[worker] cannot connect ...` / `[client] cannot connect ...`：通常是 scheduler 未启动、地址不一致或端口错误。
- `scheduler failed to listen on ...`：通常是端口占用或地址绑定失败。
- `Invalid --listen endpoint` / `Invalid --connect endpoint`：参数格式必须为 `host:port`。

## 示例源码扩展名核对

当前示例源码仍统一使用 `.cc`，`BUILD.bazel` 中可直接看到：

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

额外核对命令：

```bash
find src/dataflow/examples -maxdepth 1 -type f ! -name '*.cc'
```

若该命令无输出，则说明示例目录下没有混入非 `.cc` 源文件。

## 一次 build/smoke 成功摘要命令

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke \
  && bazel run //:actor_rpc_smoke \
  && echo '[summary] build+smoke ok'
```
