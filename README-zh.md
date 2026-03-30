# Velaria：纯 C++17 Streaming-First 数据流内核

`README-zh.md` 是中文镜像文档，对应英文主文档位于 [README.md](./README.md)。后续修改必须保持这两份文件结构和语义同步。

Velaria 是一个本地优先的 C++17 数据流引擎研究项目。当前目标刻意收敛：先把单机流式主链路稳定下来，让 batch 和 stream 落在同一个执行模型里，再谨慎扩展到本机多进程执行，而不是宣称已经完成分布式运行时。

## 它当前是什么

当前主执行路线是：

`micro-batch in-proc + query-local backpressure + local worker scale-up`

仓库里同时保留 `actor + rpc + jobmaster` 作为同机多进程实验路径。这个路径用于执行和观测研究，不代表已经具备完整分布式调度、故障恢复、状态迁移或资源治理能力。

核心公开对象：

- `DataflowSession`
- `DataFrame`
- `StreamingDataFrame`
- `StreamingQuery`

核心本地执行链：

```text
source -> StreamingDataFrame/operator chain -> sink
```

同机实验链：

```text
client -> scheduler(jobmaster) -> worker -> in-proc operator chain -> result
```

## 当前能力边界

当前已具备：

- batch + streaming 共用一个本地执行框架
- `read_csv`, `readStream(...)`, `readStreamCsvDir(...)`
- query-local 反压、有界 backlog、progress snapshot、checkpoint path
- 执行模式：`single-process`、`local-workers`、`actor-credit`、`auto`
- 本地文件 source/sink
- 基础流式算子：`select / filter / withColumn / drop / limit / window`
- stateful `sum` 和 `count`
- 建立在现有 streaming operators 上的最小 stream SQL
- Python Arrow 输入/输出
- 同机 actor/rpc/jobmaster smoke 链路

当前明确不做：

- 宣称完成 distributed runtime
- 把 Python callback 拉进热路径
- Python UDF
- Python sink callback 直接进入 native sink ABI
- 把 `actor-credit` 扩成通用计划并行化
- 横向扩很多 SQL 面，例如完整 `JOIN / CTE / subquery / UNION / streaming AVG/MIN/MAX`

## Streaming 运行时 Contract

主要 streaming 入口：

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.explainStreamSql(sql, options)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`

### Progress 与 Strategy

`StreamingQueryProgress` 和 `snapshotJson()` 会暴露：

- 执行选择：`execution_mode`、`execution_reason`、`transport_mode`
- 工作量估算：`estimated_state_size_bytes`、`estimated_batch_cost`
- source/sink 状态：`source_is_bounded`、`sink_is_blocking`
- 流控计数：backlog、inflight、blocked、水位字段
- checkpoint 字段：`checkpoint_delivery_mode`、`last_source_offset`

`explainStreamSql(...)` 返回三段：

- `logical`
- `physical`
- `strategy`

其中 `strategy` 会解释 selected mode、fallback reason、actor 热路径命中与否、transport、batch/state 估算，以及 actor/shared-memory 决策参数。

### 反压

当前反压语义是 query-local 且有界：

- `backlog` 表示 pull 之后、drain 之前的队列 batch 数
- `blocked_count` 统计 producer 进入 wait 的事件次数，不统计循环轮数
- `max_backlog_batches` 表示 enqueue 后观测到的最大 backlog
- `inflight_batches` 和 `inflight_partitions` 表示尚未消费的排队工作量
- sink 变慢、state finalize 变慢、或局部分区压力过大，都会反馈到同一个 query-local backlog 计数

延迟字段定义：

- `last_batch_latency_ms`：从 batch 开始执行到 sink flush 完成
- `last_sink_latency_ms`：sink write + flush
- `last_state_latency_ms`：state/window finalize，stateless batch 为 `0`

### Checkpoint 与 Resume

checkpoint 文件是本地文件，并采用原子替换写入。

当前交付语义：

- 默认 `at-least-once`：不恢复 source offset；允许 replay，sink 允许重复输出
- `best-effort`：仅当 source 实现了 `restoreOffsetToken(...)` 时恢复 offset；仍然不是 exactly-once sink 交付

当前内置 source 行为：

- `MemoryStreamSource`：在 `best-effort` 下可按 batch offset 恢复
- `DirectoryCsvStreamSource`：在 `best-effort` 下可按最后完成的文件恢复

### Actor-Credit 与 Auto

`actor-credit` 和 `auto` 只服务一个很窄的热路径：

- 前置变换必须全部是 partition-local
- 最终 barrier 必须按 `window_start + key` 分组
- 聚合必须是 `sum(value)`

不满足这组条件的 query 必须回退到 `single-process`，并通过 `execution_reason` 说明原因。

## 流式 SQL

Velaria 刻意把 stream SQL 保持在一个很小的子集内，并映射回现有 streaming operators。

### 入口

- `session.streamSql("SELECT ...") -> StreamingDataFrame`
- `session.startStreamSql("INSERT INTO sink_table SELECT ...", options) -> StreamingQuery`
- `session.explainStreamSql(...) -> string`

### 当前支持

- 单表 `SELECT`
- `WHERE`
- `GROUP BY`
- `HAVING`
- `LIMIT`
- `SUM(col)`
- `COUNT(*)`
- 最小 window SQL：`WINDOW BY <time_col> EVERY <window_ms> AS <output_col>`

支持的 DDL/DML：

- `CREATE SOURCE TABLE ... USING csv`
- `CREATE SINK TABLE ... USING csv`
- `INSERT INTO sink_table SELECT ...`

当前 stream SQL 不支持：

- `JOIN`
- `AVG / MIN / MAX`
- `INSERT INTO ... VALUES`
- 宽泛 ANSI window SQL
- CTE / 子查询 / `UNION`

示例：

```sql
CREATE SOURCE TABLE stream_events (ts STRING, key STRING, value INT)
USING csv OPTIONS(path '/tmp/stream-input', delimiter ',');

CREATE SINK TABLE stream_summary (window_start STRING, key STRING, value_sum INT)
USING csv OPTIONS(path '/tmp/stream-output.csv', delimiter ',');

INSERT INTO stream_summary
SELECT window_start, key, SUM(value) AS value_sum
FROM stream_events
WINDOW BY ts EVERY 60000 AS window_start
GROUP BY window_start, key;
```

## Python API

Python 继续只做前端和交换层，不进入执行热路径。

主要 API：

- `Session.read_csv(...)`
- `Session.sql(...)`
- `Session.create_dataframe_from_arrow(...)`
- `Session.create_stream_from_arrow(...)`
- `Session.create_temp_view(...)`
- `Session.read_stream_csv_dir(...)`
- `Session.stream_sql(...)`
- `Session.explain_stream_sql(...)`
- `Session.start_stream_sql(...)`

Arrow ingestion 支持：

- `pyarrow.Table`
- `pyarrow.RecordBatch`
- `RecordBatchReader`
- 实现了 `__arrow_c_stream__` 的对象
- Arrow batch 的 Python 序列

### XLSX 读取

仓库也支持直接读取 `.xlsx` 文件为 Velaria DataFrame。

使用方式为 `velaria.read_excel(session, path, ...)`：

```python
from velaria import Session, read_excel

session = Session()
df = read_excel(session, "/path/to/file.xlsx", sheet_name="Sheet1")
session.create_temp_view("excel_source", df)
print(session.sql("SELECT * FROM excel_source LIMIT 5").to_rows())
```

该能力依赖 `pandas` 与 `openpyxl`（已作为 Python 包依赖）：

```bash
uv run python -c "import pandas, openpyxl"
```

本仓库里的 Python 命令统一使用 `uv`：

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python python3.12
uv run --project python_api python python_api/demo_batch_sql_arrow.py
uv run --project python_api python python_api/demo_stream_sql.py
```

同时在 Session 侧新增了向量查询入口：`Session.vectorQuery(table, vector_column, query_vector, top_k, metric)`（metric 支持 cosine/dot/l2），以及 explain 接口 `Session.explainVectorQuery(...)`。

支持打包单文件 CLI 可执行产物（内含 Python 运行时依赖 + native `_velaria.so`）：

```bash
./scripts/build_py_cli_executable.sh
./dist/velaria-cli csv-sql \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"
```

额外支持直接编译 native CLI 二进制（运行时不依赖 Python 环境）：

```bash
bazel build //:velaria_cli
./bazel-bin/velaria_cli \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"
```

native CLI 向量查询（fixed length vector，支持 cosine/cosin、dot 与 l2）：

```bash
./bazel-bin/velaria_cli \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --metric cosine \
  --top-k 5
```

runtime 传输层现已在 proto-like 与 binary row batch codec 中保留 `FixedVector` 类型，跨进程传输时不会丢失向量维度语义。
FixedVector 在内部 codec 里改为 raw float bit payload 编码，避免文本往返造成的精度损耗。
当前向量检索范围为本地 exact scan（`mode=exact-scan`）+ 固定维度 float 向量；v0.1 不包含 ANN 与分布式执行路径。
Arrow ingestion 已增加 `FixedSizeList<float32>` 的 native 快路径，可减少向量列的 Python 对象转换开销。

## 同机多进程实验路径

同机路径刻意保持最小：

- scheduler 接收提交并维护 snapshot
- worker 执行本地 operator chain
- dashboard 和 client 都必须走 worker 执行链

构建：

```bash
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

smoke：

```bash
bazel run //:actor_rpc_smoke
```

三进程本地运行：

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler --dashboard-enabled --dashboard-listen 127.0.0.1:8080
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

Dashboard：

- 地址：`http://127.0.0.1:8080`
- 源码：`src/dataflow/runner/dashboard/app.ts`
- 构建目标：`//:dashboard_app_js`

## Benchmark 与 Observability

常用本地目标：

- `//:stream_benchmark`
- `//:stream_actor_benchmark`
- `//:tpch_q1_style_benchmark`
- `//:vector_search_benchmark`

同机 observability regression：

```bash
./scripts/run_stream_observability_regression.sh
```

这些 benchmark 会输出结构化 profile，用于同机执行路径诊断和回归跟踪，不用于把绝对吞吐做成机器敏感的硬门槛。

## 构建与验证

单机基线：

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

核心回归集：

```bash
bazel test //:sql_regression_test //:planner_v03_test //:stream_runtime_test //:stream_actor_credit_test //:source_sink_abi_test //:stream_strategy_explain_test
bazel test //python_api:custom_stream_source_test //python_api:streaming_v05_test //python_api:arrow_stream_ingestion_test
```

一行 build/smoke 摘要：

```bash
bazel build //:sql_demo //:df_demo //:stream_demo \
  //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke \
  && bazel run //:actor_rpc_smoke \
  && echo '[summary] build+smoke ok'
```

## 仓库规则

- 语言基线：`C++17`
- 构建系统：`Bazel`
- 对外 session 入口保持为 `DataflowSession`
- 扩展同机实验时不要破坏 `sql_demo / df_demo / stream_demo`
- 示例源码统一使用 `.cc`
- 本仓库中的 Python 命令统一使用 `uv`

## CI 与打包

CI 维持收敛：

- PR CI 覆盖 native build、回归测试和 Python smoke
- wheel job 生成 Linux 与 macOS native wheel
- release 通过 tag 驱动，并校验 `velaria.__version__`

目标是让日常开发成本保持可控，同时验证当前真正暴露出去的接口面。
