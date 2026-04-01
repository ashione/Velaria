# Velaria：纯 C++17 本地数据流内核

`README-zh.md` 是中文镜像文档，对应英文主文档位于 [README.md](./README.md)。后续修改必须保持这两份文件结构和语义同步。

Velaria 是一个本地优先的 C++17 数据流引擎研究项目。仓库现在围绕“一个内核 + 两个非内核层”组织：

- `Core Kernel`
  - 本地执行语义
  - batch + stream 共用一个模型
  - 稳定的 explain / progress / checkpoint contract
- `Python Ecosystem`
  - 正式支持 Arrow / wheel / CLI / `uv` / Excel / Bitable / custom stream adapter
  - 向外投影内核能力，但不进入热路径
- `Experimental Runtime`
  - 同机 `actor/rpc/jobmaster`
  - 用于执行与观测研究，不是第二套内核

## 黄金路径

唯一黄金路径是：

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

同机 actor/rpc 路径仍保留在仓库里，但不再作为主叙事。

## 仓库分层

### Core Kernel

Core 负责：

- logical planning 与最小 SQL 映射
- table/value 执行模型
- 本地 batch 与 streaming runtime
- source/sink ABI
- runtime contract surface
- 本地 vector search 能力

仓库入口：

- 文档：
  - [docs/core-boundary.md](./docs/core-boundary.md)
  - [docs/runtime-contract.md](./docs/runtime-contract.md)
  - [docs/streaming_runtime_design.md](./docs/streaming_runtime_design.md)
- Bazel source group：
  - `//:velaria_core_logical_sources`
  - `//:velaria_core_execution_sources`
  - `//:velaria_core_contract_sources`
- 回归套件：
  - `//:core_regression`

### Python Ecosystem

Python 是正式支持的生态层，不是顺手附带的 wrapper。

它包括：

- `python_api` 里的 native binding
- Arrow 输入与输出
- `uv` 工作流
- wheel / native wheel / CLI 打包
- Excel 与 Bitable 适配
- custom source / custom sink adapter
- `python_api/velaria_cli.py` 里的正式 CLI 工具入口
- `python_api/examples` 里的 Python 生态 demo
- `python_api/benchmarks` 里的 Python benchmark

它不定义：

- 执行热路径行为
- 独立的 progress/checkpoint 语义
- 独立的 vector-search 语义

仓库入口：

- 文档：
  - [python_api/README.md](./python_api/README.md)
- Bazel source group：
  - `//:velaria_python_ecosystem_sources`
- Python 层 source group：
  - `//python_api:velaria_python_supported_sources`
  - `//python_api:velaria_python_example_sources`
  - `//python_api:velaria_python_experimental_sources`
- 回归套件：
  - `//:python_ecosystem_regression`
- Python 层回归套件：
  - `//python_api:velaria_python_supported_regression`
- shell 入口：
  - `./scripts/run_python_ecosystem_regression.sh`

### Experimental Runtime

Experimental runtime 包括：

- actor runtime
- rpc codec / transport 实验
- scheduler / worker / client 链路
- 同机 smoke 与 benchmark 工具

仓库入口：

- Bazel source group：
  - `//:velaria_experimental_sources`
- 回归套件：
  - `//:experimental_regression`
- shell 入口：
  - `./scripts/run_experimental_regression.sh`

### Examples

examples 与 helper scripts 只用于说明各层，不定义各层。

- Bazel source group：
  - `//:velaria_examples_sources`

## Runtime Contract

稳定的 runtime contract 文档位于 [docs/runtime-contract.md](./docs/runtime-contract.md)。

主要 stream 入口：

- `session.readStream(source)`
- `session.readStreamCsvDir(path)`
- `session.streamSql(sql)`
- `session.explainStreamSql(sql, options)`
- `session.startStreamSql(sql, options)`
- `StreamingDataFrame.writeStream(sink, options)`

稳定 stream contract surface：

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

其中 `strategy` 是 mode 选择、fallback reason、transport、backpressure threshold 与 checkpoint delivery mode 的唯一解释出口。

## 当前能力边界

当前已具备：

- 本地 batch + streaming 共用一个内核
- `read_csv`, `readStream(...)`, `readStreamCsvDir(...)`
- query-local 反压、有界 backlog、progress snapshot、checkpoint path
- 执行模式：`single-process`、`local-workers`、`actor-credit`、`auto`
- 文件 source/sink
- 基础 streaming operators：`select / filter / withColumn / drop / limit / window`
- stateful streaming 聚合：`sum / count / min / max / avg`
- stream SQL grouped aggregate output：`SUM(col)`、`COUNT(*)`、`MIN(col)`、`MAX(col)`、`AVG(col)`
- 最小 stream SQL 子集
- 固定维度 float vector 的本地检索
- Python Arrow 输入/输出
- 同机 actor/rpc/jobmaster smoke 链路

当前明确不做：

- 宣称已完成 distributed runtime
- 把 Python callback 拉进热路径
- Python UDF
- 把 actor 并行化扩成任意 plan 的通用机制
- 超出当前 `window_start,key + SUM(value)` / `COUNT(*)` 热路径之外的 actor acceleration
- 宽泛 SQL 扩展，例如完整 `JOIN / CTE / subquery / UNION`
- ANN / 独立 vector DB / 分布式 vector 执行

## Python Ecosystem

Python 继续是正式支持的 ingress 与打包层，但不成为执行内核。

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

本仓库中的 Python 命令统一使用 `uv`：

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python python3.12
uv run --project python_api python python_api/examples/demo_batch_sql_arrow.py
uv run --project python_api python python_api/examples/demo_stream_sql.py
uv run --project python_api python python_api/examples/demo_vector_search.py
```

Python ecosystem 构建 / 测试前提：

- `uv`
- 一个带 `Python.h` 的本地 CPython
- 当 Bazel 不能自动发现可用解释器时，设置 `VELARIA_PYTHON_BIN`

推荐回归入口：

```bash
./scripts/run_python_ecosystem_regression.sh
```

## Local Vector Search

vector search 是本地内核能力，不是新子系统。

`v0.1` 范围：

- fixed-dimension `float32`
- 指标：`cosine`、`dot`、`l2`
- `top-k`
- exact scan only
- `DataFrame` / `DataflowSession`
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
bazel build //:velaria_cli
./bazel-bin/velaria_cli \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

./dist/velaria-cli vector-search \
  --csv /path/to/vectors.csv \
  --vector-column embedding \
  --query-vector "0.1,0.2,0.3" \
  --metric cosine \
  --top-k 5
```

vector explain 是稳定 contract 的一部分，当前要求至少包含：

- `mode=exact-scan`
- `metric=<cosine|dot|l2>`
- `dimension=<N>`
- `top_k=<K>`
- `candidate_rows=<M>`
- `filter_pushdown=false`
- `acceleration=flat-buffer+heap-topk`

benchmark 基线入口：

```bash
./scripts/run_vector_search_benchmark.sh
```

该脚本默认跑轻量 `--quick` 基线；如需完整 sweep，直接执行 `bazel run //:vector_search_benchmark`。

## Experimental Runtime

同机路径继续刻意保持收敛：

```text
client -> scheduler(jobmaster) -> worker -> in-proc operator chain -> result
```

它存在的目的：

- 同机执行实验
- transport 与 codec 观测
- benchmark 与 observability 开发

它不代表：

- 已完成 distributed scheduling
- 已完成 distributed fault recovery
- 已完成 cluster resource governance
- 已支持 production 级 distributed vector execution

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
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

## 构建与验证

单机基线：

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

分层回归入口：

```bash
./scripts/run_core_regression.sh
./scripts/run_python_ecosystem_regression.sh
./scripts/run_experimental_regression.sh
```

直接使用 Bazel suite：

```bash
bazel test //:core_regression
bazel test //:python_ecosystem_regression
bazel test //:experimental_regression
```

同机 observability regression：

```bash
./scripts/run_stream_observability_regression.sh
```

## 仓库规则

- 语言基线：`C++17`
- 构建系统：`Bazel`
- 对外 session 入口保持为 `DataflowSession`
- 不要破坏 `sql_demo / df_demo / stream_demo`
- 示例源码统一使用 `.cc`
- 本仓库中的 Python 命令统一使用 `uv`
- `README.md` 与 `README-zh.md` 必须保持同步
