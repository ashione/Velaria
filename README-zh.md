# Velaria：纯 C++17 本地数据流内核

`README-zh.md` 是中文镜像文档，对应英文主文档位于 [README.md](./README.md)。两份文档必须保持同步。

Velaria 是一个本地优先的 C++17 数据流引擎研究项目。当前目标保持收敛：

- 保持一个 native kernel 作为执行真相来源
- 稳住单机链路
- 通过正式支持的 Python 生态层向外暴露能力
- 把同机 actor/rpc 路径放在实验通道，而不是做成第二套内核

## 分层模型

### Core Kernel

负责：

- 本地 batch 与 streaming 执行
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

workspace 落盘会保留内核 contract，不会重定义它们：

- `explain.json` 保存 `logical / physical / strategy`
- `progress.jsonl` 逐行追加原生 `snapshotJson()` 输出
- 大结果保留为文件；SQLite 只保存索引元数据和小 preview

## 当前范围

当前已具备：

- 一个 native kernel 同时支持 batch + streaming
- `read_csv`, `readStream(...)`, `readStreamCsvDir(...)`
- query-local 反压、有界 backlog、progress snapshot、checkpoint path
- 执行模式：`single-process`、`local-workers`
- 文件 source/sink
- 基础 streaming operators：`select / filter / withColumn / drop / limit / window`
- stateful streaming 聚合：`sum / count / min / max / avg`
- 最小 stream SQL 子集
- 固定维度 float vector 的本地检索
- Python Arrow 输入/输出
- 本地 tracked run、run 目录落盘与 artifact 索引
- 同机 actor/rpc/jobmaster smoke 链路

当前不做：

- 宣称已完成 distributed runtime
- 把 Python callback 或 Python UDF 拉进热路径
- 宽泛 SQL 扩展，例如完整 `JOIN / CTE / subquery / UNION`
- ANN / 独立 vector DB / 分布式 vector 执行

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
- 打包产物：
  - `./dist/velaria-cli ...`

不要默认存在全局 `velaria-cli` 命令，除非你另外安装并暴露了这个入口。

### Python 工作流

初始化：

```bash
bazel build //:velaria_pyext
bazel run //python_api:sync_native_extension
uv sync --project python_api --python python3.13
```

运行示例：

```bash
uv run --project python_api python python_api/examples/demo_batch_sql_arrow.py
uv run --project python_api python python_api/examples/demo_stream_sql.py
uv run --project python_api python python_api/examples/demo_vector_search.py
```

tracked run 示例：

```bash
uv run --project python_api python python_api/velaria_cli.py run start -- csv-sql \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python_api python python_api/velaria_cli.py run show --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts preview --artifact-id <artifact_id>
```

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

## Experimental Runtime

同机执行链路：

```text
client -> scheduler(jobmaster) -> worker -> in-proc operator chain -> result
```

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
./scripts/run_stream_observability_regression.sh
```

直接使用 Bazel suite：

```bash
bazel test //:core_regression
bazel test //:python_ecosystem_regression
bazel test //:experimental_regression
```

## 仓库规则

- 语言基线：`C++17`
- 构建系统：`Bazel`
- 对外 session 入口保持为 `DataflowSession`
- 不要破坏 `sql_demo / df_demo / stream_demo`
- 示例源码统一使用 `.cc`
- 本仓库中的 Python 命令统一使用 `uv`
- `README.md` 与 `README-zh.md` 必须保持同步
