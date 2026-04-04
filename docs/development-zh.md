# 开发与验证

这份文档集中放置仓库内的开发、构建、smoke 和回归入口。
根 [README-zh.md](../README-zh.md) 只保留最小项目说明和公开入口。

## Python 生态工作流

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
uv run --project python_api python python_api/velaria_cli.py -i

uv run --project python_api python python_api/velaria_cli.py run start -- csv-sql \
  --run-name "score_demo" \
  --description "score filter result for demo input" \
  --tag demo \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python_api python python_api/velaria_cli.py run list --tag demo --query "score"
uv run --project python_api python python_api/velaria_cli.py run result --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py run diff --run-id <run_id> --other-run-id <other_run_id>
uv run --project python_api python python_api/velaria_cli.py run show --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python_api python python_api/velaria_cli.py artifacts preview --artifact-id <artifact_id>
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

`run_stream_observability_regression.sh` 会校验 stream 执行、actor strategy/explain 输出、actor RPC smoke，以及字符串 builtin benchmark case 的 JSON 基线。

直接使用 Bazel suite：

```bash
bazel test //:core_regression
bazel test //:python_ecosystem_regression
bazel test //:experimental_regression
```
