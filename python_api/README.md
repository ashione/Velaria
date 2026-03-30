# Velaria Python API

Python API package for the Velaria dataflow engine.

Notes:

- Dependency management and demo execution use `uv`.
- The pure-Python wheel is built by Bazel target `//python_api:velaria_whl`.
- The native extension is built separately by Bazel target `//:velaria_pyext`.
- Bazel runtime loading uses `//python_api:velaria_py_pkg`, which packages the Python sources together with a package-local `_velaria.so`.
- `python_api/pyproject.toml` also declares `velaria/_velaria.so` as package data so setuptools/uv packaging will include the native module whenever that file is present in the package tree.
- Runtime loading prefers the package-local extension, then auto-discovers `bazel-bin/_velaria.so` in a source checkout.
- Version bumps should use `./scripts/bump_velaria_version.sh <version>`, which updates the Bazel version source, Python package version source, and refreshes `uv.lock`.

Quick start:

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python python3.12
uv run --project python_api python python_api/demo_batch_sql_arrow.py
uv run --project python_api python python_api/demo_stream_sql.py
```

Single-file CLI packaging (Python deps + native `_velaria.so`):

```bash
./scripts/build_py_cli_executable.sh
./dist/velaria-cli csv-sql --csv /path/to/input.csv --query "SELECT * FROM input_table LIMIT 5"
./dist/velaria-cli vector-search --csv /path/to/vectors.csv --vector-column embedding --query-vector "0.1,0.2,0.3" --metric cosine --top-k 5
```

Python Session API for local vector search:

```python
from velaria import Session

session = Session()
# assume a temp view named "vec_src" already exists
out = session.vector_search("vec_src", "embedding", [0.1, 0.2, 0.3], top_k=5, metric="dot")
print(out.to_rows())
print(session.explain_vector_search("vec_src", "embedding", [0.1, 0.2, 0.3], top_k=5, metric="dot"))
```

Current vector search scope is local exact scan only (`cosine`/`dot`/`l2`) on fixed-dimension float vectors.

Native binary CLI alternative (runtime does not require Python environment):

```bash
bazel build //:velaria_cli
./bazel-bin/velaria_cli --csv /path/to/input.csv --query "SELECT * FROM input_table LIMIT 5"
./bazel-bin/velaria_cli --csv /path/to/vectors.csv --vector-column embedding --query-vector "0.1,0.2,0.3" --metric l2 --top-k 5
```

## CI packaging

PR CI builds and uploads two native wheel variants:

- manylinux wheel from the Linux job
- macOS wheel from the macOS job

The Linux path uses `auditwheel repair` after building `//python_api:velaria_native_whl`. The macOS path uploads the Bazel-built native wheel directly.

Tag-based release publishing is separate:

- bump the package version with `./scripts/bump_velaria_version.sh <version>`
- create a matching Git tag such as `v0.1.1`
- the release workflow verifies the tag matches `velaria.__version__` and publishes Linux and macOS wheel assets


## v0.5 Python 用例与测试

- `python_api/demo_batch_sql_arrow.py`：Arrow batch + SQL 临时视图路径。
- `python_api/demo_stream_sql.py`：stream SQL + sink 路径。
- `python_api/bench_arrow_ingestion.py`：对比 table / `RecordBatchReader` / `__arrow_c_stream__` ingestion 路径。
- `bazel test //python_api:streaming_v05_test`：自动化覆盖 Arrow 输入、stream SQL 启动、progress 合同字段。
- `Session.explain_stream_sql(...)`：直接返回 `logical / physical / strategy` 三段 explain 文本。
- `bazel test //python_api:arrow_stream_ingestion_test`：自动化覆盖 `RecordBatchReader`、`__arrow_c_stream__` 和 stream batch 边界。

建议本地顺序：

```bash
bazel build //:velaria_pyext
bazel test //python_api:streaming_v05_test
bazel test //python_api:arrow_stream_ingestion_test
uv run --project python_api python python_api/demo_batch_sql_arrow.py
uv run --project python_api python python_api/demo_stream_sql.py
uv run --project python_api python python_api/bench_arrow_ingestion.py
```


## Custom Stream Source（Python）

现在 Python API 提供可复用的 custom stream source 适配：

- `CustomArrowStreamSource`：把 Python 行数据转换成 Arrow micro-batches。
- `Session.create_dataframe_from_arrow(...)` / `Session.create_stream_from_arrow(...)` 现在优先接受 `RecordBatchReader` 和实现 `__arrow_c_stream__` 的对象，再回退到 `Table / RecordBatch / batch sequence`。
- 默认 emit 策略：`1 秒` 或 `1024 行` 触发一次 batch（可配置）。
- `create_stream_from_custom_source(session, rows, ...)`：直接转换并调用 `session.create_stream_from_arrow(...)`。
- `CustomArrowStreamSink`：消费 Arrow micro-batches，并按“1秒或N条”聚合后触发 `on_emit` 回调。

示例：

```python
from velaria import (
    CustomArrowStreamSink,
    CustomArrowStreamSource,
    consume_arrow_batches_with_custom_sink,
)

source = CustomArrowStreamSource(emit_interval_seconds=1.0, emit_rows=500)
stream_df = source.to_stream_dataframe(session, rows_iterable)

sink = CustomArrowStreamSink(lambda table: print(table.num_rows), emit_interval_seconds=1.0, emit_rows=500)
consume_arrow_batches_with_custom_sink([stream_df_batch_1, stream_df_batch_2], sink)
```

测试：

```bash
bazel test //python_api:custom_stream_source_test
# 该测试同时覆盖 custom source 与 custom sink 的 emit 逻辑
```


`Session.start_stream_sql(...)` 额外支持 `checkpoint_delivery_mode` 参数：

- `at-least-once`（默认）
- `best-effort`

`Session.explain_stream_sql(...)` 复用同一组选项参数：

- `sql`
- `trigger_interval_ms`
- `checkpoint_path`
- `checkpoint_delivery_mode`

### XLSX 数据读取

仓库里已提供 `velaria.read_excel(...)` 直接读 `.xlsx`：

- 先调用 `pandas.read_excel`
- 再转成 `pyarrow.Table`
- 再通过 `Session.create_dataframe_from_arrow(...)` 变成 Velaria DataFrame

示例：

```python
from velaria import Session, read_excel

session = Session()
df = read_excel(session, "/path/to/file.xlsx", sheet_name="Sheet1")
session.create_temp_view("staff", df)
out = session.sql("SELECT * FROM staff LIMIT 5")
print(out.to_rows())
```

该能力依赖运行时安装 `pandas` 与 `openpyxl`（已作为 Python 包依赖）：

```bash
uv run python -c "import pandas, openpyxl"
```
