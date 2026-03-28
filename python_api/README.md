# Velaria Python API

Python API package for the Velaria dataflow engine.

Notes:

- Dependency management and demo execution use `uv`.
- The pure-Python wheel is built by Bazel target `//python_api:velaria_whl`.
- The native extension is built separately by Bazel target `//:velaria_pyext`.
- Runtime loading prefers the package-local extension, then `bazel-bin/_velaria.so`, with `VELARIA_PYTHON_EXT` kept as an explicit override.

Quick start:

```bash
bazel build //:velaria_pyext
uv sync --project python_api --python python3.12
uv run --project python_api python python_api/demo_batch_sql_arrow.py
uv run --project python_api python python_api/demo_stream_sql.py
```
