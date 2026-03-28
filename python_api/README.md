# Velaria Python API

Python API package for the Velaria dataflow engine.

Notes:

- The pure-Python wheel is built by Bazel target `//python_api:velaria_whl`.
- The native extension is built separately by Bazel target `//:velaria_pyext`.
- Runtime loading of the native extension currently uses `VELARIA_PYTHON_EXT`.
