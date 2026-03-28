import importlib.util
import os
import pathlib
import sys


def _load_native():
    ext_path = os.environ.get("VELARIA_PYTHON_EXT")
    if ext_path:
        spec = importlib.util.spec_from_file_location("velaria._velaria", ext_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load velaria extension from {ext_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    try:
        from . import _velaria as module
        return module
    except ImportError as exc:
        raise ImportError(
            "Velaria native extension is not bundled in the pure-Python wheel target yet. "
            "Build //:velaria_pyext and set VELARIA_PYTHON_EXT to the generated _velaria.so path."
        ) from exc


_native = _load_native()

Session = _native.Session
DataFrame = _native.DataFrame
StreamingDataFrame = _native.StreamingDataFrame
StreamingQuery = _native.StreamingQuery

__all__ = [
    "Session",
    "DataFrame",
    "StreamingDataFrame",
    "StreamingQuery",
]
