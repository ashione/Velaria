import importlib.util
import os
import pathlib
import subprocess
import sys


def _load_from_path(ext_path: pathlib.Path):
    spec = importlib.util.spec_from_file_location("velaria._velaria", ext_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load velaria extension from {ext_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _find_repo_root(start: pathlib.Path):
    for path in [start, *start.parents]:
        if (path / "MODULE.bazel").exists() or (path / "WORKSPACE").exists():
            return path
    return None


def _find_dev_extension():
    repo_root = _find_repo_root(pathlib.Path(__file__).resolve())
    if repo_root is None:
        return None

    direct = repo_root / "bazel-bin" / "_velaria.so"
    if direct.exists():
        return direct

    try:
        result = subprocess.run(
            ["bazel", "info", "bazel-bin"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None

    bazel_bin = pathlib.Path(result.stdout.strip())
    candidate = bazel_bin / "_velaria.so"
    return candidate if candidate.exists() else None


def _load_native():
    ext_path = os.environ.get("VELARIA_PYTHON_EXT")
    if ext_path:
        return _load_from_path(pathlib.Path(ext_path))

    try:
        from . import _velaria as module
        return module
    except ImportError as exc:
        candidate = _find_dev_extension()
        if candidate is not None:
            return _load_from_path(candidate)
        raise ImportError(
            "Velaria native extension was not found. Install the native wheel, or in a source "
            "checkout build //:velaria_pyext so the package can auto-discover bazel-bin/_velaria.so."
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
