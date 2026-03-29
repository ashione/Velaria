import importlib.util
import pathlib
import subprocess
import sys

from .custom_stream import (
    CustomArrowStreamSink,
    CustomArrowStreamSource,
    CustomStreamEmitOptions,
    consume_arrow_batches_with_custom_sink,
    create_stream_from_custom_source,
)
from .excel import read_excel
from .bitable import BitableClient, group_rows_by_field, group_rows_count_by_field
from ._version import __version__


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


def _candidate_repo_roots():
    seen = set()
    starts = [pathlib.Path(__file__).resolve(), pathlib.Path.cwd().resolve()]
    for start in starts:
        repo_root = _find_repo_root(start)
        if repo_root is None:
            continue
        repo_key = str(repo_root)
        if repo_key in seen:
            continue
        seen.add(repo_key)
        yield repo_root


def _find_dev_extension():
    for repo_root in _candidate_repo_roots():
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
            continue

        bazel_bin = pathlib.Path(result.stdout.strip())
        candidate = bazel_bin / "_velaria.so"
        if candidate.exists():
            return candidate

    return None


def _load_native():
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


class _NativeUnavailable:
    def __init__(self, *args, **kwargs):
        raise ImportError(
            "Velaria native extension is required for Session/DataFrame/Streaming APIs. "
            "Install the native wheel or build //:velaria_pyext in the source checkout."
        )


try:
    _native = _load_native()
except ImportError:
    _native = None

Session = _native.Session if _native is not None else _NativeUnavailable
DataFrame = _native.DataFrame if _native is not None else _NativeUnavailable
StreamingDataFrame = _native.StreamingDataFrame if _native is not None else _NativeUnavailable
StreamingQuery = _native.StreamingQuery if _native is not None else _NativeUnavailable

__all__ = [
    "__version__",
    "Session",
    "DataFrame",
    "StreamingDataFrame",
    "StreamingQuery",
    "CustomArrowStreamSource",
    "CustomArrowStreamSink",
    "CustomStreamEmitOptions",
    "create_stream_from_custom_source",
    "consume_arrow_batches_with_custom_sink",
    "read_excel",
    "BitableClient",
    "group_rows_by_field",
    "group_rows_count_by_field",
]
