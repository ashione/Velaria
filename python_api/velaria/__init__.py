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
from .embedding import (
    DEFAULT_LOCAL_EMBEDDING_MODEL_DIR,
    DEFAULT_LOCAL_EMBEDDING_MODEL,
    DEFAULT_EMBEDDING_WARMUP_TEXT,
    EMBEDDING_CACHE_DIR_ENV,
    EMBEDDING_MODEL_DIR_ENV,
    EmbeddingProvider,
    HashEmbeddingProvider,
    SentenceTransformerEmbeddingProvider,
    build_embedding_rows,
    build_file_embeddings,
    build_mixed_text_embedding_rows,
    default_embedding_model_dir,
    download_embedding_model,
    embed_query_text,
    is_embedding_model_ready,
    format_embedding_version,
    load_embedding_dataframe,
    materialize_embeddings,
    materialize_mixed_text_embeddings,
    query_file_embeddings,
    read_embedding_table,
    render_text_template,
    resolve_embedding_model_name,
    run_file_mixed_text_hybrid_search,
    run_mixed_text_hybrid_search,
    select_embedding_updates,
)
from .excel import read_excel
from .bitable import BitableClient, group_rows_by_field, group_rows_count_by_field
from ._version import __version__


def _is_frozen_runtime():
    return bool(getattr(sys, "frozen", False) or hasattr(sys, "_MEIPASS"))


def _native_missing_message():
    if _is_frozen_runtime():
        return (
            "Velaria desktop package is missing its native engine files. "
            "Please reinstall the app or download a newer release."
        )
    return (
        "Velaria native extension was not found. Install the native wheel, or in a source "
        "checkout build //:velaria_pyext so the package can auto-discover bazel-bin/_velaria.so."
    )


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
        if (path / "MODULE.bazel").exists():
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
        raise ImportError(_native_missing_message()) from exc


class _NativeUnavailable:
    def __init__(self, *args, **kwargs):
        if _is_frozen_runtime():
            raise ImportError(
                "Velaria desktop package is missing its native engine files. "
                "Please reinstall the app or download a newer release."
            )
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
    "EmbeddingProvider",
    "DEFAULT_LOCAL_EMBEDDING_MODEL",
    "DEFAULT_LOCAL_EMBEDDING_MODEL_DIR",
    "DEFAULT_EMBEDDING_WARMUP_TEXT",
    "EMBEDDING_CACHE_DIR_ENV",
    "EMBEDDING_MODEL_DIR_ENV",
    "HashEmbeddingProvider",
    "SentenceTransformerEmbeddingProvider",
    "build_embedding_rows",
    "build_file_embeddings",
    "build_mixed_text_embedding_rows",
    "default_embedding_model_dir",
    "download_embedding_model",
    "embed_query_text",
    "is_embedding_model_ready",
    "format_embedding_version",
    "load_embedding_dataframe",
    "materialize_embeddings",
    "materialize_mixed_text_embeddings",
    "query_file_embeddings",
    "read_embedding_table",
    "render_text_template",
    "resolve_embedding_model_name",
    "run_file_mixed_text_hybrid_search",
    "run_mixed_text_hybrid_search",
    "select_embedding_updates",
    "read_excel",
    "BitableClient",
    "group_rows_by_field",
    "group_rows_count_by_field",
]
