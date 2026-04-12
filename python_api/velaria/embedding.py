from __future__ import annotations

import abc
import hashlib
import importlib
import math
import os
import pathlib
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Sequence

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

from .excel import read_excel


_DEFAULT_TEMPLATE_FIELDS = ("title", "summary", "content", "body", "tags")
DEFAULT_LOCAL_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_LOCAL_EMBEDDING_MODEL_DIR = pathlib.Path(__file__).resolve().parents[1] / "models" / "all-MiniLM-L6-v2"
DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL = "BAAI/bge-small-zh-v1.5"
DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL_DIR = pathlib.Path(__file__).resolve().parents[1] / "models" / "BAAI--bge-small-zh-v1.5"
EMBEDDING_MODEL_DIR_ENV = "VELARIA_EMBEDDING_MODEL_DIR"
EMBEDDING_CACHE_DIR_ENV = "VELARIA_EMBEDDING_CACHE_DIR"
DEFAULT_EMBEDDING_WARMUP_TEXT = "warmup embedding text"


class EmbeddingProvider(abc.ABC):
    provider_name = "unknown"

    @abc.abstractmethod
    def embed(
        self,
        texts: Sequence[str],
        *,
        model: str,
        batch_size: int | None = None,
    ) -> list[list[float]]:
        raise NotImplementedError

    def warmup(
        self,
        *,
        model: str | pathlib.Path | None = None,
        batch_size: int | None = None,
        warmup_text: str = DEFAULT_EMBEDDING_WARMUP_TEXT,
    ) -> dict[str, Any]:
        vectors = self.embed([warmup_text], model=str(model or ""), batch_size=batch_size or 1)
        return {
            "provider": self.provider_name,
            "model": str(model or ""),
            "dimension": len(vectors[0]) if vectors else 0,
            "texts": 1,
        }


class HashEmbeddingProvider(EmbeddingProvider):
    provider_name = "hash"

    def __init__(self, dimension: int = 16):
        if dimension <= 0:
            raise ValueError("dimension must be positive")
        self.dimension = dimension

    def embed(
        self,
        texts: Sequence[str],
        *,
        model: str,
        batch_size: int | None = None,
    ) -> list[list[float]]:
        del model, batch_size
        out: list[list[float]] = []
        for text in texts:
            out.append(_hash_text_to_unit_vector(text, self.dimension))
        return out

    def warmup(
        self,
        *,
        model: str | pathlib.Path | None = None,
        batch_size: int | None = None,
        warmup_text: str = DEFAULT_EMBEDDING_WARMUP_TEXT,
    ) -> dict[str, Any]:
        del model, batch_size, warmup_text
        return {
            "provider": self.provider_name,
            "model": "hash-demo",
            "dimension": self.dimension,
            "texts": 0,
        }


class SentenceTransformerEmbeddingProvider(EmbeddingProvider):
    provider_name = "sentence-transformers"

    def __init__(
        self,
        model_name: str | pathlib.Path | None = None,
        *,
        normalize_embeddings: bool = True,
        device: str | None = None,
        cache_folder: str | pathlib.Path | None = None,
        default_batch_size: int = 32,
    ):
        if model_name is None:
            model_name = resolve_embedding_model_name(DEFAULT_LOCAL_EMBEDDING_MODEL)
        if not model_name:
            raise ValueError("model_name must not be empty")
        if default_batch_size <= 0:
            raise ValueError("default_batch_size must be positive")
        self.model_name = str(model_name)
        self.normalize_embeddings = normalize_embeddings
        self.device = device
        resolved_cache = cache_folder if cache_folder is not None else os.environ.get(EMBEDDING_CACHE_DIR_ENV)
        self.cache_folder = str(resolved_cache) if resolved_cache is not None else None
        self.default_batch_size = default_batch_size
        self._loaded_model_name: str | None = None
        self._model = None

    def _load_model(self, requested_model: str | pathlib.Path | None):
        model_name = str(resolve_embedding_model_name(requested_model or self.model_name))
        if self._model is not None and self._loaded_model_name == model_name:
            return self._model
        try:
            module = importlib.import_module("sentence_transformers")
        except ImportError as exc:
            raise ImportError(
                "sentence-transformers is required for local MiniLM embeddings. "
                "Enable the embedding extra with `uv sync --project python_api --extra embedding`."
            ) from exc

        kwargs: dict[str, Any] = {}
        if self.device is not None:
            kwargs["device"] = self.device
        if self.cache_folder is not None:
            kwargs["cache_folder"] = self.cache_folder
        self._model = module.SentenceTransformer(model_name, **kwargs)
        self._loaded_model_name = model_name
        return self._model

    def embed(
        self,
        texts: Sequence[str],
        *,
        model: str,
        batch_size: int | None = None,
    ) -> list[list[float]]:
        if not texts:
            return []
        encoder = self._load_model(model)
        encoded = encoder.encode(
            list(texts),
            batch_size=batch_size or self.default_batch_size,
            normalize_embeddings=self.normalize_embeddings,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        if hasattr(encoded, "tolist"):
            encoded = encoded.tolist()
        if encoded and encoded and isinstance(encoded[0], (int, float)):
            encoded = [encoded]
        return [[float(value) for value in row] for row in encoded]

    def warmup(
        self,
        *,
        model: str | pathlib.Path | None = None,
        batch_size: int | None = None,
        warmup_text: str = DEFAULT_EMBEDDING_WARMUP_TEXT,
        download_if_missing: bool = False,
        target_dir: str | pathlib.Path | None = None,
    ) -> dict[str, Any]:
        model_ref: str | pathlib.Path = model or self.model_name
        if download_if_missing:
            model_ref = download_embedding_model(model_name=model_ref, target_dir=target_dir)
        vectors = self.embed([warmup_text], model=str(model_ref), batch_size=batch_size or 1)
        return {
            "provider": self.provider_name,
            "model": str(model_ref),
            "dimension": len(vectors[0]) if vectors else 0,
            "texts": 1,
        }


def default_embedding_model_dir(model_name: str | pathlib.Path = DEFAULT_LOCAL_EMBEDDING_MODEL) -> pathlib.Path:
    if isinstance(model_name, pathlib.Path):
        return model_name
    if model_name == DEFAULT_LOCAL_EMBEDDING_MODEL:
        return DEFAULT_LOCAL_EMBEDDING_MODEL_DIR
    if model_name == DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL:
        return DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL_DIR
    return pathlib.Path(__file__).resolve().parents[1] / "models" / model_name.replace("/", "--")


def is_embedding_model_ready(path: str | pathlib.Path) -> bool:
    path = pathlib.Path(path)
    return (
        path.exists()
        and (path / "config.json").exists()
        and (path / "modules.json").exists()
        and (path / "tokenizer.json").exists()
    )


def download_embedding_model(
    model_name: str | pathlib.Path = DEFAULT_LOCAL_EMBEDDING_MODEL,
    *,
    target_dir: str | pathlib.Path | None = None,
) -> pathlib.Path:
    target = pathlib.Path(target_dir) if target_dir is not None else default_embedding_model_dir(model_name)
    if is_embedding_model_ready(target):
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        hub = importlib.import_module("huggingface_hub")
    except ImportError as exc:
        raise ImportError(
            "huggingface_hub is required to download local embedding models. "
            "Enable the embedding extra with `uv sync --project python_api --extra embedding`."
        ) from exc
    hub.snapshot_download(
        repo_id=str(model_name),
        local_dir=str(target),
    )
    return target


def resolve_embedding_model_name(model_name: str | pathlib.Path) -> str:
    if isinstance(model_name, pathlib.Path):
        return str(model_name)
    if model_name not in {DEFAULT_LOCAL_EMBEDDING_MODEL, DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL}:
        return model_name

    env_dir = os.environ.get(EMBEDDING_MODEL_DIR_ENV)
    if env_dir:
        return env_dir
    if model_name == DEFAULT_LOCAL_EMBEDDING_MODEL and DEFAULT_LOCAL_EMBEDDING_MODEL_DIR.exists():
        return str(DEFAULT_LOCAL_EMBEDDING_MODEL_DIR)
    if model_name == DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL and DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL_DIR.exists():
        return str(DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL_DIR)
    return model_name


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, pathlib.Path):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, (list, tuple)):
        return ", ".join(str(item).strip() for item in value if str(item).strip())
    return value


def _stringify_template_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return ", ".join(parts)
    return str(value).strip()


def render_text_template(
    record: Mapping[str, Any],
    *,
    text_field: str = "text",
    template_fields: Sequence[str] = _DEFAULT_TEMPLATE_FIELDS,
) -> str:
    direct_text = _stringify_template_value(record.get(text_field))
    if direct_text:
        return direct_text

    parts: list[str] = []
    for field in template_fields:
        value = _stringify_template_value(record.get(field))
        if value:
            parts.append(f"{field}: {value}")
    if not parts:
        ignored_fields = {
            text_field,
            "doc_id",
            "source_updated_at",
            "embedding",
            "embedding_version",
            "text_template_version",
            "provider_name",
            "model_name",
            "dimension",
            "embedded_at",
        }
        for field, raw_value in record.items():
            if field in ignored_fields:
                continue
            value = _stringify_template_value(raw_value)
            if value:
                parts.append(f"{field}: {value}")
    if not parts:
        raise ValueError("record does not contain usable text fields")
    return "\n".join(parts)


def format_embedding_version(
    *,
    template_version: str,
    model_name: str,
    dimension: int,
) -> str:
    return f"{template_version}__{model_name}__dim-{dimension}"


def build_embedding_rows(
    records: Iterable[Mapping[str, Any]],
    *,
    template_version: str,
    doc_id_field: str = "doc_id",
    source_updated_at_field: str = "source_updated_at",
    text_builder: Callable[[Mapping[str, Any]], str] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for index, record in enumerate(records, start=1):
        row = {key: _normalize_scalar(value) for key, value in dict(record).items()}
        if doc_id_field not in row:
            row[doc_id_field] = f"doc-{index}"
        text = text_builder(record) if text_builder is not None else render_text_template(record)
        if not text.strip():
            raise ValueError("embedding text must not be empty")

        doc_id_value = row.pop(doc_id_field)
        source_updated_at = row.pop(source_updated_at_field, None)
        built: dict[str, Any] = dict(row)
        built["doc_id"] = doc_id_value
        built["text"] = text
        built["text_template_version"] = template_version
        built["source_updated_at"] = source_updated_at
        out.append(built)
    return out


def build_mixed_text_embedding_rows(
    records: Iterable[Mapping[str, Any]],
    *,
    template_version: str,
    text_fields: Sequence[str] = _DEFAULT_TEMPLATE_FIELDS,
    text_field: str = "text",
    doc_id_field: str = "doc_id",
    source_updated_at_field: str = "source_updated_at",
) -> list[dict[str, Any]]:
    return build_embedding_rows(
        records,
        template_version=template_version,
        doc_id_field=doc_id_field,
        source_updated_at_field=source_updated_at_field,
        text_builder=lambda record: render_text_template(
            record,
            text_field=text_field,
            template_fields=text_fields,
        ),
    )


def select_embedding_updates(
    records: Iterable[Mapping[str, Any]],
    existing_embeddings: pa.Table | Iterable[Mapping[str, Any]],
    *,
    embedding_version: str,
    doc_id_field: str = "doc_id",
    source_updated_at_field: str = "source_updated_at",
) -> list[dict[str, Any]]:
    existing_rows = existing_embeddings.to_pylist() if isinstance(existing_embeddings, pa.Table) else list(existing_embeddings)
    existing_index = {row["doc_id"]: row for row in existing_rows if "doc_id" in row}

    out: list[dict[str, Any]] = []
    for record in records:
        normalized = dict(record)
        if doc_id_field not in normalized:
            raise ValueError(f"record missing doc id field: {doc_id_field}")
        doc_id = normalized[doc_id_field]
        source_updated_at = normalized.get(source_updated_at_field)
        current = existing_index.get(doc_id)
        if current is None:
            out.append(normalized)
            continue
        if current.get("embedding_version") != embedding_version:
            out.append(normalized)
            continue
        if current.get("source_updated_at") != _normalize_scalar(source_updated_at):
            out.append(normalized)
            continue
    return out


def embed_query_text(
    text: str,
    *,
    provider: EmbeddingProvider,
    model: str,
) -> list[float]:
    vectors = provider.embed([text], model=model, batch_size=1)
    if len(vectors) != 1:
        raise ValueError("provider returned unexpected query embedding batch size")
    return vectors[0]


def materialize_mixed_text_embeddings(
    records: Iterable[Mapping[str, Any]],
    *,
    provider: EmbeddingProvider,
    model: str,
    template_version: str,
    text_fields: Sequence[str] = _DEFAULT_TEMPLATE_FIELDS,
    text_field: str = "text",
    doc_id_field: str = "doc_id",
    source_updated_at_field: str = "source_updated_at",
    output_path: str | pathlib.Path | None = None,
    embedding_version: str | None = None,
    batch_size: int = 128,
    embedded_at: str | None = None,
    include_text: bool = False,
) -> pa.Table:
    rows = build_mixed_text_embedding_rows(
        records,
        template_version=template_version,
        text_fields=text_fields,
        text_field=text_field,
        doc_id_field=doc_id_field,
        source_updated_at_field=source_updated_at_field,
    )
    return materialize_embeddings(
        rows,
        provider=provider,
        model=model,
        output_path=output_path,
        embedding_version=embedding_version,
        batch_size=batch_size,
        embedded_at=embedded_at,
        include_text=include_text,
    )


def _load_input_dataframe(
    session: Any,
    input_path: str | pathlib.Path,
    *,
    input_type: str = "auto",
    delimiter: str = ",",
    json_columns: Sequence[str] | None = None,
    json_format: str = "json_lines",
    mappings: str | Sequence[str] | Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]] | None = None,
    line_mode: str = "split",
    regex_pattern: str | None = None,
    sheet_name: Any = 0,
    date_format: str = "%Y-%m-%d",
) -> Any:
    source_path = pathlib.Path(input_path)
    path = str(source_path)
    suffix = source_path.suffix.lower()

    def load_arrow_table() -> pa.Table:
        if suffix in {".parquet", ".pq"}:
            return pq.read_table(source_path)
        if suffix in {".arrow", ".ipc", ".feather"}:
            with source_path.open("rb") as handle:
                try:
                    return pa_ipc.open_file(handle).read_all()
                except Exception:
                    handle.seek(0)
                    return pa_ipc.open_stream(handle).read_all()
        raise ValueError(f"unsupported embedding dataset format: {source_path}")

    def parse_mappings(
        raw: str | Sequence[str] | Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]] | None,
    ) -> list[tuple[str, int]]:
        if raw is None:
            return []
        items: Sequence[Any]
        if isinstance(raw, str):
            items = [piece.strip() for piece in raw.split(",") if piece.strip()]
        else:
            items = list(raw)
        parsed: list[tuple[str, int]] = []
        for item in items:
            if isinstance(item, str):
                if ":" not in item:
                    raise ValueError(f"invalid mappings entry: {item}")
                name, index_text = item.split(":", 1)
                parsed.append((name.strip(), int(index_text.strip())))
                continue
            if isinstance(item, Mapping):
                name = item.get("name") or item.get("column")
                index_value = item.get("index")
                if name is None or index_value is None:
                    raise ValueError(f"invalid mappings entry: {item}")
                parsed.append((str(name).strip(), int(index_value)))
                continue
            if isinstance(item, Sequence) and not isinstance(item, (bytes, bytearray)) and len(item) == 2:
                parsed.append((str(item[0]).strip(), int(item[1])))
                continue
            raise ValueError(f"invalid mappings entry: {item}")
        return parsed

    if input_type == "auto":
        if suffix in {".xlsx", ".xlsm", ".xls"}:
            return read_excel(session, path, sheet_name=sheet_name, date_format=date_format)
        if suffix in {".parquet", ".pq", ".arrow", ".ipc", ".feather"}:
            return session.create_dataframe_from_arrow(load_arrow_table())
        return session.read(path)
    if input_type == "csv":
        return session.read_csv(path, delimiter=delimiter)
    if input_type == "json":
        kwargs: dict[str, Any] = {"format": json_format}
        if json_columns:
            kwargs["columns"] = list(json_columns)
        return session.read_json(path, **kwargs)
    if input_type == "excel":
        return read_excel(session, path, sheet_name=sheet_name, date_format=date_format)
    if input_type in {"parquet", "arrow"}:
        return session.create_dataframe_from_arrow(load_arrow_table())
    if input_type == "line":
        parsed_mappings = parse_mappings(mappings)
        if not parsed_mappings:
            raise ValueError("line input requires mappings")
        kwargs: dict[str, Any] = {"mappings": parsed_mappings}
        if line_mode == "regex":
            kwargs["mode"] = "regex"
            kwargs["regex_pattern"] = regex_pattern or ""
        else:
            kwargs["split_delimiter"] = delimiter
        return session.read_line_file(path, **kwargs)
    raise ValueError(f"unsupported input_type: {input_type}")


def build_file_embeddings(
    session: Any,
    input_path: str | pathlib.Path,
    *,
    provider: EmbeddingProvider,
    model: str,
    template_version: str,
    text_columns: Sequence[str] = _DEFAULT_TEMPLATE_FIELDS,
    input_type: str = "auto",
    delimiter: str = ",",
    json_columns: Sequence[str] | None = None,
    json_format: str = "json_lines",
    mappings: str | Sequence[str] | Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]] | None = None,
    line_mode: str = "split",
    regex_pattern: str | None = None,
    sheet_name: Any = 0,
    date_format: str = "%Y-%m-%d",
    doc_id_field: str = "doc_id",
    source_updated_at_field: str = "source_updated_at",
    output_path: str | pathlib.Path | None = None,
    embedding_version: str | None = None,
    batch_size: int = 128,
    embedded_at: str | None = None,
    include_text: bool = False,
) -> pa.Table:
    source_df = _load_input_dataframe(
        session,
        input_path,
        input_type=input_type,
        delimiter=delimiter,
        json_columns=json_columns,
        json_format=json_format,
        mappings=mappings,
        line_mode=line_mode,
        regex_pattern=regex_pattern,
        sheet_name=sheet_name,
        date_format=date_format,
    )
    source_table = source_df.to_arrow()
    return materialize_mixed_text_embeddings(
        source_table.to_pylist(),
        provider=provider,
        model=model,
        template_version=template_version,
        text_fields=text_columns,
        doc_id_field=doc_id_field,
        source_updated_at_field=source_updated_at_field,
        output_path=output_path,
        embedding_version=embedding_version,
        batch_size=batch_size,
        embedded_at=embedded_at,
        include_text=include_text,
    )


def materialize_embeddings(
    rows: Sequence[Mapping[str, Any]],
    *,
    provider: EmbeddingProvider,
    model: str,
    output_path: str | pathlib.Path | None = None,
    embedding_version: str | None = None,
    batch_size: int = 128,
    embedded_at: str | None = None,
    include_text: bool = False,
) -> pa.Table:
    if not rows:
        raise ValueError("rows must not be empty")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    normalized_rows = [dict(row) for row in rows]
    template_versions = {row.get("text_template_version") for row in normalized_rows}
    if len(template_versions) != 1 or None in template_versions:
        raise ValueError("rows must share one non-empty text_template_version")
    template_version = next(iter(template_versions))

    all_texts = []
    for row in normalized_rows:
        text = row.get("text")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("each row must contain non-empty text")
        all_texts.append(text)

    vectors: list[list[float]] = []
    dimension: int | None = None
    for offset in range(0, len(all_texts), batch_size):
        chunk_texts = all_texts[offset : offset + batch_size]
        chunk_vectors = provider.embed(chunk_texts, model=model, batch_size=batch_size)
        if len(chunk_vectors) != len(chunk_texts):
            raise ValueError("provider returned vector count different from input text count")
        for vector in chunk_vectors:
            if not vector:
                raise ValueError("embedding vector must not be empty")
            if dimension is None:
                dimension = len(vector)
            elif len(vector) != dimension:
                raise ValueError("provider returned inconsistent embedding dimensions")
        vectors.extend(chunk_vectors)

    assert dimension is not None
    version = embedding_version or format_embedding_version(
        template_version=template_version,
        model_name=model,
        dimension=dimension,
    )
    embedded_at_value = embedded_at or _utc_now()

    materialized_rows: list[dict[str, Any]] = []
    for row, vector in zip(normalized_rows, vectors):
        item = {
            key: _normalize_scalar(value)
            for key, value in row.items()
            if include_text or key != "text"
        }
        item["embedding_version"] = version
        item["provider_name"] = provider.provider_name
        item["model_name"] = model
        item["dimension"] = dimension
        item["embedded_at"] = embedded_at_value
        materialized_rows.append(item)

    table = _table_from_embedding_rows(materialized_rows, vectors, dimension)
    if output_path is not None:
        _write_embedding_table(table, pathlib.Path(output_path))
    return table


def read_embedding_table(path: str | pathlib.Path) -> pa.Table:
    path = pathlib.Path(path)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pq.read_table(path)
    if suffix in {".arrow", ".ipc"}:
        with pa.memory_map(path, "r") as source:
            return pa_ipc.RecordBatchFileReader(source).read_all()
    raise ValueError(f"unsupported embedding dataset format: {path}")


def load_embedding_dataframe(session: Any, path: str | pathlib.Path) -> Any:
    return session.create_dataframe_from_arrow(read_embedding_table(path))


def _load_embedding_dataset(session: Any, dataset: Any) -> Any:
    if isinstance(dataset, (str, pathlib.Path)):
        return load_embedding_dataframe(session, dataset)
    if isinstance(dataset, pa.Table):
        return session.create_dataframe_from_arrow(dataset)
    return dataset


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def _split_where_sql_clauses(where_sql: str) -> list[str]:
    clauses: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    i = 0
    while i < len(where_sql):
        ch = where_sql[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            current.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            current.append(ch)
            i += 1
            continue
        if not in_single and not in_double and where_sql[i : i + 5].upper() == " AND ":
            clause = "".join(current).strip()
            if clause:
                clauses.append(clause)
            current = []
            i += 5
            continue
        current.append(ch)
        i += 1
    tail = "".join(current).strip()
    if tail:
        clauses.append(tail)
    return clauses


def run_mixed_text_hybrid_search(
    session: Any,
    dataset: Any,
    *,
    provider: EmbeddingProvider,
    model: str,
    query_text: str,
    table_name: str = "embedding_search_input",
    vector_column: str = "embedding",
    top_k: int = 10,
    metric: str = "cosine",
    where_sql: str | None = None,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: Any = None,
) -> Any:
    df = _load_embedding_dataset(session, dataset)
    session.create_temp_view(table_name, df)
    if where_sql is not None:
        clauses = _split_where_sql_clauses(where_sql)
        if not clauses:
            raise ValueError("where_sql must not be empty")
        for index, clause in enumerate(clauses):
            df = session.sql(f"SELECT * FROM {table_name} WHERE {clause}")
            table_name = f"{table_name}_filtered_{index + 1}"
            session.create_temp_view(table_name, df)
    elif where_column is not None or where_op is not None or where_value is not None:
        if where_column is None or where_op is None:
            raise ValueError("where_column and where_op must be provided together")
        df = session.sql(
            f"SELECT * FROM {table_name} WHERE {where_column} {where_op} {_sql_literal(where_value)}"
        )
        table_name = f"{table_name}_filtered"
        session.create_temp_view(table_name, df)

    query_vector = embed_query_text(query_text, provider=provider, model=model)
    return session.hybrid_search(
        table=table_name,
        vector_column=vector_column,
        query_vector=query_vector,
        top_k=top_k,
        metric=metric,
    )


def query_file_embeddings(
    session: Any,
    dataset: Any,
    *,
    provider: EmbeddingProvider,
    model: str,
    query_text: str,
    table_name: str = "embedding_query_input",
    vector_column: str = "embedding",
    top_k: int = 10,
    metric: str = "cosine",
    where_sql: str | None = None,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: Any = None,
) -> Any:
    return run_mixed_text_hybrid_search(
        session,
        dataset,
        provider=provider,
        model=model,
        query_text=query_text,
        table_name=table_name,
        vector_column=vector_column,
        top_k=top_k,
        metric=metric,
        where_sql=where_sql,
        where_column=where_column,
        where_op=where_op,
        where_value=where_value,
    )


def run_file_mixed_text_hybrid_search(
    session: Any,
    input_path: str | pathlib.Path,
    *,
    provider: EmbeddingProvider,
    model: str,
    query_text: str,
    template_version: str,
    text_columns: Sequence[str] = _DEFAULT_TEMPLATE_FIELDS,
    input_type: str = "auto",
    delimiter: str = ",",
    json_columns: Sequence[str] | None = None,
    json_format: str = "json_lines",
    mappings: str | Sequence[str] | Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]] | None = None,
    line_mode: str = "split",
    regex_pattern: str | None = None,
    sheet_name: Any = 0,
    date_format: str = "%Y-%m-%d",
    doc_id_field: str = "doc_id",
    source_updated_at_field: str = "source_updated_at",
    output_path: str | pathlib.Path | None = None,
    embedding_version: str | None = None,
    batch_size: int = 128,
    embedded_at: str | None = None,
    include_text: bool = False,
    table_name: str = "embedding_query_input",
    vector_column: str = "embedding",
    top_k: int = 10,
    metric: str = "cosine",
    where_sql: str | None = None,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: Any = None,
) -> Any:
    table = build_file_embeddings(
        session,
        input_path,
        provider=provider,
        model=model,
        template_version=template_version,
        text_columns=text_columns,
        input_type=input_type,
        delimiter=delimiter,
        json_columns=json_columns,
        json_format=json_format,
        mappings=mappings,
        line_mode=line_mode,
        regex_pattern=regex_pattern,
        sheet_name=sheet_name,
        date_format=date_format,
        doc_id_field=doc_id_field,
        source_updated_at_field=source_updated_at_field,
        output_path=output_path,
        embedding_version=embedding_version,
        batch_size=batch_size,
        embedded_at=embedded_at,
        include_text=include_text,
    )
    return query_file_embeddings(
        session,
        table,
        provider=provider,
        model=model,
        query_text=query_text,
        table_name=table_name,
        vector_column=vector_column,
        top_k=top_k,
        metric=metric,
        where_sql=where_sql,
        where_column=where_column,
        where_op=where_op,
        where_value=where_value,
    )


def _table_from_embedding_rows(
    rows: Sequence[Mapping[str, Any]],
    vectors: Sequence[Sequence[float]],
    dimension: int,
) -> pa.Table:
    if len(rows) != len(vectors):
        raise ValueError("row and vector count mismatch")

    scalar_columns: dict[str, list[Any]] = {}
    keys: list[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                keys.append(key)
                seen.add(key)
    for key in keys:
        scalar_columns[key] = [row.get(key) for row in rows]

    flat_values = [float(item) for vector in vectors for item in vector]
    embedding_array = pa.FixedSizeListArray.from_arrays(
        pa.array(flat_values, type=pa.float32()),
        dimension,
    )
    scalar_columns["embedding"] = embedding_array
    return pa.table(scalar_columns)


def _write_embedding_table(table: pa.Table, path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        pq.write_table(table, path)
        return
    if suffix in {".arrow", ".ipc"}:
        with pa.OSFile(path, "wb") as sink:
            writer = pa_ipc.new_file(sink, table.schema)
            writer.write_table(table)
            writer.close()
        return
    raise ValueError(f"unsupported embedding dataset format: {path}")


def _hash_text_to_unit_vector(text: str, dimension: int) -> list[float]:
    values = []
    counter = 0
    while len(values) < dimension:
        digest = hashlib.sha256(f"{text}\n{counter}".encode("utf-8")).digest()
        counter += 1
        for offset in range(0, len(digest), 4):
            chunk = digest[offset : offset + 4]
            if len(chunk) < 4:
                continue
            raw = int.from_bytes(chunk, "little", signed=False)
            values.append((raw / 0xFFFFFFFF) * 2.0 - 1.0)
            if len(values) == dimension:
                break
    norm = math.sqrt(sum(value * value for value in values))
    if norm == 0.0:
        return [0.0 for _ in values]
    return [float(value / norm) for value in values]
