from __future__ import annotations

import threading
import traceback
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import velaria.cli as cli_impl
from velaria import (
    BitableClient,
    Session,
    __version__,
    annotate_source_arrow_table,
    stream_mixed_text_embeddings_to_parquet,
)
from velaria.workspace import (
    ArtifactIndex,
    append_stderr,
    create_run,
    finalize_run,
    read_run,
    write_inputs,
)

from ._helpers import (
    _BITABLE_PAGE_SIZE,
    _BITABLE_TIMEOUT_SECONDS,
    _default_bitable_dataset_name,
    _default_embedding_model,
    _default_embedding_provider,
    _enrich_run,
    _execute_keyword_index_build,
    _load_dataframe,
    _normalize_bitable_rows,
    _normalize_bitable_value,
    _normalize_path,
    _parse_csv_list,
    _parse_string_list,
    _preview_from_dataframe,
    _register_artifacts,
    _resolve_auto_input_payload,
    _resolve_bitable_credentials,
    _timestamp_suffix,
    _update_run_progress,
)


def _preview_bitable_import(payload: dict[str, Any]) -> dict[str, Any]:
    import velaria_service as _pkg
    bitable_url = str(payload.get("bitable_url") or payload.get("input_path") or "").strip()
    if not bitable_url:
        raise ValueError("bitable_url is required")
    app_id, app_secret = _resolve_bitable_credentials(payload)
    client = _pkg.BitableClient(
        app_id=app_id,
        app_secret=app_secret,
        request_timeout_seconds=int(payload.get("timeout_seconds", _BITABLE_TIMEOUT_SECONDS)),
    )
    preview_limit = int(payload.get("limit", payload.get("preview_limit", 100)))
    rows = client.list_records_from_url(
        bitable_url,
        page_size=min(max(preview_limit, 1), 100),
        max_rows=preview_limit,
    )
    normalized_rows = _normalize_bitable_rows(rows)
    table = pa.Table.from_pylist(normalized_rows)
    preview = cli_impl._preview_payload_from_table(table, limit=preview_limit)
    preview["schema"] = table.schema.names
    preview["row_count"] = table.num_rows
    dataset_name = str(payload.get("dataset_name") or "").strip() or _default_bitable_dataset_name(bitable_url)
    return {
        "dataset": {
            "name": dataset_name,
            "source_type": "bitable",
            "source_path": bitable_url,
            "source_label": bitable_url,
        },
        "preview": preview,
    }


def _register_artifacts_preview_table(path: Path, table: pa.Table) -> dict[str, Any]:
    return cli_impl._table_artifact(path, table, ["dataset", "bitable-import", "result"])


def _execute_bitable_import(payload: dict[str, Any], run_dir: Path, *, run_id: str | None = None) -> dict[str, Any]:
    import velaria_service as _pkg
    bitable_url = str(payload.get("bitable_url") or payload.get("input_path") or "").strip()
    if not bitable_url:
        raise ValueError("bitable_url is required")

    app_id, app_secret = _resolve_bitable_credentials(payload)

    client = _pkg.BitableClient(
        app_id=app_id,
        app_secret=app_secret,
        request_timeout_seconds=int(payload.get("timeout_seconds", _BITABLE_TIMEOUT_SECONDS)),
    )
    fetched_pages = 0

    def _on_page(fetched_rows: int, page_rows: int) -> None:
        nonlocal fetched_pages
        fetched_pages += 1
        if run_id:
            _update_run_progress(
                run_id,
                progress_phase="fetching_bitable",
                fetched_rows=fetched_rows,
                pages_fetched=fetched_pages,
                last_page_rows=page_rows,
            )

    output_path = (
        _normalize_path(payload["output_path"])
        if payload.get("output_path")
        else run_dir / "artifacts" / "bitable_dataset.parquet"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    preview_limit = int(payload.get("preview_limit", 50))
    preview_rows: list[dict[str, Any]] = []
    schema: pa.Schema | None = None
    writer: pq.ParquetWriter | None = None
    total_rows = 0
    try:
        try:
            page_iter = client.iter_record_pages_from_url(
                bitable_url,
                page_size=int(payload.get("page_size", _BITABLE_PAGE_SIZE)),
                on_page=_on_page,
            )
            iterator = iter(page_iter)
        except TypeError:
            rows = client.list_records_from_url(
                bitable_url,
                page_size=int(payload.get("page_size", _BITABLE_PAGE_SIZE)),
                on_page=_on_page,
            )
            iterator = iter([rows])

        for page_rows in iterator:
            normalized_rows = _normalize_bitable_rows(page_rows)
            page_table = pa.Table.from_pylist(normalized_rows)
            if writer is None:
                writer = pq.ParquetWriter(str(output_path), page_table.schema)
                schema = page_table.schema
            writer.write_table(page_table)
            total_rows += page_table.num_rows
            if len(preview_rows) < preview_limit:
                remaining = preview_limit - len(preview_rows)
                preview_rows.extend(normalized_rows[:remaining])
    finally:
        if writer is not None:
            writer.close()

    if schema is None:
        raise ValueError("bitable import returned no rows")

    preview_table = pa.Table.from_pylist(preview_rows, schema=schema) if preview_rows else pa.Table.from_arrays([], schema=schema)

    preview = cli_impl._preview_payload_from_table(preview_table, limit=preview_limit)
    preview["schema"] = schema.names
    preview["row_count"] = total_rows
    dataset_name = str(payload.get("dataset_name") or "").strip() or _default_bitable_dataset_name(bitable_url)
    artifacts = [{
        "type": "file",
        "uri": cli_impl._uri_from_path(output_path),
        "format": "parquet",
        "row_count": total_rows,
        "schema_json": schema.names,
        "preview_json": preview,
        "tags_json": ["dataset", "bitable-import", "result"],
    }]
    result_payload: dict[str, Any] = {
        "dataset_name": dataset_name,
        "source_type": "bitable",
        "source_path": str(output_path),
        "source_label": bitable_url,
        "row_count": total_rows,
        "schema": schema.names,
        "imported_at": _timestamp_suffix(),
        "fetched_rows": total_rows,
        "pages_fetched": fetched_pages,
    }

    raw_keyword_index_config = payload.get("keyword_index_config")
    keyword_index_config = raw_keyword_index_config if isinstance(raw_keyword_index_config, dict) else {}

    worker_errors: list[BaseException] = []
    worker_lock = threading.Lock()
    embedding_result: dict[str, Any] | None = None
    keyword_result: dict[str, Any] | None = None

    raw_embedding_config = payload.get("embedding_config")
    embedding_config = raw_embedding_config if isinstance(raw_embedding_config, dict) else {}

    def record_worker_error(exc: BaseException) -> None:
        with worker_lock:
            worker_errors.append(exc)

    def run_embedding() -> None:
        nonlocal embedding_result
        try:
            text_columns = _parse_string_list(embedding_config.get("text_columns"))
            if not text_columns:
                raise ValueError("embedding_config.text_columns must not be empty")

            provider_name = str(embedding_config.get("provider") or _default_embedding_provider())
            resolved_model = _default_embedding_model(provider_name, embedding_config.get("model"))
            provider, resolved_model = cli_impl._make_embedding_provider(provider_name, resolved_model)
            template_version = str(embedding_config.get("template_version") or "text-v1")
            vector_column = str(embedding_config.get("vector_column") or "embedding")
            if vector_column != "embedding":
                raise ValueError("bitable import currently materializes vectors into the 'embedding' column only")
            embedding_batch_size = int(embedding_config.get("batch_size", 128))
            embedding_output_path = (
                _normalize_path(embedding_config["output_path"])
                if embedding_config.get("output_path")
                else run_dir / "artifacts" / "bitable_embedding_dataset.parquet"
            )
            if run_id:
                _update_run_progress(
                    run_id,
                    progress_phase="building_embedding",
                    embedding_provider=provider_name,
                    embedding_model=resolved_model,
                    embedding_batch_size=embedding_batch_size,
                )

            def _on_batch(total_rows: int, batch_rows: int) -> None:
                if run_id:
                    _update_run_progress(
                        run_id,
                        progress_phase="building_embedding",
                        embedding_rows=total_rows,
                        embedding_last_batch_rows=batch_rows,
                    )

            embedding_source_table = annotate_source_arrow_table(pq.read_table(str(output_path)), source_label=output_path.stem)
            embedding_meta = stream_mixed_text_embeddings_to_parquet(
                embedding_source_table,
                provider=provider,
                model=resolved_model,
                template_version=template_version,
                output_path=embedding_output_path,
                text_fields=text_columns,
                batch_size=embedding_batch_size,
                preview_limit=int(payload.get("preview_limit", 50)),
                on_batch=_on_batch,
            )
            embedding_result = {
                "artifact": {
                    "type": "file",
                    "uri": cli_impl._uri_from_path(Path(embedding_meta["output_path"])),
                    "format": "parquet",
                    "row_count": embedding_meta["row_count"],
                    "schema_json": embedding_meta["schema"],
                    "preview_json": cli_impl._preview_payload_from_table(
                        embedding_meta["preview_table"],
                        limit=int(payload.get("preview_limit", 50)),
                    ),
                    "tags_json": ["dataset", "bitable-import", "embedding-build"],
                },
                "payload": {
                    "dataset_path": embedding_meta["output_path"],
                    "schema": embedding_meta["schema"],
                    "row_count": embedding_meta["row_count"],
                    "provider": provider_name,
                    "model": resolved_model,
                    "template_version": template_version,
                    "vector_column": vector_column,
                    "text_columns": text_columns,
                },
            }
            if run_id:
                _update_run_progress(
                    run_id,
                    progress_phase="embedding_ready",
                    embedding_rows=embedding_meta["row_count"],
                )
        except BaseException as exc:  # noqa: BLE001
            record_worker_error(exc)

    def run_keyword_index() -> None:
        nonlocal keyword_result
        try:
            text_columns = _parse_string_list(keyword_index_config.get("text_columns"))
            if not text_columns:
                raise ValueError("keyword_index_config.text_columns must not be empty")
            analyzer = str(keyword_index_config.get("analyzer") or "jieba")
            keyword_output_dir = (
                _normalize_path(keyword_index_config["output_path"])
                if keyword_index_config.get("output_path")
                else run_dir / "artifacts" / "bitable_keyword_index"
            )
            keyword_result = _execute_keyword_index_build(
                {
                    "dataset_paths": [str(output_path)],
                    "text_columns": text_columns,
                    "output_path": str(keyword_output_dir),
                    "analyzer": analyzer,
                    "preview_limit": int(keyword_index_config.get("preview_limit", payload.get("preview_limit", 50))),
                },
                run_dir,
                run_id=run_id,
            )
        except BaseException as exc:  # noqa: BLE001
            record_worker_error(exc)

    workers: list[threading.Thread] = []
    if embedding_config.get("enabled"):
        workers.append(threading.Thread(target=run_embedding, daemon=True))
    if keyword_index_config.get("enabled"):
        workers.append(threading.Thread(target=run_keyword_index, daemon=True))
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    if worker_errors:
        raise worker_errors[0]

    if embedding_result is not None:
        artifacts.append(embedding_result["artifact"])
        result_payload["embedding_dataset"] = embedding_result["payload"]

    if keyword_result is not None:
        artifacts.extend(keyword_result.get("artifacts", []))
        result_payload["keyword_index"] = {
            "index_path": keyword_result["payload"]["index_path"],
            "dataset_count": keyword_result["payload"]["dataset_count"],
            "text_columns": keyword_result["payload"]["text_columns"],
            "analyzer": keyword_result["payload"]["analyzer"],
            "doc_count": keyword_result["payload"]["doc_count"],
            "term_count": keyword_result["payload"]["term_count"],
            "posting_count": keyword_result["payload"]["posting_count"],
            "schema": keyword_result["payload"]["schema"],
        }

    return {
        "payload": result_payload,
        "preview": preview,
        "artifacts": artifacts,
    }


def _finalize_bitable_import_run(
    *,
    run_id: str,
    run_dir: Path,
    action_args: dict[str, Any],
) -> None:
    index = ArtifactIndex()
    try:
        index.upsert_run(read_run(run_id))
        result = _execute_bitable_import(action_args, run_dir, run_id=run_id)
        created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
        result_payload = result.get("payload") or {}
        if created_artifacts:
            result_payload = {
                **result_payload,
                "artifact_uri": created_artifacts[0].get("uri", ""),
                "artifact_id": created_artifacts[0].get("artifact_id", ""),
            }
            embedding_dataset = result_payload.get("embedding_dataset")
            if isinstance(embedding_dataset, dict):
                for artifact in created_artifacts:
                    tags = artifact.get("tags_json") or []
                    if "embedding-build" not in tags:
                        continue
                    result_payload["embedding_dataset"] = {
                        **embedding_dataset,
                        "artifact_uri": artifact.get("uri", ""),
                        "artifact_id": artifact.get("artifact_id", ""),
                        "run_id": run_id,
                    }
                    break
            keyword_index = result_payload.get("keyword_index")
            if isinstance(keyword_index, dict):
                for artifact in created_artifacts:
                    tags = artifact.get("tags_json") or []
                    if "keyword-index-build" not in tags:
                        continue
                    result_payload["keyword_index"] = {
                        **keyword_index,
                        "artifact_uri": artifact.get("uri", ""),
                        "artifact_id": artifact.get("artifact_id", ""),
                        "run_id": run_id,
                    }
                    break
        finalized = finalize_run(run_id, "succeeded", details=result_payload)
        index.upsert_run(finalized)
    except Exception as exc:
        append_stderr(run_id, traceback.format_exc())
        finalized = finalize_run(
            run_id,
            "failed",
            error=str(exc),
            details=cli_impl._error_payload_from_exception(exc),
        )
        index.upsert_run(finalized)
    finally:
        index.close()
