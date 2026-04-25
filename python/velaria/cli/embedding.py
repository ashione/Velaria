from __future__ import annotations

import argparse
import pathlib
from typing import Any

from velaria import (
    Session,
    build_file_embeddings,
    query_file_embeddings,
    run_file_mixed_text_hybrid_search,
)

from velaria.cli._common import (
    CliStructuredError,
    _emit_json,
    _infer_embedding_json_columns,
    _make_embedding_provider,
    _parse_scalar_text,
    _split_csv_values,
    _table_artifact,
)


def _add_embedding_common_arguments(parser: argparse.ArgumentParser, *, require_input: bool) -> None:
    if require_input:
        parser.add_argument("--input-path", required=True, help="Input batch file path.")
    else:
        parser.add_argument("--input-path", help="Optional source file path when querying without a prebuilt dataset.")
    parser.add_argument(
        "--input-type",
        default="auto",
        choices=["auto", "csv", "json"],
        help="Input file type for source data.",
    )
    parser.add_argument("--delimiter", default=",", help="Delimiter for CSV input.")
    parser.add_argument("--json-columns", help="Comma-separated JSON columns to read.")
    parser.add_argument(
        "--json-format",
        default="json_lines",
        choices=["json_lines", "json_array"],
        help="JSON source format.",
    )
    parser.add_argument("--text-columns", required=require_input, help="Comma-separated columns used to build embedding text.")
    parser.add_argument("--provider", choices=["hash", "minilm"], default="hash")
    parser.add_argument("--model", help="Embedding model name. Defaults to provider-specific default.")
    parser.add_argument("--template-version", default="text-v1")
    parser.add_argument("--doc-id-field", default="doc_id")
    parser.add_argument("--source-updated-at-field", default="source_updated_at")


def _add_embedding_build_arguments(parser: argparse.ArgumentParser) -> None:
    _add_embedding_common_arguments(parser, require_input=True)
    parser.add_argument("--output-path", required=True, help="Output embedding dataset path (.parquet/.arrow/.ipc).")


def _add_embedding_query_arguments(parser: argparse.ArgumentParser) -> None:
    _add_embedding_common_arguments(parser, require_input=False)
    parser.add_argument("--dataset-path", help="Prebuilt embedding dataset path.")
    parser.add_argument("--query-text", required=True, help="Query text to embed and search.")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--metric", choices=["cosine", "dot", "l2"], default="cosine")
    parser.add_argument("--where-sql", help="Optional SQL WHERE fragment, e.g. \"bucket = 1 AND region = 'apac'\".")
    parser.add_argument("--where-column")
    parser.add_argument("--where-op")
    parser.add_argument("--where-value")


def _execute_embedding_build(
    input_path: pathlib.Path,
    *,
    input_type: str,
    text_columns: str,
    provider: str,
    model: str | None,
    output_path: pathlib.Path,
    doc_id_field: str,
    source_updated_at_field: str,
    delimiter: str = ",",
    json_columns: str | None = None,
    json_format: str = "json_lines",
    template_version: str = "text-v1",
) -> dict[str, Any]:
    session = Session()
    embedding_provider, resolved_model = _make_embedding_provider(provider, model)
    text_fields = _split_csv_values(text_columns)
    if not text_fields:
        raise CliStructuredError(
            "--text-columns must not be empty",
            phase="argument_parse",
            details={"text_columns": text_columns},
        )
    json_field_list = (
        _split_csv_values(json_columns)
        if json_columns
        else (
            _infer_embedding_json_columns(
                text_columns=text_fields,
                doc_id_field=doc_id_field,
                source_updated_at_field=source_updated_at_field,
            )
            if input_type == "json"
            else None
        )
    )
    try:
        table = build_file_embeddings(
            session,
            input_path,
            provider=embedding_provider,
            model=resolved_model,
            template_version=template_version,
            text_columns=text_fields,
            input_type=input_type,
            delimiter=delimiter,
            json_columns=json_field_list,
            json_format=json_format,
            doc_id_field=doc_id_field,
            source_updated_at_field=source_updated_at_field,
            output_path=output_path,
        )
    except Exception as exc:
        raise CliStructuredError(
            "failed to build embedding dataset",
            phase="embedding_build",
            details={
                "input_path": str(input_path),
                "input_type": input_type,
                "text_columns": text_fields,
                "provider": provider,
                "model": resolved_model,
                "output_path": str(output_path),
            },
        ) from exc
    artifacts = [_table_artifact(output_path, table, ["result", "embedding-build"])]
    return {
        "payload": {
            "input_path": str(input_path),
            "input_type": input_type,
            "text_columns": text_fields,
            "provider": provider,
            "model": resolved_model,
            "output_path": str(output_path),
            "schema": table.schema.names,
            "row_count": table.num_rows,
        },
        "artifacts": artifacts,
    }


def _execute_embedding_query(
    *,
    dataset_path: pathlib.Path | None,
    input_path: pathlib.Path | None,
    input_type: str,
    text_columns: str,
    provider: str,
    model: str | None,
    query_text: str,
    top_k: int,
    metric: str,
    doc_id_field: str,
    source_updated_at_field: str,
    delimiter: str = ",",
    json_columns: str | None = None,
    json_format: str = "json_lines",
    template_version: str = "text-v1",
    where_sql: str | None = None,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: str | None = None,
    output_path: pathlib.Path | None = None,
) -> dict[str, Any]:
    session = Session()
    embedding_provider, resolved_model = _make_embedding_provider(provider, model)
    text_fields = _split_csv_values(text_columns)
    if dataset_path is None and input_path is None:
        raise CliStructuredError(
            "either --dataset-path or --input-path must be provided",
            phase="argument_parse",
        )
    if dataset_path is None and not text_fields:
        raise CliStructuredError(
            "--text-columns must not be empty when --input-path is used",
            phase="argument_parse",
            details={"text_columns": text_columns},
        )
    if where_sql is not None and (
        where_column is not None or where_op is not None or where_value is not None
    ):
        raise CliStructuredError(
            "--where-sql cannot be used together with --where-column/--where-op/--where-value",
            phase="argument_parse",
        )
    if where_sql is None and (where_column is not None or where_op is not None or where_value is not None) and (
        where_column is None or where_op is None or where_value is None
    ):
        raise CliStructuredError(
            "--where-column, --where-op, and --where-value must be provided together",
            phase="argument_parse",
            details={
                "where_column": where_column,
                "where_op": where_op,
                "where_value": where_value,
            },
        )

    try:
        if dataset_path is not None:
            result_df = query_file_embeddings(
                session,
                dataset_path,
                provider=embedding_provider,
                model=resolved_model,
                query_text=query_text,
                top_k=top_k,
                metric=metric,
                where_sql=where_sql,
                where_column=where_column,
                where_op=where_op,
                where_value=_parse_scalar_text(where_value) if where_value is not None else None,
            )
        else:
            json_field_list = (
                _split_csv_values(json_columns)
                if json_columns
                else (
                    _infer_embedding_json_columns(
                        text_columns=text_fields,
                        doc_id_field=doc_id_field,
                        source_updated_at_field=source_updated_at_field,
                        where_column=where_column,
                    )
                    if input_type == "json"
                    else None
                )
            )
            result_df = run_file_mixed_text_hybrid_search(
                session,
                input_path,
                provider=embedding_provider,
                model=resolved_model,
                query_text=query_text,
                template_version=template_version,
                text_columns=text_fields,
                input_type=input_type,
                delimiter=delimiter,
                json_columns=json_field_list,
                json_format=json_format,
                doc_id_field=doc_id_field,
                source_updated_at_field=source_updated_at_field,
                top_k=top_k,
                metric=metric,
                where_sql=where_sql,
                where_column=where_column,
                where_op=where_op,
                where_value=_parse_scalar_text(where_value) if where_value is not None else None,
            )
        result = result_df.to_arrow()
    except Exception as exc:
        raise CliStructuredError(
            "failed to execute embedding query",
            phase="embedding_query",
            details={
                "dataset_path": str(dataset_path) if dataset_path else None,
                "input_path": str(input_path) if input_path else None,
                "input_type": input_type,
                "provider": provider,
                "model": resolved_model,
                "query_text": query_text,
            },
        ) from exc

    artifacts: list[dict[str, Any]] = []
    if output_path is not None:
        artifacts.append(_table_artifact(output_path, result, ["result", "embedding-query"]))
    return {
        "payload": {
            "dataset_path": str(dataset_path) if dataset_path else None,
            "input_path": str(input_path) if input_path else None,
            "input_type": input_type,
            "text_columns": text_fields,
            "provider": provider,
            "model": resolved_model,
            "query_text": query_text,
            "top_k": top_k,
            "metric": metric,
            "where_sql": where_sql,
            "where_column": where_column,
            "where_op": where_op,
            "where_value": where_value,
            "schema": result.schema.names,
            "rows": result.to_pylist(),
        },
        "artifacts": artifacts,
    }


def _run_embedding_build(
    input_path: pathlib.Path,
    *,
    input_type: str,
    text_columns: str,
    provider: str,
    model: str | None,
    output_path: pathlib.Path,
    doc_id_field: str,
    source_updated_at_field: str,
    delimiter: str = ",",
    json_columns: str | None = None,
    json_format: str = "json_lines",
    template_version: str = "text-v1",
) -> int:
    return _emit_json(
        _execute_embedding_build(
            input_path,
            input_type=input_type,
            text_columns=text_columns,
            provider=provider,
            model=model,
            output_path=output_path,
            doc_id_field=doc_id_field,
            source_updated_at_field=source_updated_at_field,
            delimiter=delimiter,
            json_columns=json_columns,
            json_format=json_format,
            template_version=template_version,
        )["payload"]
    )


def _run_embedding_query(
    *,
    dataset_path: pathlib.Path | None,
    input_path: pathlib.Path | None,
    input_type: str,
    text_columns: str,
    provider: str,
    model: str | None,
    query_text: str,
    top_k: int,
    metric: str,
    doc_id_field: str,
    source_updated_at_field: str,
    delimiter: str = ",",
    json_columns: str | None = None,
    json_format: str = "json_lines",
    template_version: str = "text-v1",
    where_sql: str | None = None,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: str | None = None,
) -> int:
    return _emit_json(
        _execute_embedding_query(
            dataset_path=dataset_path,
            input_path=input_path,
            input_type=input_type,
            text_columns=text_columns,
            provider=provider,
            model=model,
            query_text=query_text,
            top_k=top_k,
            metric=metric,
            doc_id_field=doc_id_field,
            source_updated_at_field=source_updated_at_field,
            delimiter=delimiter,
            json_columns=json_columns,
            json_format=json_format,
            template_version=template_version,
            where_sql=where_sql,
            where_column=where_column,
            where_op=where_op,
            where_value=where_value,
        )["payload"]
    )


def register(subparsers: argparse._SubParsersAction) -> None:
    embedding_build = subparsers.add_parser(
        "embedding-build",
        help="Read a batch file, mix selected columns into text, and materialize an embedding dataset.",
    )
    _add_embedding_build_arguments(embedding_build)

    embedding_query = subparsers.add_parser(
        "embedding-query",
        help="Run mixed-text embedding plus hybrid search from a dataset or directly from an input file.",
    )
    _add_embedding_query_arguments(embedding_query)
