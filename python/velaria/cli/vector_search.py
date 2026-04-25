from __future__ import annotations

import argparse
import pathlib
from typing import Any

from velaria import Session

from velaria.cli._common import (
    CliStructuredError,
    _emit_json,
    _normalize_metric,
    _parse_scalar_text,
    _parse_vector_text,
    _sql_literal_value,
    _table_artifact,
    _text_artifact,
)


def _add_vector_search_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--csv", required=True, help="CSV file path.")
    parser.add_argument(
        "--vector-column",
        required=True,
        help="Vector column name. CSV row value format should use bracketed vectors like '[1 2 3]' or '[1,2,3]'.",
    )
    parser.add_argument("--query-vector", required=True, help="Query vector, e.g. '0.1,0.2,0.3'.")
    parser.add_argument(
        "--metric",
        default="cosine",
        choices=["cosine", "cosin", "dot", "l2"],
        help="Distance metric.",
    )
    parser.add_argument("--top-k", type=int, default=5, help="Return top-k nearest rows.")
    parser.add_argument("--where-column", help="Optional column filter for hybrid search mode.")
    parser.add_argument("--where-op", help="Optional filter operator such as =, !=, <, >, <=, >=.")
    parser.add_argument("--where-value", help="Optional filter literal for hybrid search mode.")
    parser.add_argument("--score-threshold", type=float, help="Optional score threshold for hybrid search mode.")


def _execute_vector_search(
    csv_path: pathlib.Path,
    vector_column: str,
    query_vector: str,
    metric: str,
    top_k: int,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: str | None = None,
    score_threshold: float | None = None,
    output_path: pathlib.Path | None = None,
    explain_path: pathlib.Path | None = None,
) -> dict[str, Any]:
    session = Session()
    try:
        df = session.read_csv(str(csv_path))
    except Exception as exc:
        raise CliStructuredError(
            "failed to read csv input",
            phase="csv_read",
            details={"csv": str(csv_path), "vector_column": vector_column},
        ) from exc
    try:
        session.create_temp_view("input_table", df)
    except Exception as exc:
        raise CliStructuredError(
            "failed to register temp view",
            phase="vector_register_view",
            details={"csv": str(csv_path), "table": "input_table"},
        ) from exc
    needle = _parse_vector_text(query_vector)
    if not needle:
        raise CliStructuredError(
            "--query-vector must not be empty",
            phase="argument_parse",
            details={"query_vector": query_vector},
        )

    base_df = df
    if where_column or where_op or where_value is not None:
        if not where_column or not where_op or where_value is None:
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
            parsed_where_value = _parse_scalar_text(where_value)
            base_df = session.sql(
                f"SELECT * FROM input_table WHERE {where_column} {where_op} "
                f"{_sql_literal_value(parsed_where_value)}"
            )
        except Exception as exc:
            raise CliStructuredError(
                "failed to apply hybrid search filter",
                phase="vector_filter",
                details={
                    "where_column": where_column,
                    "where_op": where_op,
                    "where_value": where_value,
                },
            ) from exc

    try:
        if where_column or score_threshold is not None:
            session.create_temp_view("input_table_hybrid", base_df)
            result = session.hybrid_search(
                table="input_table_hybrid",
                vector_column=vector_column,
                query_vector=needle,
                top_k=top_k,
                metric=metric,
                score_threshold=score_threshold,
            ).to_arrow()
        else:
            result = session.vector_search(
                table="input_table",
                vector_column=vector_column,
                query_vector=needle,
                top_k=top_k,
                metric=metric,
            ).to_arrow()
    except Exception as exc:
        raise CliStructuredError(
            "failed to execute hybrid search" if (where_column or score_threshold is not None)
            else "failed to execute vector search",
            phase="hybrid_search" if (where_column or score_threshold is not None) else "vector_search",
            details={
                "csv": str(csv_path),
                "vector_column": vector_column,
                "metric": _normalize_metric(metric),
                "top_k": top_k,
                "where_column": where_column,
                "where_op": where_op,
                "score_threshold": score_threshold,
            },
        ) from exc
    try:
        if where_column or score_threshold is not None:
            explain = session.explain_hybrid_search(
                table="input_table_hybrid",
                vector_column=vector_column,
                query_vector=needle,
                top_k=top_k,
                metric=metric,
                score_threshold=score_threshold,
            )
        else:
            explain = session.explain_vector_search(
                table="input_table",
                vector_column=vector_column,
                query_vector=needle,
                top_k=top_k,
                metric=metric,
            )
    except Exception as exc:
        raise CliStructuredError(
            "failed to explain hybrid search" if (where_column or score_threshold is not None)
            else "failed to explain vector search",
            phase="hybrid_explain" if (where_column or score_threshold is not None) else "vector_explain",
            details={
                "csv": str(csv_path),
                "vector_column": vector_column,
                "metric": _normalize_metric(metric),
                "top_k": top_k,
                "where_column": where_column,
                "where_op": where_op,
                "score_threshold": score_threshold,
            },
        ) from exc
    artifacts: list[dict[str, Any]] = []
    if output_path is not None:
        artifacts.append(
            _table_artifact(
                output_path,
                result,
                ["result", "hybrid-search" if (where_column or score_threshold is not None) else "vector-search"],
            )
        )
    if explain_path is not None:
        artifacts.append(
            _text_artifact(
                explain_path,
                explain,
                ["explain", "hybrid-search" if (where_column or score_threshold is not None) else "vector-search"],
            )
        )
    return {
        "payload": {
            "metric": _normalize_metric(metric),
            "top_k": top_k,
            "where_column": where_column,
            "where_op": where_op,
            "where_value": where_value,
            "score_threshold": score_threshold,
            "schema": result.schema.names,
            "rows": result.to_pylist(),
            "explain": explain,
        },
        "artifacts": artifacts,
    }


def _run_vector_search(
    csv_path: pathlib.Path,
    vector_column: str,
    query_vector: str,
    metric: str,
    top_k: int,
    where_column: str | None = None,
    where_op: str | None = None,
    where_value: str | None = None,
    score_threshold: float | None = None,
) -> int:
    return _emit_json(
        _execute_vector_search(
            csv_path,
            vector_column,
            query_vector,
            metric,
            top_k,
            where_column=where_column,
            where_op=where_op,
            where_value=where_value,
            score_threshold=score_threshold,
        )["payload"]
    )


def register(subparsers: argparse._SubParsersAction) -> None:
    vector_search = subparsers.add_parser(
        "vector-search",
        help="Read CSV and run fixed-length vector nearest search.",
    )
    _add_vector_search_arguments(vector_search)
