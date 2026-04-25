from __future__ import annotations

import argparse
import pathlib
from typing import Any

from velaria import Session
from velaria.workspace import write_explain

from velaria.cli._common import (
    CliStructuredError,
    _build_batch_explain,
    _build_batch_source_create_sql,
    _emit_json,
    _parse_explain_sections,
    _table_artifact,
)


def _add_csv_sql_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--csv", "--input-path", dest="input_path", required=True, help="Input file path.")
    parser.add_argument(
        "--table",
        default="input_table",
        help="Temporary table name exposed to session.sql(...).",
    )
    parser.add_argument(
        "--input-type",
        default="auto",
        choices=["auto", "csv", "line", "json"],
        help="Input file type. Defaults to auto probe.",
    )
    parser.add_argument("--delimiter", default=",", help="Delimiter for csv or line split sources.")
    parser.add_argument(
        "--line-mode",
        default="split",
        choices=["split", "regex"],
        help="Line source mode when --input-type=line.",
    )
    parser.add_argument("--regex-pattern", help="Regex pattern for line regex mode.")
    parser.add_argument("--mappings", help="Line source mappings like 'uid:1,action:2'.")
    parser.add_argument(
        "--columns",
        help="Comma-separated source column names for json or sequential line parsing.",
    )
    parser.add_argument(
        "--json-format",
        default="json_lines",
        choices=["json_lines", "json_array"],
        help="JSON source format when --input-type=json.",
    )
    parser.add_argument("--query", required=True, help="SQL query text.")


def _execute_csv_sql(
    input_path: pathlib.Path,
    table: str,
    query: str,
    input_type: str = "csv",
    delimiter: str = ",",
    line_mode: str = "split",
    regex_pattern: str | None = None,
    mappings: str | None = None,
    columns: str | None = None,
    json_format: str = "json_lines",
    output_path: pathlib.Path | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    session = Session()
    create_sql = _build_batch_source_create_sql(
        table=table,
        input_path=input_path,
        input_type=input_type,
        delimiter=delimiter,
        line_mode=line_mode,
        regex_pattern=regex_pattern,
        mappings=mappings,
        columns=columns,
        json_format=json_format,
    )
    try:
        session.sql(create_sql)
    except Exception as exc:
        raise CliStructuredError(
            "failed to register sql input table",
            phase="sql_input_register",
            details={
                "input_path": str(input_path),
                "input_type": input_type,
                "table": table,
                "create_sql": create_sql,
            },
            run_id=run_id,
        ) from exc
    batch_explain = ""
    if hasattr(session, "explain_sql"):
        try:
            batch_explain = session.explain_sql(query)
        except Exception:
            batch_explain = ""
    try:
        result_df = session.sql(query)
    except Exception as exc:
        raise CliStructuredError(
            "failed to execute sql query",
            phase="sql_execute",
            details={
                "input_path": str(input_path),
                "input_type": input_type,
                "table": table,
                "query": query,
            },
            run_id=run_id,
        ) from exc
    logical = result_df.explain() if hasattr(result_df, "explain") else ""
    try:
        result = result_df.to_arrow()
    except Exception as exc:
        raise CliStructuredError(
            "failed to materialize sql result",
            phase="result_materialize",
            details={
                "input_path": str(input_path),
                "input_type": input_type,
                "table": table,
                "query": query,
            },
            run_id=run_id,
        ) from exc
    artifacts: list[dict[str, Any]] = []
    if output_path is not None:
        artifacts.append(_table_artifact(output_path, result, ["result", "file-sql"]))
        if run_id is not None:
            write_explain(
                run_id,
                _parse_explain_sections(batch_explain)
                if batch_explain
                else _build_batch_explain(logical, output_path),
            )
    return {
        "payload": {
            "table": table,
            "query": query,
            "input_type": input_type,
            "schema": result.schema.names,
            "rows": result.to_pylist(),
        },
        "artifacts": artifacts,
    }


def _run_csv_sql(
    input_path: pathlib.Path,
    table: str,
    query: str,
    *,
    input_type: str = "csv",
    delimiter: str = ",",
    line_mode: str = "split",
    regex_pattern: str | None = None,
    mappings: str | None = None,
    columns: str | None = None,
    json_format: str = "json_lines",
) -> int:
    return _emit_json(
        _execute_csv_sql(
            input_path,
            table,
            query,
            input_type=input_type,
            delimiter=delimiter,
            line_mode=line_mode,
            regex_pattern=regex_pattern,
            mappings=mappings,
            columns=columns,
            json_format=json_format,
        )["payload"]
    )


def register(subparsers: argparse._SubParsersAction) -> None:
    csv_sql = subparsers.add_parser(
        "file-sql",
        help="Register a batch file source and run a SQL query through DataflowSession.",
    )
    _add_csv_sql_arguments(csv_sql)
