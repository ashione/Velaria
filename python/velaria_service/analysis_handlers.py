from __future__ import annotations

from pathlib import Path
from typing import Any

import velaria.cli as cli_impl
from velaria import Session

from ._helpers import (
    _load_dataframe,
    _normalize_path,
    _parse_csv_list,
    _resolve_json_columns,
    _parse_string_list,
    _score_semantics,
)


def _execute_file_sql(payload: dict[str, Any], run_id: str, run_dir: Path) -> dict[str, Any]:
    from velaria.workspace import write_explain

    session = Session()
    input_df = _load_dataframe(session, payload)
    table_name = payload.get("table") or "input_table"
    session.create_temp_view(table_name, input_df)
    query = payload["query"]

    explain = ""
    if hasattr(session, "explain_sql"):
        try:
            explain = session.explain_sql(query)
        except Exception:
            explain = ""

    result_df = session.sql(query)
    logical = result_df.explain() if hasattr(result_df, "explain") else ""
    result_table = result_df.to_arrow()
    output_path = (
        _normalize_path(payload["output_path"])
        if payload.get("output_path")
        else run_dir / "artifacts" / "result.parquet"
    )
    artifacts = [cli_impl._table_artifact(output_path, result_table, ["result", "file-sql"])]
    write_explain(
        run_id,
        cli_impl._parse_explain_sections(explain) if explain else cli_impl._build_batch_explain(logical, output_path),
    )
    return {
        "payload": {
            "table": table_name,
            "query": query,
            "input_type": payload.get("input_type", "auto"),
            "schema": result_table.schema.names,
            "preview": cli_impl._preview_payload_from_table(result_table, limit=int(payload.get("preview_limit", 50))),
            "row_count": result_table.num_rows,
        },
        "artifacts": artifacts,
    }
