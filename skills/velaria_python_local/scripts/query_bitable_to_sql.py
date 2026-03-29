#!/usr/bin/env python3
"""Query a Feishu Bitable table with a Velaria SQL query."""

from __future__ import annotations

import argparse
import json
import os
from typing import Dict, Iterable, List

import pyarrow as pa

from velaria import BitableClient, Session


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Bitable records into Session and run SQL."
    )
    parser.add_argument("--base-url", help="Bitable base table url")
    parser.add_argument("--app-id", help="FEISHU_BITABLE_APP_ID")
    parser.add_argument("--app-secret", help="FEISHU_BITABLE_APP_SECRET")
    parser.add_argument(
        "--query",
        required=True,
        help="SQL query referencing the temp view name.",
    )
    parser.add_argument(
        "--view-name",
        default="bitable_data",
        help="Temporary view name used by SQL (default: bitable_data)",
    )
    parser.add_argument(
        "--limit-columns",
        nargs="+",
        help="Optional field whitelist for normalization and query fields.",
    )
    return parser.parse_args()


def _resolve_env(name: str, cli_value: str | None) -> str:
    value = cli_value or os.getenv(name)
    if not value:
        raise ValueError(f"missing required value: {name}")
    return value


def _normalize_bitable_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        if len(value) == 1:
            return _normalize_bitable_value(value[0])
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, dict):
        if "name" in value and isinstance(value["name"], str):
            return value["name"]
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _to_arrow_table(rows: Iterable[Dict[str, object]], limit_columns: List[str] | None) -> pa.Table:
    records: List[Dict[str, object]] = []
    all_columns = set()
    for row in rows:
        normalized: Dict[str, object] = {}
        for key, value in row.items():
            normalized[str(key)] = _normalize_bitable_value(value)
            all_columns.add(str(key))
        records.append(normalized)

    columns = list(limit_columns) if limit_columns else sorted(all_columns)
    payload: List[Dict[str, object]] = []
    for row in records:
        payload.append({column: row.get(column) for column in columns})

    return pa.Table.from_pylist(payload)


def main() -> None:
    args = _parse_args()
    base_url = _resolve_env("FEISHU_BITABLE_BASE_URL", args.base_url)
    app_id = _resolve_env("FEISHU_BITABLE_APP_ID", args.app_id)
    app_secret = _resolve_env("FEISHU_BITABLE_APP_SECRET", args.app_secret)

    client = BitableClient(app_id=app_id, app_secret=app_secret)
    rows = client.list_records_from_url(base_url)
    if not rows:
        raise RuntimeError("No rows returned from Bitable")

    session = Session()
    table = _to_arrow_table(rows, args.limit_columns)
    data_frame = session.create_dataframe_from_arrow(table)
    session.create_temp_view(args.view_name, data_frame)

    query = args.query
    result = session.sql(query)
    rows_result = result.to_arrow().to_pylist()
    print(f"sql: {query}")
    print(rows_result)


if __name__ == "__main__":
    main()
