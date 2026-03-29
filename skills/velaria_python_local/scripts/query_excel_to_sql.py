#!/usr/bin/env python3
"""Query an Excel file with Velaria SQL."""

from __future__ import annotations

import argparse
import pathlib

from velaria import Session, read_excel


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load an Excel sheet into Session and execute a SQL query."
    )
    parser.add_argument("path", help="Path to the xlsx file")
    parser.add_argument(
        "--sheet",
        default=0,
        help="Sheet name (字符串)或索引（默认 0）",
    )
    parser.add_argument(
        "--query",
        required=True,
        help="SQL query referencing the temp view name.",
    )
    parser.add_argument(
        "--view-name",
        default="excel_data",
        help="Temporary view name used by SQL (default: excel_data)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path = pathlib.Path(args.path)
    if not path.exists():
        raise FileNotFoundError(f"excel file not found: {path}")

    session = Session()
    data_frame = read_excel(
        session,
        str(path),
        sheet_name=args.sheet,
    )
    session.create_temp_view(args.view_name, data_frame)

    result = session.sql(args.query)
    rows = result.to_arrow().to_pylist()
    print(f"sql: {args.query}")
    print(rows)


if __name__ == "__main__":
    main()
