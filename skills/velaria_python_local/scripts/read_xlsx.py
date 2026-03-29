#!/usr/bin/env python3
"""Read an Excel file with Velaria and execute a SQL query."""

from __future__ import annotations

import argparse
import pathlib
from velaria import Session, read_excel


def main() -> None:
    parser = argparse.ArgumentParser(description="Read one xlsx file via Velaria Session")
    parser.add_argument("path", help="Path to .xlsx file")
    parser.add_argument("--sheet", default=0, help="Sheet name or index")
    parser.add_argument(
        "--query",
        required=True,
        help="SQL query referencing the temp view name.",
    )
    parser.add_argument(
        "--view-name",
        default="sheet_data",
        help="Temporary view name used by SQL (default: sheet_data)",
    )
    args = parser.parse_args()

    path = pathlib.Path(args.path)
    if not path.exists():
        raise FileNotFoundError(f"file not found: {path}")

    session = Session()
    vdf = read_excel(session, str(path), sheet_name=args.sheet)
    session.create_temp_view(args.view_name, vdf)
    result = session.sql(args.query)
    rows = result.to_arrow().to_pylist()
    print(f"sql: {args.query}")
    print(rows)


if __name__ == "__main__":
    main()
