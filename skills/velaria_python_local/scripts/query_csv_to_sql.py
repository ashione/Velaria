#!/usr/bin/env python3
"""Query a local CSV file with Velaria SQL."""

from __future__ import annotations

import argparse
import pathlib

from velaria import Session


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a CSV file into Session and execute a SQL query."
    )
    parser.add_argument("path", help="Path to the CSV file")
    parser.add_argument(
        "--query",
        required=True,
        help="SQL query referencing the temp view name.",
    )
    parser.add_argument(
        "--view-name",
        default="csv_data",
        help="Temporary view name used by SQL (default: csv_data)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    path = pathlib.Path(args.path)
    if not path.exists():
        raise FileNotFoundError(f"csv file not found: {path}")

    session = Session()
    data_frame = session.read_csv(str(path))
    session.create_temp_view(args.view_name, data_frame)

    result = session.sql(args.query)
    rows = result.to_arrow().to_pylist()
    print(f"sql: {args.query}")
    print(rows)


if __name__ == "__main__":
    main()
