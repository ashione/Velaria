import argparse
import json
import pathlib

from velaria import Session


def _run_csv_sql(csv_path: pathlib.Path, table: str, query: str) -> int:
    session = Session()
    df = session.read_csv(str(csv_path))
    session.create_temp_view(table, df)
    result = session.sql(query).to_arrow()
    print(
        json.dumps(
            {
                "table": table,
                "query": query,
                "schema": result.schema.names,
                "rows": result.to_pylist(),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


def _parse_vector_text(text: str) -> list[float]:
    value = text.strip()
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1].strip()
    if not value:
        return []
    return [float(part.strip()) for part in value.split(",") if part.strip()]


def _run_vector_search(
    csv_path: pathlib.Path,
    vector_column: str,
    query_vector: str,
    metric: str,
    top_k: int,
) -> int:
    session = Session()
    df = session.read_csv(str(csv_path))
    session.create_temp_view("input_table", df)
    needle = _parse_vector_text(query_vector)
    if not needle:
        raise ValueError("--query-vector must not be empty")

    result = session.vector_search(
        table="input_table",
        vector_column=vector_column,
        query_vector=needle,
        top_k=top_k,
        metric=metric,
    ).to_arrow()
    explain = session.explain_vector_search(
        table="input_table",
        vector_column=vector_column,
        query_vector=needle,
        top_k=top_k,
        metric=metric,
    )
    payload = {
        "metric": "cosine" if metric in ("cosine", "cosin") else metric,
        "top_k": top_k,
        "schema": result.schema.names,
        "rows": result.to_pylist(),
        "explain": explain,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="velaria-cli",
        description="Velaria CLI for SQL query execution.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    csv_sql = subparsers.add_parser(
        "csv-sql",
        help="Read CSV and run a SQL query through DataflowSession.",
    )
    csv_sql.add_argument(
        "--csv",
        required=True,
        help="CSV file path.",
    )
    csv_sql.add_argument(
        "--table",
        default="input_table",
        help="Temporary table name exposed to session.sql(...).",
    )
    csv_sql.add_argument(
        "--query",
        required=True,
        help="SQL query text.",
    )

    vector_search = subparsers.add_parser(
        "vector-search",
        help="Read CSV and run fixed-length vector nearest search.",
    )
    vector_search.add_argument("--csv", required=True, help="CSV file path.")
    vector_search.add_argument(
        "--vector-column",
        required=True,
        help="Vector column name. CSV row value format should use bracketed vectors like '[1 2 3]' or '[1,2,3]'.",
    )
    vector_search.add_argument(
        "--query-vector",
        required=True,
        help="Query vector, e.g. '0.1,0.2,0.3'.",
    )
    vector_search.add_argument(
        "--metric",
        default="cosine",
        choices=["cosine", "cosin", "dot", "l2"],
        help="Distance metric.",
    )
    vector_search.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Return top-k nearest rows.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "csv-sql":
        return _run_csv_sql(pathlib.Path(args.csv), args.table, args.query)
    if args.command == "vector-search":
        return _run_vector_search(
            csv_path=pathlib.Path(args.csv),
            vector_column=args.vector_column,
            query_vector=args.query_vector,
            metric=args.metric,
            top_k=args.top_k,
        )

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
