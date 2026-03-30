import argparse
import json
import math
import pathlib
from typing import Iterable

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


def _extract_row_vector(raw_value) -> list[float]:
    if isinstance(raw_value, (list, tuple)):
        return [float(v) for v in raw_value]
    return _parse_vector_text(str(raw_value))


def _cosine_distance(lhs: Iterable[float], rhs: Iterable[float]) -> float:
    lhs_values = list(lhs)
    rhs_values = list(rhs)
    dot = sum(a * b for a, b in zip(lhs_values, rhs_values))
    lhs_norm = math.sqrt(sum(a * a for a in lhs_values))
    rhs_norm = math.sqrt(sum(b * b for b in rhs_values))
    if lhs_norm == 0.0 or rhs_norm == 0.0:
        return 1.0
    similarity = dot / (lhs_norm * rhs_norm)
    similarity = max(-1.0, min(1.0, similarity))
    return 1.0 - similarity


def _l2_distance(lhs: Iterable[float], rhs: Iterable[float]) -> float:
    lhs_values = list(lhs)
    rhs_values = list(rhs)
    return math.sqrt(sum((a - b) * (a - b) for a, b in zip(lhs_values, rhs_values)))


def _dot_score(lhs: Iterable[float], rhs: Iterable[float]) -> float:
    lhs_values = list(lhs)
    rhs_values = list(rhs)
    return sum(a * b for a, b in zip(lhs_values, rhs_values))


def _run_vector_search(
    csv_path: pathlib.Path,
    vector_column: str,
    query_vector: str,
    metric: str,
    top_k: int,
) -> int:
    session = Session()
    table = session.read_csv(str(csv_path)).to_arrow()
    rows = table.to_pylist()
    needle = _parse_vector_text(query_vector)
    if not needle:
        raise ValueError("--query-vector must not be empty")

    scored = []
    expected_dim = len(needle)
    for row_index, row in enumerate(rows):
        if vector_column not in row:
            raise KeyError(f"vector column not found: {vector_column}")
        vector = _extract_row_vector(row[vector_column])
        if len(vector) != expected_dim:
            raise ValueError(
                f"fixed length vector mismatch at row {row_index}: expect {expected_dim}, got {len(vector)}"
            )
        if metric in ("cosine", "cosin"):
            distance = _cosine_distance(vector, needle)
        elif metric == "dot":
            distance = _dot_score(vector, needle)
        else:
            distance = _l2_distance(vector, needle)
        scored.append({"row_index": row_index, "distance": distance, "row": row})

    scored.sort(key=lambda item: item["distance"], reverse=(metric == "dot"))
    payload = {
        "metric": "cosine" if metric in ("cosine", "cosin") else metric,
        "top_k": top_k,
        "rows": scored[:top_k],
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
        help="Vector column name. Row value format supports '1,2,3' or '[1,2,3]'.",
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
