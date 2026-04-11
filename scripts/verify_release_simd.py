#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re

import pyarrow as pa

from velaria import Session


def _extract_field(text: str, key: str) -> str | None:
    pattern = re.compile(rf"^{re.escape(key)}=(.+)$", re.MULTILINE)
    match = pattern.search(text)
    return match.group(1).strip() if match else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify release SIMD backends from an installed wheel.")
    parser.add_argument("--arch", required=True, choices=["x86_64", "aarch64"])
    args = parser.parse_args()

    session = Session()

    table = pa.table(
        {
            "grp": pa.array(["a", "a", "b", "b"]),
            "val": pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float64()),
            "embedding": pa.array(
                [[1.0, 0.0], [0.8, 0.2], [0.0, 1.0], [0.1, 0.9]],
                type=pa.list_(pa.float32(), 2),
            ),
        }
    )
    df = session.create_dataframe_from_arrow(table)
    session.create_temp_view("simd_verify", df)

    aggregate_explain = session.explain_sql(
        "SELECT grp, SUM(val) AS total FROM simd_verify GROUP BY grp"
    )
    vector_explain = session.explain_vector_search(
        table="simd_verify",
        vector_column="embedding",
        query_vector=[1.0, 0.0],
        top_k=2,
        metric="cosine",
    )

    compiled = _extract_field(aggregate_explain, "compiled_backends")
    active_sql = _extract_field(aggregate_explain, "simd_backend")
    active_vector = _extract_field(vector_explain, "backend")

    if compiled is None:
        raise SystemExit("compiled_backends not found in aggregate explain output")
    if active_sql is None:
        raise SystemExit("simd_backend not found in aggregate explain output")
    if active_vector is None:
        raise SystemExit("backend not found in vector explain output")

    compiled_set = {item.strip() for item in compiled.split(",") if item.strip()}
    expected_compiled = {"scalar", "avx2"} if args.arch == "x86_64" else {"scalar", "neon"}
    missing = expected_compiled - compiled_set
    if missing:
        raise SystemExit(
            f"compiled backends mismatch for {args.arch}: missing {sorted(missing)}, got {sorted(compiled_set)}"
        )

    if args.arch == "aarch64":
        if active_sql != "neon" or active_vector != "neon":
            raise SystemExit(
                f"expected neon active backend on aarch64, got sql={active_sql} vector={active_vector}"
            )
    else:
        if active_sql not in {"avx2", "scalar"}:
            raise SystemExit(f"unexpected sql backend on x86_64: {active_sql}")
        if active_vector not in {"avx2", "scalar"}:
            raise SystemExit(f"unexpected vector backend on x86_64: {active_vector}")

    print(
        f"simd verification ok arch={args.arch} compiled={sorted(compiled_set)} "
        f"sql_backend={active_sql} vector_backend={active_vector}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
