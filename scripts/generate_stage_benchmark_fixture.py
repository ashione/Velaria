#!/usr/bin/env python3

import argparse
import csv
import hashlib
import pathlib


KEEP_COLUMNS = [
    "caller_psm",
    "method",
    "query_time_ms",
    "total_latency_ms",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate an anonymized stage-benchmark fixture from a raw Velaria slow-query CSV."
    )
    parser.add_argument("--input", required=True, help="path to the raw input CSV")
    parser.add_argument("--output", required=True, help="path to the anonymized output CSV")
    return parser.parse_args()


def anonymize_token(prefix: str, value: str, width: int = 12) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:width]
    return f"{prefix}_{digest}"


def main():
    args = parse_args()
    src = pathlib.Path(args.input)
    dst = pathlib.Path(args.output)
    dst.parent.mkdir(parents=True, exist_ok=True)

    caller_map: dict[str, str] = {}
    method_map: dict[str, str] = {}

    with src.open("r", encoding="utf-8", newline="") as fin, dst.open(
        "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=KEEP_COLUMNS, lineterminator="\n")
        writer.writeheader()
        for row in reader:
            caller = row.get("caller_psm") or ""
            method = row.get("method") or ""
            if caller not in caller_map:
                caller_map[caller] = anonymize_token("psm", caller)
            if method not in method_map:
                method_map[method] = anonymize_token("method", method, width=8)
            writer.writerow(
                {
                    "caller_psm": caller_map[caller],
                    "method": method_map[method],
                    "query_time_ms": row.get("query_time_ms") or "0",
                    "total_latency_ms": row.get("total_latency_ms") or "0",
                }
            )


if __name__ == "__main__":
    main()
