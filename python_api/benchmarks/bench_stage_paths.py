import argparse
import csv
import json
import pathlib
import statistics
import time
from collections import defaultdict
from typing import Callable

from velaria import Session


GROUPBY_COUNT_MAX_QUERY = (
    "SELECT caller_psm, COUNT(*) AS cnt, MAX(total_latency_ms) AS max_latency_ms "
    "FROM input_table GROUP BY caller_psm LIMIT 1000"
)
FILTER_LOWER_LIMIT_QUERY = (
    "SELECT LOWER(method) AS method_lower, total_latency_ms, query_time_ms "
    "FROM input_table WHERE total_latency_ms >= 100 LIMIT 1000"
)

SCENARIO_QUERIES = {
    "groupby_count_max": GROUPBY_COUNT_MAX_QUERY,
    "filter_lower_limit": FILTER_LOWER_LIMIT_QUERY,
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark hardcode vs Velaria stage costs on a shared CSV input."
    )
    parser.add_argument("--csv", dest="csv_path", help="existing CSV input path")
    parser.add_argument("--outdir", required=True, help="directory for summary.json/report.md")
    parser.add_argument(
        "--scenario",
        choices=sorted(SCENARIO_QUERIES.keys()),
        default="groupby_count_max",
        help="benchmark scenario; hardcode baseline matches this scenario",
    )
    parser.add_argument(
        "--query",
        help="optional custom SQL; requires --skip-hardcode unless it matches the scenario query exactly",
    )
    parser.add_argument(
        "--skip-hardcode",
        action="store_true",
        help="skip hardcode baseline for Velaria-only experiments",
    )
    parser.add_argument("--rounds", type=int, default=7)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--caller-psm-count", type=int, default=14)
    parser.add_argument(
        "--materialization-format",
        choices=["binary_row_batch", "nanoarrow_ipc"],
        help="enable source materialization with the given on-disk format",
    )
    parser.add_argument(
        "--materialization-dir",
        help="materialization cache directory; defaults to <outdir>/materialization-cache when format is set",
    )
    return parser.parse_args()


def ensure_csv(csv_path: str | None, outdir: pathlib.Path, rows: int,
               caller_psm_count: int) -> pathlib.Path:
    if csv_path:
        return pathlib.Path(csv_path)
    generated = outdir / "generated_stage_input.csv"
    with generated.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "caller_psm",
                "method",
                "query_time_ms",
                "total_latency_ms",
                "status_code",
                "endpoint",
                "bucket",
            ]
        )
        for row_id in range(rows):
            bucket = row_id % caller_psm_count
            writer.writerow(
                [
                    f"service_{bucket:02d}",
                    f"QueryV{row_id % 4}",
                    1_700_000_000_000 + row_id,
                    50 + ((row_id * 17) % 10_000),
                    200 if row_id % 5 else 500,
                    f"/api/{row_id % 32}",
                    bucket,
                ]
            )
    return generated


def hardcode_groupby_count_max_once(csv_path: pathlib.Path):
    started = time.perf_counter()
    grouped = defaultdict(lambda: {"cnt": 0, "max_latency_ms": 0})
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = row.get("caller_psm") or ""
            latency = int(row.get("total_latency_ms") or 0)
            grouped[key]["cnt"] += 1
            if latency > grouped[key]["max_latency_ms"]:
                grouped[key]["max_latency_ms"] = latency
    ended = time.perf_counter()
    return {"elapsed": ended - started, "row_count": len(grouped)}


def hardcode_filter_lower_limit_once(csv_path: pathlib.Path):
    started = time.perf_counter()
    rows = []
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            latency = int(row.get("total_latency_ms") or 0)
            if latency < 100:
                continue
            rows.append(
                [
                    (row.get("method") or "").lower(),
                    latency,
                    int(row.get("query_time_ms") or 0),
                ]
            )
            if len(rows) >= 1000:
                break
    ended = time.perf_counter()
    return {"elapsed": ended - started, "row_count": len(rows)}


def resolve_query_and_hardcode(args) -> tuple[str, Callable[[pathlib.Path], dict] | None]:
    scenario_query = SCENARIO_QUERIES[args.scenario]
    query = args.query or scenario_query
    if args.skip_hardcode:
        return query, None
    if query != scenario_query:
        raise SystemExit(
            "custom --query does not match the selected --scenario; "
            "use --skip-hardcode for Velaria-only experiments"
        )
    if args.scenario == "groupby_count_max":
        return query, hardcode_groupby_count_max_once
    if args.scenario == "filter_lower_limit":
        return query, hardcode_filter_lower_limit_once
    raise SystemExit(f"unsupported scenario: {args.scenario}")


def build_read_csv_kwargs(args, outdir: pathlib.Path):
    if not args.materialization_format:
        return {}
    cache_dir = (pathlib.Path(args.materialization_dir)
                 if args.materialization_dir
                 else outdir / "materialization-cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    return {
        "materialization": True,
        "materialization_dir": str(cache_dir),
        "materialization_format": args.materialization_format,
    }


def velaria_full_once(csv_path: pathlib.Path, query: str, read_csv_kwargs: dict):
    session = Session()
    t0 = time.perf_counter()
    df = session.read_csv(str(csv_path), **read_csv_kwargs)
    t1 = time.perf_counter()
    session.create_temp_view("input_table", df)
    t2 = time.perf_counter()
    result = session.sql(query)
    t3 = time.perf_counter()
    arrow = result.to_arrow()
    t4 = time.perf_counter()
    rows = arrow.to_pylist()
    t5 = time.perf_counter()
    return {
        "read_csv": t1 - t0,
        "create_temp_view": t2 - t1,
        "sql": t3 - t2,
        "to_arrow": t4 - t3,
        "to_pylist": t5 - t4,
        "to_arrow_pylist": t5 - t3,
        "total": t5 - t0,
        "row_count": len(rows),
    }


def velaria_reuse_once(session: Session, df, query: str):
    session.create_temp_view("input_table", df)
    t0 = time.perf_counter()
    result = session.sql(query)
    t1 = time.perf_counter()
    arrow = result.to_arrow()
    t2 = time.perf_counter()
    rows = arrow.to_pylist()
    t3 = time.perf_counter()
    return {
        "sql": t1 - t0,
        "to_arrow": t2 - t1,
        "to_pylist": t3 - t2,
        "to_arrow_pylist": t3 - t1,
        "total": t3 - t0,
        "row_count": len(rows),
    }


def summarize_numeric_runs(runs, keys):
    out = {}
    for key in keys:
        values = [run[key] for run in runs]
        out[key] = {
            "min": min(values),
            "avg": statistics.mean(values),
            "median": statistics.median(values),
            "max": max(values),
            "raw": values,
        }
    out["row_counts"] = [run["row_count"] for run in runs]
    return out


def write_report(summary: dict, outdir: pathlib.Path, csv_path: pathlib.Path, query: str):
    lines = [
        "# Velaria Stage Benchmark",
        "",
        f"- csv={csv_path}",
        f"- query={query}",
        f"- scenario={summary['metadata']['scenario']}",
        "",
    ]
    sections = ["velaria_full", "velaria_reuse"]
    if "hardcode" in summary:
        sections.insert(0, "hardcode")
    for section in sections:
        lines.append(f"## {section}")
        sec = summary[section]
        for key, value in sec.items():
            if key == "row_counts":
                lines.append(f"- row_counts={value}")
                continue
            lines.append(f"### {key}")
            lines.append(f"- min={value['min']:.6f}s")
            lines.append(f"- avg={value['avg']:.6f}s")
            lines.append(f"- median={value['median']:.6f}s")
            lines.append(f"- max={value['max']:.6f}s")
            lines.append(f"- raw={[round(item, 6) for item in value['raw']]}")
            lines.append("")
    lines.append("## reuse_prep")
    lines.append(f"- read_csv_once={summary['reuse_prep']['read_csv_once']:.6f}s")
    lines.append("")
    lines.append("## Ratios")
    if "hardcode" in summary:
        lines.append(
            f"- velaria_full.total / hardcode = "
            f"{summary['velaria_full']['total']['avg'] / summary['hardcode']['elapsed']['avg']:.2f}x"
        )
        lines.append(
            f"- velaria_reuse.total / hardcode = "
            f"{summary['velaria_reuse']['total']['avg'] / summary['hardcode']['elapsed']['avg']:.2f}x"
        )
    else:
        lines.append("- hardcode baseline = skipped")
    lines.append(
        f"- velaria_full.read_csv share = "
        f"{summary['velaria_full']['read_csv']['avg'] / summary['velaria_full']['total']['avg']:.2%}"
    )
    lines.append(
        f"- velaria_full.sql share = "
        f"{summary['velaria_full']['sql']['avg'] / summary['velaria_full']['total']['avg']:.2%}"
    )
    lines.append(
        f"- velaria_full.to_arrow share = "
        f"{summary['velaria_full']['to_arrow']['avg'] / summary['velaria_full']['total']['avg']:.2%}"
    )
    lines.append(
        f"- velaria_full.to_pylist share = "
        f"{summary['velaria_full']['to_pylist']['avg'] / summary['velaria_full']['total']['avg']:.2%}"
    )
    lines.append(
        f"- velaria_full.to_arrow_pylist share = "
        f"{summary['velaria_full']['to_arrow_pylist']['avg'] / summary['velaria_full']['total']['avg']:.2%}"
    )
    (outdir / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    args = parse_args()
    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    query, hardcode_once = resolve_query_and_hardcode(args)
    csv_path = ensure_csv(args.csv_path, outdir, args.rows, args.caller_psm_count)
    read_csv_kwargs = build_read_csv_kwargs(args, outdir)

    velaria_full_runs = [velaria_full_once(csv_path, query, read_csv_kwargs) for _ in range(args.rounds)]

    session = Session()
    t0 = time.perf_counter()
    df = session.read_csv(str(csv_path), **read_csv_kwargs)
    t1 = time.perf_counter()
    reuse_prep = {"read_csv_once": t1 - t0}
    velaria_reuse_runs = [velaria_reuse_once(session, df, query) for _ in range(args.rounds)]

    summary = {
        "velaria_full": summarize_numeric_runs(
            velaria_full_runs,
            ["read_csv", "create_temp_view", "sql", "to_arrow", "to_pylist", "to_arrow_pylist", "total"],
        ),
        "velaria_reuse": summarize_numeric_runs(
            velaria_reuse_runs, ["sql", "to_arrow", "to_pylist", "to_arrow_pylist", "total"]
        ),
        "reuse_prep": reuse_prep,
        "metadata": {
            "csv": str(csv_path),
            "query": query,
            "scenario": args.scenario,
            "rounds": args.rounds,
            "rows": args.rows,
            "caller_psm_count": args.caller_psm_count,
            "read_csv_kwargs": read_csv_kwargs,
            "hardcode_enabled": hardcode_once is not None,
        },
    }
    if hardcode_once is not None:
        hardcode_runs = [hardcode_once(csv_path) for _ in range(args.rounds)]
        summary["hardcode"] = summarize_numeric_runs(hardcode_runs, ["elapsed"])

    (outdir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_report(summary, outdir, csv_path, query)
    print(outdir)


if __name__ == "__main__":
    main()
