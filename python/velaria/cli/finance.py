from __future__ import annotations

import argparse
import textwrap

from velaria.finance_pack.cli import main as finance_pack_main


def register(subparsers: argparse._SubParsersAction) -> None:
    finance = subparsers.add_parser(
        "finance",
        help="Fetch public finance data and ingest quote rows for agentic monitors.",
        description=(
            "Public-data finance helpers for A-share and U.S. stock workflows. "
            "Use this command to fetch provider-backed rows or ingest quote rows "
            "as Velaria external_event observations."
        ),
        epilog=textwrap.dedent(
            """\
            Examples:
              velaria finance fetch-quotes --provider tencent --market cn --symbols 000001,600519
              velaria finance fetch-quotes --provider tencent --market us --symbols AAPL
              velaria finance ingest-quotes --provider tencent --market cn --symbols 000001 --source-id finance_cn_quotes
              velaria finance watch --provider tencent --market cn --symbol 000001 --interval-sec 30 --iterations 0
              velaria finance fetch-history --provider akshare --market cn --symbol 000001 --start-date 20250101 --end-date 20250131 --adjust qfq

            Agent mode:
              In velaria_cli.py -i, call the registered agent tool velaria_cli_run
              with only the Velaria subcommand, for example:
              finance fetch-quotes --provider tencent --market cn --symbols 000001

            Data-source notes:
              - provider=akshare supports historical OHLCV and quote rows when upstream endpoints are reachable.
              - provider=tencent supports lightweight public quote rows.
              - Results include provider, source_url, fetched_at, freshness, delay_sec, and license_note.
              - Finance outputs are research inputs, not investment advice.
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    finance_subparsers = finance.add_subparsers(dest="finance_command", required=True)

    history = finance_subparsers.add_parser(
        "fetch-history",
        help="Fetch public historical OHLCV data.",
        description="Fetch historical OHLCV rows from a public provider and optionally write Parquet or JSONL.",
    )
    _add_provider_market(history)
    history.add_argument("--symbol", required=True, help="Provider-specific symbol, e.g. 000001 or 105.AAPL.")
    history.add_argument("--start-date", required=True, help="YYYYMMDD.")
    history.add_argument("--end-date", required=True, help="YYYYMMDD.")
    history.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    history.add_argument("--adjust", default="", help="Provider adjustment flag, e.g. qfq/hfq for AkShare.")
    _add_output(history)

    quotes = finance_subparsers.add_parser(
        "fetch-quotes",
        help="Fetch public quote rows.",
        description="Fetch current public quote rows with provider evidence metadata.",
    )
    _add_provider_market(quotes)
    quotes.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    _add_output(quotes)

    ingest = finance_subparsers.add_parser(
        "ingest-quotes",
        help="Fetch quotes and append them to a Velaria external_event source.",
        description=(
            "Fetch public quote rows and append them to an external_event source "
            "so monitors can generate FocusEvent objects."
        ),
    )
    _add_provider_market(ingest)
    ingest.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    ingest.add_argument("--source-id", help="Defaults to finance_<market>_quotes.")
    ingest.add_argument("--name", help="Source display name.")

    watch = finance_subparsers.add_parser(
        "watch",
        help="Watch one symbol, ingest quotes, run a monitor, and emit analysis context.",
        description=(
            "Poll one public quote symbol, append each observation to a Velaria "
            "external_event source, run a monitor, and return FocusEvent plus "
            "analysis prompt context."
        ),
    )
    _add_provider_market(watch)
    watch.add_argument("--symbol", required=True, help="Single symbol to watch, e.g. 000001 or AAPL.")
    watch.add_argument("--source-id", help="Defaults to finance_<market>_<symbol>_watch.")
    watch.add_argument("--monitor-id", help="Defaults to monitor_<source_id>.")
    watch.add_argument("--name", help="Source and monitor display name.")
    watch.add_argument("--interval-sec", type=float, default=30.0, help="Seconds between polls.")
    watch.add_argument("--iterations", type=int, default=1, help="Number of polls. Use 0 to run until interrupted.")
    watch.add_argument("--pct-change-threshold", type=float, help="Only create focus events when ABS(pct_change) is at least this value.")
    watch.add_argument("--min-price", type=float, help="Only create focus events when price is at least this value.")
    watch.add_argument("--max-price", type=float, help="Only create focus events when price is at most this value.")
    watch.add_argument("--cooldown-sec", type=int, default=0, help="FocusEvent suppression cooldown for this watch monitor.")
    watch.add_argument("--jsonl", action="store_true", help="Emit one JSON object per tick.")
    watch.add_argument("--no-analysis-prompt", action="store_true", help="Omit the Velaria Agent research prompt.")


def _run_finance(args: argparse.Namespace) -> int:
    return finance_pack_main(_to_finance_pack_argv(args))


def _add_provider_market(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--provider",
        default="akshare",
        choices=["akshare", "tencent"],
        help="Public data provider. Use akshare for history; tencent is a lightweight quote provider.",
    )
    parser.add_argument("--market", required=True, choices=["cn", "us"], help="Market: cn for A-share, us for U.S. stocks.")


def _add_output(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", help="Optional output path for fetched rows.")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "jsonl"], help="Output format when --output is set.")
    parser.add_argument("--preview-rows", type=int, default=5, help="Number of rows to include in JSON stdout preview.")


def _to_finance_pack_argv(args: argparse.Namespace) -> list[str]:
    argv = [str(args.finance_command)]
    for name in (
        "provider",
        "market",
        "symbol",
        "symbols",
        "start_date",
        "end_date",
        "period",
        "adjust",
        "output",
        "output_format",
        "preview_rows",
        "source_id",
        "monitor_id",
        "name",
        "interval_sec",
        "iterations",
        "pct_change_threshold",
        "min_price",
        "max_price",
        "cooldown_sec",
    ):
        if not hasattr(args, name):
            continue
        value = getattr(args, name)
        if value is None:
            continue
        flag = f"--{name.replace('_', '-')}"
        argv.extend([flag, str(value)])
    if getattr(args, "jsonl", False):
        argv.append("--jsonl")
    if getattr(args, "no_analysis_prompt", False):
        argv.append("--no-analysis-prompt")
    return argv
