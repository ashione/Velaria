from __future__ import annotations

import argparse

from velaria.finance_pack.cli import main as finance_pack_main


def register(subparsers: argparse._SubParsersAction) -> None:
    finance = subparsers.add_parser("finance", help="Run public-data finance helper commands.")
    finance_subparsers = finance.add_subparsers(dest="finance_command", required=True)

    history = finance_subparsers.add_parser("fetch-history", help="Fetch public historical OHLCV data.")
    _add_provider_market(history)
    history.add_argument("--symbol", required=True, help="Provider-specific symbol, e.g. 000001 or 105.AAPL.")
    history.add_argument("--start-date", required=True, help="YYYYMMDD.")
    history.add_argument("--end-date", required=True, help="YYYYMMDD.")
    history.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    history.add_argument("--adjust", default="", help="Provider adjustment flag, e.g. qfq/hfq for AkShare.")
    _add_output(history)

    quotes = finance_subparsers.add_parser("fetch-quotes", help="Fetch public quote rows.")
    _add_provider_market(quotes)
    quotes.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    _add_output(quotes)

    ingest = finance_subparsers.add_parser("ingest-quotes", help="Fetch quotes and append them to a Velaria external_event source.")
    _add_provider_market(ingest)
    ingest.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    ingest.add_argument("--source-id", help="Defaults to finance_<market>_quotes.")
    ingest.add_argument("--name", help="Source display name.")


def _run_finance(args: argparse.Namespace) -> int:
    return finance_pack_main(_to_finance_pack_argv(args))


def _add_provider_market(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--provider", default="akshare", choices=["akshare", "tencent"])
    parser.add_argument("--market", required=True, choices=["cn", "us"])


def _add_output(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", help="Optional output path.")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "jsonl"])
    parser.add_argument("--preview-rows", type=int, default=5)


def _to_finance_pack_argv(args: argparse.Namespace) -> list[str]:
    argv = [str(args.finance_command)]
    for name in ("provider", "market", "symbol", "symbols", "start_date", "end_date", "period", "adjust", "output", "output_format", "preview_rows", "source_id", "name"):
        if not hasattr(args, name):
            continue
        value = getattr(args, name)
        if value is None:
            continue
        flag = f"--{name.replace('_', '-')}"
        argv.extend([flag, str(value)])
    return argv
