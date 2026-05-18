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
    for name in ("provider", "market", "symbol", "symbols", "start_date", "end_date", "period", "adjust", "output", "output_format", "preview_rows", "source_id", "name"):
        if not hasattr(args, name):
            continue
        value = getattr(args, name)
        if value is None:
            continue
        flag = f"--{name.replace('_', '-')}"
        argv.extend([flag, str(value)])
    return argv
