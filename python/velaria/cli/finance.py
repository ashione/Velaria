from __future__ import annotations

import argparse
import textwrap

from velaria.finance_pack import provider_names_for_operation
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
              velaria finance doctor
              velaria finance sources
              velaria finance analyze --market cn --symbol 000001
              velaria finance pipeline --market cn --symbol 000001 --start-date 20250101 --end-date 20250131 --iterations 1
              velaria finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --top 3
              velaria finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --native-stream --ingest-raw --iterations 0
              velaria finance watch-session start --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --iterations 0
              velaria finance watch-session start --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --iterations 0 --async-run --format json
              velaria finance watch-session status --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance watch-session logs --session-id finance_us_watch_20260519T133000Z --limit 20 --format json
              velaria finance watch-session summarize --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance stream-history --market us --source-id finance_us_rank_candidates_native_stream_signals --format json
              velaria finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --stream-monitor --until-time 2026-05-18T16:00:00-04:00
              velaria finance fetch-quotes --provider tencent --market cn --symbols 000001,600519
              velaria finance fetch-news --provider google-news --market us --symbol AAPL --limit 5
              velaria finance fetch-quotes --provider tencent --market us --symbols AAPL
              velaria finance ingest-quotes --provider tencent --market cn --symbols 000001 --source-id finance_cn_quotes
              velaria finance watch --provider tencent --market cn --symbol 000001 --interval-sec 30 --iterations 0
              velaria finance fetch-history --provider akshare --market cn --symbol 000001 --start-date 20250101 --end-date 20250131 --adjust qfq

            Agent mode:
              In velaria_cli.py -i, call the registered agent tool velaria_cli_run
              with only the Velaria subcommand, for example:
              finance doctor
              finance sources
              finance analyze --market cn --symbol 000001
              finance pipeline --market cn --symbol 000001 --start-date 20250101 --end-date 20250131 --iterations 1 --format json
              finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --top 3 --format json
              finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --native-stream --ingest-raw --iterations 0 --format json
              finance watch-session start --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --iterations 0 --format json
              finance watch-session start --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --iterations 0 --async-run --format json
              finance watch-session status --session-id finance_us_watch_20260519T133000Z --format json
              finance watch-session logs --session-id finance_us_watch_20260519T133000Z --limit 20 --format json
              finance watch-session summarize --session-id finance_us_watch_20260519T133000Z --format json
              finance stream-history --market us --source-id finance_us_rank_candidates_native_stream_signals --format json
              finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --stream-monitor --until-time 2026-05-18T16:00:00-04:00 --format json
              finance fetch-news --provider google-news --market us --symbol AAPL --limit 5
              finance fetch-quotes --provider tencent --market cn --symbols 000001

            Data-source notes:
              - provider=yahoo supports historical OHLCV through public chart JSON.
              - provider=google-news supports public RSS news rows for sentiment evidence.
              - provider=akshare supports historical OHLCV and quote rows when upstream endpoints are reachable.
              - provider=tencent supports lightweight public quote rows.
              - Results include provider, source_url, fetched_at, freshness, delay_sec, and license_note.
              - Finance outputs are research inputs, not investment advice.
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    finance_subparsers = finance.add_subparsers(dest="finance_command", required=True)

    sources = finance_subparsers.add_parser(
        "sources",
        help="List public finance providers and supported workflows.",
        description="Show available public data providers, supported markets, freshness, and recommended first commands.",
    )
    _add_report_format(sources)

    doctor = finance_subparsers.add_parser(
        "doctor",
        help="Check finance dependencies and public quote provider reachability.",
        description="Run a product readiness check for finance commands before analyze/watch.",
    )
    doctor.add_argument("--market", default="cn", choices=["cn", "us"], help="Market used for the quote provider probe.")
    doctor.add_argument("--symbol", default="000001", help="Symbol used for the quote provider probe.")
    doctor.add_argument("--skip-network", action="store_true", help="Skip public provider network probes.")
    _add_report_format(doctor)

    analyze = finance_subparsers.add_parser(
        "analyze",
        help="Fetch one quote, ingest it, run a monitor, and print a readable research report.",
        description=(
            "One-command finance workflow for users: fetch a public quote, store the observation, "
            "run a monitor, and print a readable report with data-source evidence."
        ),
    )
    _add_provider_market(analyze, default_provider="tencent")
    analyze.add_argument("--symbol", required=True, help="Single symbol to analyze, e.g. 000001 or AAPL.")
    analyze.add_argument("--source-id", help="Defaults to finance_<market>_<symbol>_analysis.")
    analyze.add_argument("--monitor-id", help="Defaults to monitor_<source_id>.")
    analyze.add_argument("--name", help="Source and monitor display name.")
    analyze.add_argument("--pct-change-threshold", type=float, help="Only create focus events when ABS(pct_change) is at least this value.")
    analyze.add_argument("--min-price", type=float, help="Only create focus events when price is at least this value.")
    analyze.add_argument("--max-price", type=float, help="Only create focus events when price is at most this value.")
    analyze.add_argument("--cooldown-sec", type=int, default=0, help="FocusEvent suppression cooldown for this analyze monitor.")
    analyze.add_argument("--no-analysis-prompt", action="store_true", help="Omit the Velaria Agent research prompt from JSON output.")
    _add_report_format(analyze)

    pipeline = finance_subparsers.add_parser(
        "pipeline",
        help="Fetch history, subscribe to live quotes, run a monitor, and emit a complete analysis chain.",
        description=(
            "Complete finance chain for users: fetch historical OHLCV, store a history artifact, "
            "poll live quote rows, run a monitor, and return analysis plus service integration metadata."
        ),
    )
    pipeline.add_argument("--market", required=True, choices=["cn", "us"], help="Market: cn for A-share, us for U.S. stocks.")
    pipeline.add_argument("--symbol", required=True, help="Single symbol, e.g. 000001 or AAPL.")
    pipeline.add_argument("--history-provider", default="yahoo", choices=provider_names_for_operation("fetch_history"), help="Historical OHLCV provider.")
    pipeline.add_argument("--quote-provider", default="tencent", choices=provider_names_for_operation("fetch_quotes"), help="Quote provider used for live subscription ticks.")
    pipeline.add_argument("--start-date", required=True, help="YYYYMMDD.")
    pipeline.add_argument("--end-date", required=True, help="YYYYMMDD.")
    pipeline.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    pipeline.add_argument("--adjust", default="", help="Provider adjustment flag, e.g. qfq/hfq for AkShare.")
    pipeline.add_argument("--history-output", help="Defaults to $VELARIA_HOME/finance/history/<market>_<symbol>.parquet.")
    pipeline.add_argument("--history-output-format", default="parquet", choices=["parquet", "jsonl"])
    pipeline.add_argument("--preview-rows", type=int, default=5)
    pipeline.add_argument("--source-id", help="Defaults to finance_<market>_<symbol>_pipeline.")
    pipeline.add_argument("--monitor-id", help="Defaults to monitor_<source_id>.")
    pipeline.add_argument("--name", help="Source and monitor display name.")
    pipeline.add_argument("--interval-sec", type=float, default=30.0, help="Seconds between quote polls.")
    pipeline.add_argument("--iterations", type=int, default=1, help="Number of quote polling iterations. Use 0 to run until interrupted.")
    pipeline.add_argument("--pct-change-threshold", type=float, help="Only create focus events when ABS(pct_change) is at least this value.")
    pipeline.add_argument("--min-price", type=float, help="Only create focus events when price is at least this value.")
    pipeline.add_argument("--max-price", type=float, help="Only create focus events when price is at most this value.")
    pipeline.add_argument("--cooldown-sec", type=int, default=0, help="FocusEvent suppression cooldown for this pipeline monitor.")
    pipeline.add_argument("--no-analysis-prompt", action="store_true", help="Omit the Velaria Agent research prompt from JSON output.")
    _add_report_format(pipeline)

    rank = finance_subparsers.add_parser(
        "rank-candidates",
        help="Rank top research candidates from quotes, history, news, and sentiment.",
        description=(
            "Continuously poll quote rows, historical OHLCV, public news RSS, and transparent "
            "sentiment evidence to emit top research candidates. This does not emit trading advice."
        ),
    )
    rank.add_argument("--market", required=True, choices=["cn", "us"], help="Market: cn for A-share, us for U.S. stocks.")
    rank.add_argument("--symbols", required=True, help="Comma-separated candidate symbols, e.g. AAPL,MSFT,NVDA.")
    rank.add_argument("--history-provider", default="yahoo", choices=provider_names_for_operation("fetch_history"), help="Historical OHLCV provider.")
    rank.add_argument("--quote-provider", default="tencent", choices=provider_names_for_operation("fetch_quotes"), help="Quote provider used for polling.")
    rank.add_argument("--news-provider", default="google-news", choices=provider_names_for_operation("fetch_news"), help="News provider used for public news and sentiment context.")
    rank.add_argument("--start-date", required=True, help="YYYYMMDD.")
    rank.add_argument("--end-date", required=True, help="YYYYMMDD.")
    rank.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    rank.add_argument("--adjust", default="", help="Provider adjustment flag, e.g. qfq/hfq for AkShare.")
    rank.add_argument("--top", type=int, default=3, help="Number of research candidates to emit.")
    rank.add_argument("--news-limit", type=int, default=5, help="Maximum news items per symbol per iteration.")
    rank.add_argument("--source-id", help="Defaults to finance_<market>_rank_candidates.")
    rank.add_argument("--monitor-id-prefix", help="Defaults to monitor_<source_id> when --stream-monitor is set.")
    rank.add_argument("--stream-monitor", action="store_true", help="Create and execute Velaria stream monitors for entry and exit research signals.")
    rank.add_argument("--native-stream", action="store_true", help="Run candidate signal detection through Velaria native realtime stream SQL.")
    rank.add_argument("--native-stream-poll-timeout-sec", type=float, default=2.0, help="Seconds to wait for native stream sink output per ranking tick.")
    rank.add_argument("--ingest-raw", action="store_true", help="Persist quote, history, news, and candidate rows into Velaria external_event sources.")
    rank.add_argument("--stream-window-size", default="60s", help="Processing-time stream window size for rank-candidate monitors.")
    rank.add_argument("--entry-score-threshold", type=float, default=8.0, help="Entry research signal score threshold.")
    rank.add_argument("--entry-return-threshold", type=float, default=5.0, help="Entry research signal period-return threshold.")
    rank.add_argument("--exit-score-threshold", type=float, default=0.0, help="Exit risk signal score threshold.")
    rank.add_argument("--exit-quote-pct-threshold", type=float, default=-3.0, help="Exit risk signal quote pct_change threshold.")
    rank.add_argument("--cooldown-sec", type=int, default=300, help="FocusEvent suppression cooldown for stream monitor signals.")
    rank.add_argument("--until-time", help="Run until this RFC3339 timestamp, e.g. 2026-05-18T16:00:00-04:00.")
    rank.add_argument("--interval-sec", type=float, default=30.0, help="Seconds between polling iterations.")
    rank.add_argument("--iterations", type=int, default=1, help="Number of ranking iterations. Use 0 to run until interrupted.")
    rank.add_argument("--jsonl", action="store_true", help="Emit one JSON object per ranking tick.")
    _add_report_format(rank)

    stream_history = finance_subparsers.add_parser(
        "stream-history",
        help="Query durable finance stream output history.",
        description=(
            "Read native stream sink output that was persisted as Velaria external_event history. "
            "Use this after rank-candidates --native-stream to inspect historical signal rows."
        ),
    )
    stream_history.add_argument("--market", default="cn", choices=["cn", "us"], help="Market used for the default source id.")
    stream_history.add_argument("--source-id", help="Defaults to finance_<market>_rank_candidates_native_stream_signals.")
    stream_history.add_argument("--start-time", help="Inclusive RFC3339 event_time lower bound.")
    stream_history.add_argument("--end-time", help="Exclusive RFC3339 event_time upper bound.")
    stream_history.add_argument("--limit", type=int, default=50, help="Return the last N matching stream rows.")
    _add_report_format(stream_history)

    watch_session = finance_subparsers.add_parser(
        "watch-session",
        help="Run or inspect durable finance watch sessions.",
        description="Create and query watch sessions that persist market context, fundamentals, news, realtime stream signals, and review data.",
    )
    watch_session_subparsers = watch_session.add_subparsers(dest="watch_session_command", required=True)
    watch_start = watch_session_subparsers.add_parser("start", help="Start a durable watch session.")
    watch_start.add_argument("--session-id", help="Defaults to finance_<market>_watch_<UTC timestamp>.")
    watch_start.add_argument("--market", required=True, choices=["cn", "us"])
    watch_start.add_argument("--symbols", required=True, help="Comma-separated candidate symbols.")
    watch_start.add_argument("--market-symbols", help="Comma-separated market context symbols.")
    watch_start.add_argument("--history-provider", default="yahoo", choices=provider_names_for_operation("fetch_history"))
    watch_start.add_argument("--quote-provider", default="tencent", choices=provider_names_for_operation("fetch_quotes"))
    watch_start.add_argument("--news-provider", default="google-news", choices=provider_names_for_operation("fetch_news"))
    watch_start.add_argument("--fundamentals-provider", default="public-unavailable")
    watch_start.add_argument("--start-date", required=True, help="YYYYMMDD.")
    watch_start.add_argument("--end-date", required=True, help="YYYYMMDD.")
    watch_start.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    watch_start.add_argument("--adjust", default="")
    watch_start.add_argument("--top", type=int, default=3)
    watch_start.add_argument("--news-limit", type=int, default=5)
    watch_start.add_argument("--entry-score-threshold", type=float, default=8.0)
    watch_start.add_argument("--entry-return-threshold", type=float, default=5.0)
    watch_start.add_argument("--exit-score-threshold", type=float, default=0.0)
    watch_start.add_argument("--exit-quote-pct-threshold", type=float, default=-3.0)
    watch_start.add_argument("--native-stream-poll-timeout-sec", type=float, default=2.0)
    watch_start.add_argument("--interval-sec", type=float, default=30.0)
    watch_start.add_argument("--iterations", type=int, default=1)
    watch_start.add_argument("--until-time", help="Run until this RFC3339 timestamp.")
    watch_start.add_argument("--jsonl", action="store_true")
    watch_start.add_argument("--async-run", action="store_true")
    _add_report_format(watch_start)
    for command in ("list", "show", "events", "signals", "summarize", "status", "logs", "stop"):
        sub = watch_session_subparsers.add_parser(command, help=f"{command} durable watch-session data.")
        if command != "list":
            sub.add_argument("--session-id", required=True)
        if command == "events":
            sub.add_argument("--feed", choices=["all", "quotes", "history", "news", "candidates", "market_context", "fundamentals", "native_stream_signals"], default="all")
        if command in {"events", "signals", "logs"}:
            sub.add_argument("--limit", type=int, default=100)
        _add_report_format(sub)

    history = finance_subparsers.add_parser(
        "fetch-history",
        help="Fetch public historical OHLCV data.",
        description="Fetch historical OHLCV rows from a public provider and optionally write Parquet or JSONL.",
    )
    _add_provider_market(history, default_provider="yahoo", choices=provider_names_for_operation("fetch_history"))
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
    _add_provider_market(quotes, default_provider="tencent")
    quotes.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    _add_output(quotes)

    news = finance_subparsers.add_parser(
        "fetch-news",
        help="Fetch public news rows and sentiment evidence.",
        description="Fetch public RSS news rows and emit lightweight, transparent sentiment evidence.",
    )
    news.add_argument("--provider", default="google-news", choices=provider_names_for_operation("fetch_news"), help="Public news provider.")
    news.add_argument("--market", required=True, choices=["cn", "us"], help="Market: cn for A-share, us for U.S. stocks.")
    news.add_argument("--symbol", required=True, help="Single symbol, e.g. 000001 or AAPL.")
    news.add_argument("--query", help="Override provider search query. Defaults to a market-aware symbol query.")
    news.add_argument("--limit", type=int, default=5, help="Maximum news rows to fetch.")
    _add_output(news)

    ingest = finance_subparsers.add_parser(
        "ingest-quotes",
        help="Fetch quotes and append them to a Velaria external_event source.",
        description=(
            "Fetch public quote rows and append them to an external_event source "
            "so monitors can generate FocusEvent objects."
        ),
    )
    _add_provider_market(ingest, default_provider="tencent")
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
    _add_provider_market(watch, default_provider="tencent")
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


def _add_provider_market(parser: argparse.ArgumentParser, *, default_provider: str, choices: list[str] | None = None) -> None:
    parser.add_argument(
        "--provider",
        default=default_provider,
        choices=choices or ["akshare", "tencent"],
        help="Public data provider. Use akshare for history; tencent is a lightweight quote provider.",
    )
    parser.add_argument("--market", required=True, choices=["cn", "us"], help="Market: cn for A-share, us for U.S. stocks.")


def _add_output(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", help="Optional output path for fetched rows.")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "jsonl"], help="Output format when --output is set.")
    parser.add_argument("--preview-rows", type=int, default=5, help="Number of rows to include in JSON stdout preview.")


def _add_report_format(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", dest="report_format", default="text", choices=["text", "json"], help="Output format.")


def _to_finance_pack_argv(args: argparse.Namespace) -> list[str]:
    argv = [str(args.finance_command)]
    if args.finance_command == "watch-session":
        argv.append(str(args.watch_session_command))
    for name in (
        "provider",
        "session_id",
        "market",
        "symbol",
        "symbols",
        "market_symbols",
        "start_date",
        "end_date",
        "period",
        "adjust",
        "output",
        "output_format",
        "history_provider",
        "quote_provider",
        "news_provider",
        "fundamentals_provider",
        "query",
        "limit",
        "start_time",
        "end_time",
        "history_output",
        "history_output_format",
        "preview_rows",
        "top",
        "news_limit",
        "source_id",
        "monitor_id_prefix",
        "native_stream_poll_timeout_sec",
        "stream_window_size",
        "entry_score_threshold",
        "entry_return_threshold",
        "exit_score_threshold",
        "exit_quote_pct_threshold",
        "until_time",
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
    if hasattr(args, "report_format") and args.report_format is not None:
        argv.extend(["--format", str(args.report_format)])
    if getattr(args, "skip_network", False):
        argv.append("--skip-network")
    if getattr(args, "jsonl", False):
        argv.append("--jsonl")
    if getattr(args, "async_run", False):
        argv.append("--async-run")
    if getattr(args, "stream_monitor", False):
        argv.append("--stream-monitor")
    if getattr(args, "native_stream", False):
        argv.append("--native-stream")
    if getattr(args, "ingest_raw", False):
        argv.append("--ingest-raw")
    if getattr(args, "no_analysis_prompt", False):
        argv.append("--no-analysis-prompt")
    return argv
