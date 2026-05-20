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
              velaria finance watch-session review --session-id finance_us_watch_20260519T133000Z --log-limit 20 --format json
              velaria finance watch-session supervise --session-id finance_us_watch_20260519T133000Z --interval-sec 60 --format json
              velaria finance watch-session summarize --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence start --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --iterations 0 --format json
              velaria finance intelligence review --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence replay --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence report --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence index --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence search --session-id finance_us_watch_20260519T133000Z --query "NVDA momentum risk news fundamentals" --format json
              velaria finance intelligence jobs --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence status --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence stop --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence resume --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence evaluate --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance intelligence eval-report --session-id finance_us_watch_20260519T133000Z --format json
              velaria finance stream-history --market us --source-id finance_us_rank_candidates_native_stream_signals --format json
              velaria finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --stream-monitor --until-time 2026-05-18T16:00:00-04:00
              velaria finance fetch-quotes --provider tencent --market cn --symbols 000001,600519
              velaria finance fetch-news --provider google-news --market us --symbol AAPL --limit 5
              velaria finance fetch-fundamentals --provider sec-companyfacts --market us --symbols AAPL,MSFT,NVDA
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
              finance watch-session review --session-id finance_us_watch_20260519T133000Z --log-limit 20 --format json
              finance watch-session supervise --session-id finance_us_watch_20260519T133000Z --interval-sec 60 --format json
              finance watch-session summarize --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence start --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --iterations 0 --format json
              finance intelligence review --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence replay --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence report --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence index --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence search --session-id finance_us_watch_20260519T133000Z --query "NVDA momentum risk news fundamentals" --format json
              finance intelligence jobs --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence status --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence stop --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence resume --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence evaluate --session-id finance_us_watch_20260519T133000Z --format json
              finance intelligence eval-report --session-id finance_us_watch_20260519T133000Z --format json
              finance stream-history --market us --source-id finance_us_rank_candidates_native_stream_signals --format json
              finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --stream-monitor --until-time 2026-05-18T16:00:00-04:00 --format json
              finance fetch-news --provider google-news --market us --symbol AAPL --limit 5
              finance fetch-fundamentals --provider sec-companyfacts --market us --symbols AAPL,MSFT,NVDA
              finance fetch-quotes --provider tencent --market cn --symbols 000001

            Data-source notes:
              - provider=yahoo supports historical OHLCV through public chart JSON.
              - provider=yahoo supports delayed quote rows through public chart metadata.
              - provider=google-news supports public RSS news rows for sentiment evidence.
              - provider=sec-companyfacts supports U.S. fundamentals evidence through SEC Company Facts.
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
        help="Rank top research candidates from quotes, history, derived metrics, news, and sentiment.",
        description=(
            "Continuously poll quote rows, historical OHLCV, public news RSS, and transparent "
            "sentiment evidence, derive replayable feature metrics, and emit top research "
            "candidates. This does not emit trading advice."
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
    rank.add_argument("--signal-policy-preset", default="balanced", choices=["balanced", "momentum", "defensive"], help="Signal policy preset used to compute native-stream entry/exit flags.")
    rank.add_argument("--signal-policy", help="JSON signal policy override. Supports entry.all/exit.any condition lists with field/op/value.")
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

    intelligence = finance_subparsers.add_parser(
        "intelligence",
        help="Run the full finance intelligence chain.",
        description=(
            "Productized finance intelligence workflow: public data providers, native realtime stream signals, "
            "Velaria external_event persistence, replay, and agent-readable AI briefs."
        ),
    )
    intelligence_subparsers = intelligence.add_subparsers(dest="intelligence_command", required=True)
    intelligence_start = intelligence_subparsers.add_parser("start", help="Start the full finance intelligence chain.")
    _add_watch_session_start_args(intelligence_start, include_intelligence_id=True)
    intelligence_review = intelligence_subparsers.add_parser("review", help="Persist an agent-readable intelligence review.")
    intelligence_review.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_review.add_argument("--session-id", required=True, help="Durable watch-session id to review.")
    intelligence_review.add_argument("--log-limit", type=int, default=20, help="Number of runtime log lines to include.")
    _add_report_format(intelligence_review)
    intelligence_replay = intelligence_subparsers.add_parser("replay", help="Replay persisted realtime watch data.")
    intelligence_replay.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_replay.add_argument("--session-id", required=True, help="Durable watch-session id to replay.")
    _add_report_format(intelligence_replay)
    intelligence_report = intelligence_subparsers.add_parser("report", help="Generate and persist the final finance intelligence scorecard.")
    intelligence_report.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_report.add_argument("--session-id", required=True, help="Durable watch-session id to report.")
    _add_report_format(intelligence_report)
    intelligence_index = intelligence_subparsers.add_parser("index", help="Build or refresh a reusable hybrid evidence index for a watch session.")
    intelligence_index.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_index.add_argument("--session-id", required=True, help="Durable watch-session id to index.")
    intelligence_index.add_argument(
        "--feed",
        choices=["all", "quotes", "history", "news", "features", "candidates", "market_context", "fundamentals", "native_stream_signals"],
        default="all",
    )
    _add_report_format(intelligence_index)
    intelligence_search = intelligence_subparsers.add_parser("search", help="Hybrid-search persisted finance evidence for a watch session.")
    intelligence_search.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_search.add_argument("--session-id", required=True, help="Durable watch-session id to search.")
    intelligence_search.add_argument("--query", required=True, help="Evidence query text, e.g. NVDA momentum risk news fundamentals.")
    intelligence_search.add_argument("--top-k", type=int, default=5, help="Number of fused evidence hits.")
    intelligence_search.add_argument(
        "--feed",
        choices=["all", "quotes", "history", "news", "features", "candidates", "market_context", "fundamentals", "native_stream_signals"],
        default="all",
    )
    intelligence_search.add_argument("--index-mode", choices=["auto", "rebuild", "off"], default="auto", help="auto reuses or refreshes a persisted evidence index, rebuild forces refresh, off uses a temporary in-memory index.")
    _add_report_format(intelligence_search)
    intelligence_jobs = intelligence_subparsers.add_parser("jobs", help="List durable finance intelligence jobs.")
    intelligence_jobs.add_argument("--session-id", help="Durable watch-session id. Omit to list recent jobs for all sessions.")
    _add_report_format(intelligence_jobs)
    intelligence_status = intelligence_subparsers.add_parser("status", help="Inspect durable finance intelligence runtime status.")
    intelligence_status.add_argument("--session-id", required=True, help="Durable watch-session id to inspect.")
    intelligence_status.add_argument("--log-limit", type=int, default=20, help="Number of runtime log lines to include.")
    _add_report_format(intelligence_status)
    intelligence_stop = intelligence_subparsers.add_parser("stop", help="Request stop for a durable finance intelligence runtime.")
    intelligence_stop.add_argument("--session-id", required=True, help="Durable watch-session id to stop.")
    _add_report_format(intelligence_stop)
    intelligence_resume = intelligence_subparsers.add_parser("resume", help="Resume a stopped finance intelligence runtime from its durable argv.")
    intelligence_resume.add_argument("--session-id", required=True, help="Durable watch-session id to resume.")
    _add_report_format(intelligence_resume)
    intelligence_evaluate = intelligence_subparsers.add_parser("evaluate", help="Evaluate persisted finance intelligence replay quality.")
    intelligence_evaluate.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_evaluate.add_argument("--session-id", required=True, help="Durable watch-session id to evaluate.")
    _add_report_format(intelligence_evaluate)
    intelligence_eval_report = intelligence_subparsers.add_parser("eval-report", help="Render the latest persisted finance intelligence evaluation report.")
    intelligence_eval_report.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_eval_report.add_argument("--session-id", required=True, help="Durable watch-session id to report.")
    _add_report_format(intelligence_eval_report)
    intelligence_supervise = intelligence_subparsers.add_parser("supervise", help="Continuously review and persist intelligence notes.")
    intelligence_supervise.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_supervise.add_argument("--session-id", required=True, help="Durable watch-session id to supervise.")
    intelligence_supervise.add_argument("--log-limit", type=int, default=20, help="Number of runtime log lines to include in each review.")
    intelligence_supervise.add_argument("--iterations", type=int, default=0, help="Number of review cycles. Use 0 to run until interrupted.")
    intelligence_supervise.add_argument("--interval-sec", type=float, default=60.0, help="Seconds between review cycles.")
    intelligence_supervise.add_argument("--jsonl", action="store_true", help="Emit one JSON intelligence review per cycle.")
    _add_report_format(intelligence_supervise)

    watch_session = finance_subparsers.add_parser(
        "watch-session",
        help="Run or inspect durable finance watch sessions.",
        description="Create and query watch sessions that persist market context, fundamentals, news, realtime stream signals, and review data.",
    )
    watch_session_subparsers = watch_session.add_subparsers(dest="watch_session_command", required=True)
    watch_start = watch_session_subparsers.add_parser("start", help="Start a durable watch session.")
    _add_watch_session_start_args(watch_start)
    for command in ("list", "show", "events", "signals", "summarize", "status", "logs", "review", "supervise", "stop"):
        sub = watch_session_subparsers.add_parser(command, help=f"{command} durable watch-session data.")
        if command != "list":
            sub.add_argument("--session-id", required=True)
        if command == "events":
            sub.add_argument("--feed", choices=["all", "quotes", "history", "news", "features", "candidates", "market_context", "fundamentals", "native_stream_signals"], default="all")
        if command in {"events", "signals", "logs"}:
            sub.add_argument("--limit", type=int, default=100)
        if command in {"review", "supervise"}:
            sub.add_argument("--log-limit", type=int, default=20, help="Number of runtime log lines to include in each review.")
        if command == "supervise":
            sub.add_argument("--iterations", type=int, default=0, help="Number of review cycles. Use 0 to run until interrupted.")
            sub.add_argument("--interval-sec", type=float, default=60.0, help="Seconds between review cycles.")
            sub.add_argument("--jsonl", action="store_true", help="Emit one JSON review per cycle.")
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
    _add_provider_market(quotes, default_provider="tencent", choices=provider_names_for_operation("fetch_quotes"))
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

    fundamentals = finance_subparsers.add_parser(
        "fetch-fundamentals",
        help="Fetch public fundamentals evidence rows.",
        description="Fetch public fundamentals evidence rows and preserve provider diagnostics instead of mocking unavailable data.",
    )
    _add_provider_market(fundamentals, default_provider="sec-companyfacts", choices=provider_names_for_operation("fetch_fundamentals"))
    fundamentals.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. AAPL,MSFT,NVDA.")
    _add_output(fundamentals)

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
        help="Public data provider. Run finance sources for provider capabilities and freshness metadata.",
    )
    parser.add_argument("--market", required=True, choices=["cn", "us"], help="Market: cn for A-share, us for U.S. stocks.")


def _add_watch_session_start_args(parser: argparse.ArgumentParser, *, include_intelligence_id: bool = False) -> None:
    if include_intelligence_id:
        parser.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    parser.add_argument("--session-id", help="Defaults to finance_<market>_watch_<UTC timestamp>.")
    parser.add_argument("--market", required=True, choices=["cn", "us"])
    parser.add_argument("--symbols", required=True, help="Comma-separated candidate symbols.")
    parser.add_argument("--market-symbols", help="Comma-separated market context symbols.")
    parser.add_argument("--history-provider", default="yahoo", choices=provider_names_for_operation("fetch_history"))
    parser.add_argument("--quote-provider", default="tencent", choices=provider_names_for_operation("fetch_quotes"))
    parser.add_argument("--news-provider", default="google-news", choices=provider_names_for_operation("fetch_news"))
    parser.add_argument("--fundamentals-provider", default="public-unavailable", choices=["public-unavailable", *provider_names_for_operation("fetch_fundamentals")])
    parser.add_argument("--start-date", required=True, help="YYYYMMDD.")
    parser.add_argument("--end-date", required=True, help="YYYYMMDD.")
    parser.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    parser.add_argument("--adjust", default="")
    parser.add_argument("--top", type=int, default=3)
    parser.add_argument("--news-limit", type=int, default=5)
    parser.add_argument("--entry-score-threshold", type=float, default=8.0)
    parser.add_argument("--entry-return-threshold", type=float, default=5.0)
    parser.add_argument("--exit-score-threshold", type=float, default=0.0)
    parser.add_argument("--exit-quote-pct-threshold", type=float, default=-3.0)
    parser.add_argument("--signal-policy-preset", default="balanced", choices=["balanced", "momentum", "defensive"])
    parser.add_argument("--signal-policy", help="JSON signal policy override. Supports entry.all/exit.any condition lists with field/op/value.")
    parser.add_argument("--native-stream-poll-timeout-sec", type=float, default=2.0)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--until-time", help="Run until this RFC3339 timestamp.")
    parser.add_argument("--jsonl", action="store_true")
    parser.add_argument("--async-run", action="store_true")
    _add_report_format(parser)


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
    if args.finance_command == "intelligence":
        argv.append(str(args.intelligence_command))
    for name in (
        "provider",
        "intelligence_id",
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
        "feed",
        "index_mode",
        "limit",
        "log_limit",
        "start_time",
        "end_time",
        "history_output",
        "history_output_format",
        "preview_rows",
        "top",
        "top_k",
        "news_limit",
        "source_id",
        "monitor_id_prefix",
        "native_stream_poll_timeout_sec",
        "stream_window_size",
        "entry_score_threshold",
        "entry_return_threshold",
        "exit_score_threshold",
        "exit_quote_pct_threshold",
        "signal_policy_preset",
        "signal_policy",
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
