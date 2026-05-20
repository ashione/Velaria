from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import velaria

from velaria.agentic_dsl import compile_rule_spec
from velaria.agentic_runtime import execute_monitor_once
from velaria.agentic_store import AgenticStore

from . import (
    FinanceProviderError,
    build_research_prompt,
    evaluate_news_sentiment,
    fetch_fundamentals,
    fetch_history,
    fetch_news,
    fetch_quotes,
    finance_quote_schema_binding,
    provider_catalog,
    provider_names_for_operation,
)
from .evidence_index import (
    DEFAULT_FINANCE_EVIDENCE_RETRIEVER,
    EvidenceSearchOptions,
    FINANCE_EVIDENCE_FEEDS,
    FINANCE_EVIDENCE_SEMANTIC_REASON,
    FINANCE_EVIDENCE_SEMANTIC_STATUS,
    build_finance_evidence_index,
    finance_evidence_index_metadata_for_payload,
    hybrid_search_finance_rows,
)
from .evaluation import (
    append_finance_evaluation_event,
    build_finance_evaluation_report,
    evaluate_finance_session,
    latest_finance_evaluation,
)
from .jobs import append_finance_job_event, finance_job_payload, latest_finance_jobs, watch_run_job_payload


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "sources":
            return _run_sources(args)
        if args.command == "doctor":
            return _run_doctor(args)
        if args.command == "analyze":
            return _analyze_symbol(args)
        if args.command == "pipeline":
            return _run_pipeline(args)
        if args.command == "rank-candidates":
            return _rank_candidates(args)
        if args.command == "stream-history":
            return _stream_history(args)
        if args.command == "watch-session":
            return _watch_session(args)
        if args.command == "intelligence":
            return _intelligence(args)
        if args.command == "fetch-history":
            rows = fetch_history(
                provider=args.provider,
                market=args.market,
                symbol=args.symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                period=args.period,
                adjust=args.adjust,
            )
            output = pathlib.Path(args.output) if args.output else None
            if output is not None:
                _write_rows(output, rows, args.output_format)
            return _emit_json(
                {
                    "ok": True,
                    "action": "fetch-history",
                    "provider": args.provider,
                    "market": args.market,
                    "symbol": args.symbol,
                    "row_count": len(rows),
                    "output": str(output) if output else None,
                    "format": args.output_format if output else None,
                    "preview": rows[: args.preview_rows],
                }
            )
        if args.command == "fetch-quotes":
            rows = fetch_quotes(provider=args.provider, market=args.market, symbols=args.symbols)
            output = pathlib.Path(args.output) if args.output else None
            if output is not None:
                _write_rows(output, rows, args.output_format)
            return _emit_json(
                {
                    "ok": True,
                    "action": "fetch-quotes",
                    "provider": args.provider,
                    "market": args.market,
                    "symbols": _split_symbols(args.symbols),
                    "row_count": len(rows),
                    "output": str(output) if output else None,
                    "format": args.output_format if output else None,
                    "preview": rows[: args.preview_rows],
                }
            )
        if args.command == "fetch-news":
            rows = fetch_news(provider=args.provider, market=args.market, symbol=args.symbol, query=args.query, limit=args.limit)
            output = pathlib.Path(args.output) if args.output else None
            if output is not None:
                _write_rows(output, rows, args.output_format)
            return _emit_json(
                {
                    "ok": True,
                    "action": "fetch-news",
                    "provider": args.provider,
                    "market": args.market,
                    "symbol": args.symbol,
                    "query": args.query,
                    "row_count": len(rows),
                    "sentiment": evaluate_news_sentiment(rows),
                    "output": str(output) if output else None,
                    "format": args.output_format if output else None,
                    "preview": rows[: args.preview_rows],
                }
            )
        if args.command == "fetch-fundamentals":
            rows = fetch_fundamentals(provider=args.provider, market=args.market, symbols=args.symbols)
            output = pathlib.Path(args.output) if args.output else None
            if output is not None:
                _write_rows(output, rows, args.output_format)
            return _emit_json(
                {
                    "ok": True,
                    "action": "fetch-fundamentals",
                    "provider": args.provider,
                    "market": args.market,
                    "symbols": _split_symbols(args.symbols),
                    "row_count": len(rows),
                    "output": str(output) if output else None,
                    "format": args.output_format if output else None,
                    "preview": rows[: args.preview_rows],
                }
            )
        if args.command == "ingest-quotes":
            rows = fetch_quotes(provider=args.provider, market=args.market, symbols=args.symbols)
            source_id = args.source_id or f"finance_{args.market}_quotes"
            with AgenticStore() as store:
                source = store.upsert_source(
                    {
                        "source_id": source_id,
                        "kind": "external_event",
                        "name": args.name or source_id,
                        "schema_binding": finance_quote_schema_binding(),
                        "metadata": {
                            "domain": "finance",
                            "provider": args.provider,
                            "market": args.market,
                            "symbols": _split_symbols(args.symbols),
                            "license_note": rows[0].get("license_note") if rows else None,
                        },
                    }
                )
                observations = [store.append_external_event(source_id, row) for row in rows]
            return _emit_json(
                {
                    "ok": True,
                    "action": "ingest-quotes",
                    "source": source,
                    "observations": observations,
                    "row_count": len(observations),
                }
            )
        if args.command == "watch":
            return _watch_quotes(args)
    except FinanceProviderError as exc:
        return _emit_json({"ok": False, **exc.to_payload()}, exit_code=1)
    raise AssertionError(f"unhandled command: {args.command}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m velaria.finance_pack.cli",
        description="Public-data finance helpers for Velaria agentic monitor workflows.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sources = subparsers.add_parser("sources", help="List public finance providers and supported workflows.")
    _add_report_format(sources)

    doctor = subparsers.add_parser("doctor", help="Check finance dependencies and public quote provider reachability.")
    doctor.add_argument("--market", default="cn", choices=["cn", "us"], help="Market used for the quote provider probe.")
    doctor.add_argument("--symbol", default="000001", help="Symbol used for the quote provider probe.")
    doctor.add_argument("--skip-network", action="store_true", help="Skip public provider network probes.")
    _add_report_format(doctor)

    analyze = subparsers.add_parser(
        "analyze",
        help="Fetch one quote, ingest it, run a monitor, and print a readable research report.",
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

    pipeline = subparsers.add_parser(
        "pipeline",
        help="Fetch history, subscribe to live quotes, run a monitor, and emit a complete analysis chain.",
    )
    pipeline.add_argument("--market", required=True, choices=["cn", "us"])
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

    rank = subparsers.add_parser(
        "rank-candidates",
        help="Continuously rank top research candidates from quotes, history, derived metrics, news, and sentiment.",
    )
    rank.add_argument("--market", required=True, choices=["cn", "us"])
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

    stream_history = subparsers.add_parser(
        "stream-history",
        help="Query durable finance stream output history stored by native stream runs.",
    )
    stream_history.add_argument("--market", default="cn", choices=["cn", "us"], help="Market used for the default source id.")
    stream_history.add_argument("--source-id", help="Defaults to finance_<market>_rank_candidates_native_stream_signals.")
    stream_history.add_argument("--start-time", help="Inclusive RFC3339 event_time lower bound.")
    stream_history.add_argument("--end-time", help="Exclusive RFC3339 event_time upper bound.")
    stream_history.add_argument("--limit", type=int, default=50, help="Return the last N matching stream rows.")
    _add_report_format(stream_history)

    intelligence = subparsers.add_parser(
        "intelligence",
        help="Run a productized finance intelligence loop over public data, native stream, persistence, replay, and agent briefs.",
    )
    intelligence_subparsers = intelligence.add_subparsers(dest="intelligence_command", required=True)
    intelligence_start = intelligence_subparsers.add_parser("start", help="Start the full finance intelligence chain.")
    _add_watch_session_start_args(intelligence_start, include_intelligence_id=True)
    intelligence_review = intelligence_subparsers.add_parser("review", help="Persist an agent-readable intelligence review for a watch session.")
    intelligence_review.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_review.add_argument("--session-id", required=True, help="Durable watch-session id to review.")
    intelligence_review.add_argument("--log-limit", type=int, default=20, help="Number of runtime log lines to include.")
    _add_report_format(intelligence_review)
    intelligence_replay = intelligence_subparsers.add_parser("replay", help="Replay persisted realtime watch data as historical research evidence.")
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
    intelligence_index.add_argument("--feed", choices=FINANCE_EVIDENCE_FEEDS, default="all")
    _add_report_format(intelligence_index)
    intelligence_search = intelligence_subparsers.add_parser("search", help="Hybrid-search persisted finance evidence for a watch session.")
    intelligence_search.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_search.add_argument("--session-id", required=True, help="Durable watch-session id to search.")
    intelligence_search.add_argument("--query", required=True, help="Evidence query text, e.g. NVDA momentum risk news fundamentals.")
    intelligence_search.add_argument("--top-k", type=int, default=5, help="Number of fused evidence hits.")
    intelligence_search.add_argument("--feed", choices=FINANCE_EVIDENCE_FEEDS, default="all")
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
    intelligence_supervise = intelligence_subparsers.add_parser("supervise", help="Continuously review and persist intelligence notes inside the CLI process.")
    intelligence_supervise.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    intelligence_supervise.add_argument("--session-id", required=True, help="Durable watch-session id to supervise.")
    intelligence_supervise.add_argument("--log-limit", type=int, default=20, help="Number of runtime log lines to include in each review.")
    intelligence_supervise.add_argument("--iterations", type=int, default=0, help="Number of review cycles. Use 0 to run until interrupted.")
    intelligence_supervise.add_argument("--interval-sec", type=float, default=60.0, help="Seconds between review cycles.")
    intelligence_supervise.add_argument("--jsonl", action="store_true", help="Emit one JSON intelligence review per cycle.")
    _add_report_format(intelligence_supervise)

    watch_session = subparsers.add_parser(
        "watch-session",
        help="Run or inspect a durable finance watch session with market context, fundamentals, news, and stream signals.",
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

    history = subparsers.add_parser("fetch-history", help="Fetch public historical OHLCV data.")
    _add_provider_market(history, default_provider="yahoo", choices=provider_names_for_operation("fetch_history"))
    history.add_argument("--symbol", required=True, help="Provider-specific symbol, e.g. 000001 or 105.AAPL.")
    history.add_argument("--start-date", required=True, help="YYYYMMDD.")
    history.add_argument("--end-date", required=True, help="YYYYMMDD.")
    history.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    history.add_argument("--adjust", default="", help="Provider adjustment flag, e.g. qfq/hfq for AkShare.")
    _add_output(history)

    quotes = subparsers.add_parser("fetch-quotes", help="Fetch public quote rows.")
    _add_provider_market(quotes, default_provider="tencent", choices=provider_names_for_operation("fetch_quotes"))
    quotes.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    _add_output(quotes)

    news = subparsers.add_parser("fetch-news", help="Fetch public news rows and sentiment evidence.")
    news.add_argument("--provider", default="google-news", choices=provider_names_for_operation("fetch_news"))
    news.add_argument("--market", required=True, choices=["cn", "us"])
    news.add_argument("--symbol", required=True, help="Single symbol, e.g. 000001 or AAPL.")
    news.add_argument("--query", help="Override provider search query. Defaults to a market-aware symbol query.")
    news.add_argument("--limit", type=int, default=5, help="Maximum news rows to fetch.")
    _add_output(news)

    fundamentals = subparsers.add_parser("fetch-fundamentals", help="Fetch public fundamentals evidence rows.")
    _add_provider_market(fundamentals, default_provider="sec-companyfacts", choices=provider_names_for_operation("fetch_fundamentals"))
    fundamentals.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. AAPL,MSFT,NVDA.")
    _add_output(fundamentals)

    ingest = subparsers.add_parser("ingest-quotes", help="Fetch quotes and append them to a Velaria external_event source.")
    _add_provider_market(ingest, default_provider="tencent")
    ingest.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    ingest.add_argument("--source-id", help="Defaults to finance_<market>_quotes.")
    ingest.add_argument("--name", help="Source display name.")

    watch = subparsers.add_parser(
        "watch",
        help="Watch one public quote symbol, ingest observations, run a monitor, and emit analysis context.",
    )
    _add_provider_market(watch, default_provider="tencent")
    watch.add_argument("--symbol", required=True, help="Single symbol to watch, e.g. 000001 or AAPL.")
    watch.add_argument("--source-id", help="Defaults to finance_<market>_<symbol>_watch.")
    watch.add_argument("--monitor-id", help="Defaults to monitor_<source_id>.")
    watch.add_argument("--name", help="Source and monitor display name.")
    watch.add_argument("--interval-sec", type=float, default=30.0, help="Seconds between polls.")
    watch.add_argument("--iterations", type=int, default=1, help="Number of polling iterations. Use 0 to run until interrupted.")
    watch.add_argument("--pct-change-threshold", type=float, help="Only create focus events when ABS(pct_change) is at least this value.")
    watch.add_argument("--min-price", type=float, help="Only create focus events when price is at least this value.")
    watch.add_argument("--max-price", type=float, help="Only create focus events when price is at most this value.")
    watch.add_argument("--cooldown-sec", type=int, default=0, help="FocusEvent suppression cooldown for this watch monitor.")
    watch.add_argument("--jsonl", action="store_true", help="Emit one JSON object per tick instead of one final JSON object.")
    watch.add_argument("--no-analysis-prompt", action="store_true", help="Omit the Velaria Agent research prompt from output.")

    return parser


def _add_provider_market(parser: argparse.ArgumentParser, *, default_provider: str, choices: list[str] | None = None) -> None:
    parser.add_argument(
        "--provider",
        default=default_provider,
        choices=choices or ["akshare", "tencent"],
        help="Public data provider. Run finance sources for provider capabilities and freshness metadata.",
    )
    parser.add_argument("--market", required=True, choices=["cn", "us"])


def _add_watch_session_start_args(parser: argparse.ArgumentParser, *, include_intelligence_id: bool = False) -> None:
    if include_intelligence_id:
        parser.add_argument("--intelligence-id", help="Defaults to intelligence_<watch session id>.")
    parser.add_argument("--session-id", help="Defaults to finance_<market>_watch_<UTC timestamp>.")
    parser.add_argument("--market", required=True, choices=["cn", "us"])
    parser.add_argument("--symbols", required=True, help="Comma-separated candidate symbols.")
    parser.add_argument("--market-symbols", help="Comma-separated market context symbols. Defaults to broad market proxies.")
    parser.add_argument("--history-provider", default="yahoo", choices=provider_names_for_operation("fetch_history"))
    parser.add_argument("--quote-provider", default="tencent", choices=provider_names_for_operation("fetch_quotes"))
    parser.add_argument("--news-provider", default="google-news", choices=provider_names_for_operation("fetch_news"))
    parser.add_argument(
        "--fundamentals-provider",
        default="public-unavailable",
        choices=["public-unavailable", *provider_names_for_operation("fetch_fundamentals")],
        help="Fundamentals provider name; unavailable providers are recorded as evidence, not mocked.",
    )
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
    parser.add_argument("--signal-policy-preset", default="balanced", choices=["balanced", "momentum", "defensive"], help="Signal policy preset used to compute entry_signal/exit_signal before native stream filtering.")
    parser.add_argument("--signal-policy", help="JSON signal policy override. Supports entry.all/exit.any condition lists with field/op/value.")
    parser.add_argument("--native-stream-poll-timeout-sec", type=float, default=2.0)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--iterations", type=int, default=1, help="Use 0 to run until interrupted.")
    parser.add_argument("--until-time", help="Run until this RFC3339 timestamp.")
    parser.add_argument("--jsonl", action="store_true", help="Emit one JSON object per watch tick.")
    parser.add_argument("--async-run", action="store_true", help="Start the watch session in a background CLI process and return immediately.")
    _add_report_format(parser)


def _add_output(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", help="Optional output path.")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "jsonl"])
    parser.add_argument("--preview-rows", type=int, default=5)


def _add_report_format(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", dest="report_format", default="text", choices=["text", "json"], help="Output format.")


def _run_sources(args: argparse.Namespace) -> int:
    payload = {
        "ok": True,
        "action": "sources",
        "sources": provider_catalog(),
        "next_steps": [
            "finance doctor",
            "finance pipeline --market cn --symbol 000001 --start-date 20250101 --end-date 20250131",
            "finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --top 3",
            "finance fetch-fundamentals --provider sec-companyfacts --market us --symbols AAPL,MSFT,NVDA",
            "finance intelligence start --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --iterations 0 --format json",
            "finance analyze --market cn --symbol 000001",
            "finance watch --market cn --symbol 000001 --iterations 0 --jsonl",
        ],
        "disclaimer": "Research assistance only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_sources(payload))
    return 0


def _stream_history(args: argparse.Namespace) -> int:
    source_id = args.source_id or f"finance_{args.market}_rank_candidates_native_stream_signals"
    with AgenticStore() as store:
        source = store.get_source(source_id)
        if source is None:
            raise FinanceProviderError(
                f"Finance stream history source not found: {source_id}",
                error_type="stream_history_not_found",
                hint="Run finance rank-candidates with --native-stream first, or pass --source-id for an existing stream output source.",
                details={"source_id": source_id},
            )
        rows = store.read_external_events(
            source_id,
            start_time=args.start_time,
            end_time=args.end_time,
            limit=max(0, int(args.limit)),
        )
    payload = {
        "ok": True,
        "action": "stream-history",
        "source": source,
        "row_count": len(rows),
        "rows": rows,
        "start_time": args.start_time,
        "end_time": args.end_time,
        "limit": max(0, int(args.limit)),
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_stream_history_report(payload))
    return 0


def _watch_session(args: argparse.Namespace) -> int:
    command = args.watch_session_command
    if command == "start":
        return _watch_session_start(args)
    if command == "list":
        return _watch_session_list(args)
    if command == "show":
        return _watch_session_show(args)
    if command == "events":
        return _watch_session_events(args)
    if command == "signals":
        return _watch_session_signals(args)
    if command == "summarize":
        return _watch_session_summarize(args)
    if command == "status":
        return _watch_session_status(args)
    if command == "logs":
        return _watch_session_logs(args)
    if command == "review":
        return _watch_session_review(args)
    if command == "supervise":
        return _watch_session_supervise(args)
    if command == "stop":
        return _watch_session_stop(args)
    raise AssertionError(f"unhandled watch-session command: {command}")


def _intelligence(args: argparse.Namespace) -> int:
    command = args.intelligence_command
    if command == "start":
        return _intelligence_start(args)
    if command == "review":
        return _intelligence_review(args)
    if command == "replay":
        return _intelligence_replay(args)
    if command == "report":
        return _intelligence_report(args)
    if command == "index":
        return _intelligence_index(args)
    if command == "search":
        return _intelligence_search(args)
    if command == "jobs":
        return _intelligence_jobs(args)
    if command == "status":
        return _intelligence_status(args)
    if command == "stop":
        return _intelligence_stop(args)
    if command == "resume":
        return _intelligence_resume(args)
    if command == "evaluate":
        return _intelligence_evaluate(args)
    if command == "eval-report":
        return _intelligence_eval_report(args)
    if command == "supervise":
        return _intelligence_supervise(args)
    raise AssertionError(f"unhandled intelligence command: {command}")


def _intelligence_start(args: argparse.Namespace) -> int:
    if getattr(args, "async_run", False):
        watch_payload = _watch_session_start_async_payload(args)
        session_id = str(watch_payload["watch_session_id"])
        intelligence_id = args.intelligence_id or _make_intelligence_id(session_id)
        intelligence_session = _append_intelligence_session_event(
            _intelligence_session_payload(
                intelligence_id=intelligence_id,
                watch_session_id=session_id,
                status="running",
                market=args.market,
                symbols=_split_symbols(args.symbols),
                raw_sources={},
                tick_count=0,
            )
        )
        _append_finance_watch_job(
            watch_session_id=session_id,
            intelligence_id=intelligence_id,
            status="running",
            run=dict(watch_payload.get("run") or {}),
            summary={"tick_count": 0, "mode": "async"},
        )
        payload = {
            "ok": True,
            "action": "intelligence-async-start",
            "intelligence_id": intelligence_id,
            "watch_session_id": session_id,
            "watch_runtime": watch_payload,
            "intelligence_session": intelligence_session,
            "runtime_plane": _intelligence_runtime_plane(watch_payload),
            "data_plane": _intelligence_data_plane({}, _empty_watch_session_summary(session_id=session_id)),
            "ai_plane": _intelligence_ai_plane(
                intelligence_id=intelligence_id,
                watch_session_id=session_id,
                summary=_empty_watch_session_summary(session_id=session_id),
            ),
            "next_steps": [
                f"finance intelligence review --session-id {session_id} --format json",
                f"finance intelligence replay --session-id {session_id} --format json",
                f"finance intelligence supervise --session-id {session_id} --interval-sec 60 --format json",
            ],
            "disclaimer": "Research candidates and realtime signals only; not investment advice.",
        }
        if args.report_format == "json":
            return _emit_json(payload)
        print(_render_intelligence_report(payload))
        return 0

    watch_payload = _watch_session_start_payload(args, emit_jsonl=bool(args.jsonl))
    watch_session = watch_payload["watch_session"]
    session_id = str(watch_session["session_id"])
    intelligence_id = args.intelligence_id or _make_intelligence_id(session_id)
    summary = _compact_watch_session_summary(_summarize_watch_session(watch_session))
    intelligence_session = _append_intelligence_session_event(
        _intelligence_session_payload(
            intelligence_id=intelligence_id,
            watch_session_id=session_id,
            status=str(watch_session.get("status") or "completed"),
            market=str(watch_session.get("market") or args.market),
            symbols=list(watch_session.get("symbols") or _split_symbols(args.symbols)),
            raw_sources=dict(watch_payload.get("raw_sources") or {}),
            tick_count=int(watch_payload.get("tick_count") or 0),
        )
    )
    ai_note = _append_intelligence_ai_note_event(
        _intelligence_ai_note_payload(
            intelligence_id=intelligence_id,
            watch_session_id=session_id,
            summary=summary,
            diagnostics=[],
        )
    )
    _append_finance_watch_job(
        watch_session_id=session_id,
        intelligence_id=intelligence_id,
        status=str(watch_session.get("status") or "completed"),
        run={},
        summary={"tick_count": int(watch_payload.get("tick_count") or 0), "mode": "sync"},
    )
    payload = {
        "ok": True,
        "action": "intelligence-start",
        "intelligence_id": intelligence_id,
        "watch_session": watch_session,
        "intelligence_session": intelligence_session,
        "runtime_plane": _intelligence_runtime_plane(watch_payload),
        "data_plane": _intelligence_data_plane(dict(watch_payload.get("raw_sources") or {}), summary),
        "ai_plane": _intelligence_ai_plane(intelligence_id=intelligence_id, watch_session_id=session_id, summary=summary, ai_note=ai_note),
        "research_candidates": watch_payload.get("research_candidates") or [],
        "stream_signals": watch_payload.get("stream_signals") or [],
        "next_steps": [
            f"finance intelligence review --session-id {session_id} --format json",
            f"finance intelligence replay --session-id {session_id} --format json",
            f"finance watch-session events --session-id {session_id} --feed all --format json",
        ],
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }
    if args.jsonl:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        return 0
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _watch_session_start(args: argparse.Namespace) -> int:
    if getattr(args, "async_run", False):
        return _watch_session_start_async(args)
    payload = _watch_session_start_payload(args, emit_jsonl=bool(args.jsonl))
    if args.jsonl:
        print(
            json.dumps(
                {
                    "ok": True,
                    "action": "watch-session-start",
                    "watch_session_id": payload["watch_session"]["session_id"],
                    "interrupted": payload["interrupted"],
                    "ticks": payload["tick_count"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 0
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_start_report(payload))
    return 0


def _watch_session_start_payload(args: argparse.Namespace, *, emit_jsonl: bool = False) -> dict[str, Any]:
    symbols = _split_symbols(args.symbols)
    session_id = args.session_id or _make_watch_session_id(args.market)
    args.watch_session_id = session_id
    args.source_id = f"{session_id}_candidates"
    args.native_stream = True
    args.ingest_raw = True
    args.stream_monitor = False
    args.monitor_id_prefix = None
    args.cooldown_sec = 0
    raw_sources: dict[str, dict[str, Any]] = {}
    native_stream: dict[str, Any] = {}
    ticks: list[dict[str, Any]] = []
    interrupted = False
    source = _upsert_rank_source(args, source_id=args.source_id, symbols=symbols)
    try:
        raw_sources = _upsert_rank_raw_sources(args, source_id=args.source_id, symbols=symbols, candidate_source=source)
        _append_watch_session_event(args, session_id=session_id, status="running", raw_sources=raw_sources, tick_count=0)
        native_stream = _start_rank_native_stream(args)
        iteration = 0
        while _should_continue_rank_loop(args, iteration):
            iteration += 1
            tick = _run_rank_tick(
                args,
                symbols=symbols,
                source_id=args.source_id,
                raw_sources=raw_sources,
                stream_monitors=[],
                native_stream=native_stream,
                iteration=iteration,
            )
            ticks.append(tick)
            if emit_jsonl:
                print(json.dumps({"watch_session_id": session_id, **tick}, ensure_ascii=False, sort_keys=True), flush=True)
            if not _should_continue_rank_loop(args, iteration):
                break
            time.sleep(max(0.0, float(args.interval_sec)))
    except KeyboardInterrupt:
        interrupted = True
    finally:
        _stop_rank_native_stream(native_stream)
        if raw_sources:
            _append_watch_session_event(
                args,
                session_id=session_id,
                status="interrupted" if interrupted else "completed",
                raw_sources=raw_sources,
                tick_count=len(ticks),
            )
    latest = ticks[-1] if ticks else {"research_candidates": []}
    return {
        "ok": True,
        "action": "watch-session-start",
        "watch_session": _watch_session_payload(args, session_id=session_id, status="interrupted" if interrupted else "completed", raw_sources=raw_sources, tick_count=len(ticks)),
        "raw_sources": raw_sources,
        "native_stream": _rank_native_stream_public_payload(native_stream),
        "ticks": ticks,
        "tick_count": len(ticks),
        "interrupted": interrupted,
        "research_candidates": latest.get("research_candidates") or [],
        "stream_signals": [signal for tick in ticks for signal in tick.get("native_stream_signals", [])],
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }


def _watch_session_start_async(args: argparse.Namespace) -> int:
    payload = _watch_session_start_async_payload(args)
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_async_start_report(payload))
    return 0


def _watch_session_start_async_payload(args: argparse.Namespace) -> dict[str, Any]:
    session_id = args.session_id or _make_watch_session_id(args.market)
    args.watch_session_id = session_id
    log_dir = get_finance_watch_session_run_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{session_id}.jsonl"
    child_argv = _watch_session_child_argv(args, session_id=session_id)
    env = os.environ.copy()
    with log_path.open("ab") as log_handle:
        process = subprocess.Popen(
            child_argv,
            cwd=str(pathlib.Path.cwd()),
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    run = {
        "session_id": session_id,
        "status": "running",
        "pid": int(process.pid),
        "process_identity": _watch_session_process_identity(pid=int(process.pid), argv=child_argv, session_id=session_id),
        "log_path": str(log_path),
        "argv": child_argv,
        "started_at": _utc_payload_time(),
        "updated_at": _utc_payload_time(),
        "runner": "velaria.finance_pack.cli",
        "core_runtime": "velaria_native_realtime_stream",
        "ai_cli_runtime": "velaria_cli_run",
        "event_time": _utc_payload_time(),
        "event_type": "watch_session_async_start",
        "source_key": session_id,
    }
    _append_watch_session_run_event(run)
    payload = {
        "ok": True,
        "action": "watch-session-async-start",
        "watch_session_id": session_id,
        "run": run,
        "next_steps": [
            f"finance watch-session status --session-id {session_id} --format json",
            f"finance watch-session logs --session-id {session_id} --limit 20 --format json",
            f"finance watch-session signals --session-id {session_id} --format json",
            f"finance watch-session summarize --session-id {session_id} --format json",
        ],
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }
    return payload


def _watch_session_child_argv(args: argparse.Namespace, *, session_id: str) -> list[str]:
    argv = [
        sys.executable,
        "-m",
        "velaria.finance_pack.cli",
        "watch-session",
        "start",
        "--session-id",
        session_id,
        "--market",
        str(args.market),
        "--symbols",
        str(args.symbols),
        "--history-provider",
        str(args.history_provider),
        "--quote-provider",
        str(args.quote_provider),
        "--news-provider",
        str(args.news_provider),
        "--fundamentals-provider",
        str(args.fundamentals_provider),
        "--start-date",
        str(args.start_date),
        "--end-date",
        str(args.end_date),
        "--period",
        str(args.period),
        "--adjust",
        str(args.adjust),
        "--top",
        str(args.top),
        "--news-limit",
        str(args.news_limit),
        "--entry-score-threshold",
        str(args.entry_score_threshold),
        "--entry-return-threshold",
        str(args.entry_return_threshold),
        "--exit-score-threshold",
        str(args.exit_score_threshold),
        "--exit-quote-pct-threshold",
        str(args.exit_quote_pct_threshold),
        "--signal-policy-preset",
        str(args.signal_policy_preset),
        "--native-stream-poll-timeout-sec",
        str(args.native_stream_poll_timeout_sec),
        "--interval-sec",
        str(args.interval_sec),
        "--iterations",
        str(args.iterations),
        "--format",
        "json",
        "--jsonl",
    ]
    if args.market_symbols:
        argv.extend(["--market-symbols", str(args.market_symbols)])
    if args.until_time:
        argv.extend(["--until-time", str(args.until_time)])
    if getattr(args, "signal_policy", None):
        argv.extend(["--signal-policy", str(args.signal_policy)])
    return argv


def get_finance_watch_session_run_dir() -> pathlib.Path:
    from velaria.workspace.paths import get_velaria_home

    return get_velaria_home() / "agentic" / "finance_watch_sessions"


def _watch_session_run_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "session_id",
        "field_mappings": {
            "session_id": "session_id",
            "status": "status",
            "pid": "pid",
        },
    }


def _append_watch_session_run_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        store.upsert_source(
            {
                "source_id": "finance_watch_session_runs",
                "kind": "external_event",
                "name": "finance watch session runtime processes",
                "schema_binding": _watch_session_run_source_binding(),
                "metadata": {"domain": "finance", "workflow": "watch-session", "runtime": "async-cli"},
            }
        )
        return store.append_external_event("finance_watch_session_runs", payload)


def _watch_session_review_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "session_id",
        "field_mappings": {
            "session_id": "session_id",
            "effective_status": "effective_status",
            "diagnostic_count": "diagnostic_count",
            "process_running": "process_running",
        },
    }


def _append_watch_session_review_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        store.upsert_source(
            {
                "source_id": "finance_watch_session_reviews",
                "kind": "external_event",
                "name": "finance watch session continuous review events",
                "schema_binding": _watch_session_review_source_binding(),
                "metadata": {
                    "domain": "finance",
                    "workflow": "watch-session",
                    "runtime": "continuous-review",
                },
            }
        )
        return store.append_external_event("finance_watch_session_reviews", payload)


def _watch_session_list(args: argparse.Namespace) -> int:
    sessions = _list_watch_sessions()
    payload = {"ok": True, "action": "watch-session-list", "sessions": sessions, "session_count": len(sessions)}
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_list_report(payload))
    return 0


def _watch_session_show(args: argparse.Namespace) -> int:
    session = _get_watch_session_or_raise(args.session_id)
    payload = {"ok": True, "action": "watch-session-show", "watch_session": session}
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_list_report({"sessions": [session], "session_count": 1}))
    return 0


def _watch_session_events(args: argparse.Namespace) -> int:
    session = _get_watch_session_or_raise(args.session_id)
    rows = _read_watch_session_events(session, feed=args.feed, limit=max(0, int(args.limit)))
    payload = {"ok": True, "action": "watch-session-events", "watch_session": session, "feed": args.feed, "row_count": len(rows), "rows": rows}
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_rows_report(payload))
    return 0


def _watch_session_signals(args: argparse.Namespace) -> int:
    session = _get_watch_session_or_raise(args.session_id)
    rows = _read_watch_session_events(session, feed="native_stream_signals", limit=max(0, int(args.limit)))
    payload = {"ok": True, "action": "watch-session-signals", "watch_session": session, "row_count": len(rows), "rows": rows}
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_rows_report(payload))
    return 0


def _watch_session_summarize(args: argparse.Namespace) -> int:
    session = _get_watch_session_or_raise(args.session_id)
    summary = _summarize_watch_session(session)
    payload = {"ok": True, "action": "watch-session-summarize", "watch_session": session, "summary": summary}
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_summary_report(payload))
    return 0


def _watch_session_status(args: argparse.Namespace) -> int:
    run = _get_watch_session_run_or_raise(args.session_id)
    session = _get_watch_session_or_none(args.session_id)
    process_running = _watch_session_run_process_running(run)
    payload = {
        "ok": True,
        "action": "watch-session-status",
        "watch_session_id": args.session_id,
        "run": run,
        "watch_session": session,
        "process_running": process_running,
        "process_identity_verified": process_running,
        "effective_status": _watch_session_effective_status(session=session, run=run, process_running=process_running),
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_status_report(payload))
    return 0


def _watch_session_logs(args: argparse.Namespace) -> int:
    run = _get_watch_session_run_or_raise(args.session_id)
    limit = max(0, int(args.limit))
    log_path, lines = _read_watch_session_log_lines(run, limit=limit)
    payload = {
        "ok": True,
        "action": "watch-session-logs",
        "watch_session_id": args.session_id,
        "run": run,
        "log_path": str(log_path),
        "line_count": len(lines),
        "lines": lines,
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_logs_report(payload))
    return 0


def _watch_session_review(args: argparse.Namespace) -> int:
    review = _build_watch_session_review(args.session_id, log_limit=max(0, int(args.log_limit)))
    _append_watch_session_review_event(_review_event_payload(review))
    payload = {"ok": True, "action": "watch-session-review", "review": review}
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_review_report(payload))
    return 0


def _watch_session_supervise(args: argparse.Namespace) -> int:
    iteration = 0
    reviews: list[dict[str, Any]] = []
    latest_review: dict[str, Any] | None = None
    interrupted = False
    keep_reviews = int(args.iterations) != 0 and not args.jsonl
    try:
        while int(args.iterations) == 0 or iteration < int(args.iterations):
            iteration += 1
            review = _build_watch_session_review(args.session_id, log_limit=max(0, int(args.log_limit)))
            review["supervisor_iteration"] = iteration
            _append_watch_session_review_event(_review_event_payload(review))
            latest_review = review
            if keep_reviews:
                reviews.append(review)
            if args.jsonl:
                print(json.dumps({"ok": True, "action": "watch-session-supervise-review", "review": review}, ensure_ascii=False, sort_keys=True), flush=True)
            if int(args.iterations) != 0 and iteration >= int(args.iterations):
                break
            time.sleep(max(0.0, float(args.interval_sec)))
    except KeyboardInterrupt:
        interrupted = True
    payload = {
        "ok": True,
        "action": "watch-session-supervise",
        "watch_session_id": args.session_id,
        "review_count": iteration,
        "interrupted": interrupted,
        "latest_review": latest_review,
        "reviews": [] if args.jsonl else reviews,
    }
    if args.jsonl:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        return 0
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_supervise_report(payload))
    return 0


def _watch_session_stop(args: argparse.Namespace) -> int:
    run = _get_watch_session_run_or_raise(args.session_id)
    pid = int(run.get("pid") or 0)
    signal_sent = False
    process_running = _watch_session_run_process_running(run)
    if process_running:
        os.kill(pid, signal.SIGTERM)
        signal_sent = True
    stopped = {
        **run,
        "status": "stop_requested" if signal_sent else "not_running",
        "updated_at": _utc_payload_time(),
        "event_time": _utc_payload_time(),
        "event_type": "watch_session_stop_requested" if signal_sent else "watch_session_stop_not_running",
        "source_key": args.session_id,
    }
    _append_watch_session_run_event(stopped)
    payload = {
        "ok": True,
        "action": "watch-session-stop",
        "watch_session_id": args.session_id,
        "run": stopped,
        "process_running": process_running,
        "process_identity_verified": process_running,
        "signal_sent": signal_sent,
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_watch_session_status_report(payload))
    return 0


def _make_watch_session_id(market: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"finance_{market}_watch_{stamp}"


def _watch_session_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "session_id",
        "field_mappings": {
            "session_id": "session_id",
            "status": "status",
            "market": "market",
            "tick_count": "tick_count",
        },
    }


def _watch_session_payload(
    args: argparse.Namespace,
    *,
    session_id: str,
    status: str,
    raw_sources: dict[str, dict[str, Any]],
    tick_count: int,
) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "status": status,
        "market": args.market,
        "symbols": _split_symbols(args.symbols),
        "market_symbols": _market_context_symbols(args),
        "history_provider": args.history_provider,
        "quote_provider": args.quote_provider,
        "news_provider": args.news_provider,
        "fundamentals_provider": args.fundamentals_provider,
        "started_at": getattr(args, "watch_session_started_at", None) or _utc_payload_time(),
        "updated_at": _utc_payload_time(),
        "tick_count": tick_count,
        "sources": {key: value.get("source_id") for key, value in raw_sources.items()},
    }


def _append_watch_session_event(
    args: argparse.Namespace,
    *,
    session_id: str,
    status: str,
    raw_sources: dict[str, dict[str, Any]],
    tick_count: int,
) -> dict[str, Any]:
    if not getattr(args, "watch_session_started_at", None):
        args.watch_session_started_at = _utc_payload_time()
    payload = _watch_session_payload(args, session_id=session_id, status=status, raw_sources=raw_sources, tick_count=tick_count)
    payload["event_time"] = _utc_payload_time()
    payload["event_type"] = f"watch_session_{status}"
    payload["source_key"] = session_id
    with AgenticStore() as store:
        store.upsert_source(
            {
                "source_id": "finance_watch_sessions",
                "kind": "external_event",
                "name": "finance watch sessions",
                "schema_binding": _watch_session_source_binding(),
                "metadata": {"domain": "finance", "workflow": "watch-session"},
            }
        )
        return store.append_external_event("finance_watch_sessions", payload)


def _list_watch_sessions() -> list[dict[str, Any]]:
    with AgenticStore() as store:
        if store.get_source("finance_watch_sessions") is None:
            return []
        rows = store.read_external_events("finance_watch_sessions")
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = dict(row.get("payload_json") or {})
        session_id = str(payload.get("session_id") or row.get("session_id") or "")
        if not session_id:
            continue
        latest[session_id] = payload
    return sorted(latest.values(), key=lambda item: str(item.get("updated_at") or ""), reverse=True)


def _get_watch_session_or_raise(session_id: str) -> dict[str, Any]:
    for session in _list_watch_sessions():
        if session.get("session_id") == session_id:
            return session
    raise FinanceProviderError(
        f"Finance watch session not found: {session_id}",
        error_type="watch_session_not_found",
        hint="Run finance watch-session list --format json to inspect available sessions.",
        details={"session_id": session_id},
    )


def _get_watch_session_or_none(session_id: str) -> dict[str, Any] | None:
    try:
        return _get_watch_session_or_raise(session_id)
    except FinanceProviderError:
        return None


def _list_watch_session_runs() -> list[dict[str, Any]]:
    with AgenticStore() as store:
        if store.get_source("finance_watch_session_runs") is None:
            return []
        rows = store.read_external_events("finance_watch_session_runs")
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = dict(row.get("payload_json") or {})
        session_id = str(payload.get("session_id") or row.get("session_id") or "")
        if not session_id:
            continue
        latest[session_id] = payload
    return sorted(latest.values(), key=lambda item: str(item.get("updated_at") or item.get("started_at") or ""), reverse=True)


def _get_watch_session_run_or_raise(session_id: str) -> dict[str, Any]:
    for run in _list_watch_session_runs():
        if run.get("session_id") == session_id:
            return run
    raise FinanceProviderError(
        f"Finance watch session runtime not found: {session_id}",
        error_type="watch_session_runtime_not_found",
        hint="Start the session with finance watch-session start --async-run, or inspect durable data with finance watch-session show.",
        details={"session_id": session_id},
    )


def _get_watch_session_run_or_none(session_id: str) -> dict[str, Any] | None:
    try:
        return _get_watch_session_run_or_raise(session_id)
    except FinanceProviderError:
        return None


def _is_process_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _watch_session_process_identity(*, pid: int, argv: list[str], session_id: str) -> dict[str, Any]:
    markers = ["velaria.finance_pack.cli", "watch-session", "start", "--session-id", session_id]
    fingerprint = hashlib.sha256(json.dumps(argv, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    return {"pid": pid, "argv_fingerprint": fingerprint, "command_markers": markers}


def _watch_session_run_process_running(run: dict[str, Any] | None) -> bool:
    if not run:
        return False
    pid = int(run.get("pid") or 0)
    if not _is_process_running(pid):
        return False
    command_line = _process_command_line(pid)
    if not command_line:
        return False
    return all(marker and str(marker) in command_line for marker in _watch_session_process_markers(run))


def _watch_session_process_markers(run: dict[str, Any]) -> list[str]:
    identity = run.get("process_identity") if isinstance(run.get("process_identity"), dict) else {}
    markers = identity.get("command_markers") if isinstance(identity.get("command_markers"), list) else []
    if markers:
        return [str(marker) for marker in markers if str(marker)]
    session_id = str(run.get("session_id") or "")
    return [marker for marker in ["velaria.finance_pack.cli", "watch-session", "start", "--session-id", session_id] if marker]


def _process_command_line(pid: int) -> str:
    try:
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=1.0,
            check=False,
        )
    except Exception:
        return ""
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()


def _watch_session_effective_status(*, session: dict[str, Any] | None, run: dict[str, Any] | None, process_running: bool) -> str:
    if process_running:
        return "running"
    session_status = str((session or {}).get("status") or "")
    if session_status and session_status not in {"running", "started"}:
        return session_status
    run_status = str((run or {}).get("status") or "")
    if run_status in {"completed", "interrupted", "stop_requested", "not_running"}:
        return run_status
    return "not_running"


def _read_watch_session_log_lines(run: dict[str, Any] | None, *, limit: int) -> tuple[pathlib.Path, list[str]]:
    raw_path = str((run or {}).get("log_path") or "")
    log_path = pathlib.Path(raw_path) if raw_path else pathlib.Path()
    lines: list[str] = []
    if raw_path and log_path.exists() and log_path.is_file():
        with log_path.open("r", encoding="utf-8", errors="replace") as handle:
            lines = [line.rstrip("\n") for line in handle if line.rstrip("\n")]
    if limit:
        lines = lines[-limit:]
    return log_path, lines


def _read_watch_session_events(session: dict[str, Any], *, feed: str, limit: int) -> list[dict[str, Any]]:
    source_ids = dict(session.get("sources") or {})
    keys = [feed] if feed != "all" else sorted(source_ids)
    rows: list[dict[str, Any]] = []
    with AgenticStore() as store:
        for key in keys:
            source_id = source_ids.get(key)
            if not source_id:
                continue
            for row in store.read_external_events(str(source_id)):
                payload = row.get("payload_json") if isinstance(row.get("payload_json"), dict) else {}
                row_session = row.get("watch_session_id") or payload.get("watch_session_id")
                if row_session == session.get("session_id"):
                    rows.append({"feed": key, **row})
    rows.sort(key=lambda item: str(item.get("ingested_at") or item.get("event_time") or ""))
    return rows[-limit:] if limit else rows


def _summarize_watch_session(session: dict[str, Any]) -> dict[str, Any]:
    all_rows = _read_watch_session_events(session, feed="all", limit=0)
    by_feed: dict[str, int] = {}
    for row in all_rows:
        by_feed[str(row.get("feed") or "unknown")] = by_feed.get(str(row.get("feed") or "unknown"), 0) + 1
    signals = [row for row in all_rows if row.get("feed") == "native_stream_signals"]
    candidates = [row for row in all_rows if row.get("feed") == "candidates"]
    latest_signal = signals[-1] if signals else None
    latest_candidates = candidates[-5:]
    return {
        "session_id": session.get("session_id"),
        "status": session.get("status"),
        "event_count": len(all_rows),
        "counts_by_feed": by_feed,
        "signal_count": len(signals),
        "market_context_count": by_feed.get("market_context", 0),
        "fundamental_count": by_feed.get("fundamentals", 0),
        "latest_signal": latest_signal,
        "latest_candidates": latest_candidates,
        "review_note": "Research summary only; not investment advice.",
    }


def _build_watch_session_review(session_id: str, *, log_limit: int) -> dict[str, Any]:
    session = _get_watch_session_or_none(session_id)
    run = _get_watch_session_run_or_none(session_id)
    if session is None and run is None:
        raise FinanceProviderError(
            f"Finance watch session and runtime not found: {session_id}",
            error_type="watch_session_not_found",
            hint="Run finance watch-session list --format json or start one with finance watch-session start --async-run.",
            details={"session_id": session_id},
        )
    process_running = _watch_session_run_process_running(run)
    effective_status = _watch_session_effective_status(session=session, run=run, process_running=process_running)
    summary = _compact_watch_session_summary(_summarize_watch_session(session)) if session else _empty_watch_session_summary(session_id=session_id)
    rows = _read_watch_session_events(session, feed="all", limit=0) if session else []
    log_path, raw_log_tail = _read_watch_session_log_lines(run, limit=log_limit)
    log_tail = [_truncate_review_log_line(line) for line in raw_log_tail]
    diagnostics = _watch_session_review_diagnostics(
        session_id=session_id,
        session=session,
        run=run,
        summary=summary,
        rows=rows,
        process_running=process_running,
        effective_status=effective_status,
        log_tail=log_tail,
    )
    return {
        "session_id": session_id,
        "event_time": _utc_payload_time(),
        "event_type": "watch_session_review",
        "effective_status": effective_status,
        "process_running": process_running,
        "runtime": {
            "run": run,
            "pid": (run or {}).get("pid"),
            "log_path": str((run or {}).get("log_path") or "") or None,
            "core_runtime": (run or {}).get("core_runtime"),
            "ai_cli_runtime": (run or {}).get("ai_cli_runtime"),
        },
        "watch_session": session,
        "summary": summary,
        "log_tail": log_tail,
        "diagnostics": diagnostics,
        "diagnostic_count": len(diagnostics),
        "next_actions": _watch_session_review_next_actions(
            session_id=session_id,
            process_running=process_running,
            effective_status=effective_status,
            run=run,
        ),
        "agent_prompt": _watch_session_review_agent_prompt(session_id=session_id, diagnostics=diagnostics, summary=summary),
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }


def _empty_watch_session_summary(*, session_id: str) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "status": None,
        "event_count": 0,
        "counts_by_feed": {},
        "signal_count": 0,
        "market_context_count": 0,
        "fundamental_count": 0,
        "latest_signal": None,
        "latest_candidates": [],
        "review_note": "Runtime exists but durable watch-session data has not been materialized yet.",
    }


def _compact_watch_session_summary(summary: dict[str, Any]) -> dict[str, Any]:
    compact = dict(summary)
    compact["latest_signal"] = _compact_watch_session_row(summary.get("latest_signal"))
    compact["latest_candidates"] = [_compact_watch_session_row(row) for row in (summary.get("latest_candidates") or [])]
    return compact


def _compact_watch_session_row(row: Any) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    payload = row.get("payload_json") if isinstance(row.get("payload_json"), dict) else {}
    keys = [
        "feed",
        "event_time",
        "event_type",
        "source_key",
        "market",
        "symbol",
        "rank",
        "score",
        "period_return_pct",
        "quote_pct_change",
        "quote_freshness",
        "news_sentiment_label",
        "signal_type",
        "iteration",
    ]
    compact: dict[str, Any] = {}
    for key in keys:
        value = row.get(key, payload.get(key))
        if value is not None:
            compact[key] = value
    if payload.get("summary"):
        compact["summary"] = payload.get("summary")
    return compact


def _watch_session_review_diagnostics(
    *,
    session_id: str,
    session: dict[str, Any] | None,
    run: dict[str, Any] | None,
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    process_running: bool,
    effective_status: str,
    log_tail: list[str],
) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    if run is None:
        diagnostics.append(
            {
                "type": "runtime_not_started",
                "severity": "warning",
                "message": "No async runtime row exists for this session.",
                "hint": "Use finance watch-session start --async-run for continuous observation.",
            }
        )
    elif not process_running and effective_status not in {"completed", "interrupted", "stop_requested", "not_running"}:
        diagnostics.append(
            {
                "type": "process_not_running",
                "severity": "error",
                "message": "The recorded async watch process is not running.",
                "hint": "Inspect logs, then restart the watch session if the market is still open.",
            }
        )
    if session is None:
        diagnostics.append(
            {
                "type": "session_not_materialized",
                "severity": "warning",
                "message": "Runtime has started but no finance_watch_sessions row exists yet.",
                "hint": "Wait for the first tick or inspect finance watch-session logs.",
            }
        )
    expected_feeds = ["quotes", "history", "news", "features", "candidates", "market_context", "fundamentals", "native_stream_signals"]
    counts = dict(summary.get("counts_by_feed") or {})
    for feed in expected_feeds:
        if session is not None and int(counts.get(feed) or 0) == 0:
            diagnostics.append(
                {
                    "type": "missing_feed",
                    "severity": "warning",
                    "feed": feed,
                    "message": f"No persisted rows found for feed: {feed}.",
                    "hint": "Check provider reachability and whether the watch loop has completed at least one tick.",
                }
            )
    if session is not None and int(summary.get("signal_count") or 0) == 0:
        diagnostics.append(
            {
                "type": "no_recent_stream_signal",
                "severity": "warning",
                "message": "No native stream signal rows have been persisted for this session.",
                "hint": "Verify native stream availability and signal thresholds.",
            }
        )
    unavailable_rows = []
    for row in rows:
        payload = row.get("payload_json") if isinstance(row.get("payload_json"), dict) else {}
        if payload.get("freshness") == "unavailable" or payload.get("error_type"):
            unavailable_rows.append(
                {
                    "feed": row.get("feed"),
                    "symbol": payload.get("symbol") or payload.get("source_key"),
                    "error_type": payload.get("error_type"),
                    "message": payload.get("message"),
                }
            )
    if unavailable_rows:
        diagnostics.append(
            {
                "type": "provider_unavailable_evidence",
                "severity": "info",
                "message": "One or more feeds recorded provider-unavailable evidence instead of mocked data.",
                "hint": "Treat unavailable feeds as evidence quality constraints in the agent analysis.",
                "rows": unavailable_rows[:10],
            }
        )
    if log_tail and any("Traceback" in line or '"ok": false' in line.lower() for line in log_tail):
        diagnostics.append(
            {
                "type": "runtime_log_error",
                "severity": "error",
                "message": "Recent runtime logs include a failure marker.",
                "hint": "Read finance watch-session logs and rerun the failing provider command with --format json.",
            }
        )
    return diagnostics


def _truncate_review_log_line(line: str, *, max_chars: int = 2000) -> str:
    if len(line) <= max_chars:
        return line
    return f"{line[:max_chars]}... [truncated {len(line) - max_chars} chars]"


def _watch_session_review_next_actions(
    *,
    session_id: str,
    process_running: bool,
    effective_status: str,
    run: dict[str, Any] | None,
) -> list[str]:
    actions = [
        f"finance watch-session status --session-id {session_id} --format json",
        f"finance watch-session logs --session-id {session_id} --limit 50 --format json",
        f"finance watch-session signals --session-id {session_id} --limit 100 --format json",
        f"finance watch-session summarize --session-id {session_id} --format json",
        f"finance watch-session review --session-id {session_id} --format json",
    ]
    if process_running:
        actions.append(f"finance watch-session supervise --session-id {session_id} --interval-sec 60 --format json")
    elif run and run.get("argv") and effective_status not in {"completed", "interrupted", "stop_requested", "not_running"}:
        actions.append("restart by re-running the original watch-session start command with --async-run after inspecting logs")
    return actions


def _watch_session_review_agent_prompt(*, session_id: str, diagnostics: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    diagnostic_types = ", ".join(str(item.get("type")) for item in diagnostics) or "none"
    counts = summary.get("counts_by_feed") or {}
    return (
        "Use velaria_cli_run to inspect the durable finance watch session "
        f"{session_id}. Review status, logs, signals, and summarize output; "
        f"feed_counts={counts}; diagnostics={diagnostic_types}. "
        "If data is stale or providers are unavailable, adjust the watch-session command or provider choice, "
        "then continue supervising. Treat all outputs as research evidence, not investment advice."
    )


def _review_event_payload(review: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": review.get("session_id"),
        "event_time": review.get("event_time"),
        "event_type": review.get("event_type"),
        "source_key": review.get("session_id"),
        "effective_status": review.get("effective_status"),
        "process_running": review.get("process_running"),
        "diagnostic_count": review.get("diagnostic_count"),
        "summary": review.get("summary"),
        "diagnostics": review.get("diagnostics"),
        "next_actions": review.get("next_actions"),
        "agent_prompt": review.get("agent_prompt"),
    }


def _intelligence_review(args: argparse.Namespace) -> int:
    intelligence_id = args.intelligence_id or _make_intelligence_id(args.session_id)
    review = _build_watch_session_review(args.session_id, log_limit=max(0, int(args.log_limit)))
    _append_watch_session_review_event(_review_event_payload(review))
    note = _append_intelligence_ai_note_event(
        _intelligence_ai_note_payload(
            intelligence_id=intelligence_id,
            watch_session_id=args.session_id,
            summary=review["summary"],
            diagnostics=review["diagnostics"],
        )
    )
    _append_finance_artifact_job(
        job_type="review_job",
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        status="completed",
        summary={"diagnostic_count": int(review.get("diagnostic_count") or 0)},
        artifacts={"ai_note_event_id": note.get("event_id")},
    )
    payload = {
        "ok": True,
        "action": "intelligence-review",
        "intelligence_id": intelligence_id,
        "watch_session_id": args.session_id,
        "review": review,
        "ai_plane": _intelligence_ai_plane(intelligence_id=intelligence_id, watch_session_id=args.session_id, summary=review["summary"], ai_note=note),
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _intelligence_replay(args: argparse.Namespace) -> int:
    intelligence_id = args.intelligence_id or _make_intelligence_id(args.session_id)
    session = _get_watch_session_or_raise(args.session_id)
    rows = _read_watch_session_events(session, feed="all", limit=0)
    summary = _compact_watch_session_summary(_summarize_watch_session(session))
    replay = _intelligence_replay_payload(intelligence_id=intelligence_id, watch_session_id=args.session_id, rows=rows, summary=summary)
    persisted = _append_intelligence_replay_event(replay)
    _append_finance_artifact_job(
        job_type="replay_job",
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        status="completed",
        summary={"event_count": int(replay.get("event_count") or 0), "signal_count": int(replay.get("signal_count") or 0)},
        artifacts={"replay_event_id": persisted.get("event_id")},
    )
    payload = {
        "ok": True,
        "action": "intelligence-replay",
        "intelligence_id": intelligence_id,
        "watch_session_id": args.session_id,
        "replay": replay,
        "persisted_replay": persisted,
        "data_plane": _intelligence_data_plane(dict(session.get("sources") or {}), summary),
        "ai_plane": _intelligence_ai_plane(intelligence_id=intelligence_id, watch_session_id=args.session_id, summary=summary),
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _intelligence_report(args: argparse.Namespace) -> int:
    intelligence_id = args.intelligence_id or _make_intelligence_id(args.session_id)
    session = _get_watch_session_or_raise(args.session_id)
    rows = _read_watch_session_events(session, feed="all", limit=0)
    summary = _compact_watch_session_summary(_summarize_watch_session(session))
    report = _intelligence_report_payload(intelligence_id=intelligence_id, watch_session_id=args.session_id, rows=rows, summary=summary)
    persisted = _append_intelligence_report_event(report)
    ai_note = _append_intelligence_ai_note_event(
        _intelligence_ai_note_payload(
            intelligence_id=intelligence_id,
            watch_session_id=args.session_id,
            summary=summary,
            diagnostics=list(report.get("diagnostics") or []),
            note_type="agent_final_scorecard",
        )
    )
    _append_finance_artifact_job(
        job_type="report_job",
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        status="completed",
        summary={"candidate_count": int(report.get("candidate_count") or 0), "signal_count": int(report.get("signal_count") or 0)},
        artifacts={"report_event_id": persisted.get("event_id"), "ai_note_event_id": ai_note.get("event_id")},
    )
    payload = {
        "ok": True,
        "action": "intelligence-report",
        "intelligence_id": intelligence_id,
        "watch_session_id": args.session_id,
        "report": report,
        "persisted_report": persisted,
        "data_plane": _intelligence_data_plane(dict(session.get("sources") or {}), summary),
        "ai_plane": _intelligence_ai_plane(intelligence_id=intelligence_id, watch_session_id=args.session_id, summary=summary, ai_note=ai_note),
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _intelligence_index(args: argparse.Namespace) -> int:
    intelligence_id = args.intelligence_id or _make_intelligence_id(args.session_id)
    session = _get_watch_session_or_raise(args.session_id)
    feed = str(args.feed)
    rows = _read_watch_session_events(session, feed=feed, limit=0)
    summary = _compact_watch_session_summary(_summarize_watch_session(session))
    index = build_finance_evidence_index(
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        rows=rows,
        feed=feed,
    )
    persisted = _append_intelligence_index_event(
        _intelligence_index_payload(
            intelligence_id=intelligence_id,
            watch_session_id=args.session_id,
            feed=feed,
            index=index,
        )
    )
    _append_finance_artifact_job(
        job_type="evidence_index_job",
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        status="completed",
        summary={"feed": feed, "doc_count": int(index.get("doc_count") or 0), "index_status": index.get("status")},
        artifacts={"index_event_id": persisted.get("event_id"), "index_dir": index.get("index_dir")},
    )
    payload = {
        "ok": True,
        "action": "intelligence-index",
        "intelligence_id": intelligence_id,
        "watch_session_id": args.session_id,
        "feed": feed,
        "index": index,
        "persisted_index": persisted,
        "data_plane": _intelligence_data_plane(dict(session.get("sources") or {}), summary),
        "ai_plane": _intelligence_ai_plane(intelligence_id=intelligence_id, watch_session_id=args.session_id, summary=summary),
        "disclaimer": "Research evidence index only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _intelligence_search(args: argparse.Namespace) -> int:
    intelligence_id = args.intelligence_id or _make_intelligence_id(args.session_id)
    session = _get_watch_session_or_raise(args.session_id)
    rows = _read_watch_session_events(session, feed=str(args.feed), limit=0)
    summary = _compact_watch_session_summary(_summarize_watch_session(session))
    search = _intelligence_search_payload(
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        query_text=str(args.query),
        rows=rows,
        top_k=max(1, int(args.top_k)),
        feed=str(args.feed),
        index_mode=str(args.index_mode),
    )
    retrieval = search.get("retrieval") if isinstance(search.get("retrieval"), dict) else {}
    index_meta = search.get("index") if isinstance(search.get("index"), dict) else {}
    if retrieval.get("index_status") in {"built", "rebuilt"} and index_meta:
        _append_intelligence_index_event(
            _intelligence_index_payload(
                intelligence_id=intelligence_id,
                watch_session_id=args.session_id,
                feed=str(args.feed),
                index=index_meta,
            )
        )
    persisted = _append_intelligence_search_event(search)
    _append_finance_artifact_job(
        job_type="evidence_search_job",
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        status="completed",
        summary={"query_text": str(args.query), "hit_count": int(search.get("hit_count") or 0), "feed": str(args.feed)},
        artifacts={"search_event_id": persisted.get("event_id")},
    )
    payload = {
        "ok": True,
        "action": "intelligence-search",
        "intelligence_id": intelligence_id,
        "watch_session_id": args.session_id,
        "search": search,
        "persisted_search": persisted,
        "data_plane": _intelligence_data_plane(dict(session.get("sources") or {}), summary),
        "ai_plane": _intelligence_ai_plane(intelligence_id=intelligence_id, watch_session_id=args.session_id, summary=summary),
        "disclaimer": "Research evidence search only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _append_finance_watch_job(
    *,
    watch_session_id: str,
    intelligence_id: str | None,
    status: str,
    run: dict[str, Any],
    summary: dict[str, Any],
) -> dict[str, Any]:
    return append_finance_job_event(
        finance_job_payload(
            job_id=f"watch_session:{watch_session_id}",
            job_type="watch_session_job",
            watch_session_id=watch_session_id,
            intelligence_id=intelligence_id,
            status=status,
            command=[str(item) for item in (run.get("argv") or [])],
            summary=summary,
            artifacts={"log_path": run.get("log_path")} if run else {},
            run=run,
            next_steps=[
                f"finance intelligence status --session-id {watch_session_id} --format json",
                f"finance intelligence search --session-id {watch_session_id} --query \"market news signal\" --format json",
                f"finance intelligence report --session-id {watch_session_id} --format json",
            ],
        )
    )


def _append_finance_artifact_job(
    *,
    job_type: str,
    intelligence_id: str,
    watch_session_id: str,
    status: str,
    summary: dict[str, Any],
    artifacts: dict[str, Any],
) -> dict[str, Any]:
    return append_finance_job_event(
        finance_job_payload(
            job_id=f"{job_type}:{intelligence_id}:{watch_session_id}",
            job_type=job_type,
            watch_session_id=watch_session_id,
            intelligence_id=intelligence_id,
            status=status,
            command=[],
            summary=summary,
            artifacts=artifacts,
            next_steps=[
                f"finance intelligence jobs --session-id {watch_session_id} --format json",
                f"finance intelligence status --session-id {watch_session_id} --format json",
            ],
        )
    )


def _intelligence_job_views(
    session_id: str | None,
    *,
    session: dict[str, Any] | None = None,
    run: dict[str, Any] | None = None,
    process_running: bool | None = None,
    effective_status: str | None = None,
) -> list[dict[str, Any]]:
    jobs = latest_finance_jobs(session_id)
    if not session_id:
        return jobs
    run = run if run is not None else _get_watch_session_run_or_none(session_id)
    if run:
        session = session if session is not None else _get_watch_session_or_none(session_id)
        process_running = _watch_session_run_process_running(run) if process_running is None else process_running
        effective_status = effective_status or _watch_session_effective_status(session=session, run=run, process_running=process_running)
        watch_job = watch_run_job_payload(
            run,
            intelligence_id=_make_intelligence_id(session_id),
            effective_status=effective_status,
            summary={"process_running": process_running, "effective_status": effective_status},
        )
        existing = {str(job.get("job_id") or ""): job for job in jobs}
        existing[str(watch_job.get("job_id"))] = watch_job
        jobs = sorted(existing.values(), key=lambda item: str(item.get("updated_at") or item.get("event_time") or ""), reverse=True)
    return jobs


def _intelligence_jobs(args: argparse.Namespace) -> int:
    jobs = _intelligence_job_views(getattr(args, "session_id", None))
    payload = {
        "ok": True,
        "action": "intelligence-jobs",
        "watch_session_id": getattr(args, "session_id", None),
        "jobs": jobs,
        "job_count": len(jobs),
        "disclaimer": "Research runtime status only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _intelligence_status(args: argparse.Namespace) -> int:
    session = _get_watch_session_or_none(args.session_id)
    run = _get_watch_session_run_or_none(args.session_id)
    process_running = _watch_session_run_process_running(run)
    effective_status = _watch_session_effective_status(session=session, run=run, process_running=process_running)
    log_path, log_lines = _read_watch_session_log_lines(run, limit=max(0, int(args.log_limit)))
    summary = _compact_watch_session_summary(_summarize_watch_session(session)) if session else _empty_watch_session_summary(session_id=args.session_id)
    payload = {
        "ok": bool(session or run),
        "action": "intelligence-status",
        "watch_session_id": args.session_id,
        "watch_session": session,
        "run": run,
        "process_running": process_running,
        "process_identity_verified": process_running,
        "effective_status": effective_status,
        "summary": summary,
        "jobs": _intelligence_job_views(args.session_id, session=session, run=run, process_running=process_running, effective_status=effective_status),
        "log_path": str(log_path) if run and run.get("log_path") else None,
        "log_lines": log_lines,
        "error_type": None if (session or run) else "watch_session_not_found",
        "message": None if (session or run) else f"Finance watch session not found: {args.session_id}",
        "hint": None if (session or run) else "Start one with finance intelligence start --async-run --format json.",
        "disclaimer": "Research runtime status only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload, exit_code=0 if payload["ok"] else 1)
    print(_render_intelligence_report(payload))
    return 0 if payload["ok"] else 1


def _intelligence_stop(args: argparse.Namespace) -> int:
    run = _get_watch_session_run_or_none(args.session_id)
    if not run:
        payload = {
            "ok": False,
            "action": "intelligence-stop",
            "watch_session_id": args.session_id,
            "signal_sent": False,
            "error_type": "watch_session_runtime_not_found",
            "message": f"Finance watch session runtime not found: {args.session_id}",
            "hint": "Start the session with finance intelligence start --async-run, or inspect durable data with finance intelligence status.",
        }
        return _emit_json(payload, exit_code=1)
    pid = int(run.get("pid") or 0)
    process_running = _watch_session_run_process_running(run)
    signal_sent = False
    if process_running:
        os.kill(pid, signal.SIGTERM)
        signal_sent = True
    stopped = {
        **run,
        "status": "stop_requested" if signal_sent else "not_running",
        "updated_at": _utc_payload_time(),
        "event_time": _utc_payload_time(),
        "event_type": "watch_session_stop_requested" if signal_sent else "watch_session_stop_not_running",
        "source_key": args.session_id,
    }
    _append_watch_session_run_event(stopped)
    _append_finance_watch_job(
        watch_session_id=args.session_id,
        intelligence_id=_make_intelligence_id(args.session_id),
        status=str(stopped["status"]),
        run=stopped,
        summary={"process_running": process_running, "signal_sent": signal_sent},
    )
    payload = {
        "ok": True,
        "action": "intelligence-stop",
        "watch_session_id": args.session_id,
        "run": stopped,
        "process_running": process_running,
        "process_identity_verified": process_running,
        "signal_sent": signal_sent,
        "jobs": _intelligence_job_views(args.session_id, run=stopped, process_running=False, effective_status=str(stopped["status"])),
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _intelligence_resume(args: argparse.Namespace) -> int:
    run = _get_watch_session_run_or_none(args.session_id)
    if not run:
        payload = {
            "ok": True,
            "action": "intelligence-resume",
            "watch_session_id": args.session_id,
            "resumed": False,
            "error_type": "resume_unavailable",
            "message": f"Finance watch session runtime not found: {args.session_id}",
            "hint": "Resume needs a prior async run record with argv. Start with finance intelligence start --async-run.",
        }
        return _emit_json(payload)
    if _watch_session_run_process_running(run):
        payload = {
            "ok": True,
            "action": "intelligence-resume",
            "watch_session_id": args.session_id,
            "resumed": False,
            "already_running": True,
            "run": run,
            "jobs": _intelligence_job_views(args.session_id, run=run, process_running=True, effective_status="running"),
        }
        if args.report_format == "json":
            return _emit_json(payload)
        print(_render_intelligence_report(payload))
        return 0
    argv = [str(item) for item in (run.get("argv") or []) if item is not None]
    if not argv or not _watch_session_resume_argv_trusted(run, argv=argv, session_id=args.session_id):
        payload = {
            "ok": True,
            "action": "intelligence-resume",
            "watch_session_id": args.session_id,
            "resumed": False,
            "error_type": "resume_unavailable",
            "message": "The durable runtime record does not contain a trusted watch-session restart argv.",
            "hint": "Resume only executes trusted watch-session argv records. Restart by running finance intelligence start with the intended providers and symbols.",
        }
        return _emit_json(payload)
    log_path = pathlib.Path(str(run.get("log_path") or (get_finance_watch_session_run_dir() / f"{args.session_id}.jsonl")))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log_handle:
        process = subprocess.Popen(
            argv,
            cwd=str(pathlib.Path.cwd()),
            env=os.environ.copy(),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    resumed = {
        **run,
        "status": "running",
        "pid": int(process.pid),
        "process_identity": _watch_session_process_identity(pid=int(process.pid), argv=argv, session_id=args.session_id),
        "log_path": str(log_path),
        "argv": argv,
        "updated_at": _utc_payload_time(),
        "event_time": _utc_payload_time(),
        "event_type": "watch_session_async_resume",
        "source_key": args.session_id,
    }
    _append_watch_session_run_event(resumed)
    _append_finance_watch_job(
        watch_session_id=args.session_id,
        intelligence_id=_make_intelligence_id(args.session_id),
        status="running",
        run=resumed,
        summary={"resumed": True},
    )
    payload = {
        "ok": True,
        "action": "intelligence-resume",
        "watch_session_id": args.session_id,
        "resumed": True,
        "run": resumed,
        "jobs": _intelligence_job_views(args.session_id, run=resumed, process_running=True, effective_status="running"),
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _watch_session_resume_argv_trusted(run: dict[str, Any], *, argv: list[str], session_id: str) -> bool:
    if not argv:
        return False
    executable_name = pathlib.Path(argv[0]).name.lower()
    if "python" not in executable_name:
        return False
    identity = run.get("process_identity") if isinstance(run.get("process_identity"), dict) else {}
    expected_fingerprint = identity.get("argv_fingerprint")
    actual_fingerprint = hashlib.sha256(json.dumps(argv, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    if expected_fingerprint and str(expected_fingerprint) != actual_fingerprint:
        return False
    if "-m" not in argv or "velaria.finance_pack.cli" not in argv:
        return False
    required_sequence = ["watch-session", "start", "--session-id", session_id]
    cursor = 0
    for item in argv:
        if item == required_sequence[cursor]:
            cursor += 1
            if cursor == len(required_sequence):
                return True
    return False


def _read_intelligence_source_events(source_id: str, watch_session_id: str) -> list[dict[str, Any]]:
    with AgenticStore() as store:
        if store.get_source(source_id) is None:
            return []
        rows = store.read_external_events(source_id)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        payload = row.get("payload_json") if isinstance(row.get("payload_json"), dict) else {}
        row_session_id = payload.get("watch_session_id") or row.get("watch_session_id")
        if row_session_id == watch_session_id:
            filtered.append(dict(row))
    return filtered


def _build_finance_evaluation_payload(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    session = _get_watch_session_or_raise(args.session_id)
    rows = _read_watch_session_events(session, feed="all", limit=0)
    jobs = latest_finance_jobs(args.session_id)
    searches = _read_intelligence_source_events("finance_intelligence_searches", args.session_id)
    indexes = _read_intelligence_source_events("finance_intelligence_evidence_indexes", args.session_id)
    evaluation = evaluate_finance_session(
        watch_session_id=args.session_id,
        intelligence_id=args.intelligence_id or _make_intelligence_id(args.session_id),
        rows=rows,
        jobs=jobs,
        searches=searches,
        indexes=indexes,
    )
    return session, evaluation, rows


def _intelligence_evaluate(args: argparse.Namespace) -> int:
    intelligence_id = args.intelligence_id or _make_intelligence_id(args.session_id)
    session, evaluation, _rows = _build_finance_evaluation_payload(args)
    persisted = append_finance_evaluation_event(evaluation)
    _append_finance_artifact_job(
        job_type="evaluation_job",
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        status="completed",
        summary={
            "quality_status": evaluation.get("quality_status"),
            "signal_count": int(evaluation.get("signal_count") or 0),
            "row_count": int(evaluation.get("row_count") or 0),
        },
        artifacts={"evaluation_event_id": persisted.get("event_id")},
    )
    summary = _compact_watch_session_summary(_summarize_watch_session(session))
    payload = {
        "ok": True,
        "action": "intelligence-evaluate",
        "intelligence_id": intelligence_id,
        "watch_session_id": args.session_id,
        "evaluation": evaluation,
        "persisted_evaluation": persisted,
        "data_plane": _intelligence_data_plane(dict(session.get("sources") or {}), summary),
        "disclaimer": "Replay evaluation only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _intelligence_eval_report(args: argparse.Namespace) -> int:
    intelligence_id = args.intelligence_id or _make_intelligence_id(args.session_id)
    session = _get_watch_session_or_raise(args.session_id)
    evaluation = latest_finance_evaluation(args.session_id)
    persisted = None
    if evaluation is None:
        _session, evaluation, _rows = _build_finance_evaluation_payload(args)
        persisted = append_finance_evaluation_event(evaluation)
    report = build_finance_evaluation_report(evaluation)
    _append_finance_artifact_job(
        job_type="evaluation_report_job",
        intelligence_id=intelligence_id,
        watch_session_id=args.session_id,
        status="completed",
        summary={"quality_status": report.get("quality_status"), "finding_count": len(report.get("findings") or [])},
        artifacts={"evaluation_event_id": (persisted or {}).get("event_id") or evaluation.get("event_id")},
    )
    summary = _compact_watch_session_summary(_summarize_watch_session(session))
    payload = {
        "ok": True,
        "action": "intelligence-eval-report",
        "intelligence_id": intelligence_id,
        "watch_session_id": args.session_id,
        "report": report,
        "evaluation": evaluation,
        "persisted_evaluation": persisted,
        "data_plane": _intelligence_data_plane(dict(session.get("sources") or {}), summary),
        "disclaimer": "Replay evaluation only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _intelligence_supervise(args: argparse.Namespace) -> int:
    intelligence_id = args.intelligence_id or _make_intelligence_id(args.session_id)
    reviews: list[dict[str, Any]] = []
    iteration = 0
    interrupted = False
    try:
        while int(args.iterations) == 0 or iteration < int(args.iterations):
            iteration += 1
            review = _build_watch_session_review(args.session_id, log_limit=max(0, int(args.log_limit)))
            review["supervisor_iteration"] = iteration
            _append_watch_session_review_event(_review_event_payload(review))
            note = _append_intelligence_ai_note_event(
                _intelligence_ai_note_payload(
                    intelligence_id=intelligence_id,
                    watch_session_id=args.session_id,
                    summary=review["summary"],
                    diagnostics=review["diagnostics"],
                    note_type="agent_supervisor_brief",
                )
            )
            review["ai_note"] = note
            reviews.append(review)
            if args.jsonl:
                print(json.dumps({"ok": True, "action": "intelligence-supervise-review", "review": review}, ensure_ascii=False, sort_keys=True), flush=True)
            if int(args.iterations) != 0 and iteration >= int(args.iterations):
                break
            time.sleep(max(0.0, float(args.interval_sec)))
    except KeyboardInterrupt:
        interrupted = True
    payload = {
        "ok": True,
        "action": "intelligence-supervise",
        "intelligence_id": intelligence_id,
        "watch_session_id": args.session_id,
        "review_count": len(reviews),
        "latest_review": reviews[-1] if reviews else None,
        "interrupted": interrupted,
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }
    if args.jsonl:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)
        return 0
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_intelligence_report(payload))
    return 0


def _make_intelligence_id(watch_session_id: str) -> str:
    return f"intelligence_{_id_part(watch_session_id)}"


def _intelligence_session_payload(
    *,
    intelligence_id: str,
    watch_session_id: str,
    status: str,
    market: str,
    symbols: list[str],
    raw_sources: dict[str, Any],
    tick_count: int,
) -> dict[str, Any]:
    return {
        "intelligence_id": intelligence_id,
        "watch_session_id": watch_session_id,
        "event_time": _utc_payload_time(),
        "event_type": "intelligence_session",
        "source_key": intelligence_id,
        "status": status,
        "market": market,
        "symbols": symbols,
        "tick_count": tick_count,
        "sources": {key: (value.get("source_id") if isinstance(value, dict) else value) for key, value in raw_sources.items()},
        "core_runtime": "velaria_native_realtime_stream",
        "data_runtime": "velaria_agentic_store",
        "ai_runtime": "velaria_cli_run",
        "workflow": "finance-intelligence",
    }


def _intelligence_session_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "intelligence_id",
        "field_mappings": {
            "intelligence_id": "intelligence_id",
            "watch_session_id": "watch_session_id",
            "status": "status",
            "market": "market",
            "tick_count": "tick_count",
        },
    }


def _intelligence_note_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "intelligence_id",
        "field_mappings": {
            "intelligence_id": "intelligence_id",
            "watch_session_id": "watch_session_id",
            "note_type": "note_type",
            "top_symbol": "top_symbol",
            "signal_count": "signal_count",
            "event_count": "event_count",
        },
    }


def _intelligence_replay_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "intelligence_id",
        "field_mappings": {
            "intelligence_id": "intelligence_id",
            "watch_session_id": "watch_session_id",
            "event_count": "event_count",
            "signal_count": "signal_count",
            "candidate_count": "candidate_count",
            "top_symbol": "top_symbol",
        },
    }


def _intelligence_report_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "intelligence_id",
        "field_mappings": {
            "intelligence_id": "intelligence_id",
            "watch_session_id": "watch_session_id",
            "event_count": "event_count",
            "signal_count": "signal_count",
            "candidate_count": "candidate_count",
            "top_symbol": "top_symbol",
            "overall_status": "overall_status",
        },
    }


def _intelligence_search_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "intelligence_id",
        "field_mappings": {
            "intelligence_id": "intelligence_id",
            "watch_session_id": "watch_session_id",
            "query_text": "query_text",
            "hit_count": "hit_count",
            "top_target_kind": "top_target_kind",
            "top_symbol": "top_symbol",
        },
    }


def _intelligence_index_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "intelligence_id",
        "field_mappings": {
            "intelligence_id": "intelligence_id",
            "watch_session_id": "watch_session_id",
            "feed": "feed",
            "index_status": "index_status",
            "doc_count": "doc_count",
            "fingerprint": "fingerprint",
        },
    }


def _append_intelligence_session_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        if store.get_source("finance_intelligence_sessions") is None:
            store.upsert_source(
                {
                    "source_id": "finance_intelligence_sessions",
                    "kind": "external_event",
                    "name": "finance intelligence sessions",
                    "schema_binding": _intelligence_session_source_binding(),
                    "metadata": {"domain": "finance", "workflow": "finance-intelligence"},
                }
            )
        return store.append_external_event("finance_intelligence_sessions", payload)


def _append_intelligence_ai_note_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        if store.get_source("finance_intelligence_ai_notes") is None:
            store.upsert_source(
                {
                    "source_id": "finance_intelligence_ai_notes",
                    "kind": "external_event",
                    "name": "finance intelligence AI notes",
                    "schema_binding": _intelligence_note_source_binding(),
                    "metadata": {"domain": "finance", "workflow": "finance-intelligence"},
                }
            )
        return store.append_external_event("finance_intelligence_ai_notes", payload)


def _append_intelligence_replay_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        if store.get_source("finance_intelligence_replays") is None:
            store.upsert_source(
                {
                    "source_id": "finance_intelligence_replays",
                    "kind": "external_event",
                    "name": "finance intelligence replays",
                    "schema_binding": _intelligence_replay_source_binding(),
                    "metadata": {"domain": "finance", "workflow": "finance-intelligence"},
                }
            )
        return store.append_external_event("finance_intelligence_replays", payload)


def _append_intelligence_report_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        if store.get_source("finance_intelligence_reports") is None:
            store.upsert_source(
                {
                    "source_id": "finance_intelligence_reports",
                    "kind": "external_event",
                    "name": "finance intelligence reports",
                    "schema_binding": _intelligence_report_source_binding(),
                    "metadata": {"domain": "finance", "workflow": "finance-intelligence"},
                }
            )
        return store.append_external_event("finance_intelligence_reports", payload)


def _append_intelligence_search_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        if store.get_source("finance_intelligence_searches") is None:
            store.upsert_source(
                {
                    "source_id": "finance_intelligence_searches",
                    "kind": "external_event",
                    "name": "finance intelligence evidence searches",
                    "schema_binding": _intelligence_search_source_binding(),
                    "metadata": {"domain": "finance", "workflow": "finance-intelligence", "retrieval": "hybrid-rrf"},
                }
            )
        return store.append_external_event("finance_intelligence_searches", payload)


def _append_intelligence_index_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        if store.get_source("finance_intelligence_evidence_indexes") is None:
            store.upsert_source(
                {
                    "source_id": "finance_intelligence_evidence_indexes",
                    "kind": "external_event",
                    "name": "finance intelligence evidence indexes",
                    "schema_binding": _intelligence_index_source_binding(),
                    "metadata": {"domain": "finance", "workflow": "finance-intelligence", "retrieval": "hybrid-rrf-index"},
                }
            )
        return store.append_external_event("finance_intelligence_evidence_indexes", payload)


def _intelligence_runtime_plane(watch_payload: dict[str, Any]) -> dict[str, Any]:
    native_stream = watch_payload.get("native_stream") or {}
    run = watch_payload.get("run") or {}
    return {
        "core_runtime": "velaria_native_realtime_stream",
        "core_engine": native_stream.get("engine") or run.get("core_runtime") or "velaria_native_realtime_stream",
        "data_runtime": "velaria_agentic_store",
        "ai_runtime": "velaria_cli_run",
        "stream_sql": native_stream.get("sql"),
        "async_pid": run.get("pid"),
        "log_path": run.get("log_path"),
    }


def _intelligence_data_plane(raw_sources: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    sources = {key: (value.get("source_id") if isinstance(value, dict) else value) for key, value in raw_sources.items()}
    return {
        "data_runtime": "velaria_agentic_store",
        "sources": sources,
        "counts_by_feed": summary.get("counts_by_feed") or {},
        "event_count": summary.get("event_count") or 0,
        "signal_count": summary.get("signal_count") or 0,
        "replayable": True,
    }


def _intelligence_ai_plane(
    *,
    intelligence_id: str,
    watch_session_id: str,
    summary: dict[str, Any],
    ai_note: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prompt = _intelligence_agent_prompt(intelligence_id=intelligence_id, watch_session_id=watch_session_id, summary=summary)
    return {
        "ai_runtime": "velaria_cli_run",
        "agent_prompt": prompt,
        "note_source_id": "finance_intelligence_ai_notes",
        "latest_note": ai_note,
        "next_commands": [
            f"finance intelligence review --session-id {watch_session_id} --format json",
            f"finance intelligence replay --session-id {watch_session_id} --format json",
            f"finance intelligence report --session-id {watch_session_id} --format json",
            f"finance intelligence index --session-id {watch_session_id} --format json",
            f"finance intelligence search --session-id {watch_session_id} --query '{_top_symbol_from_summary(summary) or 'market'} risk momentum news fundamentals' --format json",
            f"finance watch-session signals --session-id {watch_session_id} --format json",
        ],
    }


def _intelligence_ai_note_payload(
    *,
    intelligence_id: str,
    watch_session_id: str,
    summary: dict[str, Any],
    diagnostics: list[dict[str, Any]],
    note_type: str = "agent_research_brief",
) -> dict[str, Any]:
    top_symbol = _top_symbol_from_summary(summary)
    return {
        "intelligence_id": intelligence_id,
        "watch_session_id": watch_session_id,
        "event_time": _utc_payload_time(),
        "event_type": "intelligence_ai_note",
        "source_key": intelligence_id,
        "note_type": note_type,
        "top_symbol": top_symbol,
        "event_count": summary.get("event_count") or 0,
        "signal_count": summary.get("signal_count") or 0,
        "candidate_count": len(summary.get("latest_candidates") or []),
        "diagnostic_count": len(diagnostics),
        "summary": summary,
        "diagnostics": diagnostics,
        "agent_prompt": _intelligence_agent_prompt(intelligence_id=intelligence_id, watch_session_id=watch_session_id, summary=summary),
        "runtime_contract": {
            "core_runtime": "velaria_native_realtime_stream",
            "data_runtime": "velaria_agentic_store",
            "ai_runtime": "velaria_cli_run",
        },
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }


def _intelligence_replay_payload(
    *,
    intelligence_id: str,
    watch_session_id: str,
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
) -> dict[str, Any]:
    candidate_rows = [row for row in rows if row.get("feed") == "candidates"]
    signal_rows = [row for row in rows if row.get("feed") == "native_stream_signals"]
    return {
        "intelligence_id": intelligence_id,
        "watch_session_id": watch_session_id,
        "event_time": _utc_payload_time(),
        "event_type": "intelligence_replay",
        "source_key": intelligence_id,
        "event_count": len(rows),
        "candidate_count": len(candidate_rows),
        "signal_count": len(signal_rows),
        "top_symbol": _top_symbol_from_summary(summary),
        "counts_by_feed": summary.get("counts_by_feed") or {},
        "latest_signal": summary.get("latest_signal"),
        "latest_candidates": summary.get("latest_candidates") or [],
        "replay_note": "Realtime watch rows were read from persisted Velaria external_event sources.",
    }


def _intelligence_report_payload(
    *,
    intelligence_id: str,
    watch_session_id: str,
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
) -> dict[str, Any]:
    scorecard = _intelligence_candidate_scorecard(rows=rows, summary=summary)
    top_symbol = (scorecard[0].get("symbol") if scorecard else None) or _top_symbol_from_summary(summary)
    checks = _intelligence_supervisor_checks(rows=rows, summary=summary)
    diagnostics = _intelligence_report_diagnostics(checks)
    overall_status = "pass" if all(check.get("status") == "pass" for check in checks.values()) else "review"
    return {
        "intelligence_id": intelligence_id,
        "watch_session_id": watch_session_id,
        "event_time": _utc_payload_time(),
        "event_type": "intelligence_report",
        "source_key": intelligence_id,
        "event_count": int(summary.get("event_count") or len(rows)),
        "candidate_count": len([row for row in rows if row.get("feed") == "candidates"]),
        "signal_count": int(summary.get("signal_count") or 0),
        "top_symbol": top_symbol,
        "overall_status": overall_status,
        "counts_by_feed": summary.get("counts_by_feed") or {},
        "scorecard": scorecard,
        "supervisor_checks": checks,
        "diagnostics": diagnostics,
        "final_research_summary": _intelligence_final_research_summary(
            top_symbol=top_symbol,
            summary=summary,
            scorecard=scorecard,
            checks=checks,
        ),
        "runtime_contract": {
            "core_runtime": "velaria_native_realtime_stream",
            "metric_engine": "velaria_python_metric_graph",
            "data_runtime": "velaria_agentic_store",
            "ai_runtime": "velaria_cli_run",
        },
        "disclaimer": "Research candidates and realtime signals only; not investment advice.",
    }


def _intelligence_index_payload(
    *,
    intelligence_id: str,
    watch_session_id: str,
    feed: str,
    index: dict[str, Any],
) -> dict[str, Any]:
    return {
        "intelligence_id": intelligence_id,
        "watch_session_id": watch_session_id,
        "event_time": _utc_payload_time(),
        "event_type": "intelligence_evidence_index",
        "source_key": intelligence_id,
        "feed": feed,
        "index_status": index.get("status"),
        "doc_count": index.get("doc_count") or 0,
        "row_count": index.get("row_count") or 0,
        "fingerprint": index.get("fingerprint"),
        "index_path": index.get("index_path"),
        "metadata_path": index.get("metadata_path"),
        "docs_path": index.get("docs_path"),
        "keyword_index_path": index.get("keyword_index_path"),
        "retrieval": {
            "mode": "finance_evidence_hybrid_search",
            "keyword": "bm25_keyword_index",
            "semantic": {
                "status": FINANCE_EVIDENCE_SEMANTIC_STATUS,
                "reason": FINANCE_EVIDENCE_SEMANTIC_REASON,
            },
            "fusion": "rrf",
        },
    }


def _intelligence_search_payload(
    *,
    intelligence_id: str,
    watch_session_id: str,
    query_text: str,
    rows: list[dict[str, Any]],
    top_k: int,
    feed: str,
    index_mode: str = "auto",
) -> dict[str, Any]:
    result = DEFAULT_FINANCE_EVIDENCE_RETRIEVER.search_rows(
        intelligence_id=intelligence_id,
        watch_session_id=watch_session_id,
        rows=rows,
        query_text=query_text,
        options=EvidenceSearchOptions(feed=feed, top_k=top_k, index_mode=index_mode),
    )
    hits = result.hits
    top = hits[0] if hits else {}
    index_ref = result.index_ref
    index_meta = finance_evidence_index_metadata_for_payload(index_ref)
    return {
        "intelligence_id": intelligence_id,
        "watch_session_id": watch_session_id,
        "event_time": _utc_payload_time(),
        "event_type": "intelligence_search",
        "source_key": intelligence_id,
        "query_text": query_text,
        "feed": feed,
        "hit_count": len(hits),
        "top_target_kind": top.get("target_kind"),
        "top_symbol": (top.get("source_ref") or {}).get("symbol"),
        "retrieval": result.retrieval,
        "index": index_meta,
        "hits": hits,
        "disclaimer": "Research evidence search only; not investment advice.",
    }


def _intelligence_candidate_scorecard(*, rows: list[dict[str, Any]], summary: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if row.get("feed") != "candidates":
            continue
        payload = _watch_row_payload(row)
        symbol = payload.get("symbol") or row.get("symbol")
        if not symbol:
            continue
        candidates.append(
            {
                "symbol": str(symbol),
                "rank": int(payload.get("rank") or row.get("rank") or 999999),
                "score": round(_float_or_zero(payload.get("score", row.get("score"))), 4),
                "period_return_pct": _optional_float(payload.get("period_return_pct")),
                "quote_pct_change": _optional_float(payload.get("quote_pct_change")),
                "news_sentiment_label": payload.get("news_sentiment_label"),
                "feature_snapshot": payload.get("feature_snapshot"),
                "summary": payload.get("summary"),
                "event_time": payload.get("event_time") or row.get("event_time"),
            }
        )
    if not candidates:
        for item in summary.get("latest_candidates") or []:
            if isinstance(item, dict) and item.get("symbol"):
                candidates.append(
                    {
                        "symbol": str(item.get("symbol")),
                        "rank": int(item.get("rank") or 999999),
                        "score": round(_float_or_zero(item.get("score")), 4),
                        "period_return_pct": _optional_float(item.get("period_return_pct")),
                        "quote_pct_change": _optional_float(item.get("quote_pct_change")),
                        "news_sentiment_label": item.get("news_sentiment_label"),
                        "feature_snapshot": item.get("feature_snapshot"),
                        "summary": item.get("summary"),
                        "event_time": item.get("event_time"),
                    }
                )
    latest_by_symbol: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        latest_by_symbol[str(candidate["symbol"])] = candidate
    ranked = sorted(latest_by_symbol.values(), key=lambda item: (int(item.get("rank") or 999999), -float(item.get("score") or 0.0), str(item.get("symbol") or "")))
    return ranked[:5]


def _intelligence_supervisor_checks(*, rows: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    counts = dict(summary.get("counts_by_feed") or {})
    expected_feeds = ["quotes", "history", "news", "features", "candidates", "market_context", "fundamentals", "native_stream_signals"]
    missing = [feed for feed in expected_feeds if int(counts.get(feed) or 0) == 0]
    unavailable_rows = [
        _watch_row_payload(row)
        for row in rows
        if str(_watch_row_payload(row).get("event_type") or "").endswith("_unavailable")
        or _watch_row_payload(row).get("error_type") in {"provider_unavailable", "provider_fetch_failed", "unsupported_market", "cik_not_found"}
    ]
    signal_count = int(summary.get("signal_count") or 0)
    event_count = int(summary.get("event_count") or len(rows))
    return {
        "replayability": {
            "status": "pass" if event_count > 0 else "fail",
            "event_count": event_count,
            "message": "Realtime rows are persisted in Velaria external_event sources." if event_count > 0 else "No persisted realtime rows are available.",
        },
        "data_quality": {
            "status": "pass" if not missing else "warning",
            "counts_by_feed": counts,
            "missing_feeds": missing,
            "message": "All expected feeds are present." if not missing else "Some expected feeds are missing or have not emitted rows yet.",
        },
        "signal_consistency": {
            "status": "pass" if signal_count > 0 else "warning",
            "signal_count": signal_count,
            "message": "Native stream produced signal rows." if signal_count > 0 else "No native stream signal rows were found.",
        },
        "provider_quality": {
            "status": "pass" if not unavailable_rows else "warning",
            "unavailable_count": len(unavailable_rows),
            "sample": unavailable_rows[:3],
            "message": "No provider-unavailable evidence was found." if not unavailable_rows else "Provider gaps were persisted as evidence instead of mocked data.",
        },
    }


def _intelligence_report_diagnostics(checks: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for name, check in checks.items():
        status = str(check.get("status") or "unknown")
        if status == "pass":
            continue
        diagnostics.append(
            {
                "type": f"supervisor_{name}",
                "severity": "error" if status == "fail" else "warning",
                "message": str(check.get("message") or f"{name} requires review."),
                "hint": "Inspect finance intelligence replay and provider rows before acting on the signal.",
            }
        )
    return diagnostics


def _intelligence_final_research_summary(
    *,
    top_symbol: str | None,
    summary: dict[str, Any],
    scorecard: list[dict[str, Any]],
    checks: dict[str, dict[str, Any]],
) -> str:
    checked = ", ".join(f"{name}={check.get('status')}" for name, check in checks.items())
    score = scorecard[0].get("score") if scorecard else None
    return (
        f"Top research candidate is {top_symbol or 'none'}"
        f"{f' with score {score}' if score is not None else ''}; "
        f"persisted_events={summary.get('event_count') or 0}, "
        f"signals={summary.get('signal_count') or 0}, "
        f"checks={checked}. "
        "This is replayable research evidence, not investment advice."
    )


def _watch_row_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload_json") if isinstance(row.get("payload_json"), dict) else {}
    return {**row, **payload}


def _top_symbol_from_summary(summary: dict[str, Any]) -> str | None:
    candidates = [row for row in (summary.get("latest_candidates") or []) if isinstance(row, dict)]
    if candidates:
        ranked = sorted(
            candidates,
            key=lambda row: (
                int(row.get("rank") or 999999),
                -float(row.get("score") or 0.0),
                str(row.get("symbol") or ""),
            ),
        )
        symbol = ranked[0].get("symbol")
        return str(symbol) if symbol is not None else None
    signal = summary.get("latest_signal")
    if isinstance(signal, dict) and signal.get("symbol") is not None:
        return str(signal.get("symbol"))
    return None


def _intelligence_agent_prompt(*, intelligence_id: str, watch_session_id: str, summary: dict[str, Any]) -> str:
    counts = summary.get("counts_by_feed") or {}
    top_symbol = _top_symbol_from_summary(summary) or "none"
    return (
        "Use velaria_cli_run to continue the finance intelligence workflow "
        f"{intelligence_id} on watch_session={watch_session_id}. "
        f"Inspect persisted feed_counts={counts}, top_symbol={top_symbol}, "
        "then call finance intelligence review/replay and finance watch-session signals as needed. "
        "All realtime rows are already persisted in Velaria external_event sources and can be treated as replayable historical evidence. "
        "Treat outputs as research evidence, not investment advice."
    )


def _run_doctor(args: argparse.Namespace) -> int:
    checks: list[dict[str, Any]] = []
    akshare_available = importlib.util.find_spec("akshare") is not None
    checks.append(
        {
            "name": "akshare_dependency",
            "status": "ok" if akshare_available else "warning",
            "required": False,
            "message": "akshare is installed" if akshare_available else "akshare is not installed; history commands need the finance extra",
            "hint": "Run: uv sync --project python --extra finance",
        }
    )
    if args.skip_network:
        checks.append(
            {
                "name": "tencent_quote_probe",
                "status": "skipped",
                "required": True,
                "message": "network probe skipped",
                "hint": "Run without --skip-network to verify public quote reachability.",
            }
        )
    else:
        try:
            rows = fetch_quotes(provider="tencent", market=args.market, symbols=[args.symbol])
            quote = rows[0] if rows else {}
            checks.append(
                {
                    "name": "tencent_quote_probe",
                    "status": "ok",
                    "required": True,
                    "message": f"received quote for {quote.get('market')}:{quote.get('symbol')}",
                    "hint": "Tencent quote path is usable for finance analyze/watch.",
                    "evidence": {
                        "provider": quote.get("provider"),
                        "source_url": quote.get("source_url"),
                        "freshness": quote.get("freshness"),
                        "delay_sec": quote.get("delay_sec"),
                        "fetched_at": quote.get("fetched_at"),
                    },
                }
            )
        except FinanceProviderError as exc:
            checks.append(
                {
                    "name": "tencent_quote_probe",
                    "status": "failed",
                    "required": True,
                    "message": str(exc),
                    "hint": exc.hint,
                    "details": exc.details,
                    "error_type": exc.error_type,
                }
            )
    ok = not any(check["required"] and check["status"] == "failed" for check in checks)
    payload = {
        "ok": ok,
        "action": "doctor",
        "checks": checks,
        "next_steps": [
            "finance sources",
            "finance pipeline --market cn --symbol 000001 --start-date 20250101 --end-date 20250131",
            "finance analyze --market cn --symbol 000001",
            "finance watch --market cn --symbol 000001 --iterations 0 --jsonl",
        ],
    }
    if args.report_format == "json":
        return _emit_json(payload, exit_code=0 if ok else 1)
    print(_render_doctor(payload))
    return 0 if ok else 1


def _write_rows(output: pathlib.Path, rows: list[dict[str, Any]], output_format: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "jsonl":
        with output.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
                handle.write("\n")
        return
    pq.write_table(pa.Table.from_pylist(rows), output)


def _split_symbols(symbols: str) -> list[str]:
    return [item.strip() for item in symbols.replace("，", ",").split(",") if item.strip()]


def _emit_json(payload: dict[str, Any], *, exit_code: int = 0) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return exit_code


def _analyze_symbol(args: argparse.Namespace) -> int:
    symbol = str(args.symbol).strip()
    source_id = args.source_id or f"finance_{args.market}_{_id_part(symbol)}_analysis"
    monitor_id = args.monitor_id or f"monitor_{source_id}"
    display_name = args.name or f"finance {args.market} {symbol} analysis"
    source, monitor = _upsert_watch_source_and_monitor(args, source_id=source_id, monitor_id=monitor_id, display_name=display_name)
    tick = _run_watch_tick(args, source_id=source_id, monitor_id=monitor_id, iteration=1)
    payload = {
        "ok": True,
        "action": "analyze",
        "provider": args.provider,
        "market": args.market,
        "symbol": tick.get("symbol") or symbol,
        "source": source,
        "monitor": monitor,
        "quote": tick.get("quote") or {},
        "observations": tick.get("observations") or [],
        "signals": tick.get("signals") or [],
        "focus_events": tick.get("focus_events") or [],
        "artifacts": tick.get("artifacts") or [],
        "analysis": tick.get("analysis") or {},
        **({"analysis_prompt": tick["analysis_prompt"]} if "analysis_prompt" in tick else {}),
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_analysis_report(payload))
    return 0


def _run_pipeline(args: argparse.Namespace) -> int:
    symbol = str(args.symbol).strip()
    history_rows = fetch_history(
        provider=args.history_provider,
        market=args.market,
        symbol=symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        period=args.period,
        adjust=args.adjust,
    )
    history_output = pathlib.Path(args.history_output) if args.history_output else _default_history_output(args.market, symbol, args.history_output_format)
    _write_rows(history_output, history_rows, args.history_output_format)
    history_artifact = {
        "type": "file",
        "path": str(history_output),
        "format": args.history_output_format,
        "row_count": len(history_rows),
        "provider": args.history_provider,
        "market": args.market,
        "symbol": symbol,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "preview": history_rows[: args.preview_rows],
    }
    quote_args = argparse.Namespace(**vars(args))
    quote_args.provider = args.quote_provider
    quote_args.no_analysis_prompt = True
    source_id = args.source_id or f"finance_{args.market}_{_id_part(symbol)}_pipeline"
    monitor_id = args.monitor_id or f"monitor_{source_id}"
    display_name = args.name or f"finance {args.market} {symbol} pipeline"
    source, monitor = _upsert_watch_source_and_monitor(quote_args, source_id=source_id, monitor_id=monitor_id, display_name=display_name)
    ticks, interrupted = _collect_watch_ticks(quote_args, source_id=source_id, monitor_id=monitor_id)
    latest_tick = ticks[-1] if ticks else {}
    latest_quote = latest_tick.get("quote") or {}
    focus_events = [event for tick in ticks for event in tick.get("focus_events", [])]
    tick_artifacts = [artifact for tick in ticks for artifact in tick.get("artifacts", [])]
    datasets = [history_artifact, *tick_artifacts]
    prompt = "" if args.no_analysis_prompt else build_research_prompt(
        focus_events=focus_events,
        datasets=datasets,
        user_question=f"结合历史行情和实时监听事件，分析 {args.market} 市场标的 {symbol}。",
    )
    payload = {
        "ok": True,
        "action": "pipeline",
        "mode": "cli",
        "market": args.market,
        "symbol": latest_quote.get("symbol") or symbol,
        "history": history_artifact,
        "source": source,
        "monitor": monitor,
        "subscription": {
            "provider": args.quote_provider,
            "ticks": ticks,
            "tick_count": len(ticks),
            "interrupted": interrupted,
        },
        "quote": latest_quote,
        "signals": [signal for tick in ticks for signal in tick.get("signals", [])],
        "focus_events": focus_events,
        "artifacts": datasets,
        "analysis": _pipeline_analysis(history_rows, latest_quote, focus_events=focus_events),
        "service_integration": _service_integration_payload(source_id=source_id, monitor_id=monitor_id),
        **({"analysis_prompt": prompt} if prompt else {}),
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_pipeline_report(payload))
    return 0


def _rank_candidates(args: argparse.Namespace) -> int:
    symbols = _split_symbols(args.symbols)
    source_id = args.source_id or f"finance_{args.market}_rank_candidates"
    source = _upsert_rank_source(args, source_id=source_id, symbols=symbols)
    raw_sources = _upsert_rank_raw_sources(args, source_id=source_id, symbols=symbols, candidate_source=source) if (args.ingest_raw or args.native_stream) else {}
    stream_monitors = _upsert_rank_stream_monitors(args, source_id=source_id) if args.stream_monitor else []
    native_stream = _start_rank_native_stream(args) if args.native_stream else {}
    ticks: list[dict[str, Any]] = []
    iteration = 0
    interrupted = False
    try:
        while _should_continue_rank_loop(args, iteration):
            iteration += 1
            tick = _run_rank_tick(
                args,
                symbols=symbols,
                source_id=source_id,
                raw_sources=raw_sources,
                stream_monitors=stream_monitors,
                native_stream=native_stream,
                iteration=iteration,
            )
            ticks.append(tick)
            if args.jsonl:
                print(json.dumps(tick, ensure_ascii=False, sort_keys=True), flush=True)
            if not _should_continue_rank_loop(args, iteration):
                break
            time.sleep(max(0.0, float(args.interval_sec)))
    except KeyboardInterrupt:
        interrupted = True
    finally:
        _stop_rank_native_stream(native_stream)
    if args.jsonl:
        if interrupted:
            print(json.dumps({"ok": True, "action": "rank-candidates", "interrupted": True, "ticks": len(ticks)}, ensure_ascii=False), flush=True)
        return 0
    latest = ticks[-1] if ticks else {"research_candidates": []}
    payload = {
        "ok": True,
        "action": "rank-candidates",
        "mode": "cli",
        "recommendation_type": "research_candidate",
        "market": args.market,
        "symbols": symbols,
        "top": max(1, int(args.top)),
        "source": source,
        "raw_sources": raw_sources,
        "native_stream": _rank_native_stream_public_payload(native_stream),
        "stream_monitors": stream_monitors,
        "ticks": ticks,
        "tick_count": len(ticks),
        "interrupted": interrupted,
        "research_candidates": latest.get("research_candidates") or [],
        "focus_events": latest.get("focus_events") or [],
        "service_integration": _rank_service_integration_payload(source_id=source_id, stream_monitors=stream_monitors),
        "disclaimer": "Research candidates only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_rank_report(payload))
    return 0


def _should_continue_rank_loop(args: argparse.Namespace, completed_iterations: int) -> bool:
    if args.until_time:
        deadline = datetime.fromisoformat(str(args.until_time).replace("Z", "+00:00")).astimezone(timezone.utc)
        if completed_iterations > 0 and datetime.now(timezone.utc) >= deadline:
            return False
        return True
    if args.iterations != 0 and completed_iterations >= int(args.iterations):
        return False
    return True


def _upsert_rank_source(args: argparse.Namespace, *, source_id: str, symbols: list[str]) -> dict[str, Any]:
    metadata = {
        "domain": "finance",
        "workflow": "rank-candidates",
        "market": args.market,
        "symbols": symbols,
        "history_provider": args.history_provider,
        "quote_provider": args.quote_provider,
        "news_provider": args.news_provider,
    }
    if getattr(args, "watch_session_id", None):
        metadata["workflow"] = "watch-session"
        metadata["watch_session_id"] = args.watch_session_id
    with AgenticStore() as store:
        return store.upsert_source(
            {
                "source_id": source_id,
                "kind": "external_event",
                "name": f"finance {args.market} research candidate ranking",
                "schema_binding": {
                    "time_field": "event_time",
                    "type_field": "event_type",
                    "key_field": "symbol",
                    "field_mappings": {
                        "market": "market",
                        "symbol": "symbol",
                        "rank": "rank",
                        "score": "score",
                        "recommendation_type": "recommendation_type",
                        "period_return_pct": "period_return_pct",
                        "quote_pct_change": "quote_pct_change",
                        "news_sentiment_label": "news_sentiment_label",
                        "quote_freshness": "quote_freshness",
                    },
                },
                "metadata": metadata,
            }
        )


def _upsert_rank_raw_sources(
    args: argparse.Namespace,
    *,
    source_id: str,
    symbols: list[str],
    candidate_source: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    quote_source_id = f"{source_id}_quotes"
    history_source_id = f"{source_id}_history"
    news_source_id = f"{source_id}_news"
    feature_source_id = f"{source_id}_features"
    watch_session_id = getattr(args, "watch_session_id", None)
    workflow = "watch-session" if watch_session_id else "rank-candidates"
    with AgenticStore() as store:
        quote_source = store.upsert_source(
            {
                "source_id": quote_source_id,
                "kind": "external_event",
                "name": f"finance {args.market} rank quote rows",
                "schema_binding": finance_quote_schema_binding(),
                "metadata": {
                    "domain": "finance",
                    "workflow": workflow,
                    "raw_feed": "quotes",
                    "market": args.market,
                    "symbols": symbols,
                    "provider": args.quote_provider,
                    **({"watch_session_id": watch_session_id} if watch_session_id else {}),
                },
            }
        )
        history_source = store.upsert_source(
            {
                "source_id": history_source_id,
                "kind": "external_event",
                "name": f"finance {args.market} rank history rows",
                "schema_binding": {
                    "time_field": "event_time",
                    "type_field": "event_type",
                    "key_field": "symbol",
                    "field_mappings": {
                        "market": "market",
                        "symbol": "symbol",
                        "date": "date",
                        "open": "open",
                        "high": "high",
                        "low": "low",
                        "close": "close",
                        "volume": "volume",
                        "provider": "provider",
                        "freshness": "freshness",
                    },
                },
                "metadata": {
                    "domain": "finance",
                    "workflow": workflow,
                    "raw_feed": "history",
                    "market": args.market,
                    "symbols": symbols,
                    "provider": args.history_provider,
                    "start_date": args.start_date,
                    "end_date": args.end_date,
                    "period": args.period,
                    **({"watch_session_id": watch_session_id} if watch_session_id else {}),
                },
            }
        )
        news_source = store.upsert_source(
            {
                "source_id": news_source_id,
                "kind": "external_event",
                "name": f"finance {args.market} rank news rows",
                "schema_binding": {
                    "time_field": "event_time",
                    "type_field": "event_type",
                    "key_field": "symbol",
                    "field_mappings": {
                        "market": "market",
                        "symbol": "symbol",
                        "published_at": "published_at",
                        "title": "title",
                        "publisher": "publisher",
                        "provider": "provider",
                        "freshness": "freshness",
                    },
                },
                "metadata": {
                    "domain": "finance",
                    "workflow": workflow,
                    "raw_feed": "news",
                    "market": args.market,
                    "symbols": symbols,
                    "provider": args.news_provider,
                    **({"watch_session_id": watch_session_id} if watch_session_id else {}),
                },
            }
        )
        feature_source = store.upsert_source(
            {
                "source_id": feature_source_id,
                "kind": "external_event",
                "name": f"finance {args.market} metric and feature rows",
                "schema_binding": _feature_schema_binding(),
                "metadata": {
                    "domain": "finance",
                    "workflow": workflow,
                    "raw_feed": "features",
                    "market": args.market,
                    "symbols": symbols,
                    "provider": "velaria-metric-engine",
                    **({"watch_session_id": watch_session_id} if watch_session_id else {}),
                },
            }
        )
        market_context_source = None
        fundamental_source = None
        if watch_session_id:
            market_context_source = store.upsert_source(
                {
                    "source_id": f"{source_id}_market_context",
                    "kind": "external_event",
                    "name": f"finance {args.market} watch market context rows",
                    "schema_binding": _market_context_schema_binding(),
                    "metadata": {
                        "domain": "finance",
                        "workflow": workflow,
                        "raw_feed": "market_context",
                        "market": args.market,
                        "symbols": _market_context_symbols(args),
                        "provider": args.quote_provider,
                        "watch_session_id": watch_session_id,
                    },
                }
            )
            fundamental_source = store.upsert_source(
                {
                    "source_id": f"{source_id}_fundamentals",
                    "kind": "external_event",
                    "name": f"finance {args.market} watch fundamental rows",
                    "schema_binding": _fundamental_schema_binding(),
                    "metadata": {
                        "domain": "finance",
                        "workflow": workflow,
                        "raw_feed": "fundamentals",
                        "market": args.market,
                        "symbols": symbols,
                        "provider": args.fundamentals_provider,
                        "watch_session_id": watch_session_id,
                    },
                }
            )
        native_stream_signal_source = None
        if args.native_stream:
            native_stream_signal_source = store.upsert_source(
                {
                    "source_id": f"{source_id}_native_stream_signals",
                    "kind": "external_event",
                    "name": f"finance {args.market} native stream signal rows",
                    "schema_binding": _rank_native_stream_signal_schema_binding(),
                    "metadata": {
                        "domain": "finance",
                        "workflow": workflow,
                        "raw_feed": "native_stream_signals",
                        "market": args.market,
                        "symbols": symbols,
                        "engine": "velaria_native_realtime_stream",
                        "sql": _rank_native_stream_sql(),
                        **({"watch_session_id": watch_session_id} if watch_session_id else {}),
                    },
                }
            )
    sources = {
        "quotes": quote_source,
        "history": history_source,
        "news": news_source,
        "features": feature_source,
        "candidates": candidate_source,
    }
    if native_stream_signal_source is not None:
        sources["native_stream_signals"] = native_stream_signal_source
    if market_context_source is not None:
        sources["market_context"] = market_context_source
    if fundamental_source is not None:
        sources["fundamentals"] = fundamental_source
    return sources


def _upsert_rank_stream_monitors(args: argparse.Namespace, *, source_id: str) -> list[dict[str, Any]]:
    prefix = args.monitor_id_prefix or f"monitor_{source_id}"
    specs = [
        (
            "entry_research_signal",
            f"{prefix}_entry",
            _rank_entry_rule_spec(args, source_id=source_id),
        ),
        (
            "exit_risk_signal",
            f"{prefix}_exit",
            _rank_exit_rule_spec(args, source_id=source_id),
        ),
    ]
    monitors: list[dict[str, Any]] = []
    with AgenticStore() as store:
        for signal_type, monitor_id, rule_spec in specs:
            compiled = compile_rule_spec(rule_spec)
            monitor = store.upsert_monitor(
                {
                    "monitor_id": monitor_id,
                    "name": rule_spec["name"],
                    "intent_text": f"stream finance rank candidate {signal_type} for {args.market}",
                    "source": {"kind": "external_event", "source_id": source_id, "binding": source_id},
                    "compiled_rules": compiled["compiled_rules"],
                    "execution_mode": compiled["execution_mode"],
                    "rule_spec": compiled["rule_spec"],
                    "validation": {
                        "status": "valid",
                        "execution_spec": compiled["execution_spec"],
                        "promotion_rule": compiled["promotion_rule"],
                        "event_extraction": compiled["event_extraction"],
                        "suppression_rule": compiled["suppression_rule"],
                    },
                    "enabled": True,
                    "cooldown_sec": max(0, int(args.cooldown_sec)),
                    "tags": ["finance", "rank-candidates", "stream", signal_type, str(args.market)],
                }
            )
            monitors.append(
                {
                    "monitor_id": monitor["monitor_id"],
                    "name": monitor["name"],
                    "execution_mode": monitor["execution_mode"],
                    "signal_type": signal_type,
                    "enabled": monitor["enabled"],
                    "cooldown_sec": monitor["cooldown_sec"],
                }
            )
    return monitors


def _rank_entry_rule_spec(args: argparse.Namespace, *, source_id: str) -> dict[str, Any]:
    return {
        "version": "v1",
        "name": f"finance {args.market} rank entry research signal",
        "source": {"kind": "external_event", "binding": source_id},
        "execution": {
            "mode": "stream",
            "window": {"kind": "tumbling", "time_semantics": "processing_time", "size": str(args.stream_window_size)},
        },
        "signal": {
            "sql": (
                "SELECT symbol, market, rank, score, period_return_pct, quote_pct_change, news_sentiment_label, "
                "recommendation_type, quote_freshness, ingested_at "
                "FROM input_table "
                f"WHERE recommendation_type = 'research_candidate' AND score >= {float(args.entry_score_threshold)} "
                f"AND period_return_pct >= {float(args.entry_return_threshold)} "
                "AND news_sentiment_label != 'negative'"
            )
        },
        "promote": {"when": {"min_rows": 1}},
        "event": {
            "title": "{market}:{symbol} entry research signal",
            "summary": "score={score}, period_return_pct={period_return_pct}, quote_pct_change={quote_pct_change}, news={news_sentiment_label}",
            "severity": {"default": "info", "rules": []},
            "key_fields": ["symbol", "score", "period_return_pct", "news_sentiment_label"],
            "sample_rows": max(1, int(args.top)),
        },
        "suppress": {"cooldown": f"{max(0, int(args.cooldown_sec))}s", "dedupe_by": ["symbol"]},
    }


def _rank_exit_rule_spec(args: argparse.Namespace, *, source_id: str) -> dict[str, Any]:
    return {
        "version": "v1",
        "name": f"finance {args.market} rank exit risk signal",
        "source": {"kind": "external_event", "binding": source_id},
        "execution": {
            "mode": "stream",
            "window": {"kind": "tumbling", "time_semantics": "processing_time", "size": str(args.stream_window_size)},
        },
        "signal": {
            "sql": (
                "SELECT symbol, market, rank, score, period_return_pct, quote_pct_change, news_sentiment_label, "
                "recommendation_type, quote_freshness, ingested_at "
                "FROM input_table "
                f"WHERE recommendation_type = 'research_candidate' AND (score <= {float(args.exit_score_threshold)} "
                f"OR quote_pct_change <= {float(args.exit_quote_pct_threshold)} "
                "OR news_sentiment_label = 'negative')"
            )
        },
        "promote": {"when": {"min_rows": 1}},
        "event": {
            "title": "{market}:{symbol} exit risk signal",
            "summary": "score={score}, period_return_pct={period_return_pct}, quote_pct_change={quote_pct_change}, news={news_sentiment_label}",
            "severity": {"default": "warning", "rules": []},
            "key_fields": ["symbol", "score", "quote_pct_change", "news_sentiment_label"],
            "sample_rows": max(1, int(args.top)),
        },
        "suppress": {"cooldown": f"{max(0, int(args.cooldown_sec))}s", "dedupe_by": ["symbol"]},
    }


def _run_rank_tick(
    args: argparse.Namespace,
    *,
    symbols: list[str],
    source_id: str,
    raw_sources: dict[str, dict[str, Any]],
    stream_monitors: list[dict[str, Any]],
    native_stream: dict[str, Any],
    iteration: int,
) -> dict[str, Any]:
    quote_rows = fetch_quotes(provider=args.quote_provider, market=args.market, symbols=symbols)
    market_context_rows = _fetch_market_context_rows(args) if "market_context" in raw_sources else []
    fundamental_rows = _fetch_fundamental_rows(args, symbols=symbols) if "fundamentals" in raw_sources else []
    quotes = {_quote_symbol_key(row.get("symbol")): row for row in quote_rows}
    history_rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    news_rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    feature_rows: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for symbol in symbols:
        history_rows = fetch_history(
            provider=args.history_provider,
            market=args.market,
            symbol=symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            period=args.period,
            adjust=args.adjust,
        )
        news_rows = fetch_news(
            provider=args.news_provider,
            market=args.market,
            symbol=symbol,
            limit=max(0, int(args.news_limit)),
        )
        history_rows_by_symbol[symbol] = history_rows
        news_rows_by_symbol[symbol] = news_rows
        quote = quotes.get(_quote_symbol_key(symbol)) or {}
        feature = _build_feature_row(symbol=symbol, market=args.market, quote=quote, history_rows=history_rows, news_rows=news_rows)
        feature_rows.append(feature)
        candidates.append(_build_candidate(symbol=symbol, market=args.market, quote=quote, history_rows=history_rows, news_rows=news_rows, feature=feature))
    ranked = sorted(candidates, key=lambda item: item["score"], reverse=True)
    top = ranked[: max(1, int(args.top))]
    event_time = (top[0].get("event_time") if top else None) or _utc_payload_time()
    for index, candidate in enumerate(top, start=1):
        candidate["rank"] = index
        candidate["event_time"] = event_time
        candidate["event_type"] = "research_candidate"
        candidate["source_key"] = candidate["symbol"]
        if getattr(args, "watch_session_id", None):
            candidate["watch_session_id"] = args.watch_session_id
    with AgenticStore() as store:
        _append_rank_raw_rows(
            store,
            args=args,
            raw_sources=raw_sources,
            quote_rows=quote_rows,
            history_rows_by_symbol=history_rows_by_symbol,
            news_rows_by_symbol=news_rows_by_symbol,
            feature_rows=feature_rows,
        )
        _append_watch_session_rows(
            store,
            raw_sources=raw_sources,
            feed="market_context",
            rows=market_context_rows,
            watch_session_id=getattr(args, "watch_session_id", None),
        )
        _append_watch_session_rows(
            store,
            raw_sources=raw_sources,
            feed="fundamentals",
            rows=fundamental_rows,
            watch_session_id=getattr(args, "watch_session_id", None),
        )
        observations = [store.append_external_event(source_id, candidate) for candidate in top]
    native_stream_signals = _push_and_poll_rank_native_stream(
        args,
        native_stream=native_stream,
        candidates=top,
        raw_sources=raw_sources,
        iteration=iteration,
    )
    stream_monitor_runs: list[dict[str, Any]] = []
    focus_events: list[dict[str, Any]] = []
    for monitor in stream_monitors:
        with AgenticStore() as store:
            result = execute_monitor_once(store, monitor["monitor_id"])
        events = result.get("focus_events") or []
        focus_events.extend(events)
        stream_monitor_runs.append(
            {
                "monitor_id": monitor["monitor_id"],
                "signal_type": monitor["signal_type"],
                "run_id": result.get("run_id"),
                "signal_count": len(result.get("signals") or []),
                "focus_event_count": len(events),
            }
        )
    return {
        "ok": True,
        "action": "rank-candidates-tick",
        "iteration": iteration,
        "market": args.market,
        "symbols": symbols,
        "recommendation_type": "research_candidate",
        "research_candidates": top,
        "candidate_count": len(top),
        "market_context": market_context_rows,
        "fundamentals": fundamental_rows,
        "features": feature_rows,
        "observations": observations,
        "raw_ingestion": _rank_raw_ingestion_payload(raw_sources),
        "native_stream_signals": native_stream_signals,
        "stream_monitor_runs": stream_monitor_runs,
        "focus_events": focus_events,
        "disclaimer": "Research candidates only; not investment advice.",
    }


def _append_rank_raw_rows(
    store: AgenticStore,
    *,
    args: argparse.Namespace,
    raw_sources: dict[str, dict[str, Any]],
    quote_rows: list[dict[str, Any]],
    history_rows_by_symbol: dict[str, list[dict[str, Any]]],
    news_rows_by_symbol: dict[str, list[dict[str, Any]]],
    feature_rows: list[dict[str, Any]],
) -> None:
    if not raw_sources:
        return
    watch_session_id = getattr(args, "watch_session_id", None)
    for row in quote_rows:
        store.append_external_event(raw_sources["quotes"]["source_id"], _with_watch_session(row, watch_session_id))
    for symbol, rows in history_rows_by_symbol.items():
        for row in rows:
            store.append_external_event(raw_sources["history"]["source_id"], _with_watch_session(_rank_history_event(row, market=args.market, symbol=symbol), watch_session_id))
    for symbol, rows in news_rows_by_symbol.items():
        for row in rows:
            store.append_external_event(raw_sources["news"]["source_id"], _with_watch_session(_rank_news_event(row, market=args.market, symbol=symbol), watch_session_id))
    for row in feature_rows:
        store.append_external_event(raw_sources["features"]["source_id"], _with_watch_session(row, watch_session_id))


def _append_watch_session_rows(
    store: AgenticStore,
    *,
    raw_sources: dict[str, dict[str, Any]],
    feed: str,
    rows: list[dict[str, Any]],
    watch_session_id: str | None,
) -> None:
    source = raw_sources.get(feed)
    if not source:
        return
    for row in rows:
        store.append_external_event(source["source_id"], _with_watch_session(row, watch_session_id))


def _with_watch_session(row: dict[str, Any], watch_session_id: str | None) -> dict[str, Any]:
    if not watch_session_id:
        return row
    return {**row, "watch_session_id": watch_session_id}


def _rank_history_event(row: dict[str, Any], *, market: str, symbol: str) -> dict[str, Any]:
    date_value = row.get("date")
    return {
        **row,
        "event_time": str(date_value or _utc_payload_time()),
        "event_type": "history_bar",
        "source_key": str(row.get("symbol") or symbol),
        "market": row.get("market") or market,
        "symbol": row.get("symbol") or symbol,
    }


def _rank_news_event(row: dict[str, Any], *, market: str, symbol: str) -> dict[str, Any]:
    published_at = row.get("published_at")
    return {
        **row,
        "event_time": str(published_at or _utc_payload_time()),
        "event_type": "news",
        "source_key": str(row.get("symbol") or symbol),
        "market": row.get("market") or market,
        "symbol": row.get("symbol") or symbol,
    }


def _market_context_symbols(args: argparse.Namespace) -> list[str]:
    explicit = getattr(args, "market_symbols", None)
    if explicit:
        return _split_symbols(str(explicit))
    if args.market == "us":
        return ["SPY", "QQQ", "DIA"]
    return ["sh000001", "sz399001"]


def _fetch_market_context_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    symbols = _market_context_symbols(args)
    if args.market == "cn":
        return _fetch_cn_tencent_market_context(symbols=symbols)
    rows = fetch_quotes(provider=args.quote_provider, market=args.market, symbols=symbols)
    return [{**row, "event_type": "market_context", "source_key": row.get("symbol")} for row in rows]


def _fetch_cn_tencent_market_context(*, symbols: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fetched_at = _utc_payload_time()
    for symbol in symbols:
        code = symbol if symbol.startswith(("sh", "sz")) else ("sh" + symbol if symbol.startswith("0") else symbol)
        try:
            quote = fetch_quotes(provider="tencent", market="cn", symbols=[code])
        except FinanceProviderError:
            quote = []
        if quote:
            rows.extend({**row, "event_type": "market_context", "source_key": code, "symbol": code} for row in quote)
            continue
        rows.append(
            {
                "event_time": fetched_at,
                "event_type": "market_context_unavailable",
                "source_key": code,
                "market": "cn",
                "symbol": code,
                "provider": "tencent",
                "freshness": "unavailable",
                "error_type": "provider_symbol_not_supported",
                "message": "Tencent market context index quote could not be normalized by the quote provider.",
                "not_mocked": True,
            }
        )
    return rows


def _fetch_fundamental_rows(args: argparse.Namespace, *, symbols: list[str]) -> list[dict[str, Any]]:
    if str(args.fundamentals_provider) != "public-unavailable":
        try:
            return fetch_fundamentals(provider=str(args.fundamentals_provider), market=args.market, symbols=symbols)
        except FinanceProviderError as exc:
            fetched_at = _utc_payload_time()
            return [
                {
                    "event_time": fetched_at,
                    "event_type": "fundamental_unavailable",
                    "source_key": symbol,
                    "market": args.market,
                    "symbol": symbol,
                    "provider": args.fundamentals_provider,
                    "freshness": "unavailable",
                    "error_type": exc.error_type,
                    "message": str(exc),
                    "hint": exc.hint,
                    "not_mocked": True,
                }
                for symbol in symbols
            ]
    fetched_at = _utc_payload_time()
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        rows.append(
            {
                "event_time": fetched_at,
                "event_type": "fundamental_unavailable",
                "source_key": symbol,
                "market": args.market,
                "symbol": symbol,
                "provider": args.fundamentals_provider,
                "freshness": "unavailable",
                "error_type": "provider_unavailable",
                "message": "No configured public fundamentals provider is available without credentials; this event records absence instead of mock data.",
                "not_mocked": True,
            }
        )
    return rows


def _rank_raw_ingestion_payload(raw_sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "enabled": bool(raw_sources),
        "sources": {key: value.get("source_id") for key, value in raw_sources.items()},
    }


def _rank_native_stream_schema() -> list[str]:
    return [
        "event_time",
        "market",
        "symbol",
        "rank",
        "score",
        "period_return_pct",
        "quote_pct_change",
        "entry_signal",
        "exit_signal",
        "quote_freshness",
        "news_sentiment_label",
        "momentum_state",
        "liquidity_score",
        "provider_quality_score",
        "signal_policy_source",
        "signal_policy_preset",
        "signal_policy_json",
    ]


def _rank_native_stream_signal_schema_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "source_key",
        "field_mappings": {
            "watch_session_id": "watch_session_id",
            "signal_type": "signal_type",
            "engine": "engine",
            "market": "market",
            "symbol": "symbol",
            "rank": "rank",
            "score": "score",
            "period_return_pct": "period_return_pct",
            "quote_pct_change": "quote_pct_change",
            "quote_freshness": "quote_freshness",
            "news_sentiment_label": "news_sentiment_label",
            "momentum_state": "momentum_state",
            "liquidity_score": "liquidity_score",
            "provider_quality_score": "provider_quality_score",
            "signal_policy_source": "signal_policy_source",
            "signal_policy_preset": "signal_policy_preset",
            "signal_policy_json": "signal_policy_json",
            "iteration": "iteration",
        },
    }


def _market_context_schema_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "source_key",
        "field_mappings": {
            "watch_session_id": "watch_session_id",
            "market": "market",
            "symbol": "symbol",
            "price": "price",
            "pct_change": "pct_change",
            "provider": "provider",
            "freshness": "freshness",
        },
    }


def _fundamental_schema_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "source_key",
        "field_mappings": {
            "watch_session_id": "watch_session_id",
            "market": "market",
            "symbol": "symbol",
            "provider": "provider",
            "freshness": "freshness",
            "error_type": "error_type",
            "market_cap": "market_cap",
        },
    }


def _feature_schema_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "source_key",
        "field_mappings": {
            "watch_session_id": "watch_session_id",
            "market": "market",
            "symbol": "symbol",
            "period_return_pct": "period_return_pct",
            "return_1d_pct": "return_1d_pct",
            "return_5d_pct": "return_5d_pct",
            "volatility_pct": "volatility_pct",
            "ma_5": "ma_5",
            "ma_20": "ma_20",
            "ma_5_distance_pct": "ma_5_distance_pct",
            "rsi_14": "rsi_14",
            "liquidity_score": "liquidity_score",
            "news_sentiment_score": "news_sentiment_score",
            "news_velocity": "news_velocity",
            "provider_quality_score": "provider_quality_score",
            "momentum_state": "momentum_state",
        },
    }


def _sql_identifier_suffix(value: str) -> str:
    suffix = "".join(ch if ch.isalnum() else "_" for ch in value.lower()).strip("_")
    suffix = suffix or "default"
    if len(suffix) <= 32:
        return suffix
    digest = hashlib.sha1(suffix.encode("utf-8")).hexdigest()[:8]
    return f"{suffix[:23].rstrip('_')}_{digest}"


def _rank_native_stream_view_name(args: argparse.Namespace) -> str:
    source_id = str(getattr(args, "source_id", "") or f"finance_{args.market}_rank_candidates")
    return f"finance_rank_candidate_stream_{_sql_identifier_suffix(source_id)}"


def _rank_native_stream_sql(view_name: str = "finance_rank_candidate_stream") -> str:
    return (
        "SELECT event_time, market, symbol, rank, score, period_return_pct, quote_pct_change, "
        "entry_signal, exit_signal, quote_freshness, news_sentiment_label, momentum_state, liquidity_score, "
        "provider_quality_score, signal_policy_source, signal_policy_preset, signal_policy_json "
        f"FROM {view_name} "
        "WHERE entry_signal >= 1 OR exit_signal >= 1"
    )


def _start_rank_native_stream(args: argparse.Namespace) -> dict[str, Any]:
    try:
        session = velaria.Session()
        source = session.create_realtime_stream_source(_rank_native_stream_schema())
        stream_df = session.read_realtime_stream_source(source)
        view_name = _rank_native_stream_view_name(args)
        session.create_temp_view(view_name, stream_df)
        sink = session.create_realtime_stream_sink()
        sql = _rank_native_stream_sql(view_name)
        query_df = session.stream_sql(sql)
        query = query_df.write_stream_queue_sink(sink, trigger_interval_ms=0)
        query.start()
        max_batches = None if args.until_time or int(args.iterations) == 0 else max(1, int(args.iterations))
        worker = threading.Thread(target=lambda: _await_rank_native_stream(query, max_batches=max_batches), daemon=True)
        worker.start()
    except Exception as exc:
        raise FinanceProviderError(
            "Velaria native realtime stream is unavailable for finance rank-candidates.",
            error_type="native_stream_unavailable",
            hint="Build //:velaria_pyext or install a Velaria package with native streaming support, then retry --native-stream.",
            details={"reason": str(exc)},
        ) from exc
    return {
        "enabled": True,
        "engine": "velaria_native_realtime_stream",
        "schema": _rank_native_stream_schema(),
        "sql": sql,
        "view_name": view_name,
        "session": session,
        "source": source,
        "sink": sink,
        "query": query,
        "worker": worker,
        "max_batches": max_batches,
        "signal_policy": _resolve_signal_policy(args),
        "started_at": _utc_payload_time(),
    }


def _await_rank_native_stream(query: Any, *, max_batches: int | None) -> None:
    if max_batches is None:
        query.await_termination()
        return
    query.await_termination(max_batches=max_batches)


def _stop_rank_native_stream(native_stream: dict[str, Any]) -> None:
    if not native_stream:
        return
    source = native_stream.get("source")
    query = native_stream.get("query")
    worker = native_stream.get("worker")
    try:
        if query is not None:
            query.stop()
        if source is not None:
            source.close()
    finally:
        if worker is not None:
            worker.join(timeout=0.2)
        native_stream["stopped_at"] = _utc_payload_time()


def _rank_native_stream_public_payload(native_stream: dict[str, Any]) -> dict[str, Any]:
    if not native_stream:
        return {"enabled": False}
    return {
        "enabled": True,
        "engine": native_stream["engine"],
        "schema": native_stream["schema"],
        "sql": native_stream["sql"],
        "max_batches": native_stream.get("max_batches"),
        "signal_policy": native_stream.get("signal_policy"),
        "started_at": native_stream["started_at"],
        **({"stopped_at": native_stream["stopped_at"]} if native_stream.get("stopped_at") else {}),
    }


def _push_and_poll_rank_native_stream(
    args: argparse.Namespace,
    *,
    native_stream: dict[str, Any],
    candidates: list[dict[str, Any]],
    raw_sources: dict[str, dict[str, Any]],
    iteration: int,
) -> list[dict[str, Any]]:
    if not native_stream:
        return []
    rows = [_rank_native_stream_row(args, candidate) for candidate in candidates]
    if not rows:
        return []
    native_stream["source"].push_rows([_rank_native_stream_source_row(row) for row in rows])
    signals: list[dict[str, Any]] = []
    deadline = time.monotonic() + max(0.0, float(args.native_stream_poll_timeout_sec))
    while time.monotonic() <= deadline:
        batch = native_stream["sink"].poll_arrow()
        if batch is not None:
            for row in batch.to_pylist():
                signals.extend(_rank_native_signal_rows(row))
            if signals:
                _append_rank_native_stream_signal_rows(
                    raw_sources,
                    signals,
                    iteration=iteration,
                    watch_session_id=getattr(args, "watch_session_id", None),
                    stream_sql=str(native_stream.get("sql") or _rank_native_stream_sql()),
                )
                return signals
        time.sleep(0.05)
    return signals


def _append_rank_native_stream_signal_rows(
    raw_sources: dict[str, dict[str, Any]],
    signals: list[dict[str, Any]],
    *,
    iteration: int,
    watch_session_id: str | None = None,
    stream_sql: str | None = None,
) -> None:
    signal_source = raw_sources.get("native_stream_signals")
    if not signal_source or not signals:
        return
    source_id = str(signal_source["source_id"])
    with AgenticStore() as store:
        for signal in signals:
            store.append_external_event(
                source_id,
                {
                    **signal,
                    "event_type": "native_stream_signal",
                    "source_key": str(signal.get("symbol") or ""),
                    "iteration": iteration,
                    "stream_sql": stream_sql or _rank_native_stream_sql(),
                    "not_investment_advice": True,
                    **({"watch_session_id": watch_session_id} if watch_session_id else {}),
                },
            )


def _rank_native_stream_row(args: argparse.Namespace, candidate: dict[str, Any]) -> dict[str, Any]:
    score = _float_or_zero(candidate.get("score"))
    period_return_pct = _float_or_zero(candidate.get("period_return_pct"))
    quote_pct_change = _float_or_zero(candidate.get("quote_pct_change"))
    news_label = str(candidate.get("news_sentiment_label") or "unknown")
    feature = candidate.get("feature_snapshot") if isinstance(candidate.get("feature_snapshot"), dict) else {}
    policy = _resolve_signal_policy(args)
    policy_json = json.dumps(policy, ensure_ascii=False, sort_keys=True)
    values = {
        "score": score,
        "period_return_pct": period_return_pct,
        "quote_pct_change": quote_pct_change,
        "news_sentiment_label": news_label,
        "quote_freshness": str(candidate.get("quote_freshness") or "unknown"),
        "momentum_state": str(feature.get("momentum_state") or candidate.get("momentum_state") or "unknown"),
        "liquidity_score": _float_or_zero(feature.get("liquidity_score", candidate.get("liquidity_score"))),
        "provider_quality_score": _float_or_zero(feature.get("provider_quality_score", candidate.get("provider_quality_score"))),
        "rank": int(candidate.get("rank") or 0),
    }
    entry_signal = int(_evaluate_signal_group(policy.get("entry"), values))
    exit_signal = int(_evaluate_signal_group(policy.get("exit"), values))
    return {
        "event_time": str(candidate.get("event_time") or _utc_payload_time()),
        "market": str(candidate.get("market") or args.market),
        "symbol": str(candidate.get("symbol") or ""),
        "rank": values["rank"],
        "score": score,
        "period_return_pct": period_return_pct,
        "quote_pct_change": quote_pct_change,
        "entry_signal": entry_signal,
        "exit_signal": exit_signal,
        "quote_freshness": values["quote_freshness"],
        "news_sentiment_label": news_label,
        "momentum_state": values["momentum_state"],
        "liquidity_score": values["liquidity_score"],
        "provider_quality_score": values["provider_quality_score"],
        "signal_policy_source": str(policy.get("source") or "preset"),
        "signal_policy_preset": str(policy.get("preset") or ""),
        "signal_policy_json": policy_json,
        "signal_policy": policy,
    }


def _rank_native_stream_source_row(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in _rank_native_stream_schema()}


def _resolve_signal_policy(args: argparse.Namespace) -> dict[str, Any]:
    preset = str(getattr(args, "signal_policy_preset", None) or "balanced")
    raw_policy = getattr(args, "signal_policy", None)
    if raw_policy:
        try:
            parsed = json.loads(str(raw_policy))
        except json.JSONDecodeError as exc:
            raise FinanceProviderError(
                "invalid finance signal policy JSON",
                error_type="invalid_signal_policy",
                hint="Pass valid JSON with optional entry.all/entry.any/exit.all/exit.any condition lists.",
                details={"reason": str(exc)},
            ) from exc
        if not isinstance(parsed, dict):
            raise FinanceProviderError(
                "finance signal policy must be a JSON object",
                error_type="invalid_signal_policy",
                hint="Use an object such as {\"entry\":{\"all\":[...]},\"exit\":{\"any\":[...]}}.",
            )
        return {
            "source": "custom",
            "preset": preset,
            "entry": _normalize_signal_group(parsed.get("entry"), strict=True),
            "exit": _normalize_signal_group(parsed.get("exit"), strict=True),
        }
    return {
        "source": "preset",
        "preset": preset,
        **_signal_policy_preset(preset, args),
    }


def _signal_policy_preset(preset: str, args: argparse.Namespace) -> dict[str, Any]:
    thresholds = {
        "entry_score_threshold": float(getattr(args, "entry_score_threshold", 8.0)),
        "entry_return_threshold": float(getattr(args, "entry_return_threshold", 5.0)),
        "exit_score_threshold": float(getattr(args, "exit_score_threshold", 0.0)),
        "exit_quote_pct_threshold": float(getattr(args, "exit_quote_pct_threshold", -3.0)),
    }
    if preset == "momentum":
        return {
            "entry": {
                "all": [
                    {"field": "score", "op": ">=", "value": thresholds["entry_score_threshold"]},
                    {"field": "momentum_state", "op": "=", "value": "bullish"},
                    {"field": "news_sentiment_label", "op": "!=", "value": "negative"},
                ]
            },
            "exit": {
                "any": [
                    {"field": "quote_pct_change", "op": "<=", "value": thresholds["exit_quote_pct_threshold"]},
                    {"field": "momentum_state", "op": "=", "value": "bearish"},
                    {"field": "news_sentiment_label", "op": "=", "value": "negative"},
                ]
            },
        }
    if preset == "defensive":
        return {
            "entry": {
                "all": [
                    {"field": "score", "op": ">=", "value": thresholds["entry_score_threshold"]},
                    {"field": "period_return_pct", "op": ">=", "value": thresholds["entry_return_threshold"]},
                    {"field": "provider_quality_score", "op": ">=", "value": 1.0},
                    {"field": "news_sentiment_label", "op": "!=", "value": "negative"},
                ]
            },
            "exit": {
                "any": [
                    {"field": "score", "op": "<=", "value": thresholds["exit_score_threshold"]},
                    {"field": "quote_pct_change", "op": "<=", "value": thresholds["exit_quote_pct_threshold"]},
                    {"field": "provider_quality_score", "op": "<", "value": 0.5},
                    {"field": "news_sentiment_label", "op": "=", "value": "negative"},
                ]
            },
        }
    return {
        "entry": {
            "all": [
                {"field": "score", "op": ">=", "value": thresholds["entry_score_threshold"]},
                {"field": "period_return_pct", "op": ">=", "value": thresholds["entry_return_threshold"]},
                {"field": "news_sentiment_label", "op": "!=", "value": "negative"},
                {"field": "momentum_state", "op": "!=", "value": "bearish"},
            ]
        },
        "exit": {
            "any": [
                {"field": "score", "op": "<=", "value": thresholds["exit_score_threshold"]},
                {"field": "quote_pct_change", "op": "<=", "value": thresholds["exit_quote_pct_threshold"]},
                {"field": "news_sentiment_label", "op": "=", "value": "negative"},
                {"field": "momentum_state", "op": "=", "value": "bearish"},
            ]
        },
    }


def _normalize_signal_group(group: Any, *, strict: bool = False) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(group, dict):
        if strict and group is not None:
            raise FinanceProviderError(
                "finance signal policy group must be an object",
                error_type="invalid_signal_policy",
                hint="Use entry/exit objects with all/any condition arrays.",
            )
        return {"all": [], "any": []}
    normalized: dict[str, list[dict[str, Any]]] = {"all": [], "any": []}
    for key in ("all", "any"):
        raw_conditions = group.get(key, [])
        if strict and not isinstance(raw_conditions, list):
            raise FinanceProviderError(
                "finance signal policy conditions must be arrays",
                error_type="invalid_signal_policy",
                hint="Use all/any arrays such as {\"entry\":{\"all\":[{\"field\":\"score\",\"op\":\">=\",\"value\":8}]}}.",
                details={"section": key},
            )
        normalized[key] = [
            _normalize_signal_condition(item, section=key) if strict else item
            for item in raw_conditions
            if isinstance(item, dict)
        ]
    return normalized


def _normalize_signal_condition(condition: dict[str, Any], *, section: str) -> dict[str, Any]:
    field = str(condition.get("field") or "")
    op = str(condition.get("op") or "=").lower()
    if field not in _SIGNAL_POLICY_FIELD_TYPES:
        raise FinanceProviderError(
            f"unknown finance signal policy field: {field}",
            error_type="invalid_signal_policy",
            hint="Use one of the supported signal policy fields returned in details.candidates.",
            details={"field": field, "section": section, "candidates": sorted(_SIGNAL_POLICY_FIELD_TYPES)},
        )
    allowed_ops = _NUMERIC_SIGNAL_OPS | {"=", "==", "!=", "<>", "in", "not_in", "not in"} if _SIGNAL_POLICY_FIELD_TYPES[field] == "number" else _STRING_SIGNAL_OPS
    if op not in allowed_ops:
        raise FinanceProviderError(
            f"unsupported finance signal policy operator: {op}",
            error_type="invalid_signal_policy",
            hint="Use numeric operators for numeric fields and equality/in operators for text fields.",
            details={"field": field, "op": op, "allowed_ops": sorted(allowed_ops)},
        )
    if _SIGNAL_POLICY_FIELD_TYPES[field] == "number":
        if op in {"in", "not_in", "not in"}:
            for item in _signal_sequence(condition.get("value")):
                _signal_number_or_raise(item, field=field)
        else:
            _signal_number_or_raise(condition.get("value"), field=field)
    return {
        "field": field,
        "op": op,
        "value": condition.get("value"),
    }


def _evaluate_signal_group(group: Any, values: dict[str, Any]) -> bool:
    normalized = _normalize_signal_group(group)
    all_conditions = normalized["all"]
    any_conditions = normalized["any"]
    all_ok = all(_evaluate_signal_condition(condition, values) for condition in all_conditions) if all_conditions else True
    any_ok = any(_evaluate_signal_condition(condition, values) for condition in any_conditions) if any_conditions else True
    return (bool(all_conditions) or bool(any_conditions)) and all_ok and any_ok


def _evaluate_signal_condition(condition: dict[str, Any], values: dict[str, Any]) -> bool:
    field = str(condition.get("field") or "")
    op = str(condition.get("op") or "=").lower()
    expected = condition.get("value")
    actual = values.get(field)
    if op in {"=", "=="}:
        return _signal_value_equal(actual, expected)
    if op in {"!=", "<>"}:
        return not _signal_value_equal(actual, expected)
    if op in {">", ">=", "<", "<="}:
        actual_number = _signal_number_or_raise(actual, field=field)
        expected_number = _signal_number_or_raise(expected, field=field)
        if op == ">":
            return actual_number > expected_number
        if op == ">=":
            return actual_number >= expected_number
        if op == "<":
            return actual_number < expected_number
        return actual_number <= expected_number
    if op == "in":
        return any(_signal_value_equal(actual, item) for item in _signal_sequence(expected))
    if op in {"not_in", "not in"}:
        return not any(_signal_value_equal(actual, item) for item in _signal_sequence(expected))
    return False


def _signal_value_equal(actual: Any, expected: Any) -> bool:
    if isinstance(actual, (int, float)) or isinstance(expected, (int, float)):
        return math.isclose(_float_or_zero(actual), _float_or_zero(expected), rel_tol=1e-9, abs_tol=1e-9)
    return str(actual).lower() == str(expected).lower()


_SIGNAL_POLICY_FIELD_TYPES = {
    "score": "number",
    "period_return_pct": "number",
    "quote_pct_change": "number",
    "liquidity_score": "number",
    "provider_quality_score": "number",
    "rank": "number",
    "news_sentiment_label": "string",
    "quote_freshness": "string",
    "momentum_state": "string",
}
_NUMERIC_SIGNAL_OPS = {">", ">=", "<", "<="}
_STRING_SIGNAL_OPS = {"=", "==", "!=", "<>", "in", "not_in", "not in"}


def _signal_number_or_raise(value: Any, *, field: str) -> float:
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise FinanceProviderError(
            f"finance signal policy field requires a numeric value: {field}",
            error_type="invalid_signal_policy",
            hint="Use numeric values with >, >=, <, and <= operators.",
            details={"field": field, "value": value},
        ) from exc
    if not math.isfinite(parsed):
        raise FinanceProviderError(
            f"finance signal policy field requires a finite numeric value: {field}",
            error_type="invalid_signal_policy",
            hint="Use finite numeric values with >, >=, <, and <= operators.",
            details={"field": field, "value": value},
        )
    return parsed


def _signal_sequence(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _rank_native_signal_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for signal_type, field in (("entry_research_signal", "entry_signal"), ("exit_risk_signal", "exit_signal")):
        if int(row.get(field) or 0) < 1:
            continue
        signals.append(
            {
                "signal_type": signal_type,
                "engine": "velaria_native_realtime_stream",
                "event_time": row.get("event_time"),
                "market": row.get("market"),
                "symbol": row.get("symbol"),
                "rank": row.get("rank"),
                "score": row.get("score"),
                "period_return_pct": row.get("period_return_pct"),
                "quote_pct_change": row.get("quote_pct_change"),
                "quote_freshness": row.get("quote_freshness"),
                "news_sentiment_label": row.get("news_sentiment_label"),
                "momentum_state": row.get("momentum_state"),
                "liquidity_score": row.get("liquidity_score"),
                "provider_quality_score": row.get("provider_quality_score"),
                "signal_policy_source": row.get("signal_policy_source"),
                "signal_policy_preset": row.get("signal_policy_preset"),
                "signal_policy_json": row.get("signal_policy_json"),
            }
        )
    return signals


def _float_or_zero(value: Any) -> float:
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if math.isfinite(parsed) else 0.0


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    parsed = _float_or_zero(value)
    return parsed if math.isfinite(parsed) else None


def _build_candidate(
    *,
    symbol: str,
    market: str,
    quote: dict[str, Any],
    history_rows: list[dict[str, Any]],
    news_rows: list[dict[str, Any]],
    feature: dict[str, Any] | None = None,
) -> dict[str, Any]:
    history_metrics = _history_metrics(history_rows)
    news_sentiment = evaluate_news_sentiment(news_rows)
    feature = feature or _build_feature_row(symbol=symbol, market=market, quote=quote, history_rows=history_rows, news_rows=news_rows)
    score_parts = _candidate_score_parts(quote=quote, history=history_metrics, news_sentiment=news_sentiment, news_rows=news_rows, feature=feature)
    score = round(sum(score_parts.values()), 6)
    risk_flags: list[str] = []
    if quote.get("freshness") not in {"realtime", "near_realtime"}:
        risk_flags.append("quote_not_exchange_grade_realtime")
    if not news_rows:
        risk_flags.append("no_recent_news_rows")
    if not history_rows:
        risk_flags.append("no_history_rows")
    return {
        "market": market,
        "symbol": quote.get("symbol") or symbol,
        "recommendation_type": "research_candidate",
        "score": score,
        "period_return_pct": history_metrics.get("period_return_pct"),
        "quote_pct_change": quote.get("pct_change"),
        "news_sentiment_label": news_sentiment.get("label"),
        "quote_freshness": quote.get("freshness"),
        "score_parts": score_parts,
        "feature_snapshot": feature,
        "quote": quote,
        "history": history_metrics,
        "news_sentiment": news_sentiment,
        "news": news_rows,
        "risk_flags": risk_flags,
        "evidence": {
            "quote_provider": quote.get("provider"),
            "quote_source_url": quote.get("source_url"),
            "quote_fetched_at": quote.get("fetched_at"),
            "quote_freshness": quote.get("freshness"),
            "history_provider": history_rows[0].get("provider") if history_rows else None,
            "history_source_url": history_rows[0].get("source_url") if history_rows else None,
            "news_provider": news_rows[0].get("provider") if news_rows else None,
            "news_source_url": news_rows[0].get("source_url") if news_rows else None,
        },
        "summary": (
            f"{market}:{quote.get('symbol') or symbol} research score={score}; "
            f"period_return_pct={history_metrics.get('period_return_pct')}; "
            f"quote_pct_change={quote.get('pct_change')}; "
            f"news_sentiment={news_sentiment.get('label')}."
        ),
        "not_investment_advice": True,
    }


def _build_feature_row(
    *,
    symbol: str,
    market: str,
    quote: dict[str, Any],
    history_rows: list[dict[str, Any]],
    news_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    history = _history_metrics(history_rows)
    closes = [float(row.get("close")) for row in history_rows if isinstance(row.get("close"), (int, float))]
    volumes = [float(row.get("volume")) for row in history_rows if isinstance(row.get("volume"), (int, float))]
    returns = [_pct_change(closes[index - 1], closes[index]) for index in range(1, len(closes))]
    news_sentiment = evaluate_news_sentiment(news_rows)
    latest_close = closes[-1] if closes else _float_or_zero(quote.get("price"))
    ma_5 = _mean(closes[-5:])
    ma_20 = _mean(closes[-20:])
    ma_distance = _pct_change(ma_5, latest_close) if ma_5 not in (None, 0) and latest_close else None
    rsi_14 = _rsi(closes[-15:])
    return_1d = returns[-1] if returns else None
    return_5d = _pct_change(closes[-6], closes[-1]) if len(closes) >= 6 else None
    volatility = _stddev(returns[-20:])
    liquidity_score = _liquidity_score(float(quote.get("volume") or (volumes[-1] if volumes else 0.0)))
    provider_quality = _provider_quality_score(quote=quote, history_rows=history_rows, news_rows=news_rows)
    momentum_state = _momentum_state(period_return=history.get("period_return_pct"), quote_pct=quote.get("pct_change"), rsi=rsi_14)
    event_time = str(quote.get("event_time") or _utc_payload_time())
    return {
        "event_time": event_time,
        "event_type": "feature_snapshot",
        "source_key": str(quote.get("symbol") or symbol),
        "market": market,
        "symbol": str(quote.get("symbol") or symbol),
        "provider": "velaria-metric-engine",
        "period_return_pct": history.get("period_return_pct"),
        "return_1d_pct": return_1d,
        "return_5d_pct": return_5d,
        "volatility_pct": volatility,
        "ma_5": ma_5,
        "ma_20": ma_20,
        "ma_5_distance_pct": ma_distance,
        "rsi_14": rsi_14,
        "latest_volume": quote.get("volume") or (volumes[-1] if volumes else None),
        "avg_volume_5": _mean(volumes[-5:]),
        "liquidity_score": liquidity_score,
        "news_sentiment_score": news_sentiment.get("score"),
        "news_sentiment_label": news_sentiment.get("label"),
        "news_velocity": len(news_rows),
        "provider_quality_score": provider_quality,
        "momentum_state": momentum_state,
        "engine": "velaria_python_metric_graph",
        "replayable": True,
    }


def _history_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closes = [row.get("close") for row in rows if isinstance(row.get("close"), (int, float))]
    period_return = None
    if len(closes) >= 2 and closes[0] not in (None, 0):
        period_return = round(((closes[-1] - closes[0]) / closes[0]) * 100.0, 6)
    return {
        "row_count": len(rows),
        "first_date": rows[0].get("date") if rows else None,
        "last_date": rows[-1].get("date") if rows else None,
        "first_close": closes[0] if closes else None,
        "last_close": closes[-1] if closes else None,
        "period_return_pct": period_return,
    }


def _candidate_score_parts(
    *,
    quote: dict[str, Any],
    history: dict[str, Any],
    news_sentiment: dict[str, Any],
    news_rows: list[dict[str, Any]],
    feature: dict[str, Any],
) -> dict[str, float]:
    history_part = _clamp(float(history.get("period_return_pct") or 0.0), -12.0, 12.0)
    quote_part = _clamp(float(quote.get("pct_change") or 0.0) * 2.0, -8.0, 8.0)
    liquidity_part = float(feature.get("liquidity_score") or 0.0)
    news_part = _clamp(float(news_sentiment.get("score") or 0.0) * 3.0, -4.0, 4.0) + min(1.0, len(news_rows) * 0.2)
    freshness_penalty = -0.5 if quote.get("freshness") not in {"realtime", "near_realtime"} else 0.0
    volatility_penalty = -min(3.0, max(0.0, float(feature.get("volatility_pct") or 0.0) / 4.0))
    provider_quality = _clamp(float(feature.get("provider_quality_score") or 0.0) - 1.0, -2.0, 0.0)
    missing_penalty = 0.0
    if not history.get("row_count"):
        missing_penalty -= 2.0
    if not news_rows:
        missing_penalty -= 1.0
    return {
        "history_momentum": round(history_part, 6),
        "quote_momentum": round(quote_part, 6),
        "liquidity": round(liquidity_part, 6),
        "news_sentiment": round(news_part, 6),
        "freshness_penalty": round(freshness_penalty, 6),
        "volatility_penalty": round(volatility_penalty, 6),
        "provider_quality": round(provider_quality, 6),
        "missing_data_penalty": round(missing_penalty, 6),
    }


def _pct_change(previous: float | None, current: float | None) -> float | None:
    if previous in (None, 0) or current is None:
        return None
    return round(((float(current) - float(previous)) / float(previous)) * 100.0, 6)


def _mean(values: list[float]) -> float | None:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return round(sum(clean) / len(clean), 6) if clean else None


def _stddev(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if len(clean) < 2:
        return None
    avg = sum(clean) / len(clean)
    variance = sum((value - avg) ** 2 for value in clean) / (len(clean) - 1)
    return round(math.sqrt(variance), 6)


def _rsi(closes: list[float]) -> float | None:
    if len(closes) < 2:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for index in range(1, len(closes)):
        delta = closes[index] - closes[index - 1]
        if delta >= 0:
            gains.append(delta)
        else:
            losses.append(abs(delta))
    avg_gain = sum(gains) / max(1, len(closes) - 1)
    avg_loss = sum(losses) / max(1, len(closes) - 1)
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 6)


def _liquidity_score(volume: float) -> float:
    return 0.0 if volume <= 0 else round(min(4.0, math.log10(volume + 1.0) / 2.0), 6)


def _provider_quality_score(*, quote: dict[str, Any], history_rows: list[dict[str, Any]], news_rows: list[dict[str, Any]]) -> float:
    score = 1.0
    if quote.get("freshness") in {"realtime", "near_realtime"}:
        score += 0.5
    elif quote.get("freshness") == "delayed":
        score += 0.2
    if history_rows:
        score += 0.3
    if news_rows:
        score += 0.2
    return round(min(2.0, score), 6)


def _momentum_state(*, period_return: Any, quote_pct: Any, rsi: Any) -> str:
    period = _float_or_zero(period_return)
    quote = _float_or_zero(quote_pct)
    rsi_value = _float_or_zero(rsi)
    if period > 5.0 and quote >= 0.0 and rsi_value >= 50.0:
        return "bullish"
    if period < -5.0 or quote <= -3.0 or (rsi_value and rsi_value < 35.0):
        return "bearish"
    return "neutral"


def _quote_symbol_key(symbol: Any) -> str:
    value = str(symbol or "").strip().upper()
    if "." in value:
        left, right = value.split(".", 1)
        value = right if left.isdigit() else left
    if value.startswith("US"):
        value = value[2:]
    return value


def _watch_quotes(args: argparse.Namespace) -> int:
    symbol = str(args.symbol).strip()
    source_id = args.source_id or f"finance_{args.market}_{_id_part(symbol)}_watch"
    monitor_id = args.monitor_id or f"monitor_{source_id}"
    display_name = args.name or f"finance {args.market} {symbol} watch"
    source, monitor = _upsert_watch_source_and_monitor(args, source_id=source_id, monitor_id=monitor_id, display_name=display_name)

    ticks, interrupted = _collect_watch_ticks(args, source_id=source_id, monitor_id=monitor_id, emit_jsonl=bool(args.jsonl))

    if args.jsonl:
        if interrupted:
            print(json.dumps({"ok": True, "action": "watch", "interrupted": True, "ticks": len(ticks)}, ensure_ascii=False), flush=True)
        return 0
    return _emit_json(
        {
            "ok": True,
            "action": "watch",
            "provider": args.provider,
            "market": args.market,
            "symbol": symbol,
            "source": source,
            "monitor": monitor,
            "ticks": ticks,
            "tick_count": len(ticks),
            "interrupted": interrupted,
        }
    )


def _collect_watch_ticks(
    args: argparse.Namespace,
    *,
    source_id: str,
    monitor_id: str,
    emit_jsonl: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    ticks: list[dict[str, Any]] = []
    iteration = 0
    interrupted = False
    try:
        while args.iterations == 0 or iteration < args.iterations:
            iteration += 1
            tick = _run_watch_tick(args, source_id=source_id, monitor_id=monitor_id, iteration=iteration)
            ticks.append(tick)
            if emit_jsonl:
                print(json.dumps(tick, ensure_ascii=False, sort_keys=True), flush=True)
            if args.iterations != 0 and iteration >= args.iterations:
                break
            time.sleep(max(0.0, float(args.interval_sec)))
    except KeyboardInterrupt:
        interrupted = True
    return ticks, interrupted


def _upsert_watch_source_and_monitor(
    args: argparse.Namespace,
    *,
    source_id: str,
    monitor_id: str,
    display_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    with AgenticStore() as store:
        source = store.upsert_source(
            {
                "source_id": source_id,
                "kind": "external_event",
                "name": display_name,
                "schema_binding": finance_quote_schema_binding(),
                "metadata": {
                    "domain": "finance",
                    "provider": args.provider,
                    "market": args.market,
                    "symbols": [str(args.symbol).strip()],
                    "watch": True,
                },
            }
        )
        monitor = store.upsert_monitor(_watch_monitor_payload(args, source_id=source_id, monitor_id=monitor_id, name=display_name))
    return source, monitor


def _run_watch_tick(args: argparse.Namespace, *, source_id: str, monitor_id: str, iteration: int) -> dict[str, Any]:
    rows = fetch_quotes(provider=args.provider, market=args.market, symbols=[args.symbol])
    with AgenticStore() as store:
        observations = [store.append_external_event(source_id, row) for row in rows]
        result = execute_monitor_once(store, monitor_id)
    focus_events = result.get("focus_events") or []
    prompt = "" if args.no_analysis_prompt else build_research_prompt(
        focus_events=focus_events,
        datasets=result.get("artifacts") or [],
        user_question=f"分析 {args.market} 市场标的 {args.symbol} 的最新监听事件。",
    )
    latest = rows[0] if rows else {}
    return {
        "ok": True,
        "action": "watch-tick",
        "iteration": iteration,
        "provider": args.provider,
        "market": args.market,
        "symbol": latest.get("symbol") or args.symbol,
        "quote": latest,
        "observations": observations,
        "run_id": result.get("run_id"),
        "signals": result.get("signals") or [],
        "focus_events": focus_events,
        "artifacts": result.get("artifacts") or [],
        "analysis": _quote_analysis(latest, focus_events=focus_events),
        **({"analysis_prompt": prompt} if prompt else {}),
    }


def _watch_monitor_payload(args: argparse.Namespace, *, source_id: str, monitor_id: str, name: str) -> dict[str, Any]:
    rule_spec = _watch_rule_spec(args, source_id=source_id, name=name)
    compiled = compile_rule_spec(rule_spec)
    return {
        "monitor_id": monitor_id,
        "name": name,
        "intent_text": f"watch public finance quote observations for {args.market}:{args.symbol}",
        "source": {"kind": "external_event", "source_id": source_id, "binding": source_id},
        "compiled_rules": compiled["compiled_rules"],
        "execution_mode": compiled["execution_mode"],
        "rule_spec": compiled["rule_spec"],
        "validation": {
            "status": "valid",
            "execution_spec": compiled["execution_spec"],
            "promotion_rule": compiled["promotion_rule"],
            "event_extraction": compiled["event_extraction"],
            "suppression_rule": compiled["suppression_rule"],
        },
        "enabled": True,
        "cooldown_sec": max(0, int(args.cooldown_sec)),
        "tags": ["finance", "watch", str(args.market), str(args.symbol)],
    }


def _watch_rule_spec(args: argparse.Namespace, *, source_id: str, name: str) -> dict[str, Any]:
    where = [f"symbol = '{_sql_literal(str(args.symbol))}'"]
    if args.pct_change_threshold is not None:
        where.append(f"ABS(pct_change) >= {float(args.pct_change_threshold)}")
    if args.min_price is not None:
        where.append(f"price >= {float(args.min_price)}")
    if args.max_price is not None:
        where.append(f"price <= {float(args.max_price)}")
    query = (
        "SELECT CONCAT(market, ':', symbol) AS display_symbol, market, price, volume, pct_change, provider, freshness, delay_sec, fetched_at, ingested_at "
        "FROM current_snapshot "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY ingested_at DESC LIMIT 1"
    )
    return {
        "version": "v1",
        "name": name,
        "source": {"kind": "external_event", "binding": source_id},
        "execution": {"mode": "batch"},
        "signal": {"sql": query},
        "promote": {"when": {"min_rows": 1}},
        "event": {
            "title": "{display_symbol} quote observed",
            "summary": "price={price}, pct_change={pct_change}, freshness={freshness}, provider={provider}",
            "severity": {"default": "info", "rules": []},
            "key_fields": ["display_symbol", "market", "price", "pct_change", "freshness"],
            "sample_rows": 5,
        },
        "suppress": {"cooldown": f"{max(0, int(args.cooldown_sec))}s", "dedupe_by": []},
    }


def _quote_analysis(row: dict[str, Any], *, focus_events: list[dict[str, Any]]) -> dict[str, Any]:
    pct_change = row.get("pct_change")
    movement = "unknown"
    if isinstance(pct_change, (int, float)):
        if pct_change > 0:
            movement = "up"
        elif pct_change < 0:
            movement = "down"
        else:
            movement = "flat"
    return {
        "summary": (
            f"{row.get('market')}:{row.get('symbol')} latest price={row.get('price')}, "
            f"pct_change={row.get('pct_change')}, volume={row.get('volume')}, "
            f"freshness={row.get('freshness')}, provider={row.get('provider')}."
        ),
        "movement": movement,
        "focus_event_count": len(focus_events),
        "evidence": {
            "provider": row.get("provider"),
            "source_url": row.get("source_url"),
            "fetched_at": row.get("fetched_at"),
            "freshness": row.get("freshness"),
            "delay_sec": row.get("delay_sec"),
        },
        "next_step": "Use analysis_prompt with Velaria Agent for live news, filings, and uncertainty checks.",
        "disclaimer": "Research assistance only; not investment advice.",
    }


def _pipeline_analysis(history_rows: list[dict[str, Any]], quote: dict[str, Any], *, focus_events: list[dict[str, Any]]) -> dict[str, Any]:
    closes = [row.get("close") for row in history_rows if isinstance(row.get("close"), (int, float))]
    history_return = None
    if len(closes) >= 2 and closes[0] not in (None, 0):
        history_return = round(((closes[-1] - closes[0]) / closes[0]) * 100.0, 6)
    quote_analysis = _quote_analysis(quote, focus_events=focus_events)
    return {
        **quote_analysis,
        "summary": (
            f"{quote.get('market')}:{quote.get('symbol')} pipeline used {len(history_rows)} historical rows "
            f"and {len(focus_events)} focus events; latest price={quote.get('price')}, pct_change={quote.get('pct_change')}."
        ),
        "history": {
            "row_count": len(history_rows),
            "first_date": history_rows[0].get("date") if history_rows else None,
            "last_date": history_rows[-1].get("date") if history_rows else None,
            "first_close": closes[0] if closes else None,
            "last_close": closes[-1] if closes else None,
            "period_return_pct": history_return,
        },
        "next_step": "Use analysis_prompt with Velaria Agent to combine historical trend, live quote events, news, filings, and uncertainty checks.",
    }


def _render_sources(payload: dict[str, Any]) -> str:
    lines = ["Velaria 公开财经数据源", ""]
    for source in payload["sources"]:
        lines.append(f"- {source['provider']}: markets={','.join(source['markets'])}; commands={','.join(source['commands'])}")
        lines.append(f"  freshness={json.dumps(source['freshness'], ensure_ascii=False, sort_keys=True)}")
        lines.append(f"  source_url={source['source_url']}")
        lines.append(f"  note={source['notes']}")
    lines.append("")
    lines.append("下一步:")
    for step in payload["next_steps"]:
        lines.append(f"- {step}")
    lines.append("")
    lines.append(payload["disclaimer"])
    return "\n".join(lines)


def _render_stream_history_report(payload: dict[str, Any]) -> str:
    source = payload.get("source") or {}
    lines = [
        "# Finance Stream History",
        "",
        f"- source_id: {source.get('source_id')}",
        f"- rows: {payload.get('row_count')}",
    ]
    for row in payload.get("rows") or []:
        lines.append(
            "- {event_time} {signal_type} {market}:{symbol} score={score} quote_pct_change={quote_pct_change}".format(
                event_time=row.get("event_time"),
                signal_type=row.get("signal_type") or row.get("event_type"),
                market=row.get("market"),
                symbol=row.get("symbol"),
                score=row.get("score"),
                quote_pct_change=row.get("quote_pct_change"),
            )
        )
    return "\n".join(lines)


def _render_watch_session_start_report(payload: dict[str, Any]) -> str:
    session = payload.get("watch_session") or {}
    return "\n".join(
        [
            "# Finance Watch Session",
            "",
            f"- session_id: {session.get('session_id')}",
            f"- status: {session.get('status')}",
            f"- ticks: {payload.get('tick_count')}",
            f"- signals: {len(payload.get('stream_signals') or [])}",
            "- disclaimer: Research signals only; not investment advice.",
        ]
    )


def _render_watch_session_async_start_report(payload: dict[str, Any]) -> str:
    run = payload.get("run") or {}
    lines = [
        "# Finance Watch Session Async Run",
        "",
        f"- session_id: {payload.get('watch_session_id')}",
        f"- pid: {run.get('pid')}",
        f"- log_path: {run.get('log_path')}",
        f"- core_runtime: {run.get('core_runtime')}",
        f"- ai_cli_runtime: {run.get('ai_cli_runtime')}",
        "- disclaimer: Research signals only; not investment advice.",
    ]
    return "\n".join(lines)


def _render_watch_session_list_report(payload: dict[str, Any]) -> str:
    lines = ["# Finance Watch Sessions", "", f"- sessions: {payload.get('session_count', len(payload.get('sessions') or []))}"]
    for session in payload.get("sessions") or []:
        lines.append(f"- {session.get('session_id')} status={session.get('status')} market={session.get('market')} ticks={session.get('tick_count')}")
    return "\n".join(lines)


def _render_watch_session_rows_report(payload: dict[str, Any]) -> str:
    session = payload.get("watch_session") or {}
    lines = ["# Finance Watch Session Rows", "", f"- session_id: {session.get('session_id')}", f"- rows: {payload.get('row_count')}"]
    for row in payload.get("rows") or []:
        lines.append(f"- {row.get('feed')} {row.get('event_time')} {row.get('event_type')} {row.get('market')}:{row.get('symbol') or row.get('source_key')}")
    return "\n".join(lines)


def _render_watch_session_summary_report(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    lines = [
        "# Finance Watch Session Summary",
        "",
        f"- session_id: {summary.get('session_id')}",
        f"- status: {summary.get('status')}",
        f"- event_count: {summary.get('event_count')}",
        f"- signal_count: {summary.get('signal_count')}",
        f"- market_context_count: {summary.get('market_context_count')}",
        f"- fundamental_count: {summary.get('fundamental_count')}",
        f"- note: {summary.get('review_note')}",
    ]
    return "\n".join(lines)


def _render_watch_session_status_report(payload: dict[str, Any]) -> str:
    run = payload.get("run") or {}
    lines = [
        "# Finance Watch Session Status",
        "",
        f"- session_id: {payload.get('watch_session_id')}",
        f"- pid: {run.get('pid')}",
        f"- process_running: {payload.get('process_running')}",
        f"- status: {payload.get('effective_status') or run.get('status')}",
        f"- log_path: {run.get('log_path')}",
    ]
    return "\n".join(lines)


def _render_watch_session_logs_report(payload: dict[str, Any]) -> str:
    lines = ["# Finance Watch Session Logs", "", f"- session_id: {payload.get('watch_session_id')}", f"- lines: {payload.get('line_count')}"]
    lines.extend(str(line) for line in payload.get("lines") or [])
    return "\n".join(lines)


def _render_watch_session_review_report(payload: dict[str, Any]) -> str:
    review = payload.get("review") or {}
    summary = review.get("summary") or {}
    lines = [
        "# Finance Watch Session Review",
        "",
        f"- session_id: {review.get('session_id')}",
        f"- status: {review.get('effective_status')}",
        f"- process_running: {review.get('process_running')}",
        f"- events: {summary.get('event_count')}",
        f"- signals: {summary.get('signal_count')}",
        f"- diagnostics: {review.get('diagnostic_count')}",
    ]
    for diagnostic in review.get("diagnostics") or []:
        lines.append(f"- {diagnostic.get('severity')} {diagnostic.get('type')}: {diagnostic.get('message')}")
    lines.append("- disclaimer: Research signals only; not investment advice.")
    return "\n".join(lines)


def _render_watch_session_supervise_report(payload: dict[str, Any]) -> str:
    latest = payload.get("latest_review") or {}
    return "\n".join(
        [
            "# Finance Watch Session Supervisor",
            "",
            f"- session_id: {payload.get('watch_session_id')}",
            f"- reviews: {payload.get('review_count')}",
            f"- interrupted: {payload.get('interrupted')}",
            f"- latest_status: {latest.get('effective_status')}",
            f"- latest_diagnostics: {latest.get('diagnostic_count')}",
            "- disclaimer: Research signals only; not investment advice.",
        ]
    )


def _render_intelligence_report(payload: dict[str, Any]) -> str:
    data_plane = payload.get("data_plane") or {}
    runtime_plane = payload.get("runtime_plane") or {}
    ai_plane = payload.get("ai_plane") or {}
    replay = payload.get("replay") or {}
    lines = [
        "# Finance Intelligence",
        "",
        f"- action: {payload.get('action')}",
        f"- intelligence_id: {payload.get('intelligence_id')}",
        f"- watch_session_id: {payload.get('watch_session_id') or (payload.get('watch_session') or {}).get('session_id')}",
        f"- core_runtime: {runtime_plane.get('core_runtime')}",
        f"- data_runtime: {data_plane.get('data_runtime')}",
        f"- ai_runtime: {ai_plane.get('ai_runtime')}",
        f"- event_count: {data_plane.get('event_count') or replay.get('event_count')}",
        f"- signal_count: {data_plane.get('signal_count') or replay.get('signal_count')}",
        "- disclaimer: Research signals only; not investment advice.",
    ]
    return "\n".join(lines)


def _render_doctor(payload: dict[str, Any]) -> str:
    lines = ["Velaria Finance Doctor", ""]
    lines.append(f"status: {'ok' if payload['ok'] else 'failed'}")
    for check in payload["checks"]:
        lines.append(f"- {check['name']}: {check['status']} - {check['message']}")
        if check.get("hint"):
            lines.append(f"  hint: {check['hint']}")
    lines.append("")
    lines.append("Next steps:")
    for step in payload["next_steps"]:
        lines.append(f"- {step}")
    return "\n".join(lines)


def _render_analysis_report(payload: dict[str, Any]) -> str:
    quote = payload["quote"]
    analysis = payload.get("analysis") or {}
    evidence = analysis.get("evidence") or {}
    focus_events = payload.get("focus_events") or []
    lines = [
        f"Velaria 金融分析: {quote.get('market')}:{quote.get('symbol')}",
        "",
        "行情快照",
        f"- 名称: {_display(quote.get('name'))}",
        f"- 最新价: {_display(quote.get('price'))}",
        f"- 涨跌幅: {_display(quote.get('pct_change'))}",
        f"- 成交量: {_display(quote.get('volume'))}",
        f"- 趋势: {_display(analysis.get('movement'))}",
        "",
        "监控事件",
        f"- FocusEvent 数量: {len(focus_events)}",
    ]
    for event in focus_events[:3]:
        lines.append(f"- {event.get('title')}: {event.get('summary')}")
    lines.extend(
        [
            "",
            "数据来源",
            f"- provider: {_display(evidence.get('provider') or quote.get('provider'))}",
            f"- source_url: {_display(evidence.get('source_url') or quote.get('source_url'))}",
            f"- fetched_at: {_display(evidence.get('fetched_at') or quote.get('fetched_at'))}",
            f"- freshness: {_display(evidence.get('freshness') or quote.get('freshness'))}",
            f"- delay_sec: {_display(evidence.get('delay_sec') if evidence.get('delay_sec') is not None else quote.get('delay_sec'))}",
            "",
            "下一步",
            f"- {_display(analysis.get('next_step'))}",
            "- 在 Velaria Agent 中可继续要求结合公告、新闻和历史数据做联网研究。",
            "",
            "声明",
            "- 这是研究辅助，不是投资建议，不包含买卖指令或收益承诺。",
        ]
    )
    return "\n".join(lines)


def _render_pipeline_report(payload: dict[str, Any]) -> str:
    quote = payload.get("quote") or {}
    history = payload.get("history") or {}
    subscription = payload.get("subscription") or {}
    analysis = payload.get("analysis") or {}
    history_analysis = analysis.get("history") or {}
    lines = [
        f"Velaria 金融完整链路: {payload.get('market')}:{payload.get('symbol')}",
        "",
        "历史数据",
        f"- provider: {_display(history.get('provider'))}",
        f"- rows: {_display(history.get('row_count'))}",
        f"- path: {_display(history.get('path'))}",
        f"- range: {_display(history.get('start_date'))} -> {_display(history.get('end_date'))}",
        f"- period_return_pct: {_display(history_analysis.get('period_return_pct'))}",
        "",
        "实时订阅",
        f"- provider: {_display(subscription.get('provider'))}",
        f"- ticks: {_display(subscription.get('tick_count'))}",
        f"- latest: {_display(quote.get('market'))}:{_display(quote.get('symbol'))} price={_display(quote.get('price'))}, pct_change={_display(quote.get('pct_change'))}",
        "",
        "分析",
        f"- {analysis.get('summary')}",
        f"- FocusEvent 数量: {len(payload.get('focus_events') or [])}",
        "",
        "Service 集成",
        "- CLI 写入 Velaria AgenticStore；同一个 VELARIA_HOME 下，现有 service generic routes 可读取 source、monitor 和 focus-events。",
        "",
        "声明",
        "- 这是研究辅助，不是投资建议，不包含买卖指令或收益承诺。",
    ]
    return "\n".join(lines)


def _render_rank_report(payload: dict[str, Any]) -> str:
    lines = [
        f"Velaria 研究候选排名: {payload.get('market')}",
        "",
        "Top research candidates",
    ]
    for candidate in payload.get("research_candidates") or []:
        sentiment = candidate.get("news_sentiment") or {}
        history = candidate.get("history") or {}
        quote = candidate.get("quote") or {}
        lines.append(
            f"- #{candidate.get('rank')} {candidate.get('symbol')}: score={candidate.get('score')}, "
            f"price={quote.get('price')}, pct_change={quote.get('pct_change')}, "
            f"period_return_pct={history.get('period_return_pct')}, news={sentiment.get('label')}"
        )
    lines.extend(
        [
            "",
            "声明",
            "- 这些是研究候选，不是投资建议，不包含买卖指令或收益承诺。",
        ]
    )
    return "\n".join(lines)


def _default_history_output(market: str, symbol: str, output_format: str) -> pathlib.Path:
    home = pathlib.Path(os.environ.get("VELARIA_HOME", ".velaria"))
    suffix = "jsonl" if output_format == "jsonl" else "parquet"
    return home / "finance" / "history" / f"{market}_{_id_part(symbol)}_history.{suffix}"


def _service_integration_payload(*, source_id: str, monitor_id: str) -> dict[str, Any]:
    return {
        "requires_service": False,
        "shared_state": "AgenticStore",
        "note": "Start velaria_service with the same VELARIA_HOME to inspect this CLI-created finance chain through generic service APIs.",
        "generic_routes": [
            "GET /api/v1/external-events/sources",
            f"GET /api/v1/monitors/{monitor_id}",
            "GET /api/v1/focus-events",
        ],
    }


def _rank_service_integration_payload(*, source_id: str, stream_monitors: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    monitor_ids = [monitor["monitor_id"] for monitor in stream_monitors or []]
    return {
        "requires_service": False,
        "shared_state": "AgenticStore",
        "note": "Start velaria_service with the same VELARIA_HOME to inspect ranking observations, stream monitors, and focus events through generic service APIs.",
        "generic_routes": [
            "GET /api/v1/external-events/sources",
            *[f"GET /api/v1/monitors/{monitor_id}" for monitor_id in monitor_ids],
            "POST /api/v1/focus-events/poll",
        ],
        "stream_monitor_ids": monitor_ids,
    }


def _display(value: Any) -> str:
    return "unknown" if value is None else str(value)


def _utc_payload_time() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _id_part(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in value).strip("_") or "symbol"


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
