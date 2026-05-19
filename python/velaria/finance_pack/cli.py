from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import pathlib
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
    fetch_history,
    fetch_news,
    fetch_quotes,
    finance_quote_schema_binding,
    provider_catalog,
    provider_names_for_operation,
)


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
        help="Continuously rank top research candidates from quotes, history, news, and sentiment.",
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
    rank.add_argument("--cooldown-sec", type=int, default=300, help="FocusEvent suppression cooldown for stream monitor signals.")
    rank.add_argument("--until-time", help="Run until this RFC3339 timestamp, e.g. 2026-05-18T16:00:00-04:00.")
    rank.add_argument("--interval-sec", type=float, default=30.0, help="Seconds between polling iterations.")
    rank.add_argument("--iterations", type=int, default=1, help="Number of ranking iterations. Use 0 to run until interrupted.")
    rank.add_argument("--jsonl", action="store_true", help="Emit one JSON object per ranking tick.")
    _add_report_format(rank)

    history = subparsers.add_parser("fetch-history", help="Fetch public historical OHLCV data.")
    _add_provider_market(history, default_provider="yahoo", choices=provider_names_for_operation("fetch_history"))
    history.add_argument("--symbol", required=True, help="Provider-specific symbol, e.g. 000001 or 105.AAPL.")
    history.add_argument("--start-date", required=True, help="YYYYMMDD.")
    history.add_argument("--end-date", required=True, help="YYYYMMDD.")
    history.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    history.add_argument("--adjust", default="", help="Provider adjustment flag, e.g. qfq/hfq for AkShare.")
    _add_output(history)

    quotes = subparsers.add_parser("fetch-quotes", help="Fetch public quote rows.")
    _add_provider_market(quotes, default_provider="tencent")
    quotes.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    _add_output(quotes)

    news = subparsers.add_parser("fetch-news", help="Fetch public news rows and sentiment evidence.")
    news.add_argument("--provider", default="google-news", choices=provider_names_for_operation("fetch_news"))
    news.add_argument("--market", required=True, choices=["cn", "us"])
    news.add_argument("--symbol", required=True, help="Single symbol, e.g. 000001 or AAPL.")
    news.add_argument("--query", help="Override provider search query. Defaults to a market-aware symbol query.")
    news.add_argument("--limit", type=int, default=5, help="Maximum news rows to fetch.")
    _add_output(news)

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
    parser.add_argument("--provider", default=default_provider, choices=choices or ["akshare", "tencent"])
    parser.add_argument("--market", required=True, choices=["cn", "us"])


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
            "finance analyze --market cn --symbol 000001",
            "finance watch --market cn --symbol 000001 --iterations 0 --jsonl",
        ],
        "disclaimer": "Research assistance only; not investment advice.",
    }
    if args.report_format == "json":
        return _emit_json(payload)
    print(_render_sources(payload))
    return 0


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
                "metadata": {
                    "domain": "finance",
                    "workflow": "rank-candidates",
                    "market": args.market,
                    "symbols": symbols,
                    "history_provider": args.history_provider,
                    "quote_provider": args.quote_provider,
                    "news_provider": args.news_provider,
                },
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
    with AgenticStore() as store:
        quote_source = store.upsert_source(
            {
                "source_id": quote_source_id,
                "kind": "external_event",
                "name": f"finance {args.market} rank quote rows",
                "schema_binding": finance_quote_schema_binding(),
                "metadata": {
                    "domain": "finance",
                    "workflow": "rank-candidates",
                    "raw_feed": "quotes",
                    "market": args.market,
                    "symbols": symbols,
                    "provider": args.quote_provider,
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
                    "workflow": "rank-candidates",
                    "raw_feed": "history",
                    "market": args.market,
                    "symbols": symbols,
                    "provider": args.history_provider,
                    "start_date": args.start_date,
                    "end_date": args.end_date,
                    "period": args.period,
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
                    "workflow": "rank-candidates",
                    "raw_feed": "news",
                    "market": args.market,
                    "symbols": symbols,
                    "provider": args.news_provider,
                },
            }
        )
    return {
        "quotes": quote_source,
        "history": history_source,
        "news": news_source,
        "candidates": candidate_source,
    }


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
    quotes = {_quote_symbol_key(row.get("symbol")): row for row in quote_rows}
    history_rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    news_rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
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
        candidates.append(_build_candidate(symbol=symbol, market=args.market, quote=quote, history_rows=history_rows, news_rows=news_rows))
    ranked = sorted(candidates, key=lambda item: item["score"], reverse=True)
    top = ranked[: max(1, int(args.top))]
    event_time = (top[0].get("event_time") if top else None) or _utc_payload_time()
    for index, candidate in enumerate(top, start=1):
        candidate["rank"] = index
        candidate["event_time"] = event_time
        candidate["event_type"] = "research_candidate"
        candidate["source_key"] = candidate["symbol"]
    with AgenticStore() as store:
        _append_rank_raw_rows(
            store,
            args=args,
            raw_sources=raw_sources,
            quote_rows=quote_rows,
            history_rows_by_symbol=history_rows_by_symbol,
            news_rows_by_symbol=news_rows_by_symbol,
        )
        observations = [store.append_external_event(source_id, candidate) for candidate in top]
    native_stream_signals = _push_and_poll_rank_native_stream(args, native_stream=native_stream, candidates=top)
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
) -> None:
    if not raw_sources:
        return
    for row in quote_rows:
        store.append_external_event(raw_sources["quotes"]["source_id"], row)
    for symbol, rows in history_rows_by_symbol.items():
        for row in rows:
            store.append_external_event(raw_sources["history"]["source_id"], _rank_history_event(row, market=args.market, symbol=symbol))
    for symbol, rows in news_rows_by_symbol.items():
        for row in rows:
            store.append_external_event(raw_sources["news"]["source_id"], _rank_news_event(row, market=args.market, symbol=symbol))


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
    ]


def _rank_native_stream_sql() -> str:
    return (
        "SELECT event_time, market, symbol, rank, score, period_return_pct, quote_pct_change, "
        "entry_signal, exit_signal, quote_freshness, news_sentiment_label "
        "FROM finance_rank_candidate_stream "
        "WHERE entry_signal >= 1 OR exit_signal >= 1"
    )


def _start_rank_native_stream(args: argparse.Namespace) -> dict[str, Any]:
    try:
        session = velaria.Session()
        source = session.create_realtime_stream_source(_rank_native_stream_schema())
        stream_df = session.read_realtime_stream_source(source)
        session.create_temp_view("finance_rank_candidate_stream", stream_df)
        sink = session.create_realtime_stream_sink()
        query_df = session.stream_sql(_rank_native_stream_sql())
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
        "sql": _rank_native_stream_sql(),
        "session": session,
        "source": source,
        "sink": sink,
        "query": query,
        "worker": worker,
        "max_batches": max_batches,
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
        "started_at": native_stream["started_at"],
        **({"stopped_at": native_stream["stopped_at"]} if native_stream.get("stopped_at") else {}),
    }


def _push_and_poll_rank_native_stream(
    args: argparse.Namespace,
    *,
    native_stream: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not native_stream:
        return []
    rows = [_rank_native_stream_row(args, candidate) for candidate in candidates]
    if not rows:
        return []
    native_stream["source"].push_rows(rows)
    signals: list[dict[str, Any]] = []
    deadline = time.monotonic() + max(0.0, float(args.native_stream_poll_timeout_sec))
    while time.monotonic() <= deadline:
        batch = native_stream["sink"].poll_arrow()
        if batch is not None:
            for row in batch.to_pylist():
                signals.extend(_rank_native_signal_rows(row))
            if signals:
                return signals
        time.sleep(0.05)
    return signals


def _rank_native_stream_row(args: argparse.Namespace, candidate: dict[str, Any]) -> dict[str, Any]:
    score = _float_or_zero(candidate.get("score"))
    period_return_pct = _float_or_zero(candidate.get("period_return_pct"))
    quote_pct_change = _float_or_zero(candidate.get("quote_pct_change"))
    news_label = str(candidate.get("news_sentiment_label") or "unknown")
    entry_signal = int(
        score >= float(args.entry_score_threshold)
        and period_return_pct >= float(args.entry_return_threshold)
        and news_label != "negative"
    )
    exit_signal = int(
        score <= float(args.exit_score_threshold)
        or quote_pct_change <= float(args.exit_quote_pct_threshold)
        or news_label == "negative"
    )
    return {
        "event_time": str(candidate.get("event_time") or _utc_payload_time()),
        "market": str(candidate.get("market") or args.market),
        "symbol": str(candidate.get("symbol") or ""),
        "rank": int(candidate.get("rank") or 0),
        "score": score,
        "period_return_pct": period_return_pct,
        "quote_pct_change": quote_pct_change,
        "entry_signal": entry_signal,
        "exit_signal": exit_signal,
        "quote_freshness": str(candidate.get("quote_freshness") or "unknown"),
        "news_sentiment_label": news_label,
    }


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


def _build_candidate(
    *,
    symbol: str,
    market: str,
    quote: dict[str, Any],
    history_rows: list[dict[str, Any]],
    news_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    history_metrics = _history_metrics(history_rows)
    news_sentiment = evaluate_news_sentiment(news_rows)
    score_parts = _candidate_score_parts(quote=quote, history=history_metrics, news_sentiment=news_sentiment, news_rows=news_rows)
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
) -> dict[str, float]:
    history_part = _clamp(float(history.get("period_return_pct") or 0.0), -12.0, 12.0)
    quote_part = _clamp(float(quote.get("pct_change") or 0.0) * 2.0, -8.0, 8.0)
    volume = float(quote.get("volume") or 0.0)
    liquidity_part = 0.0 if volume <= 0 else min(4.0, math.log10(volume + 1.0) / 2.0)
    news_part = _clamp(float(news_sentiment.get("score") or 0.0) * 3.0, -4.0, 4.0) + min(1.0, len(news_rows) * 0.2)
    freshness_penalty = -0.5 if quote.get("freshness") not in {"realtime", "near_realtime"} else 0.0
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
        "missing_data_penalty": round(missing_penalty, 6),
    }


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
