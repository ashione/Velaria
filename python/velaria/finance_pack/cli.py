from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from velaria.agentic_dsl import compile_rule_spec
from velaria.agentic_runtime import execute_monitor_once
from velaria.agentic_store import AgenticStore

from . import (
    FinanceProviderError,
    build_research_prompt,
    fetch_history,
    fetch_quotes,
    finance_quote_schema_binding,
)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
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

    history = subparsers.add_parser("fetch-history", help="Fetch public historical OHLCV data.")
    _add_provider_market(history)
    history.add_argument("--symbol", required=True, help="Provider-specific symbol, e.g. 000001 or 105.AAPL.")
    history.add_argument("--start-date", required=True, help="YYYYMMDD.")
    history.add_argument("--end-date", required=True, help="YYYYMMDD.")
    history.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    history.add_argument("--adjust", default="", help="Provider adjustment flag, e.g. qfq/hfq for AkShare.")
    _add_output(history)

    quotes = subparsers.add_parser("fetch-quotes", help="Fetch public quote rows.")
    _add_provider_market(quotes)
    quotes.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    _add_output(quotes)

    ingest = subparsers.add_parser("ingest-quotes", help="Fetch quotes and append them to a Velaria external_event source.")
    _add_provider_market(ingest)
    ingest.add_argument("--symbols", required=True, help="Comma-separated provider-specific symbols.")
    ingest.add_argument("--source-id", help="Defaults to finance_<market>_quotes.")
    ingest.add_argument("--name", help="Source display name.")

    watch = subparsers.add_parser(
        "watch",
        help="Watch one public quote symbol, ingest observations, run a monitor, and emit analysis context.",
    )
    _add_provider_market(watch)
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


def _add_provider_market(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--provider", default="akshare", choices=["akshare", "tencent"])
    parser.add_argument("--market", required=True, choices=["cn", "us"])


def _add_output(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", help="Optional output path.")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "jsonl"])
    parser.add_argument("--preview-rows", type=int, default=5)


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


def _watch_quotes(args: argparse.Namespace) -> int:
    symbol = str(args.symbol).strip()
    source_id = args.source_id or f"finance_{args.market}_{_id_part(symbol)}_watch"
    monitor_id = args.monitor_id or f"monitor_{source_id}"
    display_name = args.name or f"finance {args.market} {symbol} watch"
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
                    "symbols": [symbol],
                    "watch": True,
                },
            }
        )
        monitor = store.upsert_monitor(_watch_monitor_payload(args, source_id=source_id, monitor_id=monitor_id, name=display_name))

    ticks: list[dict[str, Any]] = []
    iteration = 0
    interrupted = False
    try:
        while args.iterations == 0 or iteration < args.iterations:
            iteration += 1
            tick = _run_watch_tick(args, source_id=source_id, monitor_id=monitor_id, iteration=iteration)
            ticks.append(tick)
            if args.jsonl:
                print(json.dumps(tick, ensure_ascii=False, sort_keys=True), flush=True)
            if args.iterations != 0 and iteration >= args.iterations:
                break
            time.sleep(max(0.0, float(args.interval_sec)))
    except KeyboardInterrupt:
        interrupted = True

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


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _id_part(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in value).strip("_") or "symbol"


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
