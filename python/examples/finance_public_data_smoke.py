from __future__ import annotations

import argparse
import json
import pathlib
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from velaria.finance_pack import (
    AKSHARE_STOCK_DOC_URL,
    FinanceProviderError,
    TENCENT_QUOTE_URL,
    YAHOO_CHART_URL,
    fetch_history,
    fetch_quotes,
    normalize_quote_frame,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test public finance data providers.")
    parser.add_argument("--provider", default="yahoo", choices=["akshare", "yahoo"], help="Historical data provider.")
    parser.add_argument("--quote-provider", default="tencent", choices=["akshare", "tencent"])
    parser.add_argument("--cn-symbol", default="000001")
    parser.add_argument("--us-symbol", help="AkShare provider-specific U.S. code, e.g. 105.AAPL. Defaults to discovery from spot data.")
    parser.add_argument("--start-date", default="20250101")
    parser.add_argument("--end-date", default="20250131")
    parser.add_argument("--output-dir", help="Defaults to a temporary directory.")
    parser.add_argument("--skip-us", action="store_true")
    parser.add_argument("--quotes-only", action="store_true", help="Only verify public quote endpoints.")
    args = parser.parse_args()

    output_dir = pathlib.Path(args.output_dir) if args.output_dir else pathlib.Path(tempfile.mkdtemp(prefix="velaria-finance-smoke-"))
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        cn_history = [] if args.quotes_only else fetch_history(
            provider=args.provider,
            market="cn",
            symbol=args.cn_symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            adjust="qfq",
        )
        cn_quotes = fetch_quotes(provider=args.quote_provider, market="cn", symbols=[args.cn_symbol])
        summary: dict[str, Any] = {
            "ok": True,
            "history_provider": args.provider,
            "quote_provider": args.quote_provider,
            "output_dir": str(output_dir),
            "sources": [YAHOO_CHART_URL, AKSHARE_STOCK_DOC_URL, TENCENT_QUOTE_URL],
            "checks": [_write_check(output_dir, "cn_quotes", cn_quotes)],
        }
        if not args.quotes_only:
            summary["checks"].insert(0, _write_check(output_dir, "cn_history", cn_history))
        if not args.skip_us:
            us_symbol = args.us_symbol or _discover_us_symbol(args.quote_provider)
            us_history = [] if args.quotes_only else fetch_history(
                provider=args.provider,
                market="us",
                symbol=us_symbol,
                start_date=args.start_date,
                end_date=args.end_date,
                adjust="",
            )
            us_quotes = fetch_quotes(provider=args.quote_provider, market="us", symbols=[us_symbol])
            if not args.quotes_only:
                summary["checks"].append(_write_check(output_dir, "us_history", us_history))
            summary["checks"].append(_write_check(output_dir, "us_quotes", us_quotes))
            summary["us_symbol"] = us_symbol
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0
    except FinanceProviderError as exc:
        print(json.dumps({"ok": False, **exc.to_payload()}, indent=2, ensure_ascii=False))
        return 1


def _discover_us_symbol(provider: str) -> str:
    if provider != "akshare":
        return "AAPL"
    import akshare as ak

    frame = ak.stock_us_spot_em()
    preferred = normalize_quote_frame(
        frame,
        market="us",
        symbols=["105.AAPL"],
        provider="akshare",
        source_url=AKSHARE_STOCK_DOC_URL,
        freshness="delayed",
        delay_sec=None,
    )
    if preferred:
        return "105.AAPL"
    if "代码" not in frame.columns or frame.empty:
        raise FinanceProviderError(
            "failed to discover a U.S. symbol from AkShare spot data",
            error_type="symbol_discovery_failed",
            details={"columns": list(frame.columns)},
        )
    return str(frame.iloc[0]["代码"])


def _write_check(output_dir: pathlib.Path, name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise FinanceProviderError(
            f"{name} returned no rows",
            error_type="empty_provider_result",
            details={"check": name},
        )
    path = output_dir / f"{name}.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    required = {"provider", "fetched_at", "freshness"}
    missing = sorted(required - set(rows[0]))
    if missing:
        raise FinanceProviderError(
            f"{name} result is missing required metadata fields",
            error_type="provider_contract_error",
            details={"check": name, "missing": missing},
        )
    return {
        "name": name,
        "row_count": len(rows),
        "path": str(path),
        "freshness": rows[0].get("freshness"),
        "fields": sorted(rows[0].keys()),
    }


if __name__ == "__main__":
    raise SystemExit(main())
