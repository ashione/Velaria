from __future__ import annotations

import importlib
import html
import json
import math
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib import parse as urllib_parse
from urllib import request as urllib_request

import pandas as pd

from .providers import FinanceProviderAdapter, FinanceProviderRegistry, FinanceProviderSpec


AKSHARE_STOCK_DOC_URL = "https://akshare.akfamily.xyz/data/stock/stock.html"
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q="
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/"
GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/"
DEFAULT_LICENSE_NOTE = (
    "Public market-data provider metadata; validate upstream terms, freshness, "
    "and exchange delay before using for decisions."
)
NEWS_LICENSE_NOTE = (
    "Public news RSS provider metadata; validate publisher, publication time, "
    "and upstream feed terms before using for decisions."
)
POSITIVE_SENTIMENT_TERMS = {
    "beat",
    "beats",
    "benefit",
    "bullish",
    "demand",
    "gain",
    "gains",
    "growth",
    "improve",
    "improves",
    "optimistic",
    "outperform",
    "positive",
    "profit",
    "rally",
    "record",
    "resilient",
    "strong",
    "surge",
    "upbeat",
}
NEGATIVE_SENTIMENT_TERMS = {
    "antitrust",
    "bearish",
    "decline",
    "drops",
    "falls",
    "fraud",
    "investigate",
    "investigation",
    "lawsuit",
    "loss",
    "miss",
    "negative",
    "pressure",
    "probe",
    "risk",
    "slump",
    "weak",
    "warning",
}


class FinanceProviderError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_type: str = "finance_provider_error",
        hint: str = "Check provider availability, symbol format, network access, and upstream terms.",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.hint = hint
        self.details = details or {}

    def to_payload(self) -> dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": str(self),
            "hint": self.hint,
            "details": self.details,
        }


def normalize_market(market: str) -> str:
    normalized = market.strip().lower()
    aliases = {
        "a": "cn",
        "a股": "cn",
        "ashare": "cn",
        "china": "cn",
        "zh": "cn",
        "cn": "cn",
        "us": "us",
        "美股": "us",
        "usa": "us",
    }
    if normalized not in aliases:
        raise FinanceProviderError(
            f"unsupported market: {market}",
            error_type="unsupported_market",
            hint="Use market 'cn' for A-share or 'us' for U.S. stocks.",
            details={"market": market},
        )
    return aliases[normalized]


_PROVIDER_REGISTRY: FinanceProviderRegistry | None = None


def normalize_provider(provider: str) -> str:
    normalized = provider.strip().lower()
    registry = _provider_registry()
    if registry.get(normalized) is None:
        raise FinanceProviderError(
            f"unsupported finance provider: {provider}",
            error_type="unsupported_provider",
            hint=(
                "Use one of the provider names from `finance sources`, or choose a provider that "
                "supports the requested operation."
            ),
            details={"provider": provider, "candidates": registry.names()},
        )
    return normalized


def provider_names_for_operation(operation: str) -> list[str]:
    return _provider_registry().names_for_operation(operation)


def provider_catalog() -> list[dict[str, Any]]:
    return _provider_registry().catalog()


def fetch_news(
    *,
    provider: str,
    market: str,
    symbol: str,
    query: str | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    provider = normalize_provider(provider)
    adapter = _provider_registry().get(provider)
    if adapter is None or adapter.fetch_news is None:
        candidates = provider_names_for_operation("fetch_news")
        raise FinanceProviderError(
            f"provider does not support news: {provider}",
            error_type="unsupported_provider_operation",
            hint=f"Use one of these news providers: {', '.join(candidates)}.",
            details={"provider": provider, "operation": "fetch_news", "candidates": candidates},
        )
    return adapter.fetch_news(market=market, symbol=symbol, query=query, limit=limit)


def fetch_fundamentals(
    *,
    provider: str,
    market: str,
    symbols: list[str] | str,
) -> list[dict[str, Any]]:
    provider = normalize_provider(provider)
    adapter = _provider_registry().get(provider)
    if adapter is None or adapter.fetch_fundamentals is None:
        candidates = provider_names_for_operation("fetch_fundamentals")
        raise FinanceProviderError(
            f"provider does not support fundamentals: {provider}",
            error_type="unsupported_provider_operation",
            hint=f"Use one of these fundamentals providers: {', '.join(candidates)}.",
            details={"provider": provider, "operation": "fetch_fundamentals", "candidates": candidates},
        )
    return adapter.fetch_fundamentals(market=market, symbols=normalize_symbols(symbols))


def normalize_symbols(symbols: str | Iterable[str]) -> list[str]:
    if isinstance(symbols, str):
        parts = symbols.replace("，", ",").split(",")
    else:
        parts = list(symbols)
    normalized = [str(item).strip() for item in parts if str(item).strip()]
    if not normalized:
        raise FinanceProviderError(
            "at least one symbol is required",
            error_type="missing_symbol",
            hint="Pass one or more comma-separated symbols.",
        )
    return normalized


def normalize_history_frame(
    frame: pd.DataFrame,
    *,
    market: str,
    symbol: str,
    provider: str,
    source_url: str,
    freshness: str,
    delay_sec: int | None = None,
    fetched_at: str | None = None,
    license_note: str = DEFAULT_LICENSE_NOTE,
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    fetched_at = fetched_at or _utc_now()
    rows: list[dict[str, Any]] = []
    for raw in frame.to_dict(orient="records"):
        rows.append(
            {
                "market": market,
                "symbol": symbol,
                "date": _date_text(_pick(raw, "日期", "date", "Date")),
                "open": _number(_pick(raw, "开盘", "open", "Open")),
                "high": _number(_pick(raw, "最高", "high", "High")),
                "low": _number(_pick(raw, "最低", "low", "Low")),
                "close": _number(_pick(raw, "收盘", "close", "Close")),
                "volume": _integer(_pick(raw, "成交量", "volume", "Volume")),
                "amount": _number(_pick(raw, "成交额", "amount", "turnover", "Amount")),
                "pct_change": _number(_pick(raw, "涨跌幅", "pct_change", "change_percent")),
                "provider": provider,
                "source_url": source_url,
                "fetched_at": fetched_at,
                "freshness": freshness,
                "delay_sec": delay_sec,
                "license_note": license_note,
            }
        )
    return rows


def normalize_quote_frame(
    frame: pd.DataFrame,
    *,
    market: str,
    symbols: str | Iterable[str],
    provider: str,
    source_url: str,
    freshness: str,
    delay_sec: int | None,
    fetched_at: str | None = None,
    license_note: str = DEFAULT_LICENSE_NOTE,
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    symbol_set = set(normalize_symbols(symbols))
    fetched_at = fetched_at or _utc_now()
    rows: list[dict[str, Any]] = []
    for raw in frame.to_dict(orient="records"):
        symbol = str(_pick(raw, "代码", "symbol", "Symbol") or "").strip()
        if symbol not in symbol_set:
            continue
        price = _number(_pick(raw, "最新价", "price", "last", "最新"))
        rows.append(
            {
                "event_time": fetched_at,
                "event_type": "quote",
                "source_key": symbol,
                "market": market,
                "symbol": symbol,
                "name": _text(_pick(raw, "名称", "name", "Name")),
                "price": price,
                "open": _number(_pick(raw, "开盘", "open", "Open")),
                "high": _number(_pick(raw, "最高", "high", "High")),
                "low": _number(_pick(raw, "最低", "low", "Low")),
                "previous_close": _number(_pick(raw, "昨收", "previous_close", "prev_close")),
                "volume": _integer(_pick(raw, "成交量", "volume", "Volume")),
                "amount": _number(_pick(raw, "成交额", "amount", "turnover", "Amount")),
                "pct_change": _number(_pick(raw, "涨跌幅", "pct_change", "change_percent")),
                "provider": provider,
                "source_url": source_url,
                "fetched_at": fetched_at,
                "freshness": freshness,
                "delay_sec": delay_sec,
                "license_note": license_note,
            }
        )
    return rows


def fetch_history(
    *,
    provider: str,
    market: str,
    symbol: str,
    start_date: str,
    end_date: str,
    period: str = "daily",
    adjust: str = "",
) -> list[dict[str, Any]]:
    provider = normalize_provider(provider)
    adapter = _provider_registry().get(provider)
    if adapter is None or adapter.fetch_history is None:
        candidates = provider_names_for_operation("fetch_history")
        raise FinanceProviderError(
            f"provider does not support history: {provider}",
            error_type="unsupported_provider_operation",
            hint=f"Use one of these history providers: {', '.join(candidates)}.",
            details={"provider": provider, "operation": "fetch_history", "candidates": candidates},
        )
    return adapter.fetch_history(
        market=market,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        period=period,
        adjust=adjust,
    )


def _fetch_akshare_history(
    *,
    market: str,
    symbol: str,
    start_date: str,
    end_date: str,
    period: str = "daily",
    adjust: str = "",
) -> list[dict[str, Any]]:
    provider = "akshare"
    market = normalize_market(market)
    ak = _load_akshare()
    try:
        if market == "cn":
            frame = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
        else:
            frame = ak.stock_us_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
    except Exception as exc:  # pragma: no cover - exercised by network smoke
        raise FinanceProviderError(
            f"failed to fetch {market} history from {provider}: {exc}",
            error_type="provider_fetch_failed",
            details={"provider": provider, "market": market, "symbol": symbol},
        ) from exc
    return normalize_history_frame(
        frame,
        market=market,
        symbol=symbol,
        provider=provider,
        source_url=AKSHARE_STOCK_DOC_URL,
        freshness="eod",
    )


def fetch_quotes(
    *,
    provider: str,
    market: str,
    symbols: str | Iterable[str],
) -> list[dict[str, Any]]:
    provider = normalize_provider(provider)
    market = normalize_market(market)
    symbol_list = normalize_symbols(symbols)
    adapter = _provider_registry().get(provider)
    if adapter is None or adapter.fetch_quotes is None:
        candidates = provider_names_for_operation("fetch_quotes")
        raise FinanceProviderError(
            f"provider does not support quotes: {provider}",
            error_type="unsupported_provider_operation",
            hint=f"Use one of these quote providers: {', '.join(candidates)}.",
            details={"provider": provider, "operation": "fetch_quotes", "candidates": candidates},
        )
    return adapter.fetch_quotes(market=market, symbols=symbol_list)


def _fetch_akshare_quotes(
    *,
    market: str,
    symbols: list[str],
) -> list[dict[str, Any]]:
    provider = "akshare"
    ak = _load_akshare()
    try:
        frame = ak.stock_zh_a_spot_em() if market == "cn" else ak.stock_us_spot_em()
    except Exception as exc:  # pragma: no cover - exercised by network smoke
        raise FinanceProviderError(
            f"failed to fetch {market} quotes from {provider}: {exc}",
            error_type="provider_fetch_failed",
            details={"provider": provider, "market": market, "symbols": symbols},
        ) from exc
    rows = normalize_quote_frame(
        frame,
        market=market,
        symbols=symbols,
        provider=provider,
        source_url=AKSHARE_STOCK_DOC_URL,
        freshness="realtime" if market == "cn" else "delayed",
        delay_sec=0 if market == "cn" else None,
    )
    if not rows:
        raise FinanceProviderError(
            "provider returned no matching quote rows",
            error_type="symbol_not_found",
            hint="For U.S. stocks, inspect akshare stock_us_spot_em() codes and pass the provider-specific code such as '105.AAPL'.",
            details={"provider": provider, "market": market, "symbols": symbols},
        )
    return rows


def parse_tencent_quote_payload(
    payload: str,
    *,
    market: str,
    symbols: str | Iterable[str],
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    symbol_set = set(normalize_symbols(symbols))
    fetched_at = fetched_at or _utc_now()
    rows: list[dict[str, Any]] = []
    for item in payload.split(";"):
        if "=\"" not in item:
            continue
        body = item.split("=\"", 1)[1].rstrip('"')
        fields = body.split("~")
        if market == "cn":
            if len(fields) < 7:
                continue
            symbol = fields[2].strip()
            requested = {_normalize_cn_tencent_symbol(item) for item in symbol_set}
            if symbol not in requested:
                continue
            rows.append(
                {
                    "event_time": fetched_at,
                    "event_type": "quote",
                    "source_key": symbol,
                    "market": "cn",
                    "symbol": symbol,
                    "name": fields[1].strip() or None,
                    "price": _number(fields[3]),
                    "open": None,
                    "high": None,
                    "low": None,
                    "previous_close": None,
                    "volume": _integer(fields[6]),
                    "amount": _number(fields[7] if len(fields) > 7 else None),
                    "pct_change": _number(fields[5]),
                    "provider": "tencent",
                    "source_url": TENCENT_QUOTE_URL,
                    "fetched_at": fetched_at,
                    "freshness": "realtime",
                    "delay_sec": 0,
                    "license_note": DEFAULT_LICENSE_NOTE,
                }
            )
            continue
        if len(fields) < 7:
            continue
        symbol = _normalize_us_tencent_symbol(fields[2])
        requested = {_normalize_us_tencent_symbol(item) for item in symbol_set}
        if symbol not in requested:
            continue
        rows.append(
            {
                "event_time": fetched_at,
                "event_type": "quote",
                "source_key": symbol,
                "market": "us",
                "symbol": symbol,
                "name": fields[1].strip() or None,
                "price": _number(fields[3]),
                "open": _number(fields[5]),
                "high": _number(fields[33] if len(fields) > 33 else None),
                "low": _number(fields[34] if len(fields) > 34 else None),
                "previous_close": _number(fields[4]),
                "volume": _integer(fields[36] if len(fields) > 36 else fields[6]),
                "amount": _number(fields[37] if len(fields) > 37 else None),
                "pct_change": _number(fields[32] if len(fields) > 32 else None),
                "provider": "tencent",
                "source_url": TENCENT_QUOTE_URL,
                "fetched_at": fetched_at,
                "freshness": "delayed",
                "delay_sec": None,
                "license_note": DEFAULT_LICENSE_NOTE,
            }
        )
    if not rows:
        raise FinanceProviderError(
            "Tencent quote payload did not include requested symbols",
            error_type="symbol_not_found",
            hint="Use A-share symbols like 000001/600519 or U.S. symbols like AAPL.",
            details={"market": market, "symbols": sorted(symbol_set)},
        )
    return rows


def _normalize_cn_tencent_symbol(symbol: str) -> str:
    normalized = symbol.strip().lower()
    if normalized.startswith("s_"):
        normalized = normalized[2:]
    if normalized.startswith(("sh", "sz")) and len(normalized) > 2:
        return normalized[2:]
    return normalized


def parse_yahoo_chart_payload(
    payload: dict[str, Any],
    *,
    market: str,
    symbol: str,
    yahoo_symbol: str,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    fetched_at = fetched_at or _utc_now()
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise FinanceProviderError(
            f"Yahoo chart returned error: {chart['error']}",
            error_type="provider_fetch_failed",
            details={"provider": "yahoo", "market": market, "symbol": symbol, "provider_symbol": yahoo_symbol},
        )
    results = chart.get("result") or []
    if not results:
        raise FinanceProviderError(
            "Yahoo chart returned no results",
            error_type="symbol_not_found",
            hint="Use U.S. tickers such as AAPL or A-share symbols such as 000001, 000001.SZ, or 600519.SS.",
            details={"provider": "yahoo", "market": market, "symbol": symbol, "provider_symbol": yahoo_symbol},
        )
    result = results[0]
    timestamps = result.get("timestamp") or []
    quote_items = ((result.get("indicators") or {}).get("quote") or [])
    quote = quote_items[0] if quote_items else {}
    rows: list[dict[str, Any]] = []
    previous_close: float | None = None
    for index, timestamp in enumerate(timestamps):
        close = _list_number(quote.get("close"), index)
        pct_change = None
        if close is not None and previous_close not in (None, 0):
            pct_change = round(((close - previous_close) / previous_close) * 100.0, 6)
        if close is not None:
            previous_close = close
        rows.append(
            {
                "market": market,
                "symbol": symbol,
                "provider_symbol": yahoo_symbol,
                "date": datetime.fromtimestamp(int(timestamp), timezone.utc).strftime("%Y-%m-%d"),
                "open": _list_number(quote.get("open"), index),
                "high": _list_number(quote.get("high"), index),
                "low": _list_number(quote.get("low"), index),
                "close": close,
                "volume": _list_integer(quote.get("volume"), index),
                "amount": None,
                "pct_change": pct_change,
                "provider": "yahoo",
                "source_url": YAHOO_CHART_URL,
                "fetched_at": fetched_at,
                "freshness": "eod",
                "delay_sec": None,
                "license_note": DEFAULT_LICENSE_NOTE,
            }
        )
    rows = [row for row in rows if row["open"] is not None or row["close"] is not None or row["volume"] is not None]
    if not rows:
        raise FinanceProviderError(
            "Yahoo chart returned no OHLCV rows",
            error_type="symbol_not_found",
            hint="Check the Yahoo provider symbol and date range.",
            details={"provider": "yahoo", "market": market, "symbol": symbol, "provider_symbol": yahoo_symbol},
        )
    return rows


def parse_yahoo_quote_payload(
    payload: dict[str, Any],
    *,
    market: str,
    symbol: str,
    yahoo_symbol: str,
    source_url: str,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    fetched_at = fetched_at or _utc_now()
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise FinanceProviderError(
            f"Yahoo quote returned error: {chart['error']}",
            error_type="provider_fetch_failed",
            details={"provider": "yahoo", "market": market, "symbol": symbol, "provider_symbol": yahoo_symbol},
        )
    results = chart.get("result") or []
    if not results:
        raise FinanceProviderError(
            "Yahoo quote returned no results",
            error_type="symbol_not_found",
            hint="Use U.S. tickers such as AAPL or A-share Yahoo symbols such as 000001.SZ.",
            details={"provider": "yahoo", "market": market, "symbol": symbol, "provider_symbol": yahoo_symbol},
        )
    result = results[0]
    meta = result.get("meta") or {}
    quote_items = ((result.get("indicators") or {}).get("quote") or [])
    quote = quote_items[0] if quote_items else {}
    price = _number(meta.get("regularMarketPrice"))
    previous_close = _number(meta.get("previousClose") or meta.get("chartPreviousClose"))
    pct_change = None
    if price is not None and previous_close not in (None, 0):
        pct_change = round(((price - previous_close) / previous_close) * 100.0, 6)
    market_time = meta.get("regularMarketTime") or ((result.get("timestamp") or [None])[-1])
    event_time = datetime.fromtimestamp(int(market_time), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if market_time else fetched_at
    return [
        {
            "event_time": event_time,
            "event_type": "quote",
            "source_key": symbol,
            "market": market,
            "symbol": symbol,
            "provider_symbol": yahoo_symbol,
            "name": meta.get("longName") or meta.get("shortName") or symbol,
            "price": price,
            "open": _number(meta.get("regularMarketOpen")),
            "high": _number(meta.get("regularMarketDayHigh")),
            "low": _number(meta.get("regularMarketDayLow")),
            "previous_close": previous_close,
            "volume": _list_integer(quote.get("volume"), -1) or _integer(meta.get("regularMarketVolume")),
            "amount": None,
            "pct_change": pct_change,
            "currency": meta.get("currency"),
            "provider": "yahoo",
            "source_url": source_url,
            "fetched_at": fetched_at,
            "freshness": "delayed",
            "delay_sec": None,
            "license_note": DEFAULT_LICENSE_NOTE,
        }
    ]


def parse_sec_companyfacts_payload(
    payload: dict[str, Any],
    *,
    market: str,
    symbol: str,
    cik: str,
    source_url: str,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    fetched_at = fetched_at or _utc_now()
    facts = ((payload.get("facts") or {}).get("us-gaap") or {})
    revenue = _latest_sec_fact(facts, ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"))
    net_income = _latest_sec_fact(facts, ("NetIncomeLoss", "ProfitLoss"))
    assets = _latest_sec_fact(facts, ("Assets",))
    latest = revenue or net_income or assets
    if latest is None:
        return [
            {
                "event_time": fetched_at,
                "event_type": "fundamental_unavailable",
                "source_key": symbol,
                "market": market,
                "symbol": symbol,
                "cik": cik,
                "provider": "sec-companyfacts",
                "source_url": source_url,
                "freshness": "unavailable",
                "error_type": "fundamental_fact_not_found",
                "message": "SEC companyfacts payload did not contain supported GAAP metrics.",
                "not_mocked": True,
            }
        ]
    return [
        {
            "event_time": str(latest.get("filed") or fetched_at),
            "event_type": "fundamental_snapshot",
            "source_key": symbol,
            "market": market,
            "symbol": symbol,
            "cik": cik,
            "provider": "sec-companyfacts",
            "source_url": source_url,
            "fetched_at": fetched_at,
            "freshness": "filing",
            "fiscal_period_end": latest.get("end"),
            "form": latest.get("form"),
            "filed": latest.get("filed"),
            "revenue": revenue.get("val") if revenue else None,
            "net_income": net_income.get("val") if net_income else None,
            "assets": assets.get("val") if assets else None,
            "license_note": "Public SEC companyfacts API metadata; verify filing taxonomy and period before using for decisions.",
        }
    ]


def _latest_sec_fact(facts: dict[str, Any], names: tuple[str, ...]) -> dict[str, Any] | None:
    rows: list[dict[str, Any]] = []
    for name in names:
        units = ((facts.get(name) or {}).get("units") or {})
        for values in units.values():
            if isinstance(values, list):
                rows.extend(value for value in values if isinstance(value, dict) and value.get("val") is not None)
    rows.sort(key=lambda row: str(row.get("filed") or row.get("end") or ""))
    return rows[-1] if rows else None


def parse_google_news_rss(
    payload: str,
    *,
    market: str,
    symbol: str,
    query: str,
    fetched_at: str | None = None,
    source_url: str = GOOGLE_NEWS_RSS_URL,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    fetched_at = fetched_at or _utc_now()
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise FinanceProviderError(
            "Google News RSS returned invalid XML",
            error_type="provider_parse_failed",
            hint="Retry later or inspect the provider RSS payload.",
            details={"provider": "google-news", "market": market, "symbol": symbol, "query": query},
        ) from exc
    items = root.findall(".//item")
    rows: list[dict[str, Any]] = []
    for item in items[: max(0, limit) if limit is not None else None]:
        title = _xml_text(item, "title")
        link = _xml_text(item, "link")
        source = item.find("source")
        summary = _xml_text(item, "description")
        rows.append(
            {
                "event_time": fetched_at,
                "event_type": "news",
                "source_key": symbol,
                "market": market,
                "symbol": symbol,
                "title": title,
                "summary": summary,
                "url": link,
                "publisher": _clean_text(source.text) if source is not None and source.text else None,
                "publisher_url": source.attrib.get("url") if source is not None else None,
                "published_at": _rss_datetime(_xml_text(item, "pubDate")),
                "query": query,
                "provider": "google-news",
                "source_url": source_url,
                "fetched_at": fetched_at,
                "freshness": "near_realtime",
                "delay_sec": None,
                "license_note": NEWS_LICENSE_NOTE,
            }
        )
    return rows


def evaluate_news_sentiment(news_rows: list[dict[str, Any]]) -> dict[str, Any]:
    positive_hits = 0
    negative_hits = 0
    matched_positive: list[str] = []
    matched_negative: list[str] = []
    for row in news_rows:
        text = f"{row.get('title') or ''} {row.get('summary') or ''}".lower()
        tokens = {token.strip(".,:;!?()[]{}\"'") for token in text.split()}
        for term in sorted(POSITIVE_SENTIMENT_TERMS & tokens):
            positive_hits += 1
            matched_positive.append(term)
        for term in sorted(NEGATIVE_SENTIMENT_TERMS & tokens):
            negative_hits += 1
            matched_negative.append(term)
    raw_score = positive_hits - negative_hits
    article_count = len(news_rows)
    normalized_score = 0.0 if article_count == 0 else round(raw_score / max(1, article_count), 6)
    if normalized_score > 0.25:
        label = "positive"
    elif normalized_score < -0.25:
        label = "negative"
    elif positive_hits or negative_hits:
        label = "mixed"
    else:
        label = "neutral"
    return {
        "label": label,
        "score": normalized_score,
        "article_count": article_count,
        "positive_hits": positive_hits,
        "negative_hits": negative_hits,
        "matched_positive": sorted(set(matched_positive)),
        "matched_negative": sorted(set(matched_negative)),
        "method": "transparent_keyword_lexicon",
    }


def finance_quote_schema_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "symbol",
        "field_mappings": {
            "market": "market",
            "symbol": "symbol",
            "price": "price",
            "volume": "volume",
            "pct_change": "pct_change",
            "provider": "provider",
            "freshness": "freshness",
            "delay_sec": "delay_sec",
        },
    }


def build_research_prompt(
    *,
    focus_events: list[dict[str, Any]],
    datasets: list[dict[str, Any]] | None = None,
    user_question: str | None = None,
) -> str:
    event_block = json.dumps(focus_events, ensure_ascii=False, indent=2, sort_keys=True)
    dataset_block = json.dumps(datasets or [], ensure_ascii=False, indent=2, sort_keys=True)
    question = user_question or "解释这些 A股/美股异动事件，并给出可继续验证的研究线索。"
    return (
        "你是 Velaria 金融事件研究助理。请基于 Velaria FocusEvent、历史数据 artifact，"
        "并进行实时联网研究来生成研究摘要。\n\n"
        f"用户问题：{question}\n\n"
        "FocusEvent 证据：\n"
        f"{event_block}\n\n"
        "相关数据集 / artifact：\n"
        f"{dataset_block}\n\n"
        "输出要求：\n"
        "1. 先说明触发事件、标的、市场、价格/成交量等结构化证据。\n"
        "2. 做实时联网研究，列出每条关键判断的来源链接、发布时间或访问时间。\n"
        "3. 区分数据事实、推断和不确定性；标明行情 freshness / delay 信息。\n"
        "4. 给出后续可在 Velaria 中执行的 SQL 或 monitor 验证建议。\n"
        "5. 明确说明这不是投资建议，不包含买卖指令或收益承诺。"
    )


def _load_akshare() -> Any:
    try:
        return importlib.import_module("akshare")
    except ModuleNotFoundError as exc:
        raise FinanceProviderError(
            "akshare is not installed",
            error_type="missing_dependency",
            hint="Install the finance extra with: uv sync --project python --extra finance",
            details={"dependency": "akshare"},
        ) from exc


def _fetch_tencent_quotes(*, market: str, symbols: list[str]) -> list[dict[str, Any]]:
    codes = [_tencent_code(market, symbol) for symbol in symbols]
    url = TENCENT_QUOTE_URL + ",".join(codes)
    req = urllib_request.Request(url, headers={"User-Agent": "Velaria/finance-pack"})
    try:
        with urllib_request.urlopen(req, timeout=15) as response:
            payload = response.read().decode("gbk", errors="replace")
    except Exception as exc:  # pragma: no cover - exercised by network smoke
        raise FinanceProviderError(
            f"failed to fetch {market} quotes from tencent: {exc}",
            error_type="provider_fetch_failed",
            details={"provider": "tencent", "market": market, "symbols": symbols, "source_url": url},
        ) from exc
    return parse_tencent_quote_payload(payload, market=market, symbols=symbols)


def _fetch_yahoo_quotes(*, market: str, symbols: list[str]) -> list[dict[str, Any]]:
    market = normalize_market(market)
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        provider_symbol = _yahoo_symbol(market, symbol)
        url = f"{YAHOO_CHART_URL}{provider_symbol}?range=1d&interval=1m"
        req = urllib_request.Request(url, headers={"User-Agent": "Velaria/finance-pack"})
        try:
            with urllib_request.urlopen(req, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # pragma: no cover - exercised by network smoke
            raise FinanceProviderError(
                f"failed to fetch {market} quote from yahoo: {exc}",
                error_type="provider_fetch_failed",
                details={"provider": "yahoo", "market": market, "symbol": symbol, "provider_symbol": provider_symbol, "source_url": url},
            ) from exc
        rows.extend(parse_yahoo_quote_payload(payload, market=market, symbol=symbol, yahoo_symbol=provider_symbol, source_url=url))
    return rows


def _fetch_yahoo_history(
    *,
    market: str,
    symbol: str,
    start_date: str,
    end_date: str,
    period: str,
    adjust: str = "",
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    interval = {"daily": "1d", "weekly": "1wk", "monthly": "1mo"}.get(period)
    if interval is None:
        raise FinanceProviderError(
            f"unsupported Yahoo history period: {period}",
            error_type="unsupported_period",
            hint="Use period daily, weekly, or monthly.",
            details={"provider": "yahoo", "period": period},
        )
    provider_symbol = _yahoo_symbol(market, symbol)
    period1 = _yyyymmdd_epoch(start_date)
    period2 = _yyyymmdd_epoch(end_date, add_days=1)
    url = (
        f"{YAHOO_CHART_URL}{provider_symbol}"
        f"?period1={period1}&period2={period2}&interval={interval}&events=history"
    )
    req = urllib_request.Request(url, headers={"User-Agent": "Velaria/finance-pack"})
    try:
        with urllib_request.urlopen(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # pragma: no cover - exercised by network smoke
        raise FinanceProviderError(
            f"failed to fetch {market} history from yahoo: {exc}",
            error_type="provider_fetch_failed",
            details={"provider": "yahoo", "market": market, "symbol": symbol, "provider_symbol": provider_symbol, "source_url": url},
        ) from exc
    return parse_yahoo_chart_payload(payload, market=market, symbol=symbol, yahoo_symbol=provider_symbol)


def _fetch_sec_companyfacts(*, market: str, symbols: list[str]) -> list[dict[str, Any]]:
    market = normalize_market(market)
    if market != "us":
        return [_fundamental_unavailable_row(market=market, symbol=symbol, provider="sec-companyfacts", error_type="unsupported_market") for symbol in symbols]
    try:
        cik_by_symbol = _fetch_sec_ticker_map()
    except Exception as exc:  # pragma: no cover - exercised by network smoke
        return [
            {
                **_fundamental_unavailable_row(market=market, symbol=symbol, provider="sec-companyfacts", error_type="provider_fetch_failed"),
                "message": f"SEC ticker map fetch failed: {exc}",
                "source_url": SEC_COMPANY_TICKERS_URL,
            }
            for symbol in symbols
        ]
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        cik = cik_by_symbol.get(symbol.upper())
        if not cik:
            rows.append(_fundamental_unavailable_row(market=market, symbol=symbol, provider="sec-companyfacts", error_type="cik_not_found"))
            continue
        url = f"{SEC_COMPANYFACTS_URL}CIK{cik}.json"
        req = urllib_request.Request(url, headers={"User-Agent": "Velaria finance research contact@example.invalid"})
        try:
            with urllib_request.urlopen(req, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # pragma: no cover - exercised by network smoke
            rows.append(
                {
                    **_fundamental_unavailable_row(market=market, symbol=symbol, provider="sec-companyfacts", error_type="provider_fetch_failed"),
                    "message": f"SEC companyfacts fetch failed: {exc}",
                    "source_url": url,
                    "cik": cik,
                }
            )
            continue
        rows.extend(parse_sec_companyfacts_payload(payload, market=market, symbol=symbol, cik=cik, source_url=url))
    return rows


def _fetch_sec_ticker_map() -> dict[str, str]:
    req = urllib_request.Request(SEC_COMPANY_TICKERS_URL, headers={"User-Agent": "Velaria finance research contact@example.invalid"})
    with urllib_request.urlopen(req, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    mapping: dict[str, str] = {}
    for item in (payload.values() if isinstance(payload, dict) else []):
        ticker = str(item.get("ticker") or "").upper()
        cik = str(item.get("cik_str") or "").zfill(10)
        if ticker and cik:
            mapping[ticker] = cik
    return mapping


def _fundamental_unavailable_row(*, market: str, symbol: str, provider: str, error_type: str) -> dict[str, Any]:
    return {
        "event_time": _utc_now(),
        "event_type": "fundamental_unavailable",
        "source_key": symbol,
        "market": market,
        "symbol": symbol,
        "provider": provider,
        "freshness": "unavailable",
        "error_type": error_type,
        "message": "Public fundamentals provider could not provide a usable row for this symbol.",
        "not_mocked": True,
    }


def _fetch_google_news(
    *,
    market: str,
    symbol: str,
    query: str | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    market = normalize_market(market)
    query_text = query or _default_news_query(market, symbol)
    params = urllib_parse.urlencode({"q": query_text, "hl": "en-US", "gl": "US", "ceid": "US:en"})
    url = f"{GOOGLE_NEWS_RSS_URL}?{params}"
    req = urllib_request.Request(url, headers={"User-Agent": "Velaria/finance-pack"})
    try:
        with urllib_request.urlopen(req, timeout=15) as response:
            payload = response.read().decode("utf-8", errors="replace")
    except Exception as exc:  # pragma: no cover - exercised by network smoke
        raise FinanceProviderError(
            f"failed to fetch {market} news from google-news: {exc}",
            error_type="provider_fetch_failed",
            details={"provider": "google-news", "market": market, "symbol": symbol, "query": query_text, "source_url": url},
        ) from exc
    return parse_google_news_rss(payload, market=market, symbol=symbol, query=query_text, source_url=url, limit=limit)


def _provider_registry() -> FinanceProviderRegistry:
    global _PROVIDER_REGISTRY
    if _PROVIDER_REGISTRY is not None:
        return _PROVIDER_REGISTRY
    _PROVIDER_REGISTRY = FinanceProviderRegistry(
        [
            FinanceProviderAdapter(
                spec=FinanceProviderSpec(
                    provider="akshare",
                    markets=("cn", "us"),
                    commands=("fetch-history", "fetch-quotes"),
                    freshness={"history": "eod", "quotes": "provider-dependent"},
                    recommended_quote_provider=False,
                    recommended_history_provider=True,
                    source_url=AKSHARE_STOCK_DOC_URL,
                    notes="Public Python data package; upstream Eastmoney endpoints may be blocked by local network policy.",
                ),
                fetch_history=_fetch_akshare_history,
                fetch_quotes=_fetch_akshare_quotes,
            ),
            FinanceProviderAdapter(
                spec=FinanceProviderSpec(
                    provider="google-news",
                    markets=("cn", "us"),
                    commands=("fetch-news", "rank-candidates"),
                    freshness={"news": "near_realtime"},
                    recommended_quote_provider=False,
                    recommended_history_provider=False,
                    source_url=GOOGLE_NEWS_RSS_URL,
                    notes="Public Google News RSS search feed used for current news context and lightweight sentiment evidence.",
                ),
                fetch_news=_fetch_google_news,
            ),
            FinanceProviderAdapter(
                spec=FinanceProviderSpec(
                    provider="sec-companyfacts",
                    markets=("us",),
                    commands=("fetch-fundamentals", "watch-session", "intelligence"),
                    freshness={"fundamentals": "filing"},
                    recommended_quote_provider=False,
                    recommended_history_provider=False,
                    source_url=SEC_COMPANYFACTS_URL,
                    notes="Public SEC companyfacts XBRL API for U.S. company fundamentals; rows are filing snapshots, not realtime data.",
                ),
                fetch_fundamentals=_fetch_sec_companyfacts,
            ),
            FinanceProviderAdapter(
                spec=FinanceProviderSpec(
                    provider="tencent",
                    markets=("cn", "us"),
                    commands=("fetch-quotes", "ingest-quotes", "analyze", "watch"),
                    freshness={"cn": "realtime", "us": "delayed"},
                    recommended_quote_provider=True,
                    recommended_history_provider=False,
                    source_url=TENCENT_QUOTE_URL,
                    notes="Lightweight public quote endpoint. Use for first-run analyze/watch validation.",
                ),
                fetch_quotes=_fetch_tencent_quotes,
            ),
            FinanceProviderAdapter(
                spec=FinanceProviderSpec(
                    provider="yahoo",
                    markets=("cn", "us"),
                    commands=("fetch-history", "fetch-quotes", "pipeline"),
                    freshness={"history": "eod", "quotes": "delayed"},
                    recommended_quote_provider=True,
                    recommended_history_provider=True,
                    source_url=YAHOO_CHART_URL,
                    notes="Public chart JSON endpoint verified for A-share Yahoo symbols such as 000001.SZ and U.S. symbols such as AAPL; quote freshness is provider-delayed.",
                ),
                fetch_history=_fetch_yahoo_history,
                fetch_quotes=_fetch_yahoo_quotes,
            ),
        ]
    )
    return _PROVIDER_REGISTRY


def _tencent_code(market: str, symbol: str) -> str:
    value = symbol.strip()
    if market == "us":
        ticker = _normalize_us_tencent_symbol(value)
        return f"us{ticker}"
    if value.startswith("s_"):
        return value
    if value.startswith(("sh", "sz")):
        return f"s_{value}"
    exchange = "sh" if value.startswith("6") else "sz"
    return f"s_{exchange}{value}"


def _normalize_us_tencent_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if value.startswith("US"):
        value = value[2:]
    if "." in value:
        left, right = value.split(".", 1)
        value = right if left.isdigit() else left
    return value


def _yahoo_symbol(market: str, symbol: str) -> str:
    value = symbol.strip().upper()
    if market == "us":
        if "." in value:
            left, right = value.split(".", 1)
            value = right if left.isdigit() else left
        if value.startswith("US"):
            value = value[2:]
        return value
    if value.endswith((".SZ", ".SS")):
        return value
    if value.startswith("SZ"):
        return f"{value[2:]}.SZ"
    if value.startswith("SH"):
        return f"{value[2:]}.SS"
    suffix = ".SS" if value.startswith("6") else ".SZ"
    return f"{value}{suffix}"


def _default_news_query(market: str, symbol: str) -> str:
    value = symbol.strip().upper()
    if market == "us":
        return f"{value} stock"
    return f"{value} 股票"


def _yyyymmdd_epoch(value: str, *, add_days: int = 0) -> int:
    try:
        dt = datetime.strptime(value, "%Y%m%d").replace(tzinfo=timezone.utc) + timedelta(days=add_days)
    except ValueError as exc:
        raise FinanceProviderError(
            f"invalid date: {value}",
            error_type="invalid_date",
            hint="Use YYYYMMDD date format, for example 20250131.",
            details={"date": value},
        ) from exc
    return int(dt.timestamp())


def _list_number(values: Any, index: int) -> float | None:
    if not isinstance(values, list) or index >= len(values):
        return None
    return _number(values[index])


def _list_integer(values: Any, index: int) -> int | None:
    number = _list_number(values, index)
    return None if number is None else int(number)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pick(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return None


def _xml_text(item: ET.Element, name: str) -> str | None:
    child = item.find(name)
    if child is None or child.text is None:
        return None
    return _clean_text(child.text)


def _clean_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value))
    return " ".join(html.unescape(text).split())


def _rss_datetime(value: str | None) -> str | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _number(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _integer(value: Any) -> int | None:
    number = _number(value)
    return None if number is None else int(number)


def _text(value: Any) -> str | None:
    if _is_missing(value):
        return None
    return str(value)


def _date_text(value: Any) -> str | None:
    if _is_missing(value):
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    return text[:10] if len(text) >= 10 else text
