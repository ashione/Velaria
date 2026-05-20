import json
import os
import pathlib
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import pandas as pd

from velaria.agentic_store import AgenticStore
from velaria.cli import main as velaria_cli_main
from velaria.finance_pack import (
    FinanceProviderError,
    build_research_prompt,
    classify_information_source,
    evaluate_news_sentiment,
    fetch_fundamentals,
    fetch_quotes,
    normalize_history_frame,
    normalize_provider,
    normalize_quote_frame,
    parse_sec_companyfacts_payload,
    parse_sec_filings_atom,
    parse_google_news_rss,
    parse_tencent_quote_payload,
    parse_yahoo_finance_news_rss,
    parse_yahoo_quote_payload,
    parse_yahoo_chart_payload,
    provider_catalog,
    provider_names_for_operation,
    sec_user_agent_policy,
)
from velaria.finance_pack.cli import (
    _intelligence_report_payload,
    _rank_native_signal_rows,
    _rank_native_stream_row,
    _rank_native_stream_source_row,
    _rank_native_stream_view_name,
    main as finance_cli_main,
)
from velaria.finance_pack.evidence_index import (
    EvidenceSearchOptions,
    FINANCE_EVIDENCE_INDEX_VERSION,
    FinanceEvidenceRetriever,
    build_finance_evidence_index,
    finance_evidence_index_dir,
    load_finance_evidence_index,
)
from velaria.finance_pack.evaluation import evaluate_finance_session
from velaria.finance_pack.jobs import (
    append_finance_job_event,
    finance_job_payload,
    latest_finance_jobs,
)


class FinancePackTest(unittest.TestCase):
    def _seed_watch_session_rows(self, session_id: str = "session_review") -> None:
        source_ids = {
            "quotes": f"{session_id}_quotes",
            "history": f"{session_id}_history",
            "news": f"{session_id}_news",
            "candidates": f"{session_id}_candidates",
            "market_context": f"{session_id}_market_context",
            "fundamentals": f"{session_id}_fundamentals",
            "native_stream_signals": f"{session_id}_native_stream_signals",
        }
        binding = {
            "time_field": "event_time",
            "type_field": "event_type",
            "key_field": "source_key",
            "field_mappings": {"watch_session_id": "watch_session_id"},
        }
        with AgenticStore() as store:
            store.upsert_source(
                {
                    "source_id": "finance_watch_sessions",
                    "kind": "external_event",
                    "name": "finance watch sessions",
                    "schema_binding": {
                        "time_field": "event_time",
                        "type_field": "event_type",
                        "key_field": "session_id",
                        "field_mappings": {"session_id": "session_id", "status": "status"},
                    },
                    "metadata": {"domain": "finance", "workflow": "watch-session"},
                }
            )
            store.append_external_event(
                "finance_watch_sessions",
                {
                    "session_id": session_id,
                    "status": "running",
                    "market": "us",
                    "symbols": ["AAPL", "MSFT", "NVDA"],
                    "sources": source_ids,
                    "tick_count": 1,
                    "event_time": "2026-05-20T14:00:00Z",
                    "updated_at": "2026-05-20T14:00:00Z",
                    "event_type": "watch_session_running",
                    "source_key": session_id,
                },
            )
            for feed, source_id in source_ids.items():
                store.upsert_source(
                    {
                        "source_id": source_id,
                        "kind": "external_event",
                        "name": source_id,
                        "schema_binding": binding,
                        "metadata": {"domain": "finance", "workflow": "watch-session", "raw_feed": feed},
                    }
                )
            for feed, source_id in source_ids.items():
                payload = {
                    "watch_session_id": session_id,
                    "event_time": "2026-05-20T14:00:01Z",
                    "event_type": feed,
                    "source_key": "AAPL",
                    "market": "us",
                    "symbol": "AAPL",
                }
                if feed == "candidates":
                    payload.update({"event_type": "research_candidate", "rank": 1, "score": 9.5})
                if feed == "native_stream_signals":
                    payload.update({"event_type": "native_stream_signal", "signal_type": "entry_research_signal", "score": 9.5})
                if feed == "fundamentals":
                    payload.update(
                        {
                            "event_type": "fundamental_unavailable",
                            "freshness": "unavailable",
                            "error_type": "provider_unavailable",
                            "message": "No configured public fundamentals provider is available without credentials.",
                        }
                    )
                store.append_external_event(source_id, payload)

    def test_provider_registry_exposes_capabilities_and_catalog(self):
        self.assertEqual(provider_names_for_operation("fetch_history"), ["akshare", "yahoo"])
        self.assertEqual(provider_names_for_operation("fetch_fundamentals"), ["sec-companyfacts"])
        self.assertEqual(provider_names_for_operation("fetch_news"), ["google-news", "sec-filings", "yahoo-finance-news"])
        self.assertEqual(provider_names_for_operation("fetch_quotes"), ["akshare", "tencent", "yahoo"])

        catalog = provider_catalog()
        providers = {item["provider"]: item for item in catalog}
        self.assertEqual(set(providers), {"akshare", "google-news", "sec-companyfacts", "sec-filings", "tencent", "yahoo", "yahoo-finance-news"})
        self.assertIn("fetch-history", providers["yahoo"]["commands"])
        self.assertIn("fetch-quotes", providers["yahoo"]["commands"])
        self.assertIn("fetch-news", providers["google-news"]["commands"])
        self.assertIn("fetch-news", providers["sec-filings"]["commands"])
        self.assertIn("fetch-news", providers["yahoo-finance-news"]["commands"])
        self.assertIn("fetch-fundamentals", providers["sec-companyfacts"]["commands"])
        self.assertIn("fetch-quotes", providers["tencent"]["commands"])
        self.assertNotIn("fetch-history", providers["tencent"]["commands"])
        self.assertEqual(providers["tencent"]["freshness"]["us"], "delayed")
        self.assertEqual(providers["sec-filings"]["source_category"], "regulatory")
        self.assertGreater(providers["sec-filings"]["source_score"], providers["google-news"]["source_score"])

    def test_information_source_classification_scores_public_sources(self):
        sec = classify_information_source(provider="sec-filings", event_type="filing", freshness="near_realtime")
        google = classify_information_source(provider="google-news", event_type="news", freshness="near_realtime")
        unknown = classify_information_source(provider="unknown-provider", event_type="news", freshness="delayed")

        self.assertEqual(sec["source_category"], "regulatory")
        self.assertEqual(sec["source_type"], "filing")
        self.assertGreater(sec["source_score"], google["source_score"])
        self.assertEqual(unknown["source_category"], "unknown")
        self.assertLess(unknown["source_score"], google["source_score"])

    def test_parse_sec_companyfacts_payload_extracts_public_fundamentals(self):
        payload = {
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [
                                {"end": "2025-12-31", "val": 1000, "form": "10-K", "filed": "2026-02-01"},
                                {"end": "2026-03-31", "val": 300, "form": "10-Q", "filed": "2026-05-01"},
                            ]
                        }
                    },
                    "NetIncomeLoss": {"units": {"USD": [{"end": "2026-03-31", "val": 42, "form": "10-Q", "filed": "2026-05-01"}]}},
                }
            }
        }

        rows = parse_sec_companyfacts_payload(payload, market="us", symbol="AAPL", cik="0000320193", source_url="https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["provider"], "sec-companyfacts")
        self.assertEqual(rows[0]["symbol"], "AAPL")
        self.assertEqual(rows[0]["revenue"], 300)
        self.assertEqual(rows[0]["net_income"], 42)
        self.assertEqual(rows[0]["fiscal_period_end"], "2026-03-31")

    def test_sec_companyfacts_user_agent_policy_is_configurable_and_not_placeholder(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            default_policy = sec_user_agent_policy()
        self.assertFalse(default_policy["configured"])
        self.assertNotIn("contact@example.invalid", default_policy["user_agent"])
        self.assertIn("VELARIA_SEC_USER_AGENT", default_policy["hint"])

        with mock.patch.dict(os.environ, {"VELARIA_SEC_USER_AGENT": "VelariaFinanceTest/1.0 ops@example.com"}):
            configured_policy = sec_user_agent_policy()
        self.assertTrue(configured_policy["configured"])
        self.assertEqual(configured_policy["source"], "VELARIA_SEC_USER_AGENT")
        self.assertEqual(configured_policy["user_agent"], "VelariaFinanceTest/1.0 ops@example.com")

    def test_sec_companyfacts_fetch_uses_configured_user_agent(self):
        captured_user_agents: list[str | None] = []

        class FakeResponse:
            def __init__(self, payload: dict[str, object]):
                self._payload = payload

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(self._payload).encode("utf-8")

        facts_payload = {
            "facts": {
                "us-gaap": {
                    "Revenues": {"units": {"USD": [{"end": "2026-03-31", "val": 300, "form": "10-Q", "filed": "2026-05-01"}]}},
                    "NetIncomeLoss": {"units": {"USD": [{"end": "2026-03-31", "val": 42, "form": "10-Q", "filed": "2026-05-01"}]}},
                }
            }
        }

        def fake_urlopen(req, timeout=0):
            captured_user_agents.append(dict(req.header_items()).get("User-agent"))
            if "company_tickers" in req.full_url:
                return FakeResponse({"0": {"ticker": "AAPL", "cik_str": 320193}})
            return FakeResponse(facts_payload)

        with mock.patch.dict(os.environ, {"VELARIA_SEC_USER_AGENT": "VelariaFinanceTest/1.0 ops@example.com"}):
            with mock.patch("velaria.finance_pack.urllib_request.urlopen", side_effect=fake_urlopen):
                rows = fetch_fundamentals(provider="sec-companyfacts", market="us", symbols="AAPL")

        self.assertEqual(rows[0]["provider"], "sec-companyfacts")
        self.assertEqual(rows[0]["revenue"], 300)
        self.assertEqual(captured_user_agents, ["VelariaFinanceTest/1.0 ops@example.com", "VelariaFinanceTest/1.0 ops@example.com"])

    def test_yahoo_quote_provider_uses_chart_meta(self):
        payload = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "AAPL",
                            "regularMarketPrice": 201.5,
                            "previousClose": 200.0,
                            "regularMarketTime": 1770000000,
                            "currency": "USD",
                        },
                        "timestamp": [1770000000],
                        "indicators": {"quote": [{"volume": [123456]}]},
                    }
                ],
                "error": None,
            }
        }

        rows = parse_yahoo_quote_payload(payload, market="us", symbol="AAPL", yahoo_symbol="AAPL", source_url="https://query1.finance.yahoo.com/v8/finance/chart/AAPL")
        self.assertEqual(rows[-1]["provider"], "yahoo")
        self.assertEqual(rows[-1]["price"], 201.5)
        self.assertEqual(rows[-1]["pct_change"], 0.75)

    def test_tencent_cn_quote_parser_accepts_exchange_prefixed_index_symbol(self):
        payload = 'v_sh000001="1~上证指数~000001~4169.54~4131.53~4122.96~617611067~0~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~~20260519161415~38.01~0.92~4170.29~4107.99~4169.54/617611067/1306522478007~617611067~130652248~1.28~18.32~~4170.29~4107.99~1.51~644374.84~694466.01~0.00~-1~-1~0.87~0~";'

        rows = parse_tencent_quote_payload(payload, market="cn", symbols=["s_sh000001"], fetched_at="2026-05-20T00:58:00Z")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "000001")
        self.assertEqual(rows[0]["name"], "上证指数")
        self.assertEqual(rows[0]["price"], 4169.54)

    def test_native_stream_view_name_stays_within_sql_identifier_limit(self):
        args = mock.Mock()
        args.market = "us"
        args.source_id = "us_continuous_e2e_20260520_candidates_with_a_very_long_suffix"

        view_name = _rank_native_stream_view_name(args)

        self.assertLessEqual(len(view_name), 63)
        self.assertTrue(view_name.startswith("finance_rank_candidate_stream_"))

    def test_provider_registry_drives_normalization_and_operation_errors(self):
        self.assertEqual(normalize_provider(" Yahoo "), "yahoo")
        with self.assertRaisesRegex(Exception, "provider does not support quotes") as ctx:
            fetch_quotes(provider="google-news", market="us", symbols=["AAPL"])

        error = ctx.exception
        self.assertEqual(error.error_type, "unsupported_provider_operation")
        self.assertEqual(error.details["operation"], "fetch_quotes")
        self.assertEqual(error.details["candidates"], ["akshare", "tencent", "yahoo"])

    def test_parse_google_news_rss_maps_news_rows_and_sentiment(self):
        rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss><channel>
          <item>
            <title>Apple stock gains after strong demand report</title>
            <link>https://news.google.com/rss/articles/example</link>
            <source url="https://example.com">Example Wire</source>
            <pubDate>Mon, 18 May 2026 15:00:00 GMT</pubDate>
            <description>Analysts see resilient iPhone demand and upbeat margins.</description>
          </item>
          <item>
            <title>Apple faces antitrust risk as regulators investigate</title>
            <link>https://news.google.com/rss/articles/example2</link>
            <source url="https://example.org">Example Risk</source>
            <pubDate>Mon, 18 May 2026 15:05:00 GMT</pubDate>
            <description>Investigation pressure raises legal risk.</description>
          </item>
        </channel></rss>"""

        rows = parse_google_news_rss(
            rss,
            market="us",
            symbol="AAPL",
            query="AAPL stock",
            fetched_at="2026-05-18T16:00:00Z",
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["provider"], "google-news")
        self.assertEqual(rows[0]["symbol"], "AAPL")
        self.assertEqual(rows[0]["publisher"], "Example Wire")
        self.assertEqual(rows[0]["publisher_url"], "https://example.com")
        self.assertEqual(rows[0]["published_at"], "2026-05-18T15:00:00Z")
        sentiment = evaluate_news_sentiment(rows)
        self.assertEqual(sentiment["article_count"], 2)
        self.assertGreater(sentiment["positive_hits"], 0)
        self.assertGreater(sentiment["negative_hits"], 0)
        self.assertIn(sentiment["label"], {"mixed", "positive", "negative", "neutral"})

    def test_parse_yahoo_finance_news_rss_maps_scored_news_rows(self):
        rss = """<?xml version="1.0" encoding="UTF-8"?>
        <rss><channel>
          <item>
            <title>Apple gains as services growth improves</title>
            <link>https://finance.yahoo.com/news/example</link>
            <pubDate>Wed, 20 May 2026 09:30:25 +0000</pubDate>
            <description>Yahoo Finance coverage cites strong demand and resilient margins.</description>
          </item>
        </channel></rss>"""

        rows = parse_yahoo_finance_news_rss(
            rss,
            market="us",
            symbol="AAPL",
            query="AAPL",
            source_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=AAPL",
            fetched_at="2026-05-20T10:00:00Z",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["provider"], "yahoo-finance-news")
        self.assertEqual(rows[0]["source_category"], "news")
        self.assertEqual(rows[0]["source_type"], "finance_news")
        self.assertGreater(rows[0]["source_score"], 0.0)
        self.assertIn("finance news", rows[0]["source_score_reason"])

    def test_parse_sec_filings_atom_maps_scored_regulatory_rows(self):
        atom = """<?xml version="1.0" encoding="ISO-8859-1" ?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <entry>
            <title>10-Q - APPLE INC. (0000320193) (Filer)</title>
            <link href="https://www.sec.gov/Archives/edgar/data/320193/example-index.htm" />
            <updated>2026-05-01T20:00:00-04:00</updated>
            <summary type="html">Filed quarterly report</summary>
          </entry>
        </feed>"""

        rows = parse_sec_filings_atom(
            atom,
            market="us",
            symbol="AAPL",
            cik="0000320193",
            source_url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&output=atom",
            fetched_at="2026-05-20T10:00:00Z",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["provider"], "sec-filings")
        self.assertEqual(rows[0]["event_type"], "filing")
        self.assertEqual(rows[0]["filing_form"], "10-Q")
        self.assertEqual(rows[0]["source_category"], "regulatory")
        self.assertEqual(rows[0]["source_type"], "filing")
        self.assertGreater(rows[0]["source_score"], 0.9)

    def test_source_score_participates_in_evidence_structured_ranking(self):
        rows = [
            {
                "feed": "news",
                "event_id": "google-aapl",
                "event_time": "2026-05-20T09:00:00Z",
                "payload_json": {
                    "event_type": "news",
                    "symbol": "AAPL",
                    "provider": "google-news",
                    "title": "AAPL neutral item",
                    "summary": "AAPL demand",
                    "source_score": 0.65,
                },
            },
            {
                "feed": "news",
                "event_id": "sec-aapl",
                "event_time": "2026-05-20T08:00:00Z",
                "payload_json": {
                    "event_type": "filing",
                    "symbol": "AAPL",
                    "provider": "sec-filings",
                    "title": "AAPL 10-Q filing",
                    "summary": "AAPL demand",
                    "source_score": 0.95,
                },
            },
        ]

        result = FinanceEvidenceRetriever().search_rows(
            intelligence_id="intel_sources",
            watch_session_id="session_sources",
            rows=rows,
            query_text="AAPL demand",
            options=EvidenceSearchOptions(feed="news", top_k=2, index_mode="off"),
        )

        self.assertEqual(result.retrieval["structured_features"][-1], "source_score")
        self.assertEqual(result.hits[0]["row"]["provider"], "sec-filings")
        self.assertGreater(result.hits[0]["score_breakdown"]["structured_score"], result.hits[1]["score_breakdown"]["structured_score"])

    def test_rank_candidates_cli_outputs_research_candidates_with_news(self):
        quote_rows = [
            {
                "event_time": "2026-05-18T16:00:00Z",
                "event_type": "quote",
                "source_key": "AAPL",
                "symbol": "AAPL",
                "market": "us",
                "price": 296.35,
                "volume": 13519420,
                "pct_change": 1.2,
                "provider": "tencent",
                "freshness": "delayed",
                "delay_sec": None,
                "fetched_at": "2026-05-18T16:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            },
            {
                "event_time": "2026-05-18T16:00:00Z",
                "event_type": "quote",
                "source_key": "MSFT",
                "symbol": "MSFT",
                "market": "us",
                "price": 520.0,
                "volume": 10000000,
                "pct_change": -0.5,
                "provider": "tencent",
                "freshness": "delayed",
                "delay_sec": None,
                "fetched_at": "2026-05-18T16:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            },
        ]
        history_by_symbol = {
            "AAPL": [
                {"symbol": "AAPL", "date": "2026-05-01", "close": 280.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
                {"symbol": "AAPL", "date": "2026-05-18", "close": 296.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
            ],
            "MSFT": [
                {"symbol": "MSFT", "date": "2026-05-01", "close": 530.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
                {"symbol": "MSFT", "date": "2026-05-18", "close": 520.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
            ],
        }
        news_by_symbol = {
            "AAPL": [
                {"symbol": "AAPL", "title": "Apple gains on strong demand", "summary": "upbeat growth", "provider": "google-news", "source_url": "https://news.google.com/rss/search", "publisher": "Wire", "published_at": "2026-05-18T15:00:00Z", "freshness": "near_realtime"}
            ],
            "MSFT": [
                {"symbol": "MSFT", "title": "Microsoft faces risk", "summary": "regulators investigate", "provider": "google-news", "source_url": "https://news.google.com/rss/search", "publisher": "Wire", "published_at": "2026-05-18T15:00:00Z", "freshness": "near_realtime"}
            ],
        }

        def fake_history(**kwargs):
            return history_by_symbol[kwargs["symbol"]]

        def fake_news(**kwargs):
            return news_by_symbol[kwargs["symbol"]]

        with tempfile.TemporaryDirectory(prefix="velaria-finance-rank-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    with mock.patch("velaria.finance_pack.cli.fetch_history", side_effect=fake_history):
                        with mock.patch("velaria.finance_pack.cli.fetch_news", side_effect=fake_news):
                            stdout = StringIO()
                            with redirect_stdout(stdout):
                                exit_code = finance_cli_main(
                                    [
                                        "rank-candidates",
                                        "--market",
                                        "us",
                                        "--symbols",
                                        "AAPL,MSFT",
                                        "--start-date",
                                        "20260501",
                                        "--end-date",
                                        "20260518",
                                        "--top",
                                        "1",
                                        "--iterations",
                                        "1",
                                        "--interval-sec",
                                        "0",
                                        "--format",
                                        "json",
                                    ]
                                )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "rank-candidates")
        self.assertEqual(payload["recommendation_type"], "research_candidate")
        self.assertEqual(len(payload["ticks"]), 1)
        self.assertEqual(payload["ticks"][0]["research_candidates"][0]["symbol"], "AAPL")
        self.assertIn("news_sentiment", payload["ticks"][0]["research_candidates"][0])
        self.assertIn("not investment advice", payload["disclaimer"])
        self.assertNotIn("buy", json.dumps(payload).lower())

    def test_rank_candidates_stream_monitor_emits_focus_events(self):
        quote_rows = [
            {
                "event_time": "2026-05-18T16:00:00Z",
                "event_type": "quote",
                "source_key": "NVDA",
                "symbol": "NVDA",
                "market": "us",
                "price": 222.0,
                "volume": 50000000,
                "pct_change": 0.4,
                "provider": "tencent",
                "freshness": "delayed",
                "delay_sec": None,
                "fetched_at": "2026-05-18T16:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        history_rows = [
            {"symbol": "NVDA", "date": "2026-05-01", "close": 190.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
            {"symbol": "NVDA", "date": "2026-05-18", "close": 222.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
        ]
        news_rows = [
            {"symbol": "NVDA", "title": "Nvidia gains on strong demand", "summary": "upbeat growth", "provider": "google-news", "source_url": "https://news.google.com/rss/search", "publisher": "Wire", "published_at": "2026-05-18T15:00:00Z", "freshness": "near_realtime"}
        ]

        with tempfile.TemporaryDirectory(prefix="velaria-finance-rank-stream-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    with mock.patch("velaria.finance_pack.cli.fetch_history", return_value=history_rows):
                        with mock.patch("velaria.finance_pack.cli.fetch_news", return_value=news_rows):
                            stdout = StringIO()
                            with redirect_stdout(stdout):
                                exit_code = finance_cli_main(
                                    [
                                        "rank-candidates",
                                        "--market",
                                        "us",
                                        "--symbols",
                                        "NVDA",
                                        "--start-date",
                                        "20260501",
                                        "--end-date",
                                        "20260518",
                                        "--top",
                                        "1",
                                        "--iterations",
                                        "1",
                                        "--interval-sec",
                                        "0",
                                        "--stream-monitor",
                                        "--entry-score-threshold",
                                        "8",
                                        "--entry-return-threshold",
                                        "5",
                                        "--stream-window-size",
                                        "60s",
                                        "--cooldown-sec",
                                        "0",
                                        "--format",
                                        "json",
                                    ]
                                )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["stream_monitors"][0]["execution_mode"], "stream")
                self.assertEqual(payload["stream_monitors"][0]["signal_type"], "entry_research_signal")
                tick = payload["ticks"][0]
                self.assertEqual(tick["stream_monitor_runs"][0]["monitor_id"], payload["stream_monitors"][0]["monitor_id"])
                self.assertGreaterEqual(len(tick["focus_events"]), 1)
                self.assertEqual(tick["focus_events"][0]["key_fields"]["symbol"], "NVDA")
                with AgenticStore() as store:
                    monitor = store.get_monitor(payload["stream_monitors"][0]["monitor_id"])
                    self.assertIsNotNone(monitor)
                    self.assertEqual(monitor["execution_mode"], "stream")

    def test_rank_candidates_native_stream_ingests_raw_data_and_emits_signals(self):
        quote_rows = [
            {
                "event_time": "2026-05-18T16:00:00Z",
                "event_type": "quote",
                "source_key": "NVDA",
                "symbol": "NVDA",
                "market": "us",
                "price": 222.0,
                "volume": 50000000,
                "pct_change": 0.4,
                "provider": "tencent",
                "freshness": "delayed",
                "delay_sec": None,
                "fetched_at": "2026-05-18T16:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        history_rows = [
            {"symbol": "NVDA", "market": "us", "date": "2026-05-01", "close": 190.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
            {"symbol": "NVDA", "market": "us", "date": "2026-05-18", "close": 222.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
        ]
        news_rows = [
            {"symbol": "NVDA", "market": "us", "title": "Nvidia gains on strong demand", "summary": "upbeat growth", "provider": "google-news", "source_url": "https://news.google.com/rss/search", "publisher": "Wire", "published_at": "2026-05-18T15:00:00Z", "freshness": "near_realtime"}
        ]

        with tempfile.TemporaryDirectory(prefix="velaria-finance-native-stream-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    with mock.patch("velaria.finance_pack.cli.fetch_history", return_value=history_rows):
                        with mock.patch("velaria.finance_pack.cli.fetch_news", return_value=news_rows):
                            stdout = StringIO()
                            with redirect_stdout(stdout):
                                exit_code = finance_cli_main(
                                    [
                                        "rank-candidates",
                                        "--market",
                                        "us",
                                        "--symbols",
                                        "NVDA",
                                        "--start-date",
                                        "20260501",
                                        "--end-date",
                                        "20260518",
                                        "--top",
                                        "1",
                                        "--iterations",
                                        "1",
                                        "--interval-sec",
                                        "0",
                                        "--native-stream",
                                        "--ingest-raw",
                                        "--until-time",
                                        "2026-01-01T00:00:00+08:00",
                                        "--entry-score-threshold",
                                        "8",
                                        "--entry-return-threshold",
                                        "5",
                                        "--format",
                                        "json",
                                    ]
                                )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertTrue(payload["native_stream"]["enabled"])
                self.assertEqual(payload["native_stream"]["engine"], "velaria_native_realtime_stream")
                self.assertIsNone(payload["native_stream"]["max_batches"])
                self.assertIn("WHERE entry_signal >= 1 OR exit_signal >= 1", payload["native_stream"]["sql"])
                self.assertEqual(set(payload["raw_sources"]), {"quotes", "history", "news", "features", "candidates", "native_stream_signals"})
                tick = payload["ticks"][0]
                self.assertEqual(tick["native_stream_signals"][0]["signal_type"], "entry_research_signal")
                self.assertEqual(tick["native_stream_signals"][0]["symbol"], "NVDA")

                with AgenticStore() as store:
                    for key, source in payload["raw_sources"].items():
                        rows = store.read_external_events(source["source_id"])
                        self.assertGreaterEqual(len(rows), 1, key)
                    signal_rows = store.read_external_events(payload["raw_sources"]["native_stream_signals"]["source_id"])
                    self.assertEqual(signal_rows[0]["event_type"], "native_stream_signal")
                    self.assertEqual(signal_rows[0]["signal_type"], "entry_research_signal")

                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "stream-history",
                                "--market",
                                "us",
                                "--source-id",
                                payload["raw_sources"]["native_stream_signals"]["source_id"],
                                "--format",
                                "json",
                            ]
                        )
                    self.assertEqual(exit_code, 0)
                    history_payload = json.loads(stdout.getvalue())
                    self.assertEqual(history_payload["row_count"], 1)
                    self.assertEqual(history_payload["rows"][0]["symbol"], "NVDA")

    def test_watch_session_persists_market_fundamental_signals_and_summary(self):
        quote_rows = [
            {
                "event_time": "2026-05-18T16:00:00Z",
                "event_type": "quote",
                "source_key": "NVDA",
                "symbol": "NVDA",
                "market": "us",
                "price": 222.0,
                "volume": 50000000,
                "pct_change": 0.4,
                "provider": "tencent",
                "freshness": "delayed",
                "delay_sec": None,
                "fetched_at": "2026-05-18T16:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        history_rows = [
            {"symbol": "NVDA", "market": "us", "date": "2026-05-01", "close": 190.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
            {"symbol": "NVDA", "market": "us", "date": "2026-05-18", "close": 222.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
        ]
        news_rows = [
            {"symbol": "NVDA", "market": "us", "title": "Nvidia gains on strong demand", "summary": "upbeat growth", "provider": "google-news", "source_url": "https://news.google.com/rss/search", "publisher": "Wire", "published_at": "2026-05-18T15:00:00Z", "freshness": "near_realtime"}
        ]
        market_rows = [
            {
                "event_time": "2026-05-18T16:00:00Z",
                "event_type": "market_context",
                "source_key": "SPY",
                "symbol": "SPY",
                "market": "us",
                "price": 600.0,
                "pct_change": 0.2,
                "provider": "tencent",
                "freshness": "delayed",
            }
        ]
        fundamental_rows = [
            {
                "event_time": "2026-05-18T16:00:00Z",
                "event_type": "fundamental_snapshot",
                "source_key": "NVDA",
                "symbol": "NVDA",
                "market": "us",
                "provider": "test-fundamentals",
                "freshness": "snapshot",
                "market_cap": 1000.0,
            }
        ]

        with tempfile.TemporaryDirectory(prefix="velaria-finance-watch-session-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    with mock.patch("velaria.finance_pack.cli.fetch_history", return_value=history_rows):
                        with mock.patch("velaria.finance_pack.cli.fetch_news", return_value=news_rows):
                            with mock.patch("velaria.finance_pack.cli._fetch_market_context_rows", return_value=market_rows):
                                with mock.patch("velaria.finance_pack.cli._fetch_fundamental_rows", return_value=fundamental_rows):
                                    stdout = StringIO()
                                    with redirect_stdout(stdout):
                                        exit_code = finance_cli_main(
                                            [
                                                "watch-session",
                                                "start",
                                                "--session-id",
                                                "session_test",
                                                "--market",
                                                "us",
                                                "--symbols",
                                                "NVDA",
                                                "--start-date",
                                                "20260501",
                                                "--end-date",
                                                "20260518",
                                                "--top",
                                                "1",
                                                "--iterations",
                                                "1",
                                                "--interval-sec",
                                                "0",
                                                "--entry-score-threshold",
                                                "8",
                                                "--entry-return-threshold",
                                                "5",
                                                "--format",
                                                "json",
                                            ]
                                        )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "watch-session-start")
                self.assertEqual(payload["watch_session"]["session_id"], "session_test")
                self.assertEqual(payload["tick_count"], 1)
                self.assertIn("market_context", payload["raw_sources"])
                self.assertIn("fundamentals", payload["raw_sources"])
                self.assertIn("native_stream_signals", payload["raw_sources"])

                with AgenticStore() as store:
                    market_events = store.read_external_events(payload["raw_sources"]["market_context"]["source_id"])
                    fundamental_events = store.read_external_events(payload["raw_sources"]["fundamentals"]["source_id"])
                    signal_events = store.read_external_events(payload["raw_sources"]["native_stream_signals"]["source_id"])
                    self.assertEqual(market_events[0]["watch_session_id"], "session_test")
                    self.assertEqual(fundamental_events[0]["watch_session_id"], "session_test")
                    self.assertEqual(signal_events[0]["watch_session_id"], "session_test")

                for command in ("list", "show", "events", "signals", "summarize"):
                    stdout = StringIO()
                    argv = ["watch-session", command, "--format", "json"]
                    if command != "list":
                        argv.extend(["--session-id", "session_test"])
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(argv)
                    self.assertEqual(exit_code, 0, command)
                    command_payload = json.loads(stdout.getvalue())
                    self.assertTrue(command_payload["ok"], command)
                    if command == "summarize":
                        self.assertGreaterEqual(command_payload["summary"]["signal_count"], 1)
                        self.assertGreaterEqual(command_payload["summary"]["market_context_count"], 1)
                        self.assertGreaterEqual(command_payload["summary"]["fundamental_count"], 1)

    def test_watch_session_async_start_records_runtime_and_controls_process(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-watch-session-async-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                fake_process = mock.Mock()
                fake_process.pid = 4321
                with mock.patch("velaria.finance_pack.cli.subprocess.Popen", return_value=fake_process) as popen:
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "watch-session",
                                "start",
                                "--session-id",
                                "session_async",
                                "--market",
                                "us",
                                "--symbols",
                                "AAPL,MSFT,NVDA",
                                "--start-date",
                                "20260501",
                                "--end-date",
                                "20260518",
                                "--iterations",
                                "2",
                                "--interval-sec",
                                "1",
                                "--async-run",
                                "--format",
                                "json",
                            ]
                        )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "watch-session-async-start")
                self.assertEqual(payload["watch_session_id"], "session_async")
                self.assertEqual(payload["run"]["pid"], 4321)
                self.assertTrue(payload["run"]["log_path"].endswith("session_async.jsonl"))
                self.assertIn("-m", popen.call_args.args[0])
                self.assertIn("velaria.finance_pack.cli", popen.call_args.args[0])
                self.assertIn("--jsonl", popen.call_args.args[0])
                self.assertIn("core_runtime", payload["run"])
                self.assertIn("ai_cli_runtime", payload["run"])

                with AgenticStore() as store:
                    rows = store.read_external_events("finance_watch_session_runs")
                    self.assertEqual(rows[-1]["session_id"], "session_async")
                    self.assertEqual(rows[-1]["pid"], 4321)

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill") as kill:
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python -m velaria.finance_pack.cli watch-session start --session-id session_async"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["watch-session", "status", "--session-id", "session_async", "--format", "json"])
                self.assertEqual(exit_code, 0)
                status_payload = json.loads(stdout.getvalue())
                self.assertTrue(status_payload["process_running"])
                kill.assert_called_once_with(4321, 0)

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill") as kill:
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python unrelated.py"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["watch-session", "status", "--session-id", "session_async", "--format", "json"])
                self.assertEqual(exit_code, 0)
                stale_payload = json.loads(stdout.getvalue())
                self.assertFalse(stale_payload["process_running"])
                self.assertEqual(stale_payload["effective_status"], "not_running")
                kill.assert_called_once_with(4321, 0)

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill") as kill:
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python unrelated.py"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["watch-session", "stop", "--session-id", "session_async", "--format", "json"])
                self.assertEqual(exit_code, 0)
                stale_stop_payload = json.loads(stdout.getvalue())
                self.assertFalse(stale_stop_payload["signal_sent"])
                self.assertEqual(kill.call_args_list, [mock.call(4321, 0)])

                log_path = payload["run"]["log_path"]
                with open(log_path, "w", encoding="utf-8") as handle:
                    handle.write('{"tick": 1}\n{"tick": 2}\n')
                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(["watch-session", "logs", "--session-id", "session_async", "--limit", "1", "--format", "json"])
                self.assertEqual(exit_code, 0)
                log_payload = json.loads(stdout.getvalue())
                self.assertEqual(log_payload["line_count"], 1)
                self.assertEqual(log_payload["lines"], ['{"tick": 2}'])

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill") as kill:
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python -m velaria.finance_pack.cli watch-session start --session-id session_async"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["watch-session", "stop", "--session-id", "session_async", "--format", "json"])
                self.assertEqual(exit_code, 0)
                stop_payload = json.loads(stdout.getvalue())
                self.assertEqual(stop_payload["action"], "watch-session-stop")
                self.assertEqual(kill.call_args_list[-1].args[1].name, "SIGTERM")

    def test_finance_job_events_are_persisted_and_latest_is_queryable(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-jobs-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                append_finance_job_event(
                    finance_job_payload(
                        job_id="job_session_test_watch",
                        job_type="watch_session_job",
                        watch_session_id="session_jobs",
                        intelligence_id="intel_jobs",
                        status="running",
                        command=["finance", "watch-session", "start"],
                        summary={"tick_count": 1},
                    )
                )
                append_finance_job_event(
                    finance_job_payload(
                        job_id="job_session_test_watch",
                        job_type="watch_session_job",
                        watch_session_id="session_jobs",
                        intelligence_id="intel_jobs",
                        status="completed",
                        command=["finance", "watch-session", "start"],
                        summary={"tick_count": 3},
                    )
                )

                jobs = latest_finance_jobs("session_jobs")

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["job_id"], "job_session_test_watch")
        self.assertEqual(jobs[0]["status"], "completed")
        self.assertEqual(jobs[0]["summary"]["tick_count"], 3)

    def test_intelligence_jobs_status_stop_and_resume_use_durable_job_surface(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-jobs-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                fake_process = mock.Mock()
                fake_process.pid = 4321
                with mock.patch("velaria.finance_pack.cli.subprocess.Popen", return_value=fake_process):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "intelligence",
                                "start",
                                "--session-id",
                                "session_jobs_cli",
                                "--intelligence-id",
                                "intel_jobs_cli",
                                "--market",
                                "us",
                                "--symbols",
                                "AAPL,MSFT,NVDA",
                                "--start-date",
                                "20260501",
                                "--end-date",
                                "20260518",
                                "--iterations",
                                "2",
                                "--interval-sec",
                                "1",
                                "--async-run",
                                "--format",
                                "json",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                start_payload = json.loads(stdout.getvalue())
                self.assertEqual(start_payload["action"], "intelligence-async-start")

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(["intelligence", "jobs", "--session-id", "session_jobs_cli", "--format", "json"])
                self.assertEqual(exit_code, 0)
                jobs_payload = json.loads(stdout.getvalue())
                self.assertEqual(jobs_payload["action"], "intelligence-jobs")
                self.assertGreaterEqual(jobs_payload["job_count"], 1)

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill") as kill:
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python -m velaria.finance_pack.cli watch-session start --session-id session_jobs_cli"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["intelligence", "status", "--session-id", "session_jobs_cli", "--format", "json"])
                self.assertEqual(exit_code, 0)
                status_payload = json.loads(stdout.getvalue())
                self.assertEqual(status_payload["action"], "intelligence-status")
                self.assertTrue(status_payload["process_running"])
                kill.assert_called_once_with(4321, 0)

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill") as kill:
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python -m velaria.finance_pack.cli watch-session start --session-id session_jobs_cli"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["intelligence", "stop", "--session-id", "session_jobs_cli", "--format", "json"])
                self.assertEqual(exit_code, 0)
                stop_payload = json.loads(stdout.getvalue())
                self.assertEqual(stop_payload["action"], "intelligence-stop")
                self.assertTrue(stop_payload["signal_sent"])
                self.assertEqual(kill.call_args_list[-1].args[1].name, "SIGTERM")

                resumed_process = mock.Mock()
                resumed_process.pid = 9876
                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.subprocess.Popen", return_value=resumed_process) as popen:
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python unrelated.py"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["intelligence", "resume", "--session-id", "session_jobs_cli", "--format", "json"])
                self.assertEqual(exit_code, 0)
                resume_payload = json.loads(stdout.getvalue())
                self.assertEqual(resume_payload["action"], "intelligence-resume")
                self.assertTrue(resume_payload["resumed"])
                self.assertEqual(resume_payload["run"]["pid"], 9876)
                self.assertIn("watch-session", popen.call_args.args[0])

    def test_intelligence_resume_rejects_untrusted_persisted_argv(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-resume-guard-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                fake_process = mock.Mock()
                fake_process.pid = 4321
                with mock.patch("velaria.finance_pack.cli.subprocess.Popen", return_value=fake_process):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "intelligence",
                                "start",
                                "--session-id",
                                "session_resume_guard",
                                "--market",
                                "us",
                                "--symbols",
                                "AAPL",
                                "--start-date",
                                "20260501",
                                "--end-date",
                                "20260518",
                                "--async-run",
                                "--format",
                                "json",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                with AgenticStore() as store:
                    store.append_external_event(
                        "finance_watch_session_runs",
                        {
                            "session_id": "session_resume_guard",
                            "status": "not_running",
                            "pid": 0,
                            "process_identity": {"command_markers": ["unexpected"]},
                            "log_path": str(pathlib.Path(tmp) / "bad.jsonl"),
                            "argv": ["/bin/echo", "not-a-watch-session"],
                            "started_at": "2026-05-20T14:00:00Z",
                            "updated_at": "2026-05-20T14:10:00Z",
                            "event_time": "2026-05-20T14:10:00Z",
                            "event_type": "watch_session_async_start",
                            "source_key": "session_resume_guard",
                        },
                    )

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.subprocess.Popen") as popen:
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(["intelligence", "resume", "--session-id", "session_resume_guard", "--format", "json"])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["action"], "intelligence-resume")
        self.assertFalse(payload["resumed"])
        self.assertEqual(payload["error_type"], "resume_unavailable")
        self.assertIn("trusted watch-session", payload["hint"])
        popen.assert_not_called()

    def test_intelligence_resume_text_mode_reports_already_running_state(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-resume-text-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                fake_process = mock.Mock()
                fake_process.pid = 4321
                with mock.patch("velaria.finance_pack.cli.subprocess.Popen", return_value=fake_process):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "intelligence",
                                "start",
                                "--session-id",
                                "session_resume_text",
                                "--market",
                                "us",
                                "--symbols",
                                "AAPL",
                                "--start-date",
                                "20260501",
                                "--end-date",
                                "20260518",
                                "--async-run",
                                "--format",
                                "json",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill"):
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python -m velaria.finance_pack.cli watch-session start --session-id session_resume_text"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["intelligence", "resume", "--session-id", "session_resume_text"])

        self.assertEqual(exit_code, 0)
        self.assertIn("action: intelligence-resume", stdout.getvalue())
        self.assertIn("watch_session_id: session_resume_text", stdout.getvalue())

    def test_finance_evaluation_scores_persisted_rows_without_refetching(self):
        rows = [
            {"feed": "native_stream_signals", "payload_json": {"event_time": "2026-05-20T14:00:01Z", "symbol": "AAPL", "signal_type": "entry_research_signal", "score": 9.5}},
            {"feed": "native_stream_signals", "payload_json": {"event_time": "2026-05-20T14:00:02Z", "symbol": "NVDA", "signal_type": "exit_risk_signal", "score": 7.5}},
            {"feed": "quotes", "payload_json": {"event_time": "2026-05-20T14:00:00Z", "symbol": "AAPL", "provider": "yahoo", "freshness": "delayed", "source_url": "https://example.invalid/aapl"}},
            {"feed": "fundamentals", "payload_json": {"event_time": "2026-05-20T14:00:00Z", "symbol": "MSFT", "event_type": "fundamental_unavailable", "error_type": "provider_unavailable"}},
        ]
        evaluation = evaluate_finance_session(
            watch_session_id="session_eval",
            rows=rows,
            jobs=[{"job_id": "job_1", "status": "completed", "artifacts": {"log_path": "/tmp/x"}}],
            searches=[{"hit_count": 3, "top_target_kind": "native_stream_signals", "retrieval": {"semantic": {"status": "disabled"}, "index_status": "hit"}}],
            indexes=[{"doc_count": 4, "index_status": "ready", "semantic_status": "disabled"}],
        )

        self.assertEqual(evaluation["watch_session_id"], "session_eval")
        self.assertEqual(evaluation["signal_quality"]["signal_count"], 2)
        self.assertEqual(evaluation["signal_quality"]["outcome_availability"]["status"], "unavailable")
        self.assertEqual(evaluation["provider_quality"]["unavailable_count"], 1)
        self.assertEqual(evaluation["retrieval_quality"]["search_count"], 1)
        self.assertTrue(evaluation["retrieval_quality"]["no_hash_embedding"])

    def test_finance_evaluation_detects_legacy_hash_vector_artifacts(self):
        evaluation = evaluate_finance_session(
            watch_session_id="session_eval_hash",
            rows=[{"feed": "native_stream_signals", "payload_json": {"event_time": "2026-05-20T14:00:01Z", "symbol": "AAPL", "signal_type": "entry_research_signal"}}],
            jobs=[{"job_id": "job_1", "status": "completed"}],
            searches=[{"retrieval": {"semantic": {"status": "disabled"}}}],
            indexes=[{"doc_count": 1, "vectors_path": "/tmp/legacy/vectors.json", "semantic_status": "disabled"}],
        )

        self.assertFalse(evaluation["retrieval_quality"]["no_hash_embedding"])
        self.assertEqual(evaluation["quality_status"], "failed")

    def test_intelligence_evaluate_and_eval_report_persist_quality_events(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-eval-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                self._seed_watch_session_rows("session_eval_cli")

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(["intelligence", "evaluate", "--session-id", "session_eval_cli", "--format", "json"])
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "intelligence-evaluate")
                self.assertEqual(payload["evaluation"]["watch_session_id"], "session_eval_cli")
                self.assertGreaterEqual(payload["evaluation"]["provider_quality"]["row_count"], 1)

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(["intelligence", "eval-report", "--session-id", "session_eval_cli", "--format", "json"])
                self.assertEqual(exit_code, 0)
                report_payload = json.loads(stdout.getvalue())
                self.assertEqual(report_payload["action"], "intelligence-eval-report")
                self.assertEqual(report_payload["report"]["watch_session_id"], "session_eval_cli")

                with AgenticStore() as store:
                    rows = store.read_external_events("finance_intelligence_evaluations")
                    jobs = store.read_external_events("finance_intelligence_jobs")
                self.assertGreaterEqual(len(rows), 1)
                self.assertTrue(any((job.get("payload_json") or {}).get("job_type") == "evaluation_job" for job in jobs))

    def test_watch_session_review_persists_continuous_diagnostics(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-watch-session-review-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                fake_process = mock.Mock()
                fake_process.pid = 4321
                with mock.patch("velaria.finance_pack.cli.subprocess.Popen", return_value=fake_process):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "watch-session",
                                "start",
                                "--session-id",
                                "session_review",
                                "--market",
                                "us",
                                "--symbols",
                                "AAPL,MSFT,NVDA",
                                "--start-date",
                                "20260501",
                                "--end-date",
                                "20260518",
                                "--iterations",
                                "0",
                                "--async-run",
                                "--format",
                                "json",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                start_payload = json.loads(stdout.getvalue())
                self._seed_watch_session_rows("session_review")
                with open(start_payload["run"]["log_path"], "w", encoding="utf-8") as handle:
                    handle.write('{"ok": true, "tick": 1}\n')

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill") as kill:
                    with mock.patch("velaria.finance_pack.cli._process_command_line", return_value="python -m velaria.finance_pack.cli watch-session start --session-id session_review"):
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(["watch-session", "review", "--session-id", "session_review", "--log-limit", "1", "--format", "json"])
                self.assertEqual(exit_code, 0)
                kill.assert_called_once_with(4321, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "watch-session-review")
                review = payload["review"]
                self.assertTrue(review["process_running"])
                self.assertEqual(review["summary"]["signal_count"], 1)
                self.assertNotIn("payload_json", review["summary"]["latest_signal"])
                self.assertIn("velaria_cli_run", review["agent_prompt"])
                self.assertTrue(any(item["type"] == "provider_unavailable_evidence" for item in review["diagnostics"]))
                self.assertTrue(any("supervise" in item for item in review["next_actions"]))

                with AgenticStore() as store:
                    rows = store.read_external_events("finance_watch_session_reviews")
                self.assertEqual(rows[-1]["session_id"], "session_review")
                self.assertEqual(rows[-1]["diagnostic_count"], review["diagnostic_count"])

    def test_watch_session_supervise_runs_review_loop_inside_cli(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-watch-session-supervise-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                fake_process = mock.Mock()
                fake_process.pid = 4321
                with mock.patch("velaria.finance_pack.cli.subprocess.Popen", return_value=fake_process):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "watch-session",
                                "start",
                                "--session-id",
                                "session_supervise",
                                "--market",
                                "us",
                                "--symbols",
                                "AAPL,MSFT,NVDA",
                                "--start-date",
                                "20260501",
                                "--end-date",
                                "20260518",
                                "--iterations",
                                "0",
                                "--async-run",
                                "--format",
                                "json",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                self._seed_watch_session_rows("session_supervise")

                stdout = StringIO()
                with mock.patch("velaria.finance_pack.cli.os.kill"):
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "watch-session",
                                "supervise",
                                "--session-id",
                                "session_supervise",
                                "--iterations",
                                "2",
                                "--interval-sec",
                                "0",
                                "--format",
                                "json",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "watch-session-supervise")
                self.assertEqual(payload["review_count"], 2)
                self.assertEqual(payload["latest_review"]["supervisor_iteration"], 2)

                with AgenticStore() as store:
                    rows = store.read_external_events("finance_watch_session_reviews")
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[-1]["session_id"], "session_supervise")

    def test_intelligence_start_fuses_stream_data_and_ai_notes(self):
        quote_rows = [
            {
                "event_time": "2026-05-20T14:00:00Z",
                "event_type": "quote",
                "source_key": "NVDA",
                "symbol": "NVDA",
                "market": "us",
                "price": 210.0,
                "pct_change": 2.1,
                "provider": "tencent",
                "source_url": "https://qt.gtimg.cn/",
                "freshness": "delayed",
                "license_note": "public provider metadata",
            }
        ]
        history_rows = [
            {"symbol": "NVDA", "market": "us", "date": "2026-05-01", "close": 190.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
            {"symbol": "NVDA", "market": "us", "date": "2026-05-18", "close": 222.0, "provider": "yahoo", "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/", "freshness": "eod"},
        ]
        news_rows = [
            {"symbol": "NVDA", "market": "us", "title": "Nvidia gains on strong demand", "summary": "upbeat growth", "provider": "google-news", "source_url": "https://news.google.com/rss/search", "publisher": "Wire", "published_at": "2026-05-18T15:00:00Z", "freshness": "near_realtime"}
        ]
        market_rows = [
            {
                "event_time": "2026-05-20T14:00:00Z",
                "event_type": "market_context",
                "source_key": "SPY",
                "symbol": "SPY",
                "market": "us",
                "price": 600.0,
                "pct_change": 0.2,
                "provider": "tencent",
                "freshness": "delayed",
            }
        ]
        fundamental_rows = [
            {
                "event_time": "2026-05-20T14:00:00Z",
                "event_type": "fundamental_unavailable",
                "source_key": "NVDA",
                "symbol": "NVDA",
                "market": "us",
                "provider": "public-unavailable",
                "freshness": "unavailable",
                "error_type": "provider_unavailable",
                "not_mocked": True,
            }
        ]

        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    with mock.patch("velaria.finance_pack.cli.fetch_history", return_value=history_rows):
                        with mock.patch("velaria.finance_pack.cli.fetch_news", return_value=news_rows):
                            with mock.patch("velaria.finance_pack.cli._fetch_market_context_rows", return_value=market_rows):
                                with mock.patch("velaria.finance_pack.cli._fetch_fundamental_rows", return_value=fundamental_rows):
                                    stdout = StringIO()
                                    with redirect_stdout(stdout):
                                        exit_code = finance_cli_main(
                                            [
                                                "intelligence",
                                                "start",
                                                "--intelligence-id",
                                                "intel_test",
                                                "--session-id",
                                                "session_intel",
                                                "--market",
                                                "us",
                                                "--symbols",
                                                "NVDA",
                                                "--start-date",
                                                "20260501",
                                                "--end-date",
                                                "20260518",
                                                "--top",
                                                "1",
                                                "--iterations",
                                                "1",
                                                "--interval-sec",
                                                "0",
                                                "--entry-score-threshold",
                                                "8",
                                                "--entry-return-threshold",
                                                "5",
                                                "--format",
                                                "json",
                                            ]
                                        )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "intelligence-start")
                self.assertEqual(payload["intelligence_id"], "intel_test")
                self.assertEqual(payload["watch_session"]["session_id"], "session_intel")
                self.assertEqual(payload["watch_session"]["tick_count"], 1)
                self.assertEqual(payload["runtime_plane"]["core_runtime"], "velaria_native_realtime_stream")
                self.assertEqual(payload["ai_plane"]["ai_runtime"], "velaria_cli_run")
                self.assertIn("velaria_cli_run", payload["ai_plane"]["agent_prompt"])
                self.assertEqual(
                    set(payload["data_plane"]["sources"]),
                    {"quotes", "history", "news", "features", "candidates", "market_context", "fundamentals", "native_stream_signals"},
                )
                self.assertEqual(payload["data_plane"]["counts_by_feed"]["features"], 1)
                self.assertEqual(payload["research_candidates"][0]["feature_snapshot"]["momentum_state"], "bullish")

                with AgenticStore() as store:
                    sessions = store.read_external_events("finance_intelligence_sessions")
                    notes = store.read_external_events("finance_intelligence_ai_notes")
                    features = store.read_external_events(payload["data_plane"]["sources"]["features"])
                self.assertEqual(sessions[-1]["intelligence_id"], "intel_test")
                self.assertEqual(sessions[-1]["watch_session_id"], "session_intel")
                self.assertEqual(notes[-1]["intelligence_id"], "intel_test")
                self.assertEqual(notes[-1]["top_symbol"], "NVDA")
                self.assertEqual(features[-1]["symbol"], "NVDA")
                self.assertIn("rsi_14", features[-1])

    def test_intelligence_replay_uses_persisted_watch_data(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-replay-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                self._seed_watch_session_rows("session_replay")

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(
                        [
                            "intelligence",
                            "replay",
                            "--intelligence-id",
                            "intel_replay",
                            "--session-id",
                            "session_replay",
                            "--format",
                            "json",
                        ]
                    )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "intelligence-replay")
                self.assertEqual(payload["watch_session_id"], "session_replay")
                self.assertEqual(payload["replay"]["event_count"], 7)
                self.assertEqual(payload["replay"]["signal_count"], 1)
                self.assertEqual(payload["replay"]["top_symbol"], "AAPL")
                self.assertIn("velaria_cli_run", payload["ai_plane"]["agent_prompt"])

                with AgenticStore() as store:
                    rows = store.read_external_events("finance_intelligence_replays")
                self.assertEqual(rows[-1]["intelligence_id"], "intel_replay")
                self.assertEqual(rows[-1]["watch_session_id"], "session_replay")

    def test_intelligence_report_persists_supervisor_scorecard(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-report-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                self._seed_watch_session_rows("session_report")

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(
                        [
                            "intelligence",
                            "report",
                            "--intelligence-id",
                            "intel_report",
                            "--session-id",
                            "session_report",
                            "--format",
                            "json",
                        ]
                    )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "intelligence-report")
                self.assertEqual(payload["report"]["top_symbol"], "AAPL")
                self.assertEqual(payload["report"]["supervisor_checks"]["replayability"]["status"], "pass")
                self.assertIn("data_quality", payload["report"]["supervisor_checks"])
                self.assertIn("final_research_summary", payload["report"])

                with AgenticStore() as store:
                    rows = store.read_external_events("finance_intelligence_reports")
                self.assertEqual(rows[-1]["intelligence_id"], "intel_report")
                self.assertEqual(rows[-1]["top_symbol"], "AAPL")

    def test_intelligence_report_scorecard_uses_latest_candidate_per_symbol(self):
        rows = [
            {
                "feed": "candidates",
                "payload_json": {
                    "event_time": "2026-05-20T14:00:00Z",
                    "event_type": "research_candidate",
                    "symbol": "NVDA",
                    "rank": 1,
                    "score": 5.0,
                },
            },
            {
                "feed": "candidates",
                "payload_json": {
                    "event_time": "2026-05-20T14:01:00Z",
                    "event_type": "research_candidate",
                    "symbol": "AAPL",
                    "rank": 2,
                    "score": 8.0,
                },
            },
            {
                "feed": "candidates",
                "payload_json": {
                    "event_time": "2026-05-20T14:02:00Z",
                    "event_type": "research_candidate",
                    "symbol": "NVDA",
                    "rank": 1,
                    "score": 9.0,
                },
            },
        ]
        summary = {
            "event_count": 3,
            "signal_count": 1,
            "counts_by_feed": {
                "quotes": 1,
                "history": 1,
                "news": 1,
                "features": 1,
                "candidates": 3,
                "market_context": 1,
                "fundamentals": 1,
                "native_stream_signals": 1,
            },
        }

        report = _intelligence_report_payload(intelligence_id="intel_scorecard", watch_session_id="session_scorecard", rows=rows, summary=summary)

        self.assertEqual(report["scorecard"][0]["symbol"], "NVDA")
        self.assertEqual(report["scorecard"][0]["score"], 9.0)

    def test_intelligence_search_uses_hybrid_evidence_and_persists_query(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-search-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                self._seed_watch_session_rows("session_search")

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(
                        [
                            "intelligence",
                            "search",
                            "--intelligence-id",
                            "intel_search",
                            "--session-id",
                            "session_search",
                            "--query",
                            "AAPL fundamental unavailable risk",
                            "--top-k",
                            "3",
                            "--format",
                            "json",
                        ]
                    )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "intelligence-search")
                self.assertEqual(payload["search"]["retrieval"]["fusion"], "rrf")
                self.assertEqual(payload["search"]["retrieval"]["semantic"]["status"], "disabled")
                self.assertGreaterEqual(len(payload["search"]["hits"]), 1)
                self.assertIn(payload["search"]["hits"][0]["match_reason"], {"keyword_match", "structured_match", "recency_match"})

                with AgenticStore() as store:
                    rows = store.read_external_events("finance_intelligence_searches")
                    index_rows = store.read_external_events("finance_intelligence_evidence_indexes")
                self.assertEqual(rows[-1]["intelligence_id"], "intel_search")
                self.assertEqual(rows[-1]["query_text"], "AAPL fundamental unavailable risk")
                self.assertEqual(index_rows[-1]["index_status"], "ready")
                self.assertGreaterEqual(index_rows[-1]["doc_count"], 1)

    def test_evidence_index_module_builds_no_hash_v2_index(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-evidence-index-module-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                rows = [
                    {
                        "feed": "candidates",
                        "event_id": "candidate-aapl",
                        "event_time": "2026-05-20T14:00:00Z",
                        "payload_json": {
                            "watch_session_id": "session_module_index",
                            "event_time": "2026-05-20T14:00:00Z",
                            "event_type": "research_candidate",
                            "symbol": "AAPL",
                            "summary": "AAPL positive momentum evidence",
                            "score": 9.0,
                        },
                    }
                ]

                index = build_finance_evidence_index(
                    intelligence_id="intel_module_index",
                    watch_session_id="session_module_index",
                    rows=rows,
                    feed="all",
                )

                index_dir = pathlib.Path(index["index_path"])
                self.assertEqual(index["index_version"], FINANCE_EVIDENCE_INDEX_VERSION)
                self.assertEqual(index["semantic_status"], "disabled")
                self.assertTrue((index_dir / "metadata.json").exists())
                self.assertTrue((index_dir / "docs.jsonl").exists())
                self.assertTrue((index_dir / "keyword" / "manifest.json").exists())
                self.assertFalse((index_dir / "vectors.json").exists())
                self.assertNotIn("vectors_path", index)

    def test_evidence_index_module_rejects_stale_v1_index(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-evidence-index-stale-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index_dir = finance_evidence_index_dir(watch_session_id="session_stale", feed="all")
                index_dir.mkdir(parents=True)
                (index_dir / "metadata.json").write_text(
                    json.dumps({"index_version": 1, "fingerprint": "old"}) + "\n",
                    encoding="utf-8",
                )
                (index_dir / "docs.jsonl").write_text("", encoding="utf-8")
                (index_dir / "keyword").mkdir()

                loaded, reason = load_finance_evidence_index(
                    watch_session_id="session_stale",
                    feed="all",
                    expected_fingerprint="old",
                )

                self.assertIsNone(loaded)
                self.assertEqual(reason, "stale")

    def test_finance_evidence_retriever_contract_searches_without_semantic_provider(self):
        rows = [
            {
                "feed": "candidates",
                "event_id": "candidate-aapl",
                "event_time": "2026-05-20T14:00:00Z",
                "payload_json": {
                    "watch_session_id": "session_retriever",
                    "event_type": "research_candidate",
                    "symbol": "AAPL",
                    "summary": "AAPL momentum risk evidence",
                    "score": 9.0,
                },
            }
        ]
        retriever = FinanceEvidenceRetriever()
        result = retriever.search_rows(
            intelligence_id="intel_retriever",
            watch_session_id="session_retriever",
            rows=rows,
            query_text="AAPL momentum",
            options=EvidenceSearchOptions(feed="all", top_k=1, index_mode="off"),
        )

        self.assertEqual(result.retrieval["semantic"]["status"], "disabled")
        self.assertEqual(result.retrieval["retriever"], "finance_evidence_retriever")
        self.assertEqual(result.retrieval["retriever_version"], retriever.retriever_version)
        self.assertEqual(len(result.hits), 1)

    def test_intelligence_index_persists_reusable_hybrid_evidence_index(self):
        with tempfile.TemporaryDirectory(prefix="velaria-finance-intelligence-index-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                self._seed_watch_session_rows("session_index")

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(
                        [
                            "intelligence",
                            "index",
                            "--intelligence-id",
                            "intel_index",
                            "--session-id",
                            "session_index",
                            "--format",
                            "json",
                        ]
                    )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["action"], "intelligence-index")
                self.assertEqual(payload["index"]["status"], "ready")
                self.assertEqual(payload["index"]["semantic_status"], "disabled")
                self.assertGreaterEqual(payload["index"]["doc_count"], 1)
                self.assertTrue(os.path.exists(payload["index"]["metadata_path"]))
                self.assertTrue(os.path.exists(payload["index"]["docs_path"]))
                self.assertNotIn("vectors_path", payload["index"])

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = finance_cli_main(
                        [
                            "intelligence",
                            "search",
                            "--intelligence-id",
                            "intel_index",
                            "--session-id",
                            "session_index",
                            "--query",
                            "AAPL risk fundamentals",
                            "--top-k",
                            "2",
                            "--format",
                            "json",
                        ]
                    )

                self.assertEqual(exit_code, 0)
                search_payload = json.loads(stdout.getvalue())
                self.assertEqual(search_payload["search"]["retrieval"]["index_status"], "hit")
                self.assertEqual(search_payload["search"]["retrieval"]["index_path"], payload["index"]["index_path"])
                self.assertGreaterEqual(len(search_payload["search"]["hits"]), 1)

    def test_signal_policy_drives_native_stream_flags(self):
        args = mock.Mock()
        args.market = "us"
        args.entry_score_threshold = 99.0
        args.entry_return_threshold = 99.0
        args.exit_score_threshold = -99.0
        args.exit_quote_pct_threshold = -99.0
        args.signal_policy_preset = "balanced"
        args.signal_policy = json.dumps(
            {
                "entry": {"all": [{"field": "momentum_state", "op": "=", "value": "bullish"}]},
                "exit": {"any": [{"field": "quote_pct_change", "op": "<=", "value": -1.0}]},
            }
        )
        candidate = {
            "event_time": "2026-05-20T14:00:00Z",
            "market": "us",
            "symbol": "NVDA",
            "rank": 1,
            "score": 1.0,
            "period_return_pct": 0.0,
            "quote_pct_change": 0.1,
            "quote_freshness": "delayed",
            "news_sentiment_label": "neutral",
            "feature_snapshot": {"momentum_state": "bullish"},
        }

        row = _rank_native_stream_row(args, candidate)

        self.assertEqual(row["entry_signal"], 1)
        self.assertEqual(row["exit_signal"], 0)
        self.assertEqual(row["signal_policy"]["source"], "custom")
        self.assertIn('"field": "momentum_state"', row["signal_policy_json"])

        stream_row = _rank_native_stream_source_row(row)
        self.assertIn("signal_policy_json", stream_row)
        signal = _rank_native_signal_rows(stream_row)[0]
        self.assertEqual(signal["signal_policy_source"], "custom")
        self.assertIn('"field": "momentum_state"', signal["signal_policy_json"])

    def test_signal_policy_rejects_unknown_fields(self):
        args = mock.Mock()
        args.market = "us"
        args.entry_score_threshold = 8.0
        args.entry_return_threshold = 5.0
        args.exit_score_threshold = 0.0
        args.exit_quote_pct_threshold = -3.0
        args.signal_policy_preset = "balanced"
        args.signal_policy = json.dumps(
            {
                "entry": {"all": [{"field": "score_typo", "op": "<=", "value": 0}]},
                "exit": {"any": []},
            }
        )

        with self.assertRaises(FinanceProviderError) as raised:
            _rank_native_stream_row(args, {"market": "us", "symbol": "NVDA"})

        self.assertEqual(raised.exception.error_type, "invalid_signal_policy")

    def test_signal_policy_rejects_unsupported_operator(self):
        args = mock.Mock()
        args.market = "us"
        args.entry_score_threshold = 8.0
        args.entry_return_threshold = 5.0
        args.exit_score_threshold = 0.0
        args.exit_quote_pct_threshold = -3.0
        args.signal_policy_preset = "balanced"
        args.signal_policy = json.dumps(
            {
                "entry": {"all": [{"field": "momentum_state", "op": ">=", "value": "neutral"}]},
                "exit": {"any": []},
            }
        )

        with self.assertRaises(FinanceProviderError) as raised:
            _rank_native_stream_row(args, {"market": "us", "symbol": "NVDA"})

        self.assertEqual(raised.exception.error_type, "invalid_signal_policy")
        self.assertIn("allowed_ops", raised.exception.details)

    def test_signal_policy_rejects_non_numeric_numeric_field_values(self):
        args = mock.Mock()
        args.market = "us"
        args.entry_score_threshold = 8.0
        args.entry_return_threshold = 5.0
        args.exit_score_threshold = 0.0
        args.exit_quote_pct_threshold = -3.0
        args.signal_policy_preset = "balanced"
        args.signal_policy = json.dumps(
            {
                "entry": {"all": [{"field": "score", "op": "=", "value": "not-a-number"}]},
                "exit": {"any": []},
            }
        )

        with self.assertRaises(FinanceProviderError) as raised:
            _rank_native_stream_row(args, {"market": "us", "symbol": "NVDA"})

        self.assertEqual(raised.exception.error_type, "invalid_signal_policy")
        self.assertEqual(raised.exception.details["field"], "score")

    def test_normalize_akshare_cn_history_keeps_provider_metadata(self):
        raw = pd.DataFrame(
            [
                {
                    "日期": "2026-01-02",
                    "开盘": 10.0,
                    "收盘": 10.5,
                    "最高": 10.8,
                    "最低": 9.9,
                    "成交量": 123456,
                    "成交额": 987654.0,
                    "涨跌幅": 2.3,
                }
            ]
        )

        rows = normalize_history_frame(
            raw,
            market="cn",
            symbol="000001",
            provider="akshare",
            source_url="https://akshare.akfamily.xyz/data/stock/stock.html",
            freshness="eod",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market"], "cn")
        self.assertEqual(rows[0]["symbol"], "000001")
        self.assertEqual(rows[0]["date"], "2026-01-02")
        self.assertEqual(rows[0]["open"], 10.0)
        self.assertEqual(rows[0]["close"], 10.5)
        self.assertEqual(rows[0]["volume"], 123456)
        self.assertEqual(rows[0]["provider"], "akshare")
        self.assertEqual(rows[0]["freshness"], "eod")
        self.assertIn("license_note", rows[0])

    def test_normalize_akshare_quote_maps_cn_and_us_shapes(self):
        raw = pd.DataFrame(
            [
                {"代码": "000001", "名称": "平安银行", "最新价": 12.34, "成交量": 1000, "涨跌幅": 1.2},
                {"代码": "105.AAPL", "名称": "Apple", "最新价": 189.01, "成交量": 2000, "涨跌幅": -0.5},
            ]
        )

        rows = normalize_quote_frame(
            raw,
            market="us",
            symbols=["105.AAPL"],
            provider="akshare",
            source_url="https://akshare.akfamily.xyz/data/stock/stock.html",
            freshness="delayed",
            delay_sec=None,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "105.AAPL")
        self.assertEqual(rows[0]["market"], "us")
        self.assertEqual(rows[0]["event_type"], "quote")
        self.assertEqual(rows[0]["price"], 189.01)
        self.assertEqual(rows[0]["volume"], 2000)
        self.assertEqual(rows[0]["freshness"], "delayed")
        self.assertIsNone(rows[0]["delay_sec"])

    def test_ingest_quotes_cli_writes_external_event_source(self):
        quote_rows = [
            {
                "event_time": "2026-01-02T00:00:00Z",
                "event_type": "quote",
                "source_key": "000001",
                "symbol": "000001",
                "market": "cn",
                "price": 12.34,
                "volume": 1000,
                "provider": "akshare",
                "freshness": "realtime",
                "delay_sec": 0,
                "fetched_at": "2026-01-02T00:00:00Z",
                "source_url": "https://akshare.akfamily.xyz/data/stock/stock.html",
                "license_note": "public provider metadata",
            }
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-finance-pack-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "ingest-quotes",
                                "--provider",
                                "akshare",
                                "--market",
                                "cn",
                                "--symbols",
                                "000001",
                                "--source-id",
                                "cn_quotes",
                            ]
                        )

                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["source"]["source_id"], "cn_quotes")
                self.assertEqual(len(payload["observations"]), 1)

                with AgenticStore() as store:
                    source = store.get_source("cn_quotes")
                    self.assertIsNotNone(source)
                    rows = store.read_external_events("cn_quotes")
                self.assertEqual(rows[0]["source_key"], "000001")
                self.assertEqual(rows[0]["price"], 12.34)

    def test_top_level_finance_cli_is_available(self):
        quote_rows = [
            {
                "event_time": "2026-01-02T00:00:00Z",
                "event_type": "quote",
                "source_key": "000001",
                "symbol": "000001",
                "market": "cn",
                "price": 12.34,
                "volume": 1000,
                "provider": "tencent",
                "freshness": "realtime",
                "delay_sec": 0,
                "fetched_at": "2026-01-02T00:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
            stdout = StringIO()
            with redirect_stdout(stdout):
                exit_code = velaria_cli_main(
                    [
                        "finance",
                        "fetch-quotes",
                        "--provider",
                        "tencent",
                        "--market",
                        "cn",
                        "--symbols",
                        "000001",
                    ]
                )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "fetch-quotes")
        self.assertEqual(payload["provider"], "tencent")
        self.assertEqual(payload["row_count"], 1)

    def test_top_level_finance_analyze_forwards_to_productized_workflow(self):
        quote_rows = [
            {
                "event_time": "2026-01-02T00:00:00Z",
                "event_type": "quote",
                "source_key": "000001",
                "symbol": "000001",
                "market": "cn",
                "price": 12.34,
                "volume": 1000,
                "pct_change": 1.2,
                "provider": "tencent",
                "freshness": "realtime",
                "delay_sec": 0,
                "fetched_at": "2026-01-02T00:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-finance-top-analyze-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli_main(
                            [
                                "finance",
                                "analyze",
                                "--market",
                                "cn",
                                "--symbol",
                                "000001",
                                "--format",
                                "json",
                            ]
                        )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "analyze")
        self.assertEqual(payload["provider"], "tencent")
        self.assertEqual(payload["quote"]["symbol"], "000001")

    def test_watch_cli_ingests_runs_monitor_and_returns_analysis(self):
        quote_rows = [
            {
                "event_time": "2026-01-02T00:00:00Z",
                "event_type": "quote",
                "source_key": "000001",
                "symbol": "000001",
                "market": "cn",
                "price": 12.34,
                "volume": 1000,
                "pct_change": 1.2,
                "provider": "tencent",
                "freshness": "realtime",
                "delay_sec": 0,
                "fetched_at": "2026-01-02T00:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-finance-watch-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "watch",
                                "--provider",
                                "tencent",
                                "--market",
                                "cn",
                                "--symbol",
                                "000001",
                                "--iterations",
                                "1",
                                "--interval-sec",
                                "0",
                            ]
                        )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "watch")
        self.assertEqual(payload["tick_count"], 1)
        tick = payload["ticks"][0]
        self.assertEqual(tick["quote"]["symbol"], "000001")
        self.assertEqual(len(tick["observations"]), 1)
        self.assertGreaterEqual(len(tick["focus_events"]), 1)
        self.assertEqual(tick["focus_events"][0]["title"], "cn:000001 quote observed")
        self.assertIn("latest price=12.34", tick["analysis"]["summary"])
        self.assertIn("实时联网研究", tick["analysis_prompt"])

    def test_sources_cli_lists_user_ready_public_providers(self):
        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = finance_cli_main(["sources", "--format", "json"])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "sources")
        provider_ids = {item["provider"] for item in payload["sources"]}
        self.assertIn("tencent", provider_ids)
        self.assertIn("akshare", provider_ids)
        self.assertIn("yahoo", provider_ids)
        tencent = next(item for item in payload["sources"] if item["provider"] == "tencent")
        self.assertIn("fetch-quotes", tencent["commands"])
        self.assertEqual(tencent["recommended_quote_provider"], True)
        self.assertIn("cn", tencent["markets"])
        yahoo = next(item for item in payload["sources"] if item["provider"] == "yahoo")
        self.assertIn("pipeline", yahoo["commands"])
        self.assertEqual(yahoo["recommended_history_provider"], True)

    def test_parse_yahoo_chart_payload_maps_history_rows(self):
        payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1735776000, 1735862400],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [10.0, 10.5],
                                    "high": [11.0, 10.8],
                                    "low": [9.8, 10.1],
                                    "close": [10.5, 10.2],
                                    "volume": [1000, 2000],
                                }
                            ]
                        },
                    }
                ],
                "error": None,
            }
        }

        rows = parse_yahoo_chart_payload(
            payload,
            market="cn",
            symbol="000001",
            yahoo_symbol="000001.SZ",
            fetched_at="2026-05-18T00:00:00Z",
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["provider"], "yahoo")
        self.assertEqual(rows[0]["market"], "cn")
        self.assertEqual(rows[0]["symbol"], "000001")
        self.assertEqual(rows[0]["provider_symbol"], "000001.SZ")
        self.assertEqual(rows[0]["date"], "2025-01-02")
        self.assertEqual(rows[0]["close"], 10.5)
        self.assertEqual(rows[0]["volume"], 1000)
        self.assertEqual(rows[0]["freshness"], "eod")

    def test_pipeline_cli_fetches_history_subscribes_and_analyzes(self):
        history_rows = [
            {
                "market": "cn",
                "symbol": "000001",
                "provider_symbol": "000001.SZ",
                "date": "2025-01-02",
                "open": 10.0,
                "high": 11.0,
                "low": 9.8,
                "close": 10.5,
                "volume": 1000,
                "amount": None,
                "pct_change": None,
                "provider": "yahoo",
                "source_url": "https://query1.finance.yahoo.com/v8/finance/chart/",
                "fetched_at": "2026-05-18T00:00:00Z",
                "freshness": "eod",
                "delay_sec": None,
                "license_note": "public provider metadata",
            }
        ]
        quote_rows = [
            {
                "event_time": "2026-01-02T00:00:00Z",
                "event_type": "quote",
                "source_key": "000001",
                "symbol": "000001",
                "market": "cn",
                "name": "平安银行",
                "price": 12.34,
                "volume": 1000,
                "pct_change": 1.2,
                "provider": "tencent",
                "freshness": "realtime",
                "delay_sec": 0,
                "fetched_at": "2026-01-02T00:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-finance-pipeline-") as tmp:
            history_output = os.path.join(tmp, "history.parquet")
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_history", return_value=history_rows):
                    with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                        stdout = StringIO()
                        with redirect_stdout(stdout):
                            exit_code = finance_cli_main(
                                [
                                    "pipeline",
                                    "--market",
                                    "cn",
                                    "--symbol",
                                    "000001",
                                    "--start-date",
                                    "20250101",
                                    "--end-date",
                                    "20250131",
                                    "--history-output",
                                    history_output,
                                    "--iterations",
                                    "1",
                                    "--interval-sec",
                                    "0",
                                    "--format",
                                    "json",
                                ]
                            )
                        self.assertTrue(os.path.exists(history_output))

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "pipeline")
        self.assertEqual(payload["history"]["row_count"], 1)
        self.assertEqual(payload["history"]["provider"], "yahoo")
        self.assertEqual(payload["subscription"]["tick_count"], 1)
        self.assertEqual(payload["quote"]["symbol"], "000001")
        self.assertGreaterEqual(len(payload["focus_events"]), 1)
        self.assertIn("service_integration", payload)
        self.assertIn("external-events", payload["service_integration"]["generic_routes"][0])

    def test_doctor_cli_reports_dependency_and_provider_probe(self):
        quote_rows = [
            {
                "event_time": "2026-01-02T00:00:00Z",
                "event_type": "quote",
                "source_key": "000001",
                "symbol": "000001",
                "market": "cn",
                "price": 12.34,
                "volume": 1000,
                "provider": "tencent",
                "freshness": "realtime",
                "delay_sec": 0,
                "fetched_at": "2026-01-02T00:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
            stdout = StringIO()
            with redirect_stdout(stdout):
                exit_code = finance_cli_main(["doctor", "--format", "json"])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "doctor")
        checks = {item["name"]: item for item in payload["checks"]}
        self.assertIn(checks["sec_companyfacts_policy"]["status"], {"ok", "warning"})
        self.assertIn("VELARIA_SEC_USER_AGENT", checks["sec_companyfacts_policy"]["hint"])
        self.assertEqual(checks["tencent_quote_probe"]["status"], "ok")
        self.assertIn("finance analyze --market cn --symbol 000001", payload["next_steps"])

    def test_analyze_cli_returns_human_readable_report_by_default(self):
        quote_rows = [
            {
                "event_time": "2026-01-02T00:00:00Z",
                "event_type": "quote",
                "source_key": "000001",
                "symbol": "000001",
                "market": "cn",
                "name": "平安银行",
                "price": 12.34,
                "volume": 1000,
                "pct_change": 1.2,
                "provider": "tencent",
                "freshness": "realtime",
                "delay_sec": 0,
                "fetched_at": "2026-01-02T00:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-finance-analyze-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "analyze",
                                "--market",
                                "cn",
                                "--symbol",
                                "000001",
                            ]
                        )

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue()
        self.assertIn("Velaria 金融分析", output)
        self.assertIn("cn:000001", output)
        self.assertIn("平安银行", output)
        self.assertIn("12.34", output)
        self.assertIn("tencent", output)
        self.assertIn("不是投资建议", output)

    def test_analyze_cli_supports_json_for_agent_automation(self):
        quote_rows = [
            {
                "event_time": "2026-01-02T00:00:00Z",
                "event_type": "quote",
                "source_key": "000001",
                "symbol": "000001",
                "market": "cn",
                "price": 12.34,
                "volume": 1000,
                "pct_change": 1.2,
                "provider": "tencent",
                "freshness": "realtime",
                "delay_sec": 0,
                "fetched_at": "2026-01-02T00:00:00Z",
                "source_url": "https://qt.gtimg.cn/q=",
                "license_note": "public provider metadata",
            }
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-finance-analyze-json-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch("velaria.finance_pack.cli.fetch_quotes", return_value=quote_rows):
                    stdout = StringIO()
                    with redirect_stdout(stdout):
                        exit_code = finance_cli_main(
                            [
                                "analyze",
                                "--market",
                                "cn",
                                "--symbol",
                                "000001",
                                "--format",
                                "json",
                            ]
                        )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "analyze")
        self.assertEqual(payload["quote"]["symbol"], "000001")
        self.assertIn("analysis_prompt", payload)

    def test_top_level_finance_help_guides_agent_cli_run_usage(self):
        stdout = StringIO()
        with redirect_stdout(stdout):
            exit_code = velaria_cli_main(["finance", "--help"])

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue()
        self.assertIn("Agent mode", output)
        self.assertIn("velaria_cli_run", output)
        self.assertIn("finance doctor", output)
        self.assertIn("finance sources", output)
        self.assertIn("finance analyze --market cn --symbol 000001", output)
        self.assertIn("finance pipeline --market cn --symbol 000001", output)
        self.assertIn("finance watch --provider tencent --market cn --symbol 000001", output)
        self.assertIn("finance fetch-quotes --provider tencent --market cn --symbols 000001", output)
        self.assertIn("freshness", output)
        self.assertIn("not investment advice", output)

    def test_research_prompt_requires_live_sources_and_not_advice(self):
        prompt = build_research_prompt(
            focus_events=[
                {
                    "event_id": "event_1",
                    "title": "price threshold reached",
                    "summary": "price >= 12",
                    "key_fields": {"symbol": "000001", "price": 12.34},
                    "artifact_ids": ["artifact_history"],
                }
            ],
            datasets=[
                {
                    "dataset_id": "artifact:artifact_history",
                    "path_or_uri": "/tmp/history.parquet",
                    "schema": ["symbol", "date", "close"],
                }
            ],
        )

        self.assertIn("event_1", prompt)
        self.assertIn("artifact_history", prompt)
        self.assertIn("实时联网研究", prompt)
        self.assertIn("来源链接", prompt)
        self.assertIn("不是投资建议", prompt)

    def test_parse_tencent_cn_quote_payload(self):
        payload = 'v_s_sz000001="51~平安银行~000001~10.99~-0.06~-0.54~974742~107465~~2132.71~GP-A~";'

        rows = parse_tencent_quote_payload(
            payload,
            market="cn",
            symbols=["000001"],
            fetched_at="2026-05-15T07:00:00Z",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["provider"], "tencent")
        self.assertEqual(rows[0]["market"], "cn")
        self.assertEqual(rows[0]["symbol"], "000001")
        self.assertEqual(rows[0]["name"], "平安银行")
        self.assertEqual(rows[0]["price"], 10.99)
        self.assertEqual(rows[0]["pct_change"], -0.54)

    def test_parse_tencent_cn_quote_payload_accepts_prefixed_index_symbol(self):
        payload = 'v_s_sh000001="51~上证指数~000001~3880.12~4.20~0.11~1000~2000~~0~GP-A~";'

        rows = parse_tencent_quote_payload(
            payload,
            market="cn",
            symbols=["sh000001"],
            fetched_at="2026-05-15T07:00:00Z",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "000001")
        self.assertEqual(rows[0]["name"], "上证指数")

    def test_parse_tencent_us_quote_accepts_akshare_prefixed_symbol(self):
        payload = 'v_usAAPL="51~Apple~AAPL.OQ~300.23~299.00~300.00~1000~~~~~~~~~~~~~~~~~~~~~~~~~~0.41~301.00~298.00~~123456~37000000~";'

        rows = parse_tencent_quote_payload(
            payload,
            market="us",
            symbols=["105.AAPL"],
            fetched_at="2026-05-15T07:00:00Z",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["provider"], "tencent")
        self.assertEqual(rows[0]["market"], "us")
        self.assertEqual(rows[0]["symbol"], "AAPL")
        self.assertEqual(rows[0]["price"], 300.23)
