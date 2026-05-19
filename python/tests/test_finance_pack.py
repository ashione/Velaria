import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import pandas as pd

from velaria.agentic_store import AgenticStore
from velaria.cli import main as velaria_cli_main
from velaria.finance_pack import (
    build_research_prompt,
    evaluate_news_sentiment,
    fetch_quotes,
    normalize_history_frame,
    normalize_provider,
    normalize_quote_frame,
    parse_google_news_rss,
    parse_tencent_quote_payload,
    parse_yahoo_chart_payload,
    provider_catalog,
    provider_names_for_operation,
)
from velaria.finance_pack.cli import main as finance_cli_main


class FinancePackTest(unittest.TestCase):
    def test_provider_registry_exposes_capabilities_and_catalog(self):
        self.assertEqual(provider_names_for_operation("fetch_history"), ["akshare", "yahoo"])
        self.assertEqual(provider_names_for_operation("fetch_news"), ["google-news"])
        self.assertEqual(provider_names_for_operation("fetch_quotes"), ["akshare", "tencent"])

        catalog = provider_catalog()
        providers = {item["provider"]: item for item in catalog}
        self.assertEqual(set(providers), {"akshare", "google-news", "tencent", "yahoo"})
        self.assertIn("fetch-history", providers["yahoo"]["commands"])
        self.assertNotIn("fetch-quotes", providers["yahoo"]["commands"])
        self.assertIn("fetch-news", providers["google-news"]["commands"])
        self.assertIn("fetch-quotes", providers["tencent"]["commands"])
        self.assertNotIn("fetch-history", providers["tencent"]["commands"])
        self.assertEqual(providers["tencent"]["freshness"]["us"], "delayed")

    def test_provider_registry_drives_normalization_and_operation_errors(self):
        self.assertEqual(normalize_provider(" Yahoo "), "yahoo")
        with self.assertRaisesRegex(Exception, "provider does not support quotes") as ctx:
            fetch_quotes(provider="yahoo", market="us", symbols=["AAPL"])

        error = ctx.exception
        self.assertEqual(error.error_type, "unsupported_provider_operation")
        self.assertEqual(error.details["operation"], "fetch_quotes")
        self.assertEqual(error.details["candidates"], ["akshare", "tencent"])

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
                self.assertEqual(set(payload["raw_sources"]), {"quotes", "history", "news", "candidates", "native_stream_signals"})
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
