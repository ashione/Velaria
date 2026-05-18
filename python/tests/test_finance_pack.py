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
    normalize_history_frame,
    normalize_quote_frame,
    parse_tencent_quote_payload,
)
from velaria.finance_pack.cli import main as finance_cli_main


class FinancePackTest(unittest.TestCase):
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
        tencent = next(item for item in payload["sources"] if item["provider"] == "tencent")
        self.assertIn("fetch-quotes", tencent["commands"])
        self.assertEqual(tencent["recommended_quote_provider"], True)
        self.assertIn("cn", tencent["markets"])

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
