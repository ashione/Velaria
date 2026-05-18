import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import pandas as pd

from velaria.finance_pack import (
    build_research_prompt,
    normalize_history_frame,
    normalize_quote_frame,
    parse_tencent_quote_payload,
)
from velaria.finance_pack.cli import main as finance_cli_main
from velaria.agentic_store import AgenticStore


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
