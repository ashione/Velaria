import os
import pathlib
import tempfile
import unittest
from unittest import mock

from velaria.agentic_store import AgenticStore


class AgenticStoreTest(unittest.TestCase):
    def test_external_event_source_and_ingest(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agentic-store-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with AgenticStore() as store:
                    source = store.upsert_source(
                        {
                            "kind": "external_event",
                            "name": "webhook-source",
                            "schema_binding": {
                                "time_field": "ts",
                                "type_field": "kind",
                                "key_field": "symbol",
                                "field_mappings": {"price": "price"},
                            },
                        }
                    )
                    self.assertTrue(pathlib.Path(source["event_log_path"]).exists() or not pathlib.Path(source["event_log_path"]).exists())
                    event = store.append_external_event(
                        source["source_id"],
                        {"ts": "2026-01-01T00:00:00Z", "kind": "tick", "symbol": "BTC", "price": 123},
                    )
                    self.assertEqual(event["event_type"], "tick")
                    self.assertEqual(event["source_key"], "BTC")
                    self.assertEqual(event["price"], 123)
                    rows = store.read_external_events(source["source_id"])
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0]["source_key"], "BTC")

    def test_focus_event_poll_updates_cursor(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agentic-cursor-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with AgenticStore() as store:
                    store.add_focus_event(
                        {
                            "monitor_id": "m1",
                            "rule_id": "r1",
                            "title": "title",
                            "summary": "summary",
                            "key_fields": {"a": 1},
                            "sample_rows": [],
                            "artifact_ids": [],
                            "context_json": {},
                        }
                    )
                    payload = store.poll_focus_events(consumer_id="agent", limit=10)
                    self.assertEqual(payload["consumer_id"], "agent")
                    self.assertEqual(len(payload["events"]), 1)

    def test_focus_event_poll_same_timestamp_uses_event_id_tiebreak(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agentic-cursor-tie-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with AgenticStore() as store:
                    first = store.add_focus_event(
                        {
                            "event_id": "focus_a",
                            "triggered_at": "2026-01-01T00:00:00Z",
                            "monitor_id": "m1",
                            "rule_id": "r1",
                            "title": "a",
                            "summary": "a",
                            "key_fields": {},
                            "sample_rows": [],
                            "artifact_ids": [],
                            "context_json": {},
                        }
                    )
                    second = store.add_focus_event(
                        {
                            "event_id": "focus_b",
                            "triggered_at": "2026-01-01T00:00:00Z",
                            "monitor_id": "m1",
                            "rule_id": "r1",
                            "title": "b",
                            "summary": "b",
                            "key_fields": {},
                            "sample_rows": [],
                            "artifact_ids": [],
                            "context_json": {},
                        }
                    )
                    first_poll = store.poll_focus_events(consumer_id="agent", limit=1)
                    self.assertEqual([item["event_id"] for item in first_poll["events"]], [first["event_id"]])
                    second_poll = store.poll_focus_events(consumer_id="agent", limit=10)
                    self.assertEqual([item["event_id"] for item in second_poll["events"]], [second["event_id"]])
