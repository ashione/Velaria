import importlib
import json
import os
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from urllib import error as urllib_error
from urllib import request as urllib_request

try:
    velaria_service = importlib.import_module("velaria_service")
except ModuleNotFoundError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    velaria_service = importlib.import_module("velaria_service")


class AgenticServiceTest(unittest.TestCase):
    def _start_server(self):
        service = velaria_service.VelariaService(host="127.0.0.1", port=0)
        server = velaria_service.ThreadingHTTPServer(("127.0.0.1", 0), service.build_handler())
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, thread, f"http://127.0.0.1:{server.server_port}"

    def _request_json(self, method, url, payload=None):
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib_request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib_request.urlopen(req) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib_error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_search_and_grounding_endpoints(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agentic-service-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/search/templates",
                        {"query_text": "count events in a window", "top_k": 3},
                    )
                    self.assertEqual(status, 200)
                    self.assertTrue(payload["hits"])
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/grounding",
                        {"query_text": "detect bursts", "top_k": 3},
                    )
                    self.assertEqual(status, 200)
                    self.assertIn("bundle_id", payload)
                    self.assertIn("template_hits", payload["payload"])
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_source_create_ingest_and_monitor_routes(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agentic-service-monitor-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/external-events/sources",
                        {
                            "name": "ticks",
                            "schema_binding": {
                                "time_field": "ts",
                                "type_field": "kind",
                                "key_field": "symbol",
                                "field_mappings": {"price": "price"},
                            },
                        },
                    )
                    self.assertEqual(status, 201)
                    source_id = payload["source"]["source_id"]
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/external-events/sources/{source_id}/ingest",
                        {"ts": "2026-01-01T00:00:00Z", "kind": "tick", "symbol": "BTC", "price": 123},
                    )
                    self.assertEqual(status, 202)
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/monitors",
                        {
                            "name": "btc bursts",
                            "intent_text": "watch bursts",
                            "template_id": "window_count",
                            "template_params": {"group_by": ["source_key", "event_type"], "count_threshold": 1},
                            "source": {"kind": "external_event", "source_id": source_id},
                            "execution_mode": "stream",
                        },
                    )
                    self.assertEqual(status, 201)
                    monitor_id = payload["monitor"]["monitor_id"]
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/monitors/{monitor_id}/validate", {})
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["monitor"]["validation"]["status"], "valid")
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_monitor_from_intent_run_and_focus_event_lifecycle(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agentic-service-e2e-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/external-events/sources",
                        {
                            "source_id": "ticks",
                            "name": "ticks",
                            "schema_binding": {
                                "time_field": "ts",
                                "type_field": "kind",
                                "key_field": "symbol",
                                "field_mappings": {"price": "price"},
                            },
                        },
                    )
                    self.assertEqual(status, 201)
                    for idx in range(2):
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/external-events/sources/ticks/ingest",
                            {"ts": f"2026-01-01T00:00:0{idx}Z", "kind": "tick", "symbol": "BTC", "price": 100 + idx},
                        )
                        self.assertEqual(status, 202)
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/monitors/from-intent",
                        {
                            "intent_text": "count events in a window",
                            "source": {"kind": "external_event", "source_id": "ticks"},
                            "template_params": {"group_by": ["source_key", "event_type"], "count_threshold": 1},
                            "execution_mode": "stream",
                        },
                    )
                    self.assertEqual(status, 201)
                    monitor_id = payload["monitor"]["monitor_id"]
                    self.assertIn("grounding_bundle", payload)
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/monitors/{monitor_id}/validate", {})
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["monitor"]["validation"]["status"], "valid")
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/monitors/{monitor_id}/enable", {})
                    self.assertEqual(status, 200)
                    self.assertTrue(payload["monitor"]["enabled"])
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/monitors/{monitor_id}/run", {})
                    self.assertEqual(status, 200)
                    self.assertGreaterEqual(len(payload["focus_events"]), 1)
                    event_id = payload["focus_events"][0]["event_id"]
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/focus-events/poll",
                        {"consumer_id": "agent", "limit": 10},
                    )
                    self.assertEqual(status, 200)
                    self.assertEqual(len(payload["events"]), 1)
                    self.assertEqual(payload["events"][0]["event_id"], event_id)
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/focus-events/{event_id}/consume", {})
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["focus_event"]["status"], "consumed")
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/focus-events/{event_id}/archive", {})
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["focus_event"]["status"], "archived")
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_realtime_stream_monitor_pipeline_without_manual_run(self):
        with tempfile.TemporaryDirectory(prefix="velaria-agentic-service-realtime-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/external-events/sources",
                        {
                            "source_id": "ticks",
                            "name": "ticks",
                            "schema_binding": {
                                "time_field": "ts",
                                "type_field": "kind",
                                "key_field": "symbol",
                                "field_mappings": {"price": "price"},
                            },
                        },
                    )
                    self.assertEqual(status, 201)
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/monitors/from-intent",
                        {
                            "intent_text": "count events in a window",
                            "source": {"kind": "external_event", "source_id": "ticks"},
                            "template_params": {"group_by": ["source_key", "event_type"], "count_threshold": 2},
                            "execution_mode": "stream",
                        },
                    )
                    self.assertEqual(status, 201)
                    monitor_id = payload["monitor"]["monitor_id"]
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/monitors/{monitor_id}/validate", {})
                    self.assertEqual(status, 200)
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/monitors/{monitor_id}/enable", {})
                    self.assertEqual(status, 200)

                    for idx in range(2):
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/external-events/sources/ticks/ingest",
                            {"ts": f"2026-01-01T00:00:0{idx}Z", "kind": "tick", "symbol": "BTC", "price": 100 + idx},
                        )
                        self.assertEqual(status, 202)

                    polled = None
                    for _ in range(50):
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/focus-events/poll",
                            {"consumer_id": "realtime-agent", "limit": 10},
                        )
                        self.assertEqual(status, 200)
                        if payload["events"]:
                            polled = payload
                            break
                        time.sleep(0.1)

                    self.assertIsNotNone(polled)
                    self.assertGreaterEqual(len(polled["events"]), 1)
                    event_id = polled["events"][0]["event_id"]
                    status, payload = self._request_json("GET", f"{base_url}/api/v1/focus-events/{event_id}")
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["focus_event"]["status"], "new")
                    self.assertEqual(payload["focus_event"]["key_fields"]["source_key"], "BTC")
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/focus-events/{event_id}/consume", {})
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["focus_event"]["status"], "consumed")
                    status, payload = self._request_json("POST", f"{base_url}/api/v1/monitors/{monitor_id}/disable", {})
                    self.assertEqual(status, 200)
                    self.assertFalse(payload["monitor"]["enabled"])
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()
