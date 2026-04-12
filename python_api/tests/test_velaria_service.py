import importlib
import json
import os
import pathlib
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest import mock
from urllib import error as urllib_error
from urllib import request as urllib_request

try:
    velaria_service = importlib.import_module("velaria_service")
except ModuleNotFoundError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    velaria_service = importlib.import_module("velaria_service")

from velaria.workspace import ArtifactIndex


class StaticEmbeddingProvider:
    provider_name = "static"

    def __init__(self, mapping):
        self._mapping = mapping

    def embed(self, texts, *, model, batch_size=None):
        del model, batch_size
        return [list(self._mapping[text]) for text in texts]


class VelariaServiceTest(unittest.TestCase):
    def _start_server(self):
        service = velaria_service.VelariaService(host="127.0.0.1", port=0)
        server = ThreadingHTTPServer(("127.0.0.1", 0), service.build_handler())
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

    def test_versioned_hybrid_search_creates_run_and_delete_removes_run_dir(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue": [0.0, 1.0, 0.0],
                "payment timeout": [1.0, 0.0, 0.0],
            }
        )
        with tempfile.TemporaryDirectory(prefix="velaria-service-hybrid-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "doc_id,title,summary,source_updated_at\n"
                "doc-1,Alpha,Payment page timeout,1\n"
                "doc-2,Beta,Refund delay in worker queue,2\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    with mock.patch.object(
                        velaria_service.cli_impl,
                        "_make_embedding_provider",
                        return_value=(provider, "static-demo"),
                    ):
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/hybrid-search",
                            {
                                "input_path": str(csv_path),
                                "input_type": "csv",
                                "query_text": "payment timeout",
                                "text_columns": ["title", "summary"],
                                "provider": "hash",
                                "top_k": 1,
                            },
                        )
                    self.assertEqual(status, 200)
                    self.assertTrue(payload["ok"])
                    self.assertEqual(payload["run"]["status"], "succeeded")
                    self.assertEqual(payload["result"]["model"], "static-demo")
                    self.assertEqual(payload["result"]["text_columns"], ["title", "summary"])
                    self.assertEqual(payload["preview"]["row_count"], 1)
                    self.assertEqual(payload["preview"]["rows"][0]["doc_id"], "doc-1")
                    self.assertEqual(payload["artifact"]["format"], "parquet")
                    self.assertEqual(len(payload["artifacts"]), 1)

                    run_dir = pathlib.Path(payload["run_dir"])
                    self.assertTrue(run_dir.exists())

                    delete_status, delete_payload = self._request_json(
                        "DELETE",
                        f"{base_url}/api/v1/runs/{payload['run_id']}",
                    )
                    self.assertEqual(delete_status, 200)
                    self.assertTrue(delete_payload["ok"])
                    self.assertEqual(delete_payload["run_id"], payload["run_id"])
                    self.assertFalse(run_dir.exists())

                    index = ArtifactIndex()
                    self.assertIsNone(index.get_run(payload["run_id"]))
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_delete_run_http_rejects_non_terminal_run_with_conflict(self):
        with tempfile.TemporaryDirectory(prefix="velaria-service-delete-running-") as tmp:
            run_dir = pathlib.Path(tmp) / "runs" / "run-running"
            run_dir.mkdir(parents=True)
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = ArtifactIndex()
                index.upsert_run(
                    {
                        "run_id": "run-running",
                        "created_at": "2026-04-01T10:00:00Z",
                        "finished_at": None,
                        "status": "running",
                        "action": "hybrid-search",
                        "cli_args": {"query_text": "alpha"},
                        "velaria_version": "0.0.test",
                        "run_dir": str(run_dir),
                    }
                )
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "DELETE",
                        f"{base_url}/api/v1/runs/run-running",
                    )
                    self.assertEqual(status, 409)
                    self.assertFalse(payload["ok"])
                    self.assertIn("cannot delete non-terminal run", payload["error"])
                    self.assertIsNotNone(index.get_run("run-running"))
                    self.assertTrue(run_dir.exists())
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()


if __name__ == "__main__":
    unittest.main()
