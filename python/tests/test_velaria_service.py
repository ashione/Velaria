import importlib
import json
import os
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from unittest import mock
from urllib import error as urllib_error
from urllib import request as urllib_request

import pyarrow as pa
import pyarrow.parquet as pq

try:
    velaria_service = importlib.import_module("velaria_service")
except ModuleNotFoundError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    velaria_service = importlib.import_module("velaria_service")

from velaria.workspace import ArtifactIndex
from velaria import DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL


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

    def _build_embedding_dataset(self, base_url, *, csv_path: pathlib.Path, provider: str | None = None):
        payload = {
            "input_path": str(csv_path),
            "input_type": "csv",
            "text_columns": ["title", "summary"],
            "top_k": 1,
        }
        if provider is not None:
            payload["provider"] = provider
        return self._request_json(
            "POST",
            f"{base_url}/api/v1/runs/embedding-build",
            payload,
        )

    def test_ai_config_reads_explicit_runtime_paths(self):
        with tempfile.TemporaryDirectory(prefix="velaria-ai-config-") as tmp:
            home = pathlib.Path(tmp)
            config_dir = home / ".velaria"
            config_dir.mkdir()
            (config_dir / "config.json").write_text(
                json.dumps(
                    {
                        "aiRuntime": "codex",
                        "aiRuntimePath": "/opt/velaria/runtime/bin/codex",
                        "aiClaudeRuntimePath": "/opt/velaria/runtime/bin/claude",
                        "aiCodexRuntimePath": "/opt/velaria/runtime/bin/codex-app",
                        "aiRuntimeWorkspace": "/var/lib/velaria/ai-runtime",
                        "aiReuseLocalConfig": False,
                        "aiRuntimeConfigPath": "/etc/velaria/codex-config.json",
                        "aiCodexNetworkAccess": False,
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"HOME": str(home)}):
                config = velaria_service.get_ai_config()
        self.assertEqual(config["runtime"], "codex")
        self.assertEqual(config["runtime_path"], "/opt/velaria/runtime/bin/codex")
        self.assertEqual(config["claude_runtime_path"], "/opt/velaria/runtime/bin/claude")
        self.assertEqual(config["codex_runtime_path"], "/opt/velaria/runtime/bin/codex-app")
        self.assertEqual(config["runtime_workspace"], "/var/lib/velaria/ai-runtime")
        self.assertFalse(config["reuse_local_config"])
        self.assertEqual(config["runtime_config_path"], "/etc/velaria/codex-config.json")
        self.assertFalse(config["network_access"])

    def test_codex_runtime_defaults_to_local_codex_command(self):
        from velaria.ai_runtime import create_runtime

        runtime = create_runtime({"runtime": "codex"})
        try:
            self.assertEqual(runtime.model, "gpt-5.4-mini")
            self.assertEqual(runtime.status()["model"], "gpt-5.4-mini")
        finally:
            runtime.shutdown()

    def test_resolve_auto_input_payload_prefers_excel_suffix(self):
        session = mock.Mock()
        resolved_type, resolved = velaria_service._resolve_auto_input_payload(
            session,
            {
                "input_path": "/tmp/employee_import.xlsx",
                "input_type": "auto",
            },
        )
        self.assertEqual(resolved_type, "excel")
        self.assertEqual(resolved["input_path"], "/tmp/employee_import.xlsx")
        session.probe.assert_not_called()

    def test_resolve_auto_input_payload_uses_probe_details_for_json_line_and_csv(self):
        session = mock.Mock()

        session.probe.return_value = {
            "kind": "json",
            "columns": ["user_id", "name"],
            "format": "json_lines",
        }
        resolved_type, resolved = velaria_service._resolve_auto_input_payload(
            session,
            {
                "input_path": "/tmp/events",
                "input_type": "auto",
            },
        )
        self.assertEqual(resolved_type, "json")
        self.assertEqual(resolved["columns"], "user_id,name")
        self.assertEqual(resolved["json_format"], "json_lines")

        session.probe.return_value = {
            "kind": "line",
            "mode": "split",
            "delimiter": "|",
            "mappings": [
                {"column": "c0", "source_index": 0},
                {"column": "c1", "source_index": 1},
            ],
        }
        resolved_type, resolved = velaria_service._resolve_auto_input_payload(
            session,
            {
                "input_path": "/tmp/events.log",
                "input_type": "auto",
            },
        )
        self.assertEqual(resolved_type, "line")
        self.assertEqual(resolved["line_mode"], "split")
        self.assertEqual(resolved["delimiter"], "|")
        self.assertEqual(
            resolved["mappings"],
            [
                {"name": "c0", "index": 0},
                {"name": "c1", "index": 1},
            ],
        )

        session.probe.return_value = {
            "kind": "csv",
            "delimiter": ",",
        }
        resolved_type, resolved = velaria_service._resolve_auto_input_payload(
            session,
            {
                "input_path": "/tmp/input.data",
                "input_type": "auto",
            },
        )
        self.assertEqual(resolved_type, "csv")
        self.assertEqual(resolved["delimiter"], ",")

    def test_resolve_auto_input_payload_preserves_explicit_fields_and_safe_fallback(self):
        session = mock.Mock()
        session.probe.return_value = {
            "kind": "json",
            "columns": ["user_id", "name"],
            "format": "json_lines",
        }
        resolved_type, resolved = velaria_service._resolve_auto_input_payload(
            session,
            {
                "input_path": "/tmp/events.jsonl",
                "input_type": "auto",
                "columns": "custom_id,custom_name",
                "json_format": "json_array",
            },
        )
        self.assertEqual(resolved_type, "json")
        self.assertEqual(resolved["columns"], "custom_id,custom_name")
        self.assertEqual(resolved["json_format"], "json_array")

        session.probe.side_effect = RuntimeError("probe failed")
        resolved_type, resolved = velaria_service._resolve_auto_input_payload(
            session,
            {
                "input_path": "/tmp/unknown.data",
                "input_type": "auto",
            },
        )
        self.assertEqual(resolved_type, "auto")
        self.assertEqual(resolved["input_path"], "/tmp/unknown.data")

    def test_import_preview_reports_resolved_source_type_for_auto_inputs(self):
        with tempfile.TemporaryDirectory(prefix="velaria-service-preview-auto-type-") as tmp:
            xlsx_path = pathlib.Path(tmp) / "people.xlsx"
            xlsx_path.write_text("placeholder", encoding="utf-8")
            server, thread, base_url = self._start_server()
            try:
                with mock.patch.object(velaria_service, "Session", return_value=mock.Mock()):
                    with mock.patch.object(
                        velaria_service,
                        "_resolve_auto_input_payload",
                        return_value=("excel", {"input_path": str(xlsx_path), "input_type": "auto"}),
                    ):
                        with mock.patch.object(velaria_service, "_load_dataframe", return_value=object()):
                            with mock.patch.object(
                                velaria_service,
                                "_preview_from_dataframe",
                                return_value={"schema": ["name"], "rows": [{"name": "alice"}], "row_count": 1},
                            ):
                                status, payload = self._request_json(
                                    "POST",
                                    f"{base_url}/api/v1/import/preview",
                                    {
                                        "input_path": str(xlsx_path),
                                        "input_type": "auto",
                                    },
                                )
                self.assertEqual(status, 200)
                self.assertEqual(payload["dataset"]["source_type"], "excel")
                self.assertEqual(payload["dataset"]["source_path"], str(xlsx_path.resolve()))
            finally:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()

    def test_execute_bitable_import_materializes_local_parquet_dataset(self):
        fake_client = mock.Mock()
        fake_client.list_records_from_url.return_value = [
            {"Name": "alice", "Score": 1, "Meta": {"owner": "ops"}},
            {"Name": "bob", "Score": 2, "Tags": ["p0", "triage"]},
        ]
        with tempfile.TemporaryDirectory(prefix="velaria-service-bitable-exec-") as tmp:
            run_dir = pathlib.Path(tmp) / "run"
            run_dir.mkdir(parents=True)
            with mock.patch.object(velaria_service, "BitableClient", return_value=fake_client):
                result = velaria_service._execute_bitable_import(
                    {
                        "bitable_url": "https://example.feishu.cn/base/app123/tables/tbl456",
                        "app_id": "cli-app",
                        "app_secret": "cli-secret",
                        "dataset_name": "ops-board",
                    },
                    run_dir,
                )
            self.assertEqual(result["payload"]["source_type"], "bitable")
            self.assertEqual(result["payload"]["dataset_name"], "ops-board")
            self.assertEqual(result["preview"]["row_count"], 2)
            self.assertEqual(result["preview"]["rows"][0]["Meta"], '{"owner":"ops"}')
            self.assertTrue(pathlib.Path(result["payload"]["source_path"]).exists())

    def test_preview_bitable_import_reads_first_100_rows_only(self):
        fake_client = mock.Mock()
        fake_client.list_records_from_url.return_value = [
            {"Name": f"user-{i}", "Score": i}
            for i in range(100)
        ]
        with mock.patch.object(velaria_service, "BitableClient", return_value=fake_client):
            result = velaria_service._preview_bitable_import(
                {
                    "input_type": "bitable",
                    "bitable_url": "https://example.feishu.cn/base/app123/tables/tbl456",
                    "app_id": "cli-app",
                    "app_secret": "cli-secret",
                    "dataset_name": "ops-board-preview",
                    "limit": 100,
                }
            )
        self.assertEqual(result["dataset"]["source_type"], "bitable")
        self.assertEqual(result["dataset"]["source_path"], "https://example.feishu.cn/base/app123/tables/tbl456")
        self.assertEqual(result["preview"]["row_count"], 100)
        self.assertEqual(result["preview"]["rows"][0]["Name"], "user-0")
        fake_client.list_records_from_url.assert_called_once()
        _, kwargs = fake_client.list_records_from_url.call_args
        self.assertEqual(kwargs["page_size"], 100)
        self.assertEqual(kwargs["max_rows"], 100)

    def test_bitable_import_run_is_async_and_persists_result_artifact(self):
        fake_client = mock.Mock()
        def _fake_list_records_from_url(*args, **kwargs):
            callback = kwargs.get("on_page")
            rows = [
                {"Name": "alice", "Score": 1},
                {"Name": "bob", "Score": 2},
            ]
            if callback is not None:
                callback(len(rows), len(rows))
            return rows

        fake_client.list_records_from_url.side_effect = _fake_list_records_from_url
        with tempfile.TemporaryDirectory(prefix="velaria-service-bitable-run-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    with mock.patch.object(velaria_service, "BitableClient", return_value=fake_client):
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/bitable-import",
                            {
                                "bitable_url": "https://example.feishu.cn/base/app123/tables/tbl456",
                                "app_id": "cli-app",
                                "app_secret": "cli-secret",
                                "dataset_name": "ops-board",
                            },
                        )
                        self.assertEqual(status, 202)
                        self.assertTrue(payload["ok"])
                        run_id = payload["run_id"]

                        run_status = 0
                        run_payload = {}
                        for _ in range(40):
                            run_status, run_payload = self._request_json(
                                "GET",
                                f"{base_url}/api/v1/runs/{run_id}",
                            )
                            if run_status == 200 and run_payload["run"]["status"] == "succeeded":
                                break
                            time.sleep(0.1)
                        self.assertEqual(run_status, 200)
                        self.assertEqual(run_payload["run"]["status"], "succeeded")
                        self.assertEqual(run_payload["run"]["details"]["source_type"], "bitable")
                        self.assertEqual(run_payload["run"]["details"]["fetched_rows"], 2)
                        self.assertEqual(run_payload["run"]["details"]["pages_fetched"], 1)

                        result_status, result_payload = self._request_json(
                            "GET",
                            f"{base_url}/api/v1/runs/{run_id}/result?limit=20",
                        )
                        self.assertEqual(result_status, 200)
                        self.assertEqual(result_payload["preview"]["row_count"], 2)
                        self.assertEqual(result_payload["preview"]["rows"][0]["Name"], "alice")
                        self.assertTrue(pathlib.Path(run_payload["run"]["details"]["source_path"]).exists())
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_bitable_import_run_with_embedding_config_persists_embedding_dataset(self):
        fake_client = mock.Mock()

        def _fake_list_records_from_url(*args, **kwargs):
            callback = kwargs.get("on_page")
            rows = [
                {
                    "doc_id": "doc-1",
                    "title": "Alpha",
                    "summary": "Payment page timeout",
                    "source_updated_at": 1,
                },
                {
                    "doc_id": "doc-2",
                    "title": "Beta",
                    "summary": "Refund delay in worker queue",
                    "source_updated_at": 2,
                },
            ]
            if callback is not None:
                callback(len(rows), len(rows))
            return rows

        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout": [1.0, 0.0, 0.0],
                "title: Beta\nsummary: Refund delay in worker queue": [0.0, 1.0, 0.0],
            }
        )
        fake_client.list_records_from_url.side_effect = _fake_list_records_from_url
        with tempfile.TemporaryDirectory(prefix="velaria-service-bitable-embed-run-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    with mock.patch.object(velaria_service, "BitableClient", return_value=fake_client), mock.patch.object(
                        velaria_service.cli_impl,
                        "_make_embedding_provider",
                        return_value=(provider, "static-demo"),
                    ):
                            status, payload = self._request_json(
                                "POST",
                                f"{base_url}/api/v1/runs/bitable-import",
                                {
                                    "bitable_url": "https://example.feishu.cn/base/app123/tables/tbl456",
                                    "app_id": "cli-app",
                                    "app_secret": "cli-secret",
                                    "dataset_name": "ops-board",
                                    "embedding_config": {
                                        "enabled": True,
                                        "text_columns": ["title", "summary"],
                                        "provider": "hash",
                                        "template_version": "text-v1",
                                    },
                                },
                            )
                            self.assertEqual(status, 202)
                            run_id = payload["run_id"]

                            run_status = 0
                            run_payload = {}
                            for _ in range(20):
                                run_status, run_payload = self._request_json(
                                    "GET",
                                    f"{base_url}/api/v1/runs/{run_id}",
                                )
                                if run_status == 200 and run_payload["run"]["status"] == "succeeded":
                                    break
                                time.sleep(0.1)
                            self.assertEqual(run_status, 200)
                            self.assertEqual(run_payload["run"]["status"], "succeeded")
                            embedding_dataset = run_payload["run"]["details"]["embedding_dataset"]
                            self.assertEqual(embedding_dataset["provider"], "hash")
                            self.assertEqual(embedding_dataset["model"], "static-demo")
                            self.assertEqual(embedding_dataset["row_count"], 2)
                            self.assertTrue(pathlib.Path(embedding_dataset["dataset_path"]).exists())
                            self.assertTrue(embedding_dataset["artifact_id"])

                            result_status, result_payload = self._request_json(
                                "GET",
                                f"{base_url}/api/v1/runs/{run_id}/result?limit=20",
                            )
                            self.assertEqual(result_status, 200)
                            self.assertEqual(result_payload["preview"]["row_count"], 2)
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_keyword_index_build_merges_multiple_arrow_like_datasets(self):
        with tempfile.TemporaryDirectory(prefix="velaria-service-keyword-index-") as tmp:
            left_path = pathlib.Path(tmp) / "left.parquet"
            right_path = pathlib.Path(tmp) / "right.parquet"
            pq.write_table(
                pa.table(
                    {
                        "title": ["支付超时", "退款延迟"],
                        "body": ["订单支付超时", "退款流程排队较慢"],
                    }
                ),
                left_path,
            )
            pq.write_table(
                pa.table(
                    {
                        "title": ["支付重试"],
                        "body": ["支付超时后重试成功"],
                    }
                ),
                right_path,
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-index-build",
                        {
                            "dataset_paths": [str(left_path), str(right_path)],
                            "text_columns": ["title", "body"],
                        },
                    )
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["run"]["status"], "succeeded")
                    self.assertEqual(payload["result"]["dataset_count"], 2)
                    self.assertEqual(payload["result"]["doc_count"], 3)
                    self.assertEqual(payload["artifact"]["format"], "keyword-index")
                    self.assertEqual(payload["preview"]["row_count"], 3)
                    index_dir = pathlib.Path(payload["result"]["index_path"])
                    self.assertTrue((index_dir / "manifest.json").exists())
                    self.assertTrue((index_dir / "docs.parquet").exists())
                    self.assertTrue((index_dir / "terms.parquet").exists())
                    self.assertTrue((index_dir / "postings.parquet").exists())
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_keyword_index_build_accepts_csv_input_path(self):
        with tempfile.TemporaryDirectory(prefix="velaria-service-keyword-index-csv-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "title,body\n"
                "支付超时,订单支付超时\n"
                "支付重试,支付超时后重试成功\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-index-build",
                        {
                            "input_path": str(csv_path),
                            "input_type": "csv",
                            "text_columns": ["title", "body"],
                        },
                    )
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["run"]["status"], "succeeded")
                    self.assertEqual(payload["result"]["doc_count"], 2)
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_keyword_index_build_supports_builtin_analyzer(self):
        with tempfile.TemporaryDirectory(prefix="velaria-service-keyword-index-builtin-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "title,body\n"
                "支付超时,订单支付超时\n"
                "支付重试,支付超时后重试成功\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-index-build",
                        {
                            "input_path": str(csv_path),
                            "input_type": "csv",
                            "text_columns": ["title", "body"],
                            "analyzer": "builtin",
                        },
                    )
                    self.assertEqual(status, 200)
                    self.assertEqual(payload["result"]["analyzer"], "builtin")
                    self.assertGreater(payload["result"]["term_count"], 0)
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_keyword_search_queries_built_index_with_where_sql(self):
        with tempfile.TemporaryDirectory(prefix="velaria-service-keyword-search-") as tmp:
            left_path = pathlib.Path(tmp) / "docs.parquet"
            pq.write_table(
                pa.table(
                    {
                        "bucket": [1, 1, 2],
                        "title": ["支付超时", "支付重试", "退款延迟"],
                        "body": ["订单支付超时", "支付超时后重试成功", "退款流程排队较慢"],
                    }
                ),
                left_path,
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    build_status, build_payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-index-build",
                        {
                            "dataset_path": str(left_path),
                            "text_columns": ["title", "body"],
                        },
                    )
                    self.assertEqual(build_status, 200)
                    search_status, search_payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-search",
                        {
                            "index_path": build_payload["result"]["index_path"],
                            "query_text": "支付超时",
                            "where_sql": "bucket = 1",
                            "top_k": 2,
                        },
                    )
                    self.assertEqual(search_status, 200)
                    self.assertEqual(search_payload["run"]["status"], "succeeded")
                    self.assertEqual(search_payload["preview"]["row_count"], 2)
                    self.assertEqual(search_payload["preview"]["rows"][0]["title"], "支付超时")
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_keyword_search_where_sql_matches_filtered_corpus_scoring(self):
        with tempfile.TemporaryDirectory(prefix="velaria-service-keyword-filtered-bm25-") as tmp:
            full_path = pathlib.Path(tmp) / "full.parquet"
            subset_path = pathlib.Path(tmp) / "subset.parquet"
            full_table = pa.table(
                {
                    "bucket": [1, 1, 2],
                    "title": ["稀有词 common", "common common common", "稀有词"],
                    "body": ["", "", "common"],
                }
            )
            subset_table = pa.table(
                {
                    "bucket": [1, 1],
                    "title": ["稀有词 common", "common common common"],
                    "body": ["", ""],
                }
            )
            pq.write_table(full_table, full_path)
            pq.write_table(subset_table, subset_path)
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    full_status, full_payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-index-build",
                        {
                            "dataset_path": str(full_path),
                            "text_columns": ["title", "body"],
                            "analyzer": "builtin",
                        },
                    )
                    subset_status, subset_payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-index-build",
                        {
                            "dataset_path": str(subset_path),
                            "text_columns": ["title", "body"],
                            "analyzer": "builtin",
                        },
                    )
                    self.assertEqual(full_status, 200)
                    self.assertEqual(subset_status, 200)
                    filtered_status, filtered_payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-search",
                        {
                            "index_path": full_payload["result"]["index_path"],
                            "query_text": "稀有词 common",
                            "where_sql": "bucket = 1",
                            "top_k": 2,
                        },
                    )
                    subset_search_status, subset_search_payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/keyword-search",
                        {
                            "index_path": subset_payload["result"]["index_path"],
                            "query_text": "稀有词 common",
                            "top_k": 2,
                        },
                    )
                    self.assertEqual(filtered_status, 200)
                    self.assertEqual(subset_search_status, 200)
                    self.assertEqual(
                        filtered_payload["preview"]["rows"][0]["title"],
                        subset_search_payload["preview"]["rows"][0]["title"],
                    )
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_hybrid_search_uses_keyword_index_as_prefilter(self):
        provider = StaticEmbeddingProvider(
            {
                "title: 支付超时\nsummary: 订单支付超时": [0.8, 0.2, 0.0],
                "title: 支付重试\nsummary: 支付超时后重试成功": [1.0, 0.0, 0.0],
                "支付超时": [1.0, 0.0, 0.0],
            }
        )
        with tempfile.TemporaryDirectory(prefix="velaria-service-hybrid-keyword-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "doc_id,bucket,title,summary,source_updated_at\n"
                "doc-1,1,支付超时,订单支付超时,1\n"
                "doc-2,1,支付重试,支付超时后重试成功,2\n",
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
                        build_status, build_payload = self._build_embedding_dataset(
                            base_url,
                            csv_path=csv_path,
                            provider="hash",
                        )
                        self.assertEqual(build_status, 200)
                        keyword_status, keyword_payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/keyword-index-build",
                            {
                                "input_path": str(csv_path),
                                "input_type": "csv",
                                "text_columns": ["title", "summary"],
                            },
                        )
                        self.assertEqual(keyword_status, 200)
                        hybrid_status, hybrid_payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/hybrid-search",
                            {
                                "dataset_path": build_payload["result"]["dataset_path"],
                                "index_path": keyword_payload["result"]["index_path"],
                                "query_text": "支付超时",
                                "provider": "hash",
                                "top_k": 1,
                                "vector_column": "embedding",
                            },
                        )
                    self.assertEqual(hybrid_status, 200)
                    self.assertEqual(hybrid_payload["run"]["status"], "succeeded")
                    self.assertEqual(hybrid_payload["preview"]["row_count"], 1)
                    self.assertEqual(hybrid_payload["preview"]["rows"][0]["doc_id"], "doc-2")
                    self.assertIn("vector_score", hybrid_payload["preview"]["schema"])
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_hybrid_search_with_duplicate_doc_id_uses_stable_join_key(self):
        provider = StaticEmbeddingProvider(
            {
                "title: 支付超时\nsummary: 订单支付超时": [1.0, 0.0, 0.0],
                "title: 支付超时\nsummary: 支付超时后重试成功": [1.0, 0.0, 0.0],
                "支付超时": [1.0, 0.0, 0.0],
            }
        )
        with tempfile.TemporaryDirectory(prefix="velaria-service-hybrid-dup-docid-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "doc_id,bucket,title,summary,source_updated_at\n"
                "dup,1,支付超时,订单支付超时,1\n"
                "dup,1,支付超时,支付超时后重试成功,2\n",
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
                        build_status, build_payload = self._build_embedding_dataset(
                            base_url,
                            csv_path=csv_path,
                            provider="hash",
                        )
                        self.assertEqual(build_status, 200)
                        keyword_status, keyword_payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/keyword-index-build",
                            {
                                "input_path": str(csv_path),
                                "input_type": "csv",
                                "text_columns": ["title", "summary"],
                                "analyzer": "builtin",
                            },
                        )
                        self.assertEqual(keyword_status, 200)
                        hybrid_status, hybrid_payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/hybrid-search",
                            {
                                "dataset_path": build_payload["result"]["dataset_path"],
                                "index_path": keyword_payload["result"]["index_path"],
                                "query_text": "支付超时",
                                "provider": "hash",
                                "top_k": 2,
                                "vector_column": "embedding",
                            },
                        )
                    self.assertEqual(hybrid_status, 200)
                    self.assertEqual(hybrid_payload["preview"]["row_count"], 2)
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_bitable_import_run_with_keyword_index_config_persists_index_metadata(self):
        fake_client = mock.Mock()

        def _fake_list_records_from_url(*args, **kwargs):
            callback = kwargs.get("on_page")
            rows = [
                {"title": "支付超时", "body": "订单支付超时"},
                {"title": "支付重试", "body": "支付超时后重试成功"},
            ]
            if callback is not None:
                callback(len(rows), len(rows))
            return rows

        fake_client.list_records_from_url.side_effect = _fake_list_records_from_url
        with tempfile.TemporaryDirectory(prefix="velaria-service-bitable-keyword-run-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    with mock.patch.object(velaria_service, "BitableClient", return_value=fake_client):
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/bitable-import",
                            {
                                "bitable_url": "https://example.feishu.cn/base/app123/tables/tbl456",
                                "app_id": "cli-app",
                                "app_secret": "cli-secret",
                                "dataset_name": "ops-board",
                                "keyword_index_config": {
                                    "enabled": True,
                                    "text_columns": ["title", "body"],
                                },
                            },
                        )
                        self.assertEqual(status, 202)
                        run_id = payload["run_id"]

                        run_status = 0
                        run_payload = {}
                        for _ in range(20):
                            run_status, run_payload = self._request_json(
                                "GET",
                                f"{base_url}/api/v1/runs/{run_id}",
                            )
                            if run_status == 200 and run_payload["run"]["status"] == "succeeded":
                                break
                            time.sleep(0.1)
                        self.assertEqual(run_status, 200)
                        self.assertEqual(run_payload["run"]["status"], "succeeded")
                        keyword_index = run_payload["run"]["details"]["keyword_index"]
                        self.assertEqual(keyword_index["doc_count"], 2)
                        self.assertEqual(keyword_index["dataset_count"], 1)
                        self.assertTrue(pathlib.Path(keyword_index["index_path"]).exists())
                        self.assertTrue(keyword_index["artifact_id"])
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

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
                        build_status, build_payload = self._build_embedding_dataset(
                            base_url,
                            csv_path=csv_path,
                            provider="hash",
                        )
                        self.assertEqual(build_status, 200)
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/hybrid-search",
                            {
                                "dataset_path": build_payload["result"]["dataset_path"],
                                "query_text": "payment timeout",
                                "provider": "hash",
                                "top_k": 1,
                                "vector_column": "embedding",
                            },
                        )
                    self.assertEqual(status, 200)
                    self.assertTrue(payload["ok"])
                    self.assertEqual(payload["run"]["status"], "succeeded")
                    self.assertEqual(payload["result"]["model"], "static-demo")
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

    def test_hybrid_search_defaults_minilm_model_to_local_chinese_model(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout": [1.0, 0.0, 0.0],
                "payment timeout": [1.0, 0.0, 0.0],
            }
        )
        captured = {}

        def fake_make_embedding_provider(provider_name, model_name):
            captured["provider_name"] = provider_name
            captured["model_name"] = model_name
            return provider, model_name

        with tempfile.TemporaryDirectory(prefix="velaria-service-default-zh-model-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "title,summary\n"
                "Alpha,Payment page timeout\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    with mock.patch.object(
                        velaria_service.cli_impl,
                        "_make_embedding_provider",
                        side_effect=fake_make_embedding_provider,
                    ):
                        build_status, build_payload = self._build_embedding_dataset(
                            base_url,
                            csv_path=csv_path,
                            provider="minilm",
                        )
                        self.assertEqual(build_status, 200)
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/hybrid-search",
                            {
                                "dataset_path": build_payload["result"]["dataset_path"],
                                "query_text": "payment timeout",
                                "provider": "minilm",
                                "top_k": 1,
                                "vector_column": "embedding",
                            },
                        )
                    self.assertEqual(status, 200)
                    self.assertEqual(captured["provider_name"], "minilm")
                    self.assertEqual(captured["model_name"], DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL)
                    self.assertEqual(payload["result"]["model"], DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL)
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_hybrid_search_without_provider_still_defaults_to_minilm(self):
        provider = StaticEmbeddingProvider(
            {
                "title: Alpha\nsummary: Payment page timeout": [1.0, 0.0, 0.0],
                "payment timeout": [1.0, 0.0, 0.0],
            }
        )
        captured = {}

        def fake_make_embedding_provider(provider_name, model_name):
            captured["provider_name"] = provider_name
            captured["model_name"] = model_name
            return provider, model_name

        with tempfile.TemporaryDirectory(prefix="velaria-service-default-provider-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "title,summary\n"
                "Alpha,Payment page timeout\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    with mock.patch.object(
                        velaria_service.cli_impl,
                        "_make_embedding_provider",
                        side_effect=fake_make_embedding_provider,
                    ):
                        build_status, build_payload = self._build_embedding_dataset(
                            base_url,
                            csv_path=csv_path,
                        )
                        self.assertEqual(build_status, 200)
                        status, payload = self._request_json(
                            "POST",
                            f"{base_url}/api/v1/runs/hybrid-search",
                            {
                                "dataset_path": build_payload["result"]["dataset_path"],
                                "query_text": "payment timeout",
                                "top_k": 1,
                                "vector_column": "embedding",
                            },
                        )
                    self.assertEqual(status, 200)
                    self.assertEqual(captured["provider_name"], "minilm")
                    self.assertEqual(captured["model_name"], DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL)
                    self.assertEqual(payload["result"]["provider"], "minilm")
                    self.assertEqual(payload["result"]["model"], DEFAULT_LOCAL_CHINESE_EMBEDDING_MODEL)
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()

    def test_hybrid_search_rejects_input_path_only_requests(self):
        with tempfile.TemporaryDirectory(prefix="velaria-service-hybrid-input-path-reject-") as tmp:
            csv_path = pathlib.Path(tmp) / "docs.csv"
            csv_path.write_text(
                "title,summary\n"
                "Alpha,Payment page timeout\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                server, thread, base_url = self._start_server()
                try:
                    status, payload = self._request_json(
                        "POST",
                        f"{base_url}/api/v1/runs/hybrid-search",
                        {
                            "input_path": str(csv_path),
                            "input_type": "csv",
                            "query_text": "payment timeout",
                            "top_k": 1,
                        },
                    )
                    self.assertEqual(status, 400)
                    self.assertFalse(payload["ok"])
                    self.assertIn("dataset_path is required", payload["error"])
                finally:
                    server.shutdown()
                    thread.join(timeout=5)
                    server.server_close()


if __name__ == "__main__":
    unittest.main()
