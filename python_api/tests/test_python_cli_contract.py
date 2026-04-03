import io
import json
import importlib
import os
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

try:
    velaria_cli = importlib.import_module("velaria_cli")
    velaria_cli_impl = importlib.import_module("velaria.cli")
except ModuleNotFoundError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    velaria_cli = importlib.import_module("velaria_cli")
    velaria_cli_impl = importlib.import_module("velaria.cli")


class _FakeArrowResult:
    @property
    def schema(self):
        return mock.Mock(names=["row_id", "score"])

    def to_pylist(self):
        return [{"row_id": 0, "score": 0.0}]


class _FakeDataFrame:
    def to_arrow(self):
        return _FakeArrowResult()


class PythonCliContractTest(unittest.TestCase):
    def test_run_start_persists_metadata_and_run_list_filters(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-run-meta-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch.object(
                    velaria_cli_impl,
                    "_run_action_with_timeout",
                    return_value={"payload": {"rows": []}, "artifacts": []},
                ):
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "run",
                                "start",
                                "--run-name",
                                "cn-slow-query",
                                "--description",
                                "CN slow query snapshot for cache replay triage",
                                "--tag",
                                "cn",
                                "--tag",
                                "slow-query,cache",
                                "--",
                                "csv-sql",
                                "--csv",
                                "/tmp/input.csv",
                                "--query",
                                "SELECT 1",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertTrue(payload["ok"])
                run_meta = workspace.read_run(payload["run_id"])
                self.assertEqual(run_meta["run_name"], "cn-slow-query")
                self.assertEqual(
                    run_meta["description"],
                    "CN slow query snapshot for cache replay triage",
                )
                self.assertEqual(run_meta["tags"], ["cn", "slow-query", "cache"])
                inputs = json.loads(
                    (pathlib.Path(run_meta["run_dir"]) / "inputs.json").read_text(encoding="utf-8")
                )
                self.assertEqual(inputs["run_name"], "cn-slow-query")
                self.assertEqual(
                    inputs["description"],
                    "CN slow query snapshot for cache replay triage",
                )
                self.assertEqual(inputs["tags"], ["cn", "slow-query", "cache"])

                list_stdout = io.StringIO()
                with redirect_stdout(list_stdout):
                    list_exit_code = velaria_cli.main(
                        ["run", "list", "--status", "succeeded", "--tag", "slow-query"]
                    )
                self.assertEqual(list_exit_code, 0)
                list_payload = json.loads(list_stdout.getvalue())
                self.assertEqual(len(list_payload["runs"]), 1)
                self.assertEqual(list_payload["runs"][0]["run_id"], payload["run_id"])
                self.assertEqual(list_payload["runs"][0]["tags"], ["cn", "slow-query", "cache"])

    def test_run_start_normalizes_duplicate_and_blank_tags(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-run-tags-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch.object(
                    velaria_cli_impl,
                    "_run_action_with_timeout",
                    return_value={"payload": {"rows": []}, "artifacts": []},
                ):
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "run",
                                "start",
                                "--tag",
                                " cn , slow-query , cn , ",
                                "--tag",
                                "analytics",
                                "--tag",
                                "slow-query",
                                "--",
                                "csv-sql",
                                "--csv",
                                "/tmp/input.csv",
                                "--query",
                                "SELECT 1",
                            ]
                        )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["tags"], ["cn", "slow-query", "analytics"])
                run_meta = workspace.read_run(payload["run_id"])
                self.assertEqual(run_meta["tags"], ["cn", "slow-query", "analytics"])

    def test_workspace_errors_return_json_without_stderr_noise(self):
        with tempfile.TemporaryDirectory(prefix="velaria-cli-errors-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                cases = [
                    (["run", "start", "--"], "run start requires an action"),
                    (["run", "show", "--run-id", "missing-run"], "run not found: missing-run"),
                    (
                        ["artifacts", "preview", "--artifact-id", "missing-artifact"],
                        "artifact not found: missing-artifact",
                    ),
                ]
                for argv, expected_error in cases:
                    stdout = io.StringIO()
                    stderr = io.StringIO()
                    with self.subTest(argv=argv):
                        with redirect_stdout(stdout), redirect_stderr(stderr):
                            exit_code = velaria_cli.main(argv)
                        self.assertEqual(exit_code, 1)
                        self.assertEqual(stderr.getvalue(), "")
                        payload = json.loads(stdout.getvalue())
                        self.assertFalse(payload["ok"])
                        self.assertIn(expected_error, payload["error"])

    def test_artifact_preview_cache_miss_reports_full_row_count_for_parquet(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-preview-") as tmp:
            parquet_path = pathlib.Path(tmp) / "artifact.parquet"
            table = pa.table({"name": ["alice", "bob", "carol"], "score": [1, 2, 3]})
            pq.write_table(table, parquet_path)
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = workspace.ArtifactIndex()
                run_dir = pathlib.Path(tmp) / "runs" / "run-1"
                run_dir.mkdir(parents=True)
                index.upsert_run(
                    {
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:00Z",
                        "finished_at": "2026-04-01T10:00:01Z",
                        "status": "succeeded",
                        "action": "csv-sql",
                        "cli_args": {},
                        "velaria_version": "0.0.test",
                        "run_dir": str(run_dir),
                    }
                )
                index.insert_artifact(
                    {
                        "artifact_id": "artifact-1",
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:01Z",
                        "type": "file",
                        "uri": parquet_path.resolve().as_uri(),
                        "format": "parquet",
                        "row_count": 3,
                        "schema_json": ["name", "score"],
                        "preview_json": None,
                        "tags_json": ["result"],
                    }
                )
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        ["artifacts", "preview", "--artifact-id", "artifact-1", "--limit", "2"]
                    )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["preview"]["row_count"], 3)
                self.assertTrue(payload["preview"]["truncated"])
                self.assertEqual(len(payload["preview"]["rows"]), 2)

    def test_vector_cli_delegates_to_session_contract(self):
        fake_session = mock.Mock()
        fake_session.read_csv.return_value = mock.Mock(name="df")
        fake_session.vector_search.return_value = _FakeDataFrame()
        fake_session.explain_vector_search.return_value = (
            "mode=exact-scan\n"
            "metric=cosine\n"
            "dimension=3\n"
            "top_k=2\n"
            "candidate_rows=3\n"
            "filter_pushdown=false\n"
            "acceleration=flat-buffer+heap-topk\n"
        )

        with tempfile.TemporaryDirectory(prefix="velaria-cli-contract-") as tmp:
            csv_path = pathlib.Path(tmp) / "vectors.csv"
            csv_path.write_text("id,embedding\n1,[1 0 0]\n", encoding="utf-8")
            with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli._run_vector_search(
                        csv_path=csv_path,
                        vector_column="embedding",
                        query_vector="1.0,0.0,0.0",
                        metric="cosine",
                        top_k=2,
                    )

        self.assertEqual(exit_code, 0)
        fake_session.read_csv.assert_called_once_with(str(csv_path))
        fake_session.create_temp_view.assert_called_once()
        fake_session.vector_search.assert_called_once_with(
            table="input_table",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
        )
        fake_session.explain_vector_search.assert_called_once_with(
            table="input_table",
            vector_column="embedding",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            metric="cosine",
        )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["metric"], "cosine")
        self.assertEqual(payload["top_k"], 2)
        self.assertEqual(payload["schema"], ["row_id", "score"])
        self.assertEqual(payload["rows"], [{"row_id": 0, "score": 0.0}])
        self.assertIn("mode=exact-scan", payload["explain"])
        self.assertIn("candidate_rows=3", payload["explain"])
        self.assertIn("filter_pushdown=false", payload["explain"])


if __name__ == "__main__":
    unittest.main()
