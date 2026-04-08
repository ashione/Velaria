import io
import json
import importlib
import os
import pathlib
import sys
import tempfile
import unittest
from builtins import EOFError
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
    def test_interactive_mode_accepts_commands_and_exits_cleanly(self):
        with tempfile.TemporaryDirectory(prefix="velaria-cli-interactive-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with mock.patch(
                    "builtins.input",
                    side_effect=["run list --limit 1", "help run diff", "quit"],
                ):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i"])
                self.assertEqual(exit_code, 0)
                self.assertEqual(stderr.getvalue(), "")
                output = stdout.getvalue()
                self.assertIn("Velaria interactive mode.", output)
                self.assertIn('"ok": true', output)
                self.assertIn("usage:", output)

    def test_interactive_mode_reports_command_errors_without_exiting_session(self):
        with tempfile.TemporaryDirectory(prefix="velaria-cli-interactive-errors-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with mock.patch(
                    "builtins.input",
                    side_effect=[
                        "run show --run-id missing-run",
                        "help run result",
                        "run list --limit 1",
                        "quit",
                    ],
                ):
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = velaria_cli.main(["-i"])
                self.assertEqual(exit_code, 0)
                self.assertEqual(stderr.getvalue(), "")
                output = stdout.getvalue()
                self.assertIn("Velaria interactive mode.", output)
                self.assertIn('"ok": false', output)
                self.assertIn('"phase": "run_lookup"', output)
                self.assertIn('"ok": true', output)
                self.assertIn("usage:", output)

    def test_interactive_mode_eof_exits_cleanly(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("builtins.input", side_effect=EOFError):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = velaria_cli.main(["-i"])
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertIn("Velaria interactive mode.", stdout.getvalue())

    def test_help_is_available_for_top_level_and_subcommands(self):
        cases = [
            [],
            ["run"],
            ["run", "start"],
            ["run", "list"],
            ["run", "result"],
            ["run", "diff"],
            ["run", "show"],
            ["run", "status"],
            ["run", "cleanup"],
            ["artifacts"],
            ["artifacts", "list"],
            ["artifacts", "preview"],
            ["file-sql"],
            ["vector-search"],
        ]
        for argv in cases:
            stdout = io.StringIO()
            stderr = io.StringIO()
            with self.subTest(argv=argv):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    exit_code = velaria_cli.main([*argv, "--help"])
                self.assertEqual(exit_code, 0)
                self.assertEqual(stderr.getvalue(), "")
                self.assertIn("usage:", stdout.getvalue())

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
                                "file-sql",
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
                        [
                            "run",
                            "list",
                            "--status",
                            "succeeded",
                            "--tag",
                            "slow-query",
                            "--query",
                            "cache replay",
                        ]
                    )
                self.assertEqual(list_exit_code, 0)
                list_payload = json.loads(list_stdout.getvalue())
                self.assertEqual(len(list_payload["runs"]), 1)
                self.assertEqual(list_payload["runs"][0]["run_id"], payload["run_id"])
                self.assertEqual(list_payload["runs"][0]["tags"], ["cn", "slow-query", "cache"])
                self.assertEqual(list_payload["runs"][0]["artifact_count"], 0)
                self.assertIsNotNone(list_payload["runs"][0]["duration_ms"])

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
                                "file-sql",
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
                    (["run", "start", "--"], "run start requires an action", "argument_parse"),
                    (
                        ["run", "show", "--run-id", "missing-run"],
                        "run not found: missing-run",
                        "run_lookup",
                    ),
                    (
                        ["run", "result", "--run-id", "missing-run"],
                        "run not found: missing-run",
                        "run_lookup",
                    ),
                    (
                        ["run", "diff", "--run-id", "missing-run", "--other-run-id", "other-run"],
                        "run not found: missing-run",
                        "run_lookup",
                    ),
                    (
                        ["artifacts", "preview", "--artifact-id", "missing-artifact"],
                        "artifact not found: missing-artifact",
                        "artifact_lookup",
                    ),
                ]
                for argv, expected_error, expected_phase in cases:
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
                        self.assertIn("error_type", payload)
                        self.assertIn("phase", payload)
                        self.assertEqual(payload["phase"], expected_phase)
                        self.assertIsInstance(payload.get("details", {}), dict)

    def test_run_start_failure_returns_structured_error_and_persists_details(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-run-failure-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                with mock.patch.object(
                    velaria_cli_impl,
                    "_run_action_with_timeout",
                    return_value={
                        "error": "failed to read csv input",
                        "error_type": "value_error",
                        "phase": "csv_read",
                        "details": {
                            "csv": "/tmp/missing.csv",
                            "table": "input_table",
                        },
                    },
                ):
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        exit_code = velaria_cli.main(
                            [
                                "run",
                                "start",
                                "--description",
                                "failure case",
                                "--",
                                "file-sql",
                                "--csv",
                                "/tmp/missing.csv",
                                "--query",
                                "SELECT 1",
                            ]
                        )
                self.assertEqual(exit_code, 1)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["error_type"], "value_error")
                self.assertEqual(payload["phase"], "csv_read")
                self.assertEqual(payload["details"]["csv"], "/tmp/missing.csv")
                self.assertIsNotNone(payload["run_id"])

                run_meta = workspace.read_run(payload["run_id"])
                self.assertEqual(run_meta["status"], "failed")
                self.assertEqual(run_meta["details"]["error_type"], "value_error")
                self.assertEqual(run_meta["details"]["phase"], "csv_read")
                self.assertEqual(
                    run_meta["details"]["error_details"]["csv"],
                    "/tmp/missing.csv",
                )

    def test_run_result_and_diff_fail_when_result_artifact_is_missing(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-missing-result-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = workspace.ArtifactIndex()
                run_one_id, run_one_dir = workspace.create_run("file-sql", {"query": "SELECT 1"}, "0.0.test")
                run_two_id, run_two_dir = workspace.create_run("file-sql", {"query": "SELECT 2"}, "0.0.test")
                index.upsert_run(workspace.read_run(run_one_id))
                index.upsert_run(workspace.read_run(run_two_id))
                pathlib.Path(run_two_dir, "artifacts", "explain.txt").write_text("logical", encoding="utf-8")
                index.insert_artifact(
                    {
                        "artifact_id": "artifact-explain",
                        "run_id": run_two_id,
                        "created_at": "2026-04-01T10:00:01Z",
                        "type": "file",
                        "uri": pathlib.Path(run_two_dir, "artifacts", "explain.txt").resolve().as_uri(),
                        "format": "text",
                        "row_count": None,
                        "schema_json": None,
                        "preview_json": None,
                        "tags_json": ["explain"],
                    }
                )

                for argv in (
                    ["run", "result", "--run-id", run_one_id],
                    ["run", "diff", "--run-id", run_one_id, "--other-run-id", run_two_id],
                ):
                    stdout = io.StringIO()
                    with self.subTest(argv=argv):
                        with redirect_stdout(stdout):
                            exit_code = velaria_cli.main(argv)
                        self.assertEqual(exit_code, 1)
                        payload = json.loads(stdout.getvalue())
                        self.assertEqual(payload["error_type"], "file_not_found")
                        self.assertEqual(payload["phase"], "run_result")
                        self.assertEqual(payload["run_id"], run_one_id)

    def test_csv_sql_sql_input_registration_failure_reports_phase(self):
        fake_session = mock.Mock()
        fake_session.sql.side_effect = RuntimeError("boom")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-csv-view-fail-") as tmp:
            csv_path = pathlib.Path(tmp) / "input.csv"
            csv_path.write_text("name,score\nalice,1\n", encoding="utf-8")
            stdout = io.StringIO()
            with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        [
                            "file-sql",
                            "--csv",
                            str(csv_path),
                            "--table",
                            "input_table",
                            "--query",
                            "SELECT * FROM input_table",
                        ]
                    )
            self.assertEqual(exit_code, 1)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["phase"], "sql_input_register")
            self.assertEqual(payload["details"]["input_path"], str(csv_path))
            self.assertEqual(payload["details"]["input_type"], "auto")
            self.assertEqual(payload["details"]["table"], "input_table")

    def test_csv_sql_json_input_builds_create_table_sql(self):
        fake_session = mock.Mock()
        fake_session.sql.side_effect = [mock.Mock(name="create_result"), _FakeDataFrame()]
        with tempfile.TemporaryDirectory(prefix="velaria-cli-json-sql-") as tmp:
            json_path = pathlib.Path(tmp) / "input.jsonl"
            json_path.write_text('{"row_id":1,"score":0.5}\n', encoding="utf-8")
            stdout = io.StringIO()
            with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        [
                            "file-sql",
                            "--input-path",
                            str(json_path),
                            "--input-type",
                            "json",
                            "--columns",
                            "row_id,score",
                            "--query",
                            "SELECT row_id, score FROM input_table",
                        ]
                    )
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["input_type"], "json")
        create_sql = fake_session.sql.call_args_list[0].args[0]
        self.assertIn("CREATE TABLE input_table USING json OPTIONS(", create_sql)
        self.assertIn("columns: 'row_id,score'", create_sql)

    def test_stream_sql_once_start_and_wait_failures_report_phase(self):
        workspace = importlib.import_module("velaria.workspace")

        class _FailingQuery:
            def __init__(self, fail_on: str) -> None:
                self.fail_on = fail_on

            def start(self):
                if self.fail_on == "start":
                    raise RuntimeError("start failed")

            def snapshot_json(self):
                return "{\"status\":\"running\"}"

            def await_termination(self, max_batches: int):
                if self.fail_on == "wait":
                    raise RuntimeError("wait failed")
                return max_batches

            def progress(self):
                return {"status": "running"}

        def _make_session(fail_on: str):
            fake_session = mock.Mock()
            fake_session.read_stream_csv_dir.return_value = mock.Mock(name="stream_df")
            fake_session.explain_stream_sql.return_value = "logical\nscan\nphysical\nplan\nstrategy\nselected_mode=single-process"
            fake_session.start_stream_sql.return_value = _FailingQuery(fail_on)
            return fake_session

        with tempfile.TemporaryDirectory(prefix="velaria-cli-stream-fail-") as tmp:
            run_id, run_dir = workspace.create_run("stream-sql-once", {"query": "demo"}, "0.0.test")
            source_dir = pathlib.Path(tmp) / "source"
            source_dir.mkdir(parents=True)
            for fail_on, expected_phase in (("start", "stream_start"), ("wait", "stream_wait")):
                fake_session = _make_session(fail_on)
                with self.subTest(fail_on=fail_on):
                    with mock.patch.object(velaria_cli, "Session", return_value=fake_session):
                        with self.assertRaises(velaria_cli.CliStructuredError) as ctx:
                            velaria_cli._execute_stream_sql_once(
                                source_csv_dir=source_dir,
                                source_table="input_stream",
                                source_delimiter=",",
                                sink_table="output_sink",
                                sink_schema="key STRING, value_sum INT",
                                sink_path=pathlib.Path(run_dir) / "artifacts" / "stream_result.csv",
                                sink_delimiter=",",
                                query="INSERT INTO output_sink SELECT key, 1 AS value_sum FROM input_stream",
                                trigger_interval_ms=0,
                                checkpoint_delivery_mode="best-effort",
                                execution_mode="single-process",
                                local_workers=1,
                                max_inflight_partitions=0,
                                max_batches=1,
                                run_id=run_id,
                            )
                    self.assertEqual(ctx.exception.phase, expected_phase)

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
                        "action": "file-sql",
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

    def test_artifact_preview_cached_payload_still_respects_requested_limit(self):
        workspace = importlib.import_module("velaria.workspace")
        with tempfile.TemporaryDirectory(prefix="velaria-cli-preview-cache-") as tmp:
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
                        "action": "file-sql",
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
                        "uri": pathlib.Path(tmp, "artifact.json").resolve().as_uri(),
                        "format": "json",
                        "row_count": 3,
                        "schema_json": ["name"],
                        "preview_json": {
                            "schema": ["name"],
                            "rows": [{"name": "alice"}, {"name": "bob"}, {"name": "carol"}],
                            "row_count": 3,
                            "truncated": False,
                        },
                        "tags_json": ["result"],
                    }
                )
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        ["artifacts", "preview", "--artifact-id", "artifact-1", "--limit", "1"]
                    )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["preview"]["row_count"], 3)
                self.assertTrue(payload["preview"]["truncated"])
                self.assertEqual(len(payload["preview"]["rows"]), 1)

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
            "acceleration=flat-buffer+simd-topk\n"
            "backend=neon\n"
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
