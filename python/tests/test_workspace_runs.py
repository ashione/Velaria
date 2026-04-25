import io
import importlib
import json
import os
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

try:
    velaria_cli = importlib.import_module("velaria_cli")
    workspace = importlib.import_module("velaria.workspace")
except ModuleNotFoundError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    velaria_cli = importlib.import_module("velaria_cli")
    workspace = importlib.import_module("velaria.workspace")

ArtifactIndex = workspace.ArtifactIndex
create_run = workspace.create_run
read_run = workspace.read_run


class WorkspaceRunsTest(unittest.TestCase):
    def test_csv_sql_handles_quoted_json_and_oversized_integer_strings(self):
        with tempfile.TemporaryDirectory(prefix="velaria-csv-quoted-") as tmp:
            csv_path = pathlib.Path(tmp) / "quoted.csv"
            csv_path.write_text(
                (
                    "record_id,extra,score\n"
                    '2026040222134901020610814336213,"{""cluster"":""query"",""data_count"":200}",7\n'
                    '42,"{""cluster"":""query"",""data_count"":201}",8\n'
                ),
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = velaria_cli.main(
                    [
                        "file-sql",
                        "--csv",
                        str(csv_path),
                        "--table",
                        "slow",
                        "--query",
                        "SELECT COUNT(*) AS cnt FROM slow",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["schema"], ["cnt"])
            self.assertEqual(payload["rows"], [{"cnt": 2}])

    def test_run_start_csv_sql_writes_run_dir_and_preview(self):
        with tempfile.TemporaryDirectory(prefix="velaria-workspace-run-") as tmp:
            csv_path = pathlib.Path(tmp) / "scores.csv"
            csv_path.write_text("name,score\nalice,10\nbob,22\ncarol,31\n", encoding="utf-8")
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    exit_code = velaria_cli.main(
                        [
                            "run",
                            "start",
                            "--run-name",
                            "score-summary",
                            "--description",
                            "filter rows with score > 20",
                            "--tag",
                            "demo",
                            "--tag",
                            "scores,csv",
                            "--",
                            "file-sql",
                            "--csv",
                            str(csv_path),
                            "--query",
                            "SELECT name, score FROM input_table WHERE score > 20",
                        ]
                    )
                self.assertEqual(exit_code, 0)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(payload["status"], "succeeded")

                run_id = payload["run_id"]
                run_dir = pathlib.Path(payload["run_dir"])
                self.assertTrue((run_dir / "run.json").exists())
                self.assertTrue((run_dir / "inputs.json").exists())
                self.assertTrue((run_dir / "stdout.log").exists())
                self.assertTrue((run_dir / "stderr.log").exists())
                self.assertTrue((run_dir / "explain.json").exists())
                self.assertTrue((run_dir / "artifacts" / "result.parquet").exists())

                run_meta = read_run(run_id)
                self.assertEqual(run_meta["status"], "succeeded")
                self.assertEqual(run_meta["action"], "file-sql")
                self.assertEqual(run_meta["run_name"], "score-summary")
                self.assertEqual(run_meta["description"], "filter rows with score > 20")
                self.assertEqual(run_meta["tags"], ["demo", "scores", "csv"])

                index = ArtifactIndex()
                runs = index.list_runs(tag="scores")
                self.assertEqual(len(runs), 1)
                self.assertEqual(runs[0]["run_id"], run_id)
                artifacts = index.list_artifacts(run_id=run_id)
                self.assertEqual(len(artifacts), 1)
                artifact_id = artifacts[0]["artifact_id"]

                preview_stdout = io.StringIO()
                with redirect_stdout(preview_stdout):
                    preview_exit = velaria_cli.main(
                        [
                            "artifacts",
                            "preview",
                            "--artifact-id",
                            artifact_id,
                            "--limit",
                            "5",
                        ]
                    )
                self.assertEqual(preview_exit, 0)
                preview_payload = json.loads(preview_stdout.getvalue())
                self.assertEqual(preview_payload["artifact_id"], artifact_id)
                self.assertEqual(len(preview_payload["preview"]["rows"]), 2)

                status_stdout = io.StringIO()
                with redirect_stdout(status_stdout):
                    status_exit = velaria_cli.main(["run", "status", "--run-id", run_id])
                self.assertEqual(status_exit, 0)
                status_payload = json.loads(status_stdout.getvalue())
                self.assertEqual(status_payload["status"], "succeeded")
                self.assertEqual(len(status_payload["artifacts"]), 1)
                self.assertEqual(status_payload["artifact_count"], 1)
                self.assertIsNotNone(status_payload["duration_ms"])

                show_stdout = io.StringIO()
                with redirect_stdout(show_stdout):
                    show_exit = velaria_cli.main(["run", "show", "--run-id", run_id])
                self.assertEqual(show_exit, 0)
                show_payload = json.loads(show_stdout.getvalue())
                self.assertEqual(show_payload["run"]["artifact_count"], 1)
                self.assertIsNotNone(show_payload["run"]["duration_ms"])

                result_stdout = io.StringIO()
                with redirect_stdout(result_stdout):
                    result_exit = velaria_cli.main(["run", "result", "--run-id", run_id, "--limit", "1"])
                self.assertEqual(result_exit, 0)
                result_payload = json.loads(result_stdout.getvalue())
                self.assertEqual(result_payload["artifact_id"], artifact_id)
                self.assertEqual(result_payload["artifact"]["artifact_id"], artifact_id)
                self.assertEqual(result_payload["preview"]["row_count"], 2)
                self.assertGreaterEqual(len(result_payload["preview"]["rows"]), 1)

    def test_run_diff_compares_metadata_and_result_preview(self):
        with tempfile.TemporaryDirectory(prefix="velaria-run-diff-") as tmp:
            csv_path = pathlib.Path(tmp) / "scores.csv"
            csv_path.write_text("name,score\nalice,10\nbob,22\ncarol,31\n", encoding="utf-8")
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                first_stdout = io.StringIO()
                with redirect_stdout(first_stdout):
                    first_exit = velaria_cli.main(
                        [
                            "run",
                            "start",
                            "--run-name",
                            "score-ge-20",
                            "--description",
                            "scores greater than or equal to 20",
                            "--tag",
                            "scores",
                            "--",
                            "file-sql",
                            "--csv",
                            str(csv_path),
                            "--query",
                            "SELECT name, score FROM input_table WHERE score >= 20",
                        ]
                    )
                self.assertEqual(first_exit, 0)
                first_payload = json.loads(first_stdout.getvalue())

                second_stdout = io.StringIO()
                with redirect_stdout(second_stdout):
                    second_exit = velaria_cli.main(
                        [
                            "run",
                            "start",
                            "--run-name",
                            "score-ge-30",
                            "--description",
                            "scores greater than or equal to 30",
                            "--tag",
                            "scores",
                            "--tag",
                            "strict",
                            "--",
                            "file-sql",
                            "--csv",
                            str(csv_path),
                            "--query",
                            "SELECT name, score FROM input_table WHERE score >= 30",
                        ]
                    )
                self.assertEqual(second_exit, 0)
                second_payload = json.loads(second_stdout.getvalue())

                diff_stdout = io.StringIO()
                with redirect_stdout(diff_stdout):
                    diff_exit = velaria_cli.main(
                        [
                            "run",
                            "diff",
                            "--run-id",
                            first_payload["run_id"],
                            "--other-run-id",
                            second_payload["run_id"],
                            "--limit",
                            "1",
                        ]
                    )
                self.assertEqual(diff_exit, 0)
                diff_payload = json.loads(diff_stdout.getvalue())
                self.assertEqual(diff_payload["left_run"]["run_id"], first_payload["run_id"])
                self.assertEqual(diff_payload["right_run"]["run_id"], second_payload["run_id"])
                self.assertIn("description", diff_payload["metadata_diff"])
                self.assertIn("tags", diff_payload["metadata_diff"])
                self.assertEqual(
                    diff_payload["result_diff"]["comparison"]["row_count"]["left"],
                    2,
                )
                self.assertEqual(
                    diff_payload["result_diff"]["comparison"]["row_count"]["right"],
                    1,
                )
                self.assertEqual(
                    diff_payload["result_diff"]["comparison"]["row_count"]["delta"],
                    -1,
                )
                self.assertTrue(
                    diff_payload["result_diff"]["comparison"]["schema"]["equal"]
                )
                self.assertEqual(
                    len(diff_payload["result_diff"]["comparison"]["preview"]["left"]["rows"]),
                    1,
                )
                self.assertEqual(
                    len(diff_payload["result_diff"]["comparison"]["preview"]["right"]["rows"]),
                    1,
                )

    def test_stream_sql_once_writes_snapshot_json_progress(self):
        with tempfile.TemporaryDirectory(prefix="velaria-stream-run-") as tmp:
            source_dir = pathlib.Path(tmp) / "source"
            source_dir.mkdir(parents=True)
            (source_dir / "part-000.csv").write_text(
                "key,value\nu1,1\nu1,2\nu2,4\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                run_id, run_dir = create_run("stream-sql-once", {"query": "demo"}, "0.0.test")
                result = velaria_cli._execute_stream_sql_once(
                    source_csv_dir=source_dir,
                    source_table="input_stream",
                    source_delimiter=",",
                    sink_table="output_sink",
                    sink_schema="key STRING, value_sum INT",
                    sink_path=run_dir / "artifacts" / "stream_result.csv",
                    sink_delimiter=",",
                    query=(
                        "INSERT INTO output_sink "
                        "SELECT key, SUM(value) AS value_sum FROM input_stream GROUP BY key"
                    ),
                    trigger_interval_ms=0,
                    checkpoint_delivery_mode="best-effort",
                    execution_mode="single-process",
                    local_workers=1,
                    max_inflight_partitions=0,
                    max_batches=0,
                    run_id=run_id,
                )
                self.assertIn("progress", result["payload"])
                progress_path = run_dir / "progress.jsonl"
                lines = [line for line in progress_path.read_text(encoding="utf-8").splitlines() if line]
                self.assertGreaterEqual(len(lines), 2)
                snapshots = [json.loads(line) for line in lines]
                self.assertIn("execution_mode", snapshots[-1])
                explain = json.loads((run_dir / "explain.json").read_text(encoding="utf-8"))
                self.assertIn("logical", explain)
                self.assertIn("physical", explain)
                self.assertIn("strategy", explain)


if __name__ == "__main__":
    unittest.main()
