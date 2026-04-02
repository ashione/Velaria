import os
import pathlib
import tempfile
import unittest
from unittest import mock

from velaria.workspace.artifact_index import ArtifactIndex


class ArtifactIndexTest(unittest.TestCase):
    def test_insert_list_preview_and_cleanup(self):
        with tempfile.TemporaryDirectory(prefix="velaria-artifact-index-") as tmp:
            run_dir = pathlib.Path(tmp) / "runs" / "run-1"
            run_dir.mkdir(parents=True)
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = ArtifactIndex()
                index.upsert_run(
                    {
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:00Z",
                        "finished_at": "2026-04-01T10:00:02Z",
                        "status": "succeeded",
                        "action": "csv-sql",
                        "cli_args": {"query": "SELECT 1"},
                        "velaria_version": "0.0.test",
                        "run_dir": str(run_dir),
                        "run_name": "cn slow query",
                        "description": "top PSM and OQL for CN cache snapshot",
                        "error": None,
                        "details": {"source": "cn-cache"},
                    }
                )
                run_meta = index.get_run("run-1")
                self.assertIsNotNone(run_meta)
                self.assertEqual(run_meta["run_name"], "cn slow query")
                self.assertEqual(
                    run_meta["description"],
                    "top PSM and OQL for CN cache snapshot",
                )
                self.assertEqual(run_meta["details"]["source"], "cn-cache")
                index.insert_artifact(
                    {
                        "artifact_id": "artifact-1",
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:01Z",
                        "type": "file",
                        "uri": "file:///tmp/result.parquet",
                        "format": "parquet",
                        "row_count": 3,
                        "schema_json": ["name", "score"],
                        "preview_json": {"rows": [{"name": "alice", "score": 10}]},
                        "tags_json": ["result"],
                    }
                )
                artifacts = index.list_artifacts()
                self.assertEqual(len(artifacts), 1)
                self.assertEqual(artifacts[0]["artifact_id"], "artifact-1")
                self.assertEqual(artifacts[0]["schema_json"], ["name", "score"])
                self.assertEqual(artifacts[0]["preview_json"]["rows"][0]["name"], "alice")

                index.update_artifact_preview(
                    "artifact-1",
                    {"rows": [{"name": "bob", "score": 20}], "truncated": False},
                )
                artifact = index.get_artifact("artifact-1")
                self.assertIsNotNone(artifact)
                self.assertEqual(artifact["preview_json"]["rows"][0]["name"], "bob")

                cleanup = index.cleanup_runs(keep_last_n=0, delete_files=False)
                self.assertEqual(cleanup["deleted_run_ids"], ["run-1"])
                self.assertTrue(run_dir.exists())
                self.assertEqual(index.list_artifacts(), [])

    def test_cleanup_skips_running_runs_even_with_delete_files(self):
        with tempfile.TemporaryDirectory(prefix="velaria-artifact-running-") as tmp:
            run_dir = pathlib.Path(tmp) / "runs" / "run-1"
            run_dir.mkdir(parents=True)
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = ArtifactIndex()
                index.upsert_run(
                    {
                        "run_id": "run-1",
                        "created_at": "2026-04-01T10:00:00Z",
                        "finished_at": None,
                        "status": "running",
                        "action": "stream-sql-once",
                        "cli_args": {"query": "INSERT INTO sink SELECT * FROM source"},
                        "velaria_version": "0.0.test",
                        "run_dir": str(run_dir),
                    }
                )
                cleanup = index.cleanup_runs(keep_last_n=0, delete_files=True)
                self.assertEqual(cleanup["deleted_run_ids"], [])
                self.assertTrue(run_dir.exists())
                self.assertIsNotNone(index.get_run("run-1"))

    def test_keep_last_and_delete_files(self):
        with tempfile.TemporaryDirectory(prefix="velaria-artifact-keep-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = ArtifactIndex()
                run_ids = ["run-1", "run-2", "run-3"]
                for offset, run_id in enumerate(run_ids, start=1):
                    run_dir = pathlib.Path(tmp) / "runs" / run_id
                    run_dir.mkdir(parents=True)
                    index.upsert_run(
                        {
                            "run_id": run_id,
                            "created_at": f"2026-04-0{offset}T10:00:00Z",
                            "finished_at": None,
                            "status": "succeeded",
                            "action": "csv-sql",
                            "cli_args": {},
                            "velaria_version": "0.0.test",
                            "run_dir": str(run_dir),
                        }
                    )
                cleanup = index.cleanup_runs(keep_last_n=1, delete_files=True)
                self.assertEqual(set(cleanup["deleted_run_ids"]), {"run-1", "run-2"})
                self.assertFalse((pathlib.Path(tmp) / "runs" / "run-1").exists())
                self.assertFalse((pathlib.Path(tmp) / "runs" / "run-2").exists())
                self.assertTrue((pathlib.Path(tmp) / "runs" / "run-3").exists())


if __name__ == "__main__":
    unittest.main()
