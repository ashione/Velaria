import os
import pathlib
import sqlite3
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

    def test_list_runs_filters_by_status_action_and_tag(self):
        with tempfile.TemporaryDirectory(prefix="velaria-run-list-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index = ArtifactIndex()
                for run_id, status, action, tags in [
                    ("run-1", "succeeded", "csv-sql", ["cn", "slow-query"]),
                    ("run-2", "failed", "csv-sql", ["my", "slow-query"]),
                    ("run-3", "succeeded", "vector-search", ["cn", "embedding"]),
                ]:
                    run_dir = pathlib.Path(tmp) / "runs" / run_id
                    run_dir.mkdir(parents=True)
                    index.upsert_run(
                        {
                            "run_id": run_id,
                            "created_at": f"2026-04-0{run_id[-1]}T10:00:00Z",
                            "finished_at": f"2026-04-0{run_id[-1]}T10:00:01Z",
                            "status": status,
                            "action": action,
                            "cli_args": {},
                            "velaria_version": "0.0.test",
                            "run_dir": str(run_dir),
                            "run_name": run_id,
                            "description": f"description for {run_id}",
                            "tags": tags,
                        }
                    )
                    index.insert_artifact(
                        {
                            "artifact_id": f"artifact-{run_id}",
                            "run_id": run_id,
                            "created_at": f"2026-04-0{run_id[-1]}T10:00:02Z",
                            "type": "file",
                            "uri": f"file:///tmp/{run_id}.json",
                            "format": "json",
                            "row_count": 1,
                            "schema_json": ["value"],
                            "preview_json": {"rows": [{"value": run_id}]},
                            "tags_json": ["result"],
                        }
                    )

                runs = index.list_runs(limit=10, status="succeeded", tag="cn")
                self.assertEqual([run["run_id"] for run in runs], ["run-3", "run-1"])
                self.assertEqual(runs[0]["tags"], ["cn", "embedding"])
                self.assertEqual(runs[0]["artifact_count"], 1)
                self.assertEqual(runs[0]["duration_ms"], 1000)

                csv_runs = index.list_runs(limit=10, action="csv-sql")
                self.assertEqual([run["run_id"] for run in csv_runs], ["run-2", "run-1"])

                searched = index.list_runs(limit=10, query="embedding")
                self.assertEqual([run["run_id"] for run in searched], ["run-3"])

    def test_existing_sqlite_index_without_tags_column_is_migrated(self):
        with tempfile.TemporaryDirectory(prefix="velaria-run-migrate-") as tmp:
            with mock.patch.dict(os.environ, {"VELARIA_HOME": tmp}):
                index_dir = pathlib.Path(tmp) / "index"
                index_dir.mkdir(parents=True, exist_ok=True)
                sqlite_path = index_dir / "artifacts.sqlite"
                conn = sqlite3.connect(sqlite_path)
                conn.executescript(
                    """
                    CREATE TABLE runs (
                        run_id TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        finished_at TEXT,
                        status TEXT NOT NULL,
                        action TEXT NOT NULL,
                        args_json TEXT NOT NULL,
                        velaria_version TEXT,
                        run_dir TEXT NOT NULL,
                        run_name TEXT,
                        description TEXT,
                        error TEXT,
                        details_json TEXT
                    );
                    CREATE TABLE artifacts (
                        artifact_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        type TEXT NOT NULL,
                        uri TEXT NOT NULL,
                        format TEXT NOT NULL,
                        row_count INTEGER,
                        schema_json TEXT,
                        preview_json TEXT,
                        tags_json TEXT
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT INTO runs (
                        run_id, created_at, finished_at, status, action, args_json, velaria_version,
                        run_dir, run_name, description, error, details_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "run-legacy",
                        "2026-04-01T10:00:00Z",
                        "2026-04-01T10:00:01Z",
                        "succeeded",
                        "csv-sql",
                        "{}",
                        "0.0.test",
                        str(pathlib.Path(tmp) / "runs" / "run-legacy"),
                        "legacy run",
                        "legacy description",
                        None,
                        "{}",
                    ),
                )
                conn.commit()
                conn.close()

                index = ArtifactIndex()
                run_meta = index.get_run("run-legacy")
                self.assertEqual(run_meta["tags"], [])

                run_dir = pathlib.Path(tmp) / "runs" / "run-new"
                run_dir.mkdir(parents=True)
                index.upsert_run(
                    {
                        "run_id": "run-new",
                        "created_at": "2026-04-02T10:00:00Z",
                        "finished_at": "2026-04-02T10:00:01Z",
                        "status": "succeeded",
                        "action": "csv-sql",
                        "cli_args": {},
                        "velaria_version": "0.0.test",
                        "run_dir": str(run_dir),
                        "tags": ["cn", "slow-query"],
                    }
                )
                migrated = index.get_run("run-new")
                self.assertEqual(migrated["tags"], ["cn", "slow-query"])

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
