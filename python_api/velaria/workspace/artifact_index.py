from __future__ import annotations

import json
import pathlib
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from .paths import ensure_dirs, get_index_dir

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    action TEXT NOT NULL,
    args_json TEXT NOT NULL,
    velaria_version TEXT,
    run_dir TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    type TEXT NOT NULL,
    uri TEXT NOT NULL,
    format TEXT NOT NULL,
    row_count INTEGER,
    schema_json TEXT,
    preview_json TEXT,
    tags_json TEXT,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at);
"""

TERMINAL_RUN_STATUSES = frozenset({"succeeded", "failed", "timed_out"})


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _json_loads(payload: str | None) -> Any:
    if not payload:
        return None
    return json.loads(payload)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ArtifactIndex:
    def __init__(self) -> None:
        ensure_dirs()
        self.sqlite_path = get_index_dir() / "artifacts.sqlite"
        self.fallback_path = get_index_dir() / "artifacts.jsonl"
        self.backend = "sqlite"
        self._conn: sqlite3.Connection | None = None
        try:
            self._conn = sqlite3.connect(self.sqlite_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.executescript(SQLITE_SCHEMA)
            self._conn.commit()
        except sqlite3.Error:
            self.backend = "jsonl"
            self._conn = None
            self.fallback_path.touch(exist_ok=True)

    def _append_event(self, payload: dict[str, Any]) -> None:
        with self.fallback_path.open("a", encoding="utf-8") as handle:
            handle.write(_json_dumps(payload))
            handle.write("\n")

    def _load_fallback_state(self) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        runs: dict[str, dict[str, Any]] = {}
        artifacts: dict[str, dict[str, Any]] = {}
        if not self.fallback_path.exists():
            return runs, artifacts
        with self.fallback_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                kind = event["kind"]
                if kind == "run_upsert":
                    payload = dict(event["payload"])
                    runs[payload["run_id"]] = payload
                elif kind == "artifact_upsert":
                    payload = dict(event["payload"])
                    artifacts[payload["artifact_id"]] = payload
                elif kind == "artifact_preview":
                    artifact_id = event["artifact_id"]
                    if artifact_id in artifacts:
                        artifacts[artifact_id]["preview_json"] = event["preview_json"]
                elif kind == "run_delete":
                    run_id = event["run_id"]
                    runs.pop(run_id, None)
                    for artifact_id in [
                        artifact["artifact_id"]
                        for artifact in artifacts.values()
                        if artifact["run_id"] == run_id
                    ]:
                        artifacts.pop(artifact_id, None)
        return runs, artifacts

    def upsert_run(self, run_meta: dict[str, Any]) -> None:
        payload = {
            "run_id": run_meta["run_id"],
            "created_at": run_meta["created_at"],
            "finished_at": run_meta.get("finished_at"),
            "status": run_meta["status"],
            "action": run_meta["action"],
            "args_json": _json_dumps(run_meta.get("cli_args", {})),
            "velaria_version": run_meta.get("velaria_version"),
            "run_dir": run_meta["run_dir"],
        }
        if self.backend == "sqlite":
            assert self._conn is not None
            self._conn.execute(
                """
                INSERT INTO runs (
                    run_id, created_at, finished_at, status, action, args_json, velaria_version, run_dir
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    created_at=excluded.created_at,
                    finished_at=excluded.finished_at,
                    status=excluded.status,
                    action=excluded.action,
                    args_json=excluded.args_json,
                    velaria_version=excluded.velaria_version,
                    run_dir=excluded.run_dir
                """,
                (
                    payload["run_id"],
                    payload["created_at"],
                    payload["finished_at"],
                    payload["status"],
                    payload["action"],
                    payload["args_json"],
                    payload["velaria_version"],
                    payload["run_dir"],
                ),
            )
            self._conn.commit()
            return
        self._append_event({"kind": "run_upsert", "payload": payload})

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        if self.backend == "sqlite":
            assert self._conn is not None
            row = self._conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            return {
                "run_id": row["run_id"],
                "created_at": row["created_at"],
                "finished_at": row["finished_at"],
                "status": row["status"],
                "action": row["action"],
                "cli_args": _json_loads(row["args_json"]) or {},
                "velaria_version": row["velaria_version"],
                "run_dir": row["run_dir"],
            }
        runs, _ = self._load_fallback_state()
        row = runs.get(run_id)
        if row is None:
            return None
        return {
            "run_id": row["run_id"],
            "created_at": row["created_at"],
            "finished_at": row.get("finished_at"),
            "status": row["status"],
            "action": row["action"],
            "cli_args": _json_loads(row.get("args_json")) or {},
            "velaria_version": row.get("velaria_version"),
            "run_dir": row["run_dir"],
        }

    def insert_artifact(self, artifact_meta: dict[str, Any]) -> None:
        payload = dict(artifact_meta)
        payload["schema_json"] = _json_dumps(payload.get("schema_json"))
        payload["preview_json"] = _json_dumps(payload.get("preview_json"))
        payload["tags_json"] = _json_dumps(payload.get("tags_json"))
        if self.backend == "sqlite":
            assert self._conn is not None
            self._conn.execute(
                """
                INSERT OR REPLACE INTO artifacts (
                    artifact_id, run_id, created_at, type, uri, format, row_count, schema_json, preview_json,
                    tags_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["artifact_id"],
                    payload["run_id"],
                    payload["created_at"],
                    payload["type"],
                    payload["uri"],
                    payload["format"],
                    payload.get("row_count"),
                    payload.get("schema_json"),
                    payload.get("preview_json"),
                    payload.get("tags_json"),
                ),
            )
            self._conn.commit()
            return
        self._append_event({"kind": "artifact_upsert", "payload": payload})

    def update_artifact_preview(self, artifact_id: str, preview_json: dict[str, Any]) -> None:
        encoded = _json_dumps(preview_json)
        if self.backend == "sqlite":
            assert self._conn is not None
            self._conn.execute(
                "UPDATE artifacts SET preview_json = ? WHERE artifact_id = ?",
                (encoded, artifact_id),
            )
            self._conn.commit()
            return
        self._append_event(
            {
                "kind": "artifact_preview",
                "artifact_id": artifact_id,
                "preview_json": encoded,
            }
        )

    def _artifact_from_row(self, row: dict[str, Any] | sqlite3.Row) -> dict[str, Any]:
        return {
            "artifact_id": row["artifact_id"],
            "run_id": row["run_id"],
            "created_at": row["created_at"],
            "type": row["type"],
            "uri": row["uri"],
            "format": row["format"],
            "row_count": row["row_count"],
            "schema_json": _json_loads(row["schema_json"]),
            "preview_json": _json_loads(row["preview_json"]),
            "tags_json": _json_loads(row["tags_json"]) or [],
        }

    def list_artifacts(
        self,
        limit: int = 50,
        run_id: str | None = None,
        since: str | None = None,
        tag: str | None = None,
    ) -> list[dict[str, Any]]:
        if self.backend == "sqlite":
            assert self._conn is not None
            clauses: list[str] = []
            params: list[Any] = []
            if run_id:
                clauses.append("run_id = ?")
                params.append(run_id)
            if since:
                clauses.append("created_at >= ?")
                params.append(since)
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = self._conn.execute(
                f"""
                SELECT * FROM artifacts
                {where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
            artifacts = [self._artifact_from_row(row) for row in rows]
        else:
            _, state = self._load_fallback_state()
            artifacts = list(state.values())
            if run_id:
                artifacts = [artifact for artifact in artifacts if artifact["run_id"] == run_id]
            if since:
                artifacts = [artifact for artifact in artifacts if artifact["created_at"] >= since]
            artifacts = [self._artifact_from_row(artifact) for artifact in artifacts]
            artifacts.sort(key=lambda item: item["created_at"], reverse=True)
            artifacts = artifacts[:limit]
        if tag:
            artifacts = [artifact for artifact in artifacts if tag in artifact["tags_json"]]
        return artifacts

    def get_artifact(self, artifact_id: str) -> dict[str, Any] | None:
        if self.backend == "sqlite":
            assert self._conn is not None
            row = self._conn.execute(
                "SELECT * FROM artifacts WHERE artifact_id = ?",
                (artifact_id,),
            ).fetchone()
            if row is None:
                return None
            return self._artifact_from_row(row)
        _, artifacts = self._load_fallback_state()
        row = artifacts.get(artifact_id)
        if row is None:
            return None
        return self._artifact_from_row(row)

    def _select_runs_for_cleanup(
        self,
        keep_last_n: int | None,
        ttl_days: int | None,
    ) -> list[dict[str, Any]]:
        if self.backend == "sqlite":
            assert self._conn is not None
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY created_at DESC"
            ).fetchall()
            runs = [
                {
                    "run_id": row["run_id"],
                    "created_at": row["created_at"],
                    "run_dir": row["run_dir"],
                    "status": row["status"],
                }
                for row in rows
            ]
        else:
            runs_state, _ = self._load_fallback_state()
            runs = [
                {
                    "run_id": row["run_id"],
                    "created_at": row["created_at"],
                    "run_dir": row["run_dir"],
                    "status": row["status"],
                }
                for row in runs_state.values()
            ]
            runs.sort(key=lambda item: item["created_at"], reverse=True)
        keep_ids = {run["run_id"] for run in runs[:keep_last_n]} if keep_last_n else set()
        threshold = None
        if ttl_days is not None:
            threshold = datetime.now(timezone.utc) - timedelta(days=ttl_days)
        selected: list[dict[str, Any]] = []
        for run in runs:
            if run["run_id"] in keep_ids:
                continue
            if run.get("status") not in TERMINAL_RUN_STATUSES:
                continue
            created_at = _parse_timestamp(run["created_at"])
            expired = threshold is not None and created_at is not None and created_at < threshold
            keep_overflow = keep_last_n is not None and run["run_id"] not in keep_ids
            if expired or keep_overflow:
                selected.append(run)
        return selected

    def cleanup_runs(
        self,
        keep_last_n: int | None = None,
        ttl_days: int | None = None,
        delete_files: bool = False,
    ) -> dict[str, Any]:
        selected = self._select_runs_for_cleanup(keep_last_n, ttl_days)
        deleted_run_ids = [run["run_id"] for run in selected]
        if self.backend == "sqlite":
            assert self._conn is not None
            for run_id in deleted_run_ids:
                self._conn.execute("DELETE FROM artifacts WHERE run_id = ?", (run_id,))
                self._conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
            self._conn.commit()
        else:
            for run_id in deleted_run_ids:
                self._append_event(
                    {
                        "kind": "run_delete",
                        "run_id": run_id,
                        "created_at": _utc_now(),
                    }
                )
        deleted_dirs: list[str] = []
        if delete_files:
            for run in selected:
                run_dir = pathlib.Path(run["run_dir"])
                if run_dir.exists():
                    shutil.rmtree(run_dir)
                    deleted_dirs.append(str(run_dir))
        return {
            "deleted_run_ids": deleted_run_ids,
            "deleted_run_dirs": deleted_dirs,
            "backend": self.backend,
        }
