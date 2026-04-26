"""SQLite-based session registry mapping Velaria session IDs to SDK session references."""
from __future__ import annotations

import json
import pathlib
import sqlite3
from datetime import datetime, timezone
from typing import Any

from velaria.workspace.paths import get_velaria_home

SCHEMA = """
CREATE TABLE IF NOT EXISTS ai_sessions (
    session_id TEXT PRIMARY KEY,
    runtime_type TEXT NOT NULL,
    runtime_session_ref TEXT NOT NULL,
    dataset_context_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_active_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
);
"""


class SessionRegistry:
    def __init__(self, db_path: pathlib.Path | None = None):
        if db_path is None:
            db_path = get_velaria_home() / "ai_sessions.sqlite"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)

    def register(
        self,
        session_id: str,
        runtime_type: str,
        runtime_session_ref: str,
        dataset_context: dict[str, Any],
    ) -> None:
        now = _utc_now()
        self._conn.execute(
            "INSERT OR REPLACE INTO ai_sessions VALUES (?,?,?,?,?,?,?)",
            (
                session_id,
                runtime_type,
                runtime_session_ref,
                json.dumps(dataset_context, ensure_ascii=False),
                now,
                now,
                "active",
            ),
        )
        self._conn.commit()

    def lookup(self, session_id: str, runtime_type: str | None = None) -> dict[str, Any] | None:
        if runtime_type:
            row = self._conn.execute(
                "SELECT * FROM ai_sessions WHERE session_id=? AND runtime_type=?",
                (session_id, runtime_type),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT * FROM ai_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
        if not row:
            return None
        return {**dict(row), "dataset_context": json.loads(row["dataset_context_json"])}

    def update_activity(self, session_id: str) -> None:
        self._conn.execute(
            "UPDATE ai_sessions SET last_active_at=? WHERE session_id=?",
            (_utc_now(), session_id),
        )
        self._conn.commit()

    def close_session(self, session_id: str) -> None:
        self._conn.execute(
            "UPDATE ai_sessions SET status='closed' WHERE session_id=?",
            (session_id,),
        )
        self._conn.commit()

    def list_active(self, runtime_type: str | None = None) -> list[dict[str, Any]]:
        if runtime_type:
            rows = self._conn.execute(
                "SELECT * FROM ai_sessions WHERE status='active' AND runtime_type=? ORDER BY last_active_at DESC",
                (runtime_type,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM ai_sessions WHERE status='active' ORDER BY last_active_at DESC"
            ).fetchall()
        return [
            {**dict(r), "dataset_context": json.loads(r["dataset_context_json"])}
            for r in rows
        ]

    def most_recent_active(self, runtime_type: str | None = None) -> dict[str, Any] | None:
        if runtime_type:
            row = self._conn.execute(
                "SELECT * FROM ai_sessions WHERE status='active' AND runtime_type=? ORDER BY last_active_at DESC LIMIT 1",
                (runtime_type,),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT * FROM ai_sessions WHERE status='active' ORDER BY last_active_at DESC LIMIT 1"
            ).fetchone()
        if not row:
            return None
        return {**dict(row), "dataset_context": json.loads(row["dataset_context_json"])}

    def close(self) -> None:
        self._conn.close()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
