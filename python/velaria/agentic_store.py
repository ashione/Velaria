from __future__ import annotations

import csv
import json
import pathlib
import secrets
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .workspace.paths import get_velaria_home


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _json_loads(payload: str | None, *, default: Any = None) -> Any:
    if not payload:
        return default
    return json.loads(payload)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


def get_agentic_dir() -> pathlib.Path:
    path = get_velaria_home() / "agentic"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_monitors_dir() -> pathlib.Path:
    path = get_velaria_home() / "monitors"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_external_events_dir() -> pathlib.Path:
    path = get_agentic_dir() / "external_events"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_external_event_streams_dir() -> pathlib.Path:
    path = get_agentic_dir() / "external_event_streams"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_agentic_index_path() -> pathlib.Path:
    return get_velaria_home() / "index" / "agentic.sqlite"


AGENTIC_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    schema_binding_json TEXT,
    auth_json TEXT,
    metadata_json TEXT,
    event_log_path TEXT,
    event_stream_dir TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitors (
    monitor_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    intent_text TEXT,
    source_binding_json TEXT NOT NULL,
    template_id TEXT,
    template_params_json TEXT NOT NULL,
    compiled_rules_json TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    interval_sec INTEGER NOT NULL,
    cooldown_sec INTEGER NOT NULL,
    tags_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    rule_spec_json TEXT NOT NULL,
    validation_json TEXT
);

CREATE TABLE IF NOT EXISTS monitor_states (
    monitor_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    last_run_at TEXT,
    last_success_at TEXT,
    last_snapshot_id TEXT,
    last_error TEXT,
    stream_query_id TEXT,
    active_window_count INTEGER,
    last_emitted_window_end TEXT,
    dropped_late_rows INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rule_states (
    rule_id TEXT PRIMARY KEY,
    monitor_id TEXT NOT NULL,
    last_triggered_at TEXT,
    cooldown_until TEXT,
    consecutive_hits INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY,
    monitor_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    result_rows_json TEXT NOT NULL,
    reason_summary TEXT,
    run_id TEXT,
    artifact_ids_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS focus_events (
    event_id TEXT PRIMARY KEY,
    monitor_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    triggered_at TEXT NOT NULL,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    key_fields_json TEXT NOT NULL,
    sample_rows_json TEXT NOT NULL,
    status TEXT NOT NULL,
    run_id TEXT,
    artifact_ids_json TEXT NOT NULL,
    context_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_cursors (
    consumer_id TEXT PRIMARY KEY,
    last_seen_event_id TEXT,
    last_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS grounding_bundles (
    bundle_id TEXT PRIMARY KEY,
    query_text TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sources_kind ON sources(kind);
CREATE INDEX IF NOT EXISTS idx_monitors_enabled ON monitors(enabled);
CREATE INDEX IF NOT EXISTS idx_focus_events_triggered_at ON focus_events(triggered_at);
CREATE INDEX IF NOT EXISTS idx_focus_events_monitor ON focus_events(monitor_id);
CREATE INDEX IF NOT EXISTS idx_signals_monitor ON signals(monitor_id);
"""


class AgenticStore:
    def __init__(self) -> None:
        get_agentic_dir()
        get_monitors_dir()
        get_external_events_dir()
        self.sqlite_path = get_agentic_index_path()
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.sqlite_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(AGENTIC_SQLITE_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "AgenticStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _write_monitor_file(self, monitor: dict[str, Any]) -> None:
        path = get_monitors_dir() / f"{monitor['monitor_id']}.json"
        path.write_text(json.dumps(monitor, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def _row_to_source(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "source_id": row["source_id"],
            "kind": row["kind"],
            "name": row["name"],
            "spec": _json_loads(row["spec_json"], default={}),
            "schema_binding": _json_loads(row["schema_binding_json"], default={}),
            "auth": _json_loads(row["auth_json"], default={}),
            "metadata": _json_loads(row["metadata_json"], default={}),
            "event_log_path": row["event_log_path"],
            "event_stream_dir": row["event_stream_dir"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def upsert_source(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = _utc_now()
        source_id = str(payload.get("source_id") or _new_id("source"))
        record = {
            "source_id": source_id,
            "kind": str(payload["kind"]),
            "name": str(payload.get("name") or source_id),
            "spec": dict(payload.get("spec") or {}),
            "schema_binding": dict(payload.get("schema_binding") or {}),
            "auth": dict(payload.get("auth") or {}),
            "metadata": dict(payload.get("metadata") or {}),
            "event_log_path": payload.get("event_log_path"),
            "event_stream_dir": payload.get("event_stream_dir"),
            "created_at": payload.get("created_at") or now,
            "updated_at": now,
        }
        if record["kind"] == "external_event" and not record["event_log_path"]:
            record["event_log_path"] = str(get_external_events_dir() / f"{source_id}.jsonl")
        if record["kind"] == "external_event" and not record["event_stream_dir"]:
            record["event_stream_dir"] = str(get_external_event_streams_dir() / source_id)
        self._conn.execute(
            """
            INSERT INTO sources (
                source_id, kind, name, spec_json, schema_binding_json, auth_json, metadata_json,
                event_log_path, event_stream_dir, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                kind=excluded.kind,
                name=excluded.name,
                spec_json=excluded.spec_json,
                schema_binding_json=excluded.schema_binding_json,
                auth_json=excluded.auth_json,
                metadata_json=excluded.metadata_json,
                event_log_path=excluded.event_log_path,
                event_stream_dir=excluded.event_stream_dir,
                updated_at=excluded.updated_at
            """,
            (
                record["source_id"],
                record["kind"],
                record["name"],
                _json_dumps(record["spec"]),
                _json_dumps(record["schema_binding"]),
                _json_dumps(record["auth"]),
                _json_dumps(record["metadata"]),
                record["event_log_path"],
                record["event_stream_dir"],
                record["created_at"],
                record["updated_at"],
            ),
        )
        self._conn.commit()
        return record

    def list_sources(self, *, kind: str | None = None) -> list[dict[str, Any]]:
        if kind:
            rows = self._conn.execute("SELECT * FROM sources WHERE kind = ? ORDER BY created_at DESC", (kind,)).fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM sources ORDER BY created_at DESC").fetchall()
        return [self._row_to_source(row) for row in rows]

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM sources WHERE source_id = ?", (source_id,)).fetchone()
        return self._row_to_source(row)

    def append_external_event(self, source_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        source = self.get_source(source_id)
        if source is None:
            raise FileNotFoundError(f"source not found: {source_id}")
        if source["kind"] != "external_event":
            raise ValueError(f"source is not external_event: {source_id}")
        binding = source.get("schema_binding") or {}
        now = _utc_now()
        record = {
            "event_id": _new_id("obs"),
            "event_time": str(payload.get(binding.get("time_field") or "event_time") or payload.get("event_time") or now),
            "event_type": str(payload.get(binding.get("type_field") or "event_type") or payload.get("event_type") or "observation"),
            "source_key": str(payload.get(binding.get("key_field") or "source_key") or payload.get("source_key") or source_id),
            "payload_json": payload,
            "ingested_at": now,
            "source_id": source_id,
        }
        for target_name, raw_name in (binding.get("field_mappings") or {}).items():
            if isinstance(raw_name, str):
                record[target_name] = payload.get(raw_name)
        path = pathlib.Path(source["event_log_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")
        stream_dir = pathlib.Path(str(source["event_stream_dir"]))
        stream_dir.mkdir(parents=True, exist_ok=True)
        csv_fields = [
            "event_time",
            "event_type",
            "source_key",
            "payload_json",
            "ingested_at",
            "source_id",
            *list((binding.get("field_mappings") or {}).keys()),
        ]
        csv_path = stream_dir / f"{record['ingested_at'].replace(':', '').replace('-', '')}_{record['event_id']}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=csv_fields)
            writer.writeheader()
            writer.writerow(
                {
                    field: (
                        json.dumps(record[field], ensure_ascii=False, sort_keys=True)
                        if isinstance(record.get(field), (dict, list))
                        else record.get(field)
                    )
                    for field in csv_fields
                }
            )
        return record

    def read_external_events(
        self,
        source_id: str,
        *,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int | None = None,
        time_field: str = "event_time",
    ) -> list[dict[str, Any]]:
        source = self.get_source(source_id)
        if source is None:
            raise FileNotFoundError(f"source not found: {source_id}")
        path = pathlib.Path(str(source["event_log_path"]))
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                event_time = str(row.get(time_field) or "")
                if start_time and event_time < start_time:
                    continue
                if end_time and event_time >= end_time:
                    continue
                rows.append(row)
        if limit is not None:
            return rows[-limit:]
        return rows

    def _row_to_monitor(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "monitor_id": row["monitor_id"],
            "name": row["name"],
            "enabled": bool(row["enabled"]),
            "intent_text": row["intent_text"],
            "source": _json_loads(row["source_binding_json"], default={}),
            "template_id": row["template_id"],
            "template_params": _json_loads(row["template_params_json"], default={}),
            "compiled_rules": _json_loads(row["compiled_rules_json"], default=[]),
            "execution_mode": row["execution_mode"],
            "interval_sec": int(row["interval_sec"]),
            "cooldown_sec": int(row["cooldown_sec"]),
            "tags": _json_loads(row["tags_json"], default=[]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "rule_spec": _json_loads(row["rule_spec_json"], default={}),
            "validation": _json_loads(row["validation_json"], default={}),
        }

    def upsert_monitor(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = _utc_now()
        monitor_id = str(payload.get("monitor_id") or _new_id("monitor"))
        record = {
            "monitor_id": monitor_id,
            "name": str(payload.get("name") or monitor_id),
            "enabled": bool(payload.get("enabled", False)),
            "intent_text": payload.get("intent_text"),
            "source": dict(payload.get("source") or {}),
            "template_id": payload.get("template_id"),
            "template_params": dict(payload.get("template_params") or {}),
            "compiled_rules": list(payload.get("compiled_rules") or []),
            "execution_mode": str(payload["execution_mode"]),
            "interval_sec": int(payload.get("interval_sec", 60)),
            "cooldown_sec": int(payload.get("cooldown_sec", 300)),
            "tags": list(payload.get("tags") or []),
            "created_at": payload.get("created_at") or now,
            "updated_at": now,
            "rule_spec": dict(payload.get("rule_spec") or {}),
            "validation": dict(payload.get("validation") or {}),
        }
        self._conn.execute(
            """
            INSERT INTO monitors (
                monitor_id, name, enabled, intent_text, source_binding_json, template_id,
                template_params_json, compiled_rules_json, execution_mode, interval_sec,
                cooldown_sec, tags_json, created_at, updated_at, rule_spec_json, validation_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(monitor_id) DO UPDATE SET
                name=excluded.name,
                enabled=excluded.enabled,
                intent_text=excluded.intent_text,
                source_binding_json=excluded.source_binding_json,
                template_id=excluded.template_id,
                template_params_json=excluded.template_params_json,
                compiled_rules_json=excluded.compiled_rules_json,
                execution_mode=excluded.execution_mode,
                interval_sec=excluded.interval_sec,
                cooldown_sec=excluded.cooldown_sec,
                tags_json=excluded.tags_json,
                updated_at=excluded.updated_at,
                rule_spec_json=excluded.rule_spec_json,
                validation_json=excluded.validation_json
            """,
            (
                record["monitor_id"],
                record["name"],
                1 if record["enabled"] else 0,
                record["intent_text"],
                _json_dumps(record["source"]),
                record["template_id"],
                _json_dumps(record["template_params"]),
                _json_dumps(record["compiled_rules"]),
                record["execution_mode"],
                record["interval_sec"],
                record["cooldown_sec"],
                _json_dumps(record["tags"]),
                record["created_at"],
                record["updated_at"],
                _json_dumps(record["rule_spec"]),
                _json_dumps(record["validation"]),
            ),
        )
        self._conn.commit()
        self._write_monitor_file(record)
        return record

    def get_monitor(self, monitor_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM monitors WHERE monitor_id = ?", (monitor_id,)).fetchone()
        return self._row_to_monitor(row)

    def list_monitors(self) -> list[dict[str, Any]]:
        rows = self._conn.execute("SELECT * FROM monitors ORDER BY created_at DESC").fetchall()
        return [self._row_to_monitor(row) for row in rows]

    def delete_monitor(self, monitor_id: str) -> bool:
        path = get_monitors_dir() / f"{monitor_id}.json"
        if path.exists():
            path.unlink()
        changed = self._conn.execute("DELETE FROM monitors WHERE monitor_id = ?", (monitor_id,)).rowcount
        self._conn.execute("DELETE FROM monitor_states WHERE monitor_id = ?", (monitor_id,))
        self._conn.execute("DELETE FROM rule_states WHERE monitor_id = ?", (monitor_id,))
        self._conn.commit()
        return bool(changed)

    def set_monitor_enabled(self, monitor_id: str, enabled: bool) -> dict[str, Any]:
        self._conn.execute(
            "UPDATE monitors SET enabled = ?, updated_at = ? WHERE monitor_id = ?",
            (1 if enabled else 0, _utc_now(), monitor_id),
        )
        self._conn.commit()
        monitor = self.get_monitor(monitor_id)
        if monitor is None:
            raise FileNotFoundError(f"monitor not found: {monitor_id}")
        self._write_monitor_file(monitor)
        return monitor

    def upsert_monitor_state(self, monitor_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        current = self.get_monitor_state(monitor_id) or {
            "monitor_id": monitor_id,
            "status": "idle",
            "last_run_at": None,
            "last_success_at": None,
            "last_snapshot_id": None,
            "last_error": None,
            "stream_query_id": None,
            "active_window_count": None,
            "last_emitted_window_end": None,
            "dropped_late_rows": 0,
        }
        merged = {**current, **updates}
        self._conn.execute(
            """
            INSERT INTO monitor_states (
                monitor_id, status, last_run_at, last_success_at, last_snapshot_id, last_error,
                stream_query_id, active_window_count, last_emitted_window_end, dropped_late_rows
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(monitor_id) DO UPDATE SET
                status=excluded.status,
                last_run_at=excluded.last_run_at,
                last_success_at=excluded.last_success_at,
                last_snapshot_id=excluded.last_snapshot_id,
                last_error=excluded.last_error,
                stream_query_id=excluded.stream_query_id,
                active_window_count=excluded.active_window_count,
                last_emitted_window_end=excluded.last_emitted_window_end,
                dropped_late_rows=excluded.dropped_late_rows
            """,
            (
                merged["monitor_id"],
                merged["status"],
                merged.get("last_run_at"),
                merged.get("last_success_at"),
                merged.get("last_snapshot_id"),
                merged.get("last_error"),
                merged.get("stream_query_id"),
                merged.get("active_window_count"),
                merged.get("last_emitted_window_end"),
                merged.get("dropped_late_rows", 0),
            ),
        )
        self._conn.commit()
        return merged

    def get_monitor_state(self, monitor_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM monitor_states WHERE monitor_id = ?", (monitor_id,)).fetchone()
        if row is None:
            return None
        return dict(row)

    def upsert_rule_state(self, rule_id: str, monitor_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM rule_states WHERE rule_id = ?", (rule_id,)).fetchone()
        current = dict(row) if row is not None else {
            "rule_id": rule_id,
            "monitor_id": monitor_id,
            "last_triggered_at": None,
            "cooldown_until": None,
            "consecutive_hits": 0,
        }
        merged = {**current, **updates, "rule_id": rule_id, "monitor_id": monitor_id}
        self._conn.execute(
            """
            INSERT INTO rule_states (
                rule_id, monitor_id, last_triggered_at, cooldown_until, consecutive_hits
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(rule_id) DO UPDATE SET
                monitor_id=excluded.monitor_id,
                last_triggered_at=excluded.last_triggered_at,
                cooldown_until=excluded.cooldown_until,
                consecutive_hits=excluded.consecutive_hits
            """,
            (
                merged["rule_id"],
                merged["monitor_id"],
                merged.get("last_triggered_at"),
                merged.get("cooldown_until"),
                int(merged.get("consecutive_hits", 0)),
            ),
        )
        self._conn.commit()
        return merged

    def get_rule_state(self, rule_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM rule_states WHERE rule_id = ?", (rule_id,)).fetchone()
        return dict(row) if row is not None else None

    def add_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "signal_id": str(payload.get("signal_id") or _new_id("signal")),
            "monitor_id": str(payload["monitor_id"]),
            "rule_id": str(payload["rule_id"]),
            "created_at": payload.get("created_at") or _utc_now(),
            "result_rows": list(payload.get("result_rows") or []),
            "reason_summary": payload.get("reason_summary"),
            "run_id": payload.get("run_id"),
            "artifact_ids": list(payload.get("artifact_ids") or []),
        }
        self._conn.execute(
            """
            INSERT INTO signals (
                signal_id, monitor_id, rule_id, created_at, result_rows_json, reason_summary, run_id, artifact_ids_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["signal_id"],
                record["monitor_id"],
                record["rule_id"],
                record["created_at"],
                _json_dumps(record["result_rows"]),
                record["reason_summary"],
                record["run_id"],
                _json_dumps(record["artifact_ids"]),
            ),
        )
        self._conn.commit()
        return record

    def get_signal(self, signal_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM signals WHERE signal_id = ?", (signal_id,)).fetchone()
        if row is None:
            return None
        return {
            "signal_id": row["signal_id"],
            "monitor_id": row["monitor_id"],
            "rule_id": row["rule_id"],
            "created_at": row["created_at"],
            "result_rows": _json_loads(row["result_rows_json"], default=[]),
            "reason_summary": row["reason_summary"],
            "run_id": row["run_id"],
            "artifact_ids": _json_loads(row["artifact_ids_json"], default=[]),
        }

    def list_signals(self, *, monitor_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if monitor_id:
            rows = self._conn.execute(
                "SELECT * FROM signals WHERE monitor_id = ? ORDER BY created_at DESC LIMIT ?",
                (monitor_id, limit),
            ).fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM signals ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [
            {
                "signal_id": row["signal_id"],
                "monitor_id": row["monitor_id"],
                "rule_id": row["rule_id"],
                "created_at": row["created_at"],
                "result_rows": _json_loads(row["result_rows_json"], default=[]),
                "reason_summary": row["reason_summary"],
                "run_id": row["run_id"],
                "artifact_ids": _json_loads(row["artifact_ids_json"], default=[]),
            }
            for row in rows
        ]

    def add_focus_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "event_id": str(payload.get("event_id") or _new_id("focus")),
            "monitor_id": str(payload["monitor_id"]),
            "rule_id": str(payload["rule_id"]),
            "triggered_at": payload.get("triggered_at") or _utc_now(),
            "severity": str(payload.get("severity") or "warning"),
            "title": str(payload["title"]),
            "summary": str(payload["summary"]),
            "key_fields": dict(payload.get("key_fields") or {}),
            "sample_rows": list(payload.get("sample_rows") or []),
            "status": str(payload.get("status") or "new"),
            "run_id": payload.get("run_id"),
            "artifact_ids": list(payload.get("artifact_ids") or []),
            "context_json": dict(payload.get("context_json") or {}),
        }
        self._conn.execute(
            """
            INSERT INTO focus_events (
                event_id, monitor_id, rule_id, triggered_at, severity, title, summary,
                key_fields_json, sample_rows_json, status, run_id, artifact_ids_json, context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["event_id"],
                record["monitor_id"],
                record["rule_id"],
                record["triggered_at"],
                record["severity"],
                record["title"],
                record["summary"],
                _json_dumps(record["key_fields"]),
                _json_dumps(record["sample_rows"]),
                record["status"],
                record["run_id"],
                _json_dumps(record["artifact_ids"]),
                _json_dumps(record["context_json"]),
            ),
        )
        self._conn.commit()
        return record

    def get_focus_event(self, event_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM focus_events WHERE event_id = ?", (event_id,)).fetchone()
        if row is None:
            return None
        return {
            "event_id": row["event_id"],
            "monitor_id": row["monitor_id"],
            "rule_id": row["rule_id"],
            "triggered_at": row["triggered_at"],
            "severity": row["severity"],
            "title": row["title"],
            "summary": row["summary"],
            "key_fields": _json_loads(row["key_fields_json"], default={}),
            "sample_rows": _json_loads(row["sample_rows_json"], default=[]),
            "status": row["status"],
            "run_id": row["run_id"],
            "artifact_ids": _json_loads(row["artifact_ids_json"], default=[]),
            "context_json": _json_loads(row["context_json"], default={}),
        }

    def list_focus_events(self, *, limit: int = 50, monitor_id: str | None = None) -> list[dict[str, Any]]:
        if monitor_id:
            rows = self._conn.execute(
                "SELECT * FROM focus_events WHERE monitor_id = ? ORDER BY triggered_at DESC LIMIT ?",
                (monitor_id, limit),
            ).fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM focus_events ORDER BY triggered_at DESC LIMIT ?", (limit,)).fetchall()
        return [self.get_focus_event(str(row["event_id"])) for row in rows]

    def update_focus_event_status(self, event_id: str, status: str) -> dict[str, Any]:
        self._conn.execute("UPDATE focus_events SET status = ? WHERE event_id = ?", (status, event_id))
        self._conn.commit()
        event = self.get_focus_event(event_id)
        if event is None:
            raise FileNotFoundError(f"focus event not found: {event_id}")
        return event

    def poll_focus_events(self, *, consumer_id: str, limit: int = 20, after_event_id: str | None = None) -> dict[str, Any]:
        if after_event_id:
            row = self._conn.execute("SELECT triggered_at FROM focus_events WHERE event_id = ?", (after_event_id,)).fetchone()
            after_time = row["triggered_at"] if row is not None else None
        else:
            cursor = self._conn.execute("SELECT * FROM event_cursors WHERE consumer_id = ?", (consumer_id,)).fetchone()
            after_time = cursor["last_seen_at"] if cursor is not None else None
            after_event_id = cursor["last_seen_event_id"] if cursor is not None else None
        if after_time:
            if after_event_id:
                rows = self._conn.execute(
                    """
                    SELECT event_id FROM focus_events
                    WHERE triggered_at > ? OR (triggered_at = ? AND event_id > ?)
                    ORDER BY triggered_at ASC, event_id ASC
                    LIMIT ?
                    """,
                    (after_time, after_time, after_event_id, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT event_id FROM focus_events WHERE triggered_at > ? ORDER BY triggered_at ASC, event_id ASC LIMIT ?",
                    (after_time, limit),
                ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT event_id FROM focus_events ORDER BY triggered_at ASC, event_id ASC LIMIT ?",
                (limit,),
            ).fetchall()
        events = [self.get_focus_event(str(row["event_id"])) for row in rows]
        if events:
            self._conn.execute(
                """
                INSERT INTO event_cursors (consumer_id, last_seen_event_id, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(consumer_id) DO UPDATE SET
                    last_seen_event_id=excluded.last_seen_event_id,
                    last_seen_at=excluded.last_seen_at
                """,
                (consumer_id, events[-1]["event_id"], events[-1]["triggered_at"]),
            )
            self._conn.commit()
        return {
            "consumer_id": consumer_id,
            "events": events,
        }

    def save_grounding_bundle(self, query_text: str, payload: dict[str, Any]) -> dict[str, Any]:
        bundle = {
            "bundle_id": _new_id("ground"),
            "query_text": query_text,
            "payload": payload,
            "created_at": _utc_now(),
        }
        self._conn.execute(
            "INSERT INTO grounding_bundles (bundle_id, query_text, payload_json, created_at) VALUES (?, ?, ?, ?)",
            (bundle["bundle_id"], bundle["query_text"], _json_dumps(bundle["payload"]), bundle["created_at"]),
        )
        self._conn.commit()
        return bundle

    def get_grounding_bundle(self, bundle_id: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM grounding_bundles WHERE bundle_id = ?", (bundle_id,)).fetchone()
        if row is None:
            return None
        return {
            "bundle_id": row["bundle_id"],
            "query_text": row["query_text"],
            "payload": _json_loads(row["payload_json"], default={}),
            "created_at": row["created_at"],
        }
