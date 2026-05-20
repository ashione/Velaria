from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from velaria.agentic_store import AgenticStore


FINANCE_INTELLIGENCE_JOB_SOURCE_ID = "finance_intelligence_jobs"


def _utc_payload_time() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def finance_job_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "job_id",
        "field_mappings": {
            "job_id": "job_id",
            "job_type": "job_type",
            "watch_session_id": "watch_session_id",
            "intelligence_id": "intelligence_id",
            "status": "status",
        },
    }


def ensure_finance_job_source(store: AgenticStore) -> None:
    if store.get_source(FINANCE_INTELLIGENCE_JOB_SOURCE_ID) is not None:
        return
    store.upsert_source(
        {
            "source_id": FINANCE_INTELLIGENCE_JOB_SOURCE_ID,
            "kind": "external_event",
            "name": "finance intelligence jobs",
            "schema_binding": finance_job_source_binding(),
            "metadata": {"domain": "finance", "workflow": "finance-intelligence", "runtime": "durable-job"},
        }
    )


def finance_job_payload(
    *,
    job_id: str,
    job_type: str,
    watch_session_id: str,
    status: str,
    intelligence_id: str | None = None,
    command: list[str] | None = None,
    summary: dict[str, Any] | None = None,
    artifacts: dict[str, Any] | None = None,
    run: dict[str, Any] | None = None,
    error: dict[str, Any] | None = None,
    next_steps: list[str] | None = None,
) -> dict[str, Any]:
    now = _utc_payload_time()
    payload: dict[str, Any] = {
        "job_id": job_id,
        "job_type": job_type,
        "watch_session_id": watch_session_id,
        "intelligence_id": intelligence_id,
        "status": status,
        "command": command or [],
        "summary": summary or {},
        "artifacts": artifacts or {},
        "run": run or {},
        "error": error,
        "next_steps": next_steps or [],
        "event_time": now,
        "updated_at": now,
        "event_type": f"finance_job_{status}",
        "source_key": job_id,
        "runtime": {
            "core_runtime": "velaria_native_realtime_stream",
            "data_runtime": "velaria_agentic_store",
            "ai_runtime": "velaria_cli_run",
        },
    }
    return payload


def watch_run_job_payload(
    run: dict[str, Any],
    *,
    intelligence_id: str | None = None,
    effective_status: str | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    session_id = str(run.get("session_id") or "")
    return finance_job_payload(
        job_id=f"watch_session:{session_id}",
        job_type="watch_session_job",
        watch_session_id=session_id,
        intelligence_id=intelligence_id,
        status=effective_status or str(run.get("status") or "unknown"),
        command=[str(item) for item in (run.get("argv") or [])],
        summary=summary or {},
        artifacts={"log_path": run.get("log_path")},
        run=run,
        next_steps=[
            f"finance intelligence status --session-id {session_id} --format json",
            f"finance intelligence search --session-id {session_id} --query \"market news signal\" --format json",
            f"finance intelligence report --session-id {session_id} --format json",
        ],
    )


def append_finance_job_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        ensure_finance_job_source(store)
        return store.append_external_event(FINANCE_INTELLIGENCE_JOB_SOURCE_ID, payload)


def read_finance_job_events(watch_session_id: str | None = None) -> list[dict[str, Any]]:
    with AgenticStore() as store:
        if store.get_source(FINANCE_INTELLIGENCE_JOB_SOURCE_ID) is None:
            return []
        rows = store.read_external_events(FINANCE_INTELLIGENCE_JOB_SOURCE_ID)
    events: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row.get("payload_json") or {})
        if watch_session_id and payload.get("watch_session_id") != watch_session_id:
            continue
        payload["event_id"] = row.get("event_id")
        payload["ingested_at"] = row.get("ingested_at")
        payload["source_id"] = row.get("source_id")
        events.append(payload)
    events.sort(key=lambda item: str(item.get("updated_at") or item.get("event_time") or item.get("ingested_at") or ""))
    return events


def latest_finance_jobs(watch_session_id: str | None = None) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for event in read_finance_job_events(watch_session_id):
        job_id = str(event.get("job_id") or "")
        if not job_id:
            continue
        latest[job_id] = event
    return sorted(latest.values(), key=lambda item: str(item.get("updated_at") or item.get("event_time") or ""), reverse=True)
