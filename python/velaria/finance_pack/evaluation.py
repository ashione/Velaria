from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from velaria.agentic_store import AgenticStore


FINANCE_INTELLIGENCE_EVALUATION_SOURCE_ID = "finance_intelligence_evaluations"


def _utc_payload_time() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload_json")
    if isinstance(payload, dict):
        return payload
    return row


def _feed(row: dict[str, Any]) -> str:
    return str(row.get("feed") or _payload(row).get("feed") or "unknown")


def finance_evaluation_source_binding() -> dict[str, Any]:
    return {
        "time_field": "event_time",
        "type_field": "event_type",
        "key_field": "evaluation_id",
        "field_mappings": {
            "evaluation_id": "evaluation_id",
            "watch_session_id": "watch_session_id",
            "intelligence_id": "intelligence_id",
            "signal_count": "signal_count",
            "row_count": "row_count",
            "quality_status": "quality_status",
        },
    }


def ensure_finance_evaluation_source(store: AgenticStore) -> None:
    if store.get_source(FINANCE_INTELLIGENCE_EVALUATION_SOURCE_ID) is not None:
        return
    store.upsert_source(
        {
            "source_id": FINANCE_INTELLIGENCE_EVALUATION_SOURCE_ID,
            "kind": "external_event",
            "name": "finance intelligence evaluations",
            "schema_binding": finance_evaluation_source_binding(),
            "metadata": {"domain": "finance", "workflow": "finance-intelligence", "runtime": "replay-evaluation"},
        }
    )


def append_finance_evaluation_event(payload: dict[str, Any]) -> dict[str, Any]:
    with AgenticStore() as store:
        ensure_finance_evaluation_source(store)
        return store.append_external_event(FINANCE_INTELLIGENCE_EVALUATION_SOURCE_ID, payload)


def read_finance_evaluations(watch_session_id: str | None = None) -> list[dict[str, Any]]:
    with AgenticStore() as store:
        if store.get_source(FINANCE_INTELLIGENCE_EVALUATION_SOURCE_ID) is None:
            return []
        rows = store.read_external_events(FINANCE_INTELLIGENCE_EVALUATION_SOURCE_ID)
    evaluations: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row.get("payload_json") or {})
        if watch_session_id and payload.get("watch_session_id") != watch_session_id:
            continue
        payload["event_id"] = row.get("event_id")
        payload["ingested_at"] = row.get("ingested_at")
        payload["source_id"] = row.get("source_id")
        evaluations.append(payload)
    evaluations.sort(key=lambda item: str(item.get("updated_at") or item.get("event_time") or item.get("ingested_at") or ""))
    return evaluations


def latest_finance_evaluation(watch_session_id: str) -> dict[str, Any] | None:
    evaluations = read_finance_evaluations(watch_session_id)
    return evaluations[-1] if evaluations else None


def evaluate_finance_session(
    *,
    watch_session_id: str,
    rows: list[dict[str, Any]],
    jobs: list[dict[str, Any]] | None = None,
    searches: list[dict[str, Any]] | None = None,
    indexes: list[dict[str, Any]] | None = None,
    intelligence_id: str | None = None,
) -> dict[str, Any]:
    jobs = jobs or []
    searches = searches or []
    indexes = indexes or []
    now = _utc_payload_time()
    row_payloads = [_payload(row) for row in rows]
    signal_rows = [row for row in rows if _feed(row) == "native_stream_signals"]
    signal_payloads = [_payload(row) for row in signal_rows]
    provider_quality = _provider_quality(rows)
    signal_quality = _signal_quality(signal_payloads, rows)
    retrieval_quality = _retrieval_quality(searches, indexes)
    runtime_quality = _runtime_quality(jobs)
    quality_status = _quality_status(provider_quality, signal_quality, retrieval_quality, runtime_quality)
    return {
        "evaluation_id": f"evaluation_{watch_session_id}_{now.replace(':', '').replace('-', '')}",
        "watch_session_id": watch_session_id,
        "intelligence_id": intelligence_id,
        "event_time": now,
        "updated_at": now,
        "event_type": "intelligence_evaluation",
        "source_key": watch_session_id,
        "row_count": len(row_payloads),
        "signal_count": len(signal_payloads),
        "quality_status": quality_status,
        "signal_quality": signal_quality,
        "provider_quality": provider_quality,
        "retrieval_quality": retrieval_quality,
        "runtime_quality": runtime_quality,
        "method": {
            "mode": "persisted_replay_only",
            "provider_refetch": False,
            "investment_advice": False,
        },
        "disclaimer": "Replay evaluation of persisted research evidence only; not investment advice.",
    }


def build_finance_evaluation_report(evaluation: dict[str, Any]) -> dict[str, Any]:
    signal_quality = dict(evaluation.get("signal_quality") or {})
    provider_quality = dict(evaluation.get("provider_quality") or {})
    retrieval_quality = dict(evaluation.get("retrieval_quality") or {})
    runtime_quality = dict(evaluation.get("runtime_quality") or {})
    findings: list[dict[str, Any]] = []
    if signal_quality.get("signal_count", 0) <= 0:
        findings.append({"type": "no_signals", "severity": "warning", "hint": "Run a watch session long enough to produce native stream signals."})
    if provider_quality.get("unavailable_count", 0) > 0:
        findings.append({"type": "provider_unavailable_rows", "severity": "info", "count": provider_quality.get("unavailable_count")})
    if not retrieval_quality.get("no_hash_embedding", True):
        findings.append({"type": "hash_embedding_detected", "severity": "error", "hint": "Disable hash embeddings for production finance retrieval."})
    if runtime_quality.get("job_count", 0) <= 0:
        findings.append({"type": "no_durable_jobs", "severity": "info", "hint": "Run finance intelligence jobs/status to verify runtime persistence."})
    return {
        "watch_session_id": evaluation.get("watch_session_id"),
        "evaluation_id": evaluation.get("evaluation_id"),
        "quality_status": evaluation.get("quality_status"),
        "signal_summary": {
            "signal_count": signal_quality.get("signal_count", 0),
            "signal_type_distribution": signal_quality.get("signal_type_distribution", {}),
            "outcome_availability": signal_quality.get("outcome_availability", {}),
        },
        "provider_summary": {
            "row_count": provider_quality.get("row_count", 0),
            "unavailable_count": provider_quality.get("unavailable_count", 0),
            "freshness_distribution": provider_quality.get("freshness_distribution", {}),
            "error_categories": provider_quality.get("error_categories", {}),
        },
        "retrieval_summary": {
            "search_count": retrieval_quality.get("search_count", 0),
            "index_count": retrieval_quality.get("index_count", 0),
            "semantic_status_distribution": retrieval_quality.get("semantic_status_distribution", {}),
            "index_status_distribution": retrieval_quality.get("index_status_distribution", {}),
            "no_hash_embedding": retrieval_quality.get("no_hash_embedding", True),
        },
        "runtime_summary": {
            "job_count": runtime_quality.get("job_count", 0),
            "status_distribution": runtime_quality.get("status_distribution", {}),
            "restart_count": runtime_quality.get("restart_count", 0),
            "artifact_available_count": runtime_quality.get("artifact_available_count", 0),
        },
        "findings": findings,
        "next_steps": [
            f"finance intelligence status --session-id {evaluation.get('watch_session_id')} --format json",
            f"finance intelligence jobs --session-id {evaluation.get('watch_session_id')} --format json",
            f"finance intelligence search --session-id {evaluation.get('watch_session_id')} --query \"signal provider retrieval quality\" --format json",
        ],
        "disclaimer": "Replay evaluation only; not investment advice.",
    }


def _provider_quality(rows: list[dict[str, Any]]) -> dict[str, Any]:
    freshness = Counter()
    providers = Counter()
    errors = Counter()
    feeds = Counter()
    unavailable_count = 0
    source_url_count = 0
    max_delay_sec: float | None = None
    for row in rows:
        payload = _payload(row)
        feeds[_feed(row)] += 1
        if payload.get("freshness"):
            freshness[str(payload.get("freshness"))] += 1
        if payload.get("provider"):
            providers[str(payload.get("provider"))] += 1
        if payload.get("source_url"):
            source_url_count += 1
        if payload.get("error_type"):
            errors[str(payload.get("error_type"))] += 1
        event_type = str(payload.get("event_type") or row.get("event_type") or "")
        if payload.get("error_type") or event_type.endswith("_unavailable") or event_type == "provider_unavailable":
            unavailable_count += 1
        try:
            if payload.get("delay_sec") is not None:
                delay_sec = float(payload.get("delay_sec"))
                max_delay_sec = delay_sec if max_delay_sec is None else max(max_delay_sec, delay_sec)
        except (TypeError, ValueError):
            pass
    return {
        "row_count": len(rows),
        "counts_by_feed": dict(feeds),
        "unavailable_count": unavailable_count,
        "freshness_distribution": dict(freshness),
        "provider_distribution": dict(providers),
        "source_url_count": source_url_count,
        "error_categories": dict(errors),
        "max_delay_sec": max_delay_sec,
    }


def _signal_quality(signal_payloads: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    signal_types = Counter(str(row.get("signal_type") or "unknown") for row in signal_payloads)
    symbols = Counter(str(row.get("symbol") or "unknown") for row in signal_payloads)
    future_outcomes = [_future_outcome(row, rows) for row in signal_payloads]
    available_outcomes = [item for item in future_outcomes if item.get("status") == "available"]
    return {
        "signal_count": len(signal_payloads),
        "signal_type_distribution": dict(signal_types),
        "symbol_distribution": dict(symbols),
        "outcome_availability": {
            "status": "available" if available_outcomes else "unavailable",
            "available_count": len(available_outcomes),
            "unavailable_count": len(signal_payloads) - len(available_outcomes),
            "reason": None if available_outcomes else "no_future_history_rows",
        },
        "sample_outcomes": future_outcomes[:5],
    }


def _future_outcome(signal: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    symbol = str(signal.get("symbol") or "")
    signal_time = str(signal.get("event_time") or "")
    future_history = []
    for row in rows:
        if _feed(row) != "history":
            continue
        payload = _payload(row)
        if str(payload.get("symbol") or "") != symbol:
            continue
        event_time = str(payload.get("event_time") or payload.get("date") or "")
        if signal_time and event_time <= signal_time:
            continue
        close_value = payload.get("close") if payload.get("close") is not None else payload.get("price")
        try:
            future_history.append((event_time, float(close_value)))
        except (TypeError, ValueError):
            continue
    if len(future_history) < 2:
        return {"symbol": symbol, "signal_type": signal.get("signal_type"), "status": "unavailable", "reason": "no_future_history_rows"}
    future_history.sort(key=lambda item: item[0])
    start = future_history[0][1]
    end = future_history[-1][1]
    if start == 0:
        return {"symbol": symbol, "signal_type": signal.get("signal_type"), "status": "unavailable", "reason": "zero_start_price"}
    return {"symbol": symbol, "signal_type": signal.get("signal_type"), "status": "available", "forward_return_pct": round((end - start) / start * 100.0, 6)}


def _retrieval_quality(searches: list[dict[str, Any]], indexes: list[dict[str, Any]]) -> dict[str, Any]:
    semantic_status = Counter()
    index_status = Counter()
    top_feeds = Counter()
    hash_detected = False
    for search in searches:
        payload = _payload(search)
        retrieval = payload.get("retrieval") if isinstance(payload.get("retrieval"), dict) else {}
        semantic = retrieval.get("semantic") if isinstance(retrieval.get("semantic"), dict) else {}
        if semantic.get("status"):
            semantic_status[str(semantic.get("status"))] += 1
        if retrieval.get("index_status"):
            index_status[str(retrieval.get("index_status"))] += 1
        if payload.get("top_target_kind"):
            top_feeds[str(payload.get("top_target_kind"))] += 1
        if "hash" in str(semantic.get("provider") or "").lower():
            hash_detected = True
    for index in indexes:
        payload = _payload(index)
        if payload.get("semantic_status"):
            semantic_status[str(payload.get("semantic_status"))] += 1
        if payload.get("index_status"):
            index_status[str(payload.get("index_status"))] += 1
        if "hash" in str(payload.get("semantic_provider") or "").lower():
            hash_detected = True
    return {
        "search_count": len(searches),
        "index_count": len(indexes),
        "semantic_status_distribution": dict(semantic_status),
        "index_status_distribution": dict(index_status),
        "top_evidence_feed_distribution": dict(top_feeds),
        "no_hash_embedding": not hash_detected,
    }


def _runtime_quality(jobs: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = Counter()
    job_types = Counter()
    artifact_available_count = 0
    restart_count = 0
    for job in jobs:
        payload = _payload(job)
        statuses[str(payload.get("status") or "unknown")] += 1
        job_types[str(payload.get("job_type") or "unknown")] += 1
        artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
        if any(value for value in artifacts.values()):
            artifact_available_count += 1
        run = payload.get("run") if isinstance(payload.get("run"), dict) else {}
        if str(run.get("event_type") or "") == "watch_session_async_resume":
            restart_count += 1
    return {
        "job_count": len(jobs),
        "status_distribution": dict(statuses),
        "job_type_distribution": dict(job_types),
        "restart_count": restart_count,
        "artifact_available_count": artifact_available_count,
    }


def _quality_status(provider_quality: dict[str, Any], signal_quality: dict[str, Any], retrieval_quality: dict[str, Any], runtime_quality: dict[str, Any]) -> str:
    if not retrieval_quality.get("no_hash_embedding", True):
        return "failed"
    if provider_quality.get("row_count", 0) <= 0:
        return "insufficient_data"
    if signal_quality.get("signal_count", 0) <= 0:
        return "no_signals"
    if runtime_quality.get("job_count", 0) <= 0:
        return "observable_without_jobs"
    return "ok"
