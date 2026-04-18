from __future__ import annotations

import json
import pathlib
import importlib
import re
from datetime import datetime, timedelta, timezone
from string import Formatter
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .workspace import ArtifactIndex, append_stderr, create_run, finalize_run, read_run, write_inputs
from .workspace.run_store import write_explain
from .agentic_store import AgenticStore, _utc_now


_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?\d+\.\d+$")


def _parse_duration_seconds(raw: str | int | float) -> int:
    if isinstance(raw, (int, float)):
        return int(raw)
    value = str(raw).strip().lower()
    if value.endswith("ms"):
        return max(1, int(float(value[:-2]) / 1000.0))
    if value.endswith("s"):
        return int(float(value[:-1]))
    if value.endswith("m"):
        return int(float(value[:-1]) * 60)
    if value.endswith("h"):
        return int(float(value[:-1]) * 3600)
    return int(float(value))


def _parse_time(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))


def _jsonl_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    if not rows:
        return pa.table({})
    normalized: list[dict[str, Any]] = []
    for row in rows:
        flat = dict(row)
        payload = flat.get("payload_json")
        if isinstance(payload, dict):
            for key, value in payload.items():
                flat.setdefault(key, value)
        if "payload_json" in flat and isinstance(flat["payload_json"], (dict, list)):
            flat["payload_json"] = json.dumps(flat["payload_json"], ensure_ascii=False, sort_keys=True)
        normalized.append(flat)
    return pa.Table.from_pylist(normalized)


def _empty_external_event_table(source_meta: dict[str, Any]) -> pa.Table:
    field_names = [
        "event_time",
        "event_type",
        "source_key",
        "payload_json",
        "ingested_at",
        "source_id",
        *list((source_meta.get("schema_binding") or {}).get("field_mappings", {}).keys()),
    ]
    return pa.table({name: pa.array([], type=pa.string()) for name in dict.fromkeys(field_names)})


def _coerce_scalar(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if text == "":
        return value
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    if _INT_RE.match(text):
        try:
            return int(text)
        except Exception:
            return value
    if _FLOAT_RE.match(text):
        try:
            return float(text)
        except Exception:
            return value
    return value


def coerce_result_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    coerced: list[dict[str, Any]] = []
    for row in rows:
        coerced.append({key: _coerce_scalar(value) for key, value in row.items()})
    return coerced


def _render_template(template: str, row: dict[str, Any]) -> str:
    values = {name: row.get(name, "") for _, name, _, _ in Formatter().parse(template) if name}
    try:
        return template.format(**values)
    except Exception:
        return template


def _eval_simple_expression(expr: str, row: dict[str, Any]) -> bool:
    scope = {
        key: value
        for key, value in row.items()
        if isinstance(value, (str, int, float, bool)) or value is None
    }
    scope["ABS"] = abs
    safe_expr = expr.replace("AND", " and ").replace("OR", " or ").replace("NOT", " not ")
    try:
        return bool(eval(safe_expr, {"__builtins__": {}}, scope))
    except Exception:
        return False


def _eval_condition_tree(condition: Any, row: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    if isinstance(condition, dict):
        if "min_rows" in condition:
            return len(rows) >= int(condition["min_rows"])
        if "all" in condition:
            return all(_eval_condition_tree(item, row, rows) for item in condition["all"])
        if "any" in condition:
            return any(_eval_condition_tree(item, row, rows) for item in condition["any"])
        if "not" in condition:
            return not _eval_condition_tree(condition["not"], row, rows)
        if "field" in condition:
            value = row.get(str(condition["field"]))
            target = condition.get("value")
            op = str(condition["op"])
            if op == "==":
                return value == target
            if op == "!=":
                return value != target
            if op == ">":
                return value is not None and value > target
            if op == ">=":
                return value is not None and value >= target
            if op == "<":
                return value is not None and value < target
            if op == "<=":
                return value is not None and value <= target
            if op == "in":
                return value in (target or [])
            if op == "not_in":
                return value not in (target or [])
    return False


def _compute_severity(event_spec: dict[str, Any], row: dict[str, Any]) -> str:
    severity = event_spec.get("severity") or {}
    for rule in severity.get("rules") or []:
        if _eval_simple_expression(str(rule.get("when") or ""), row):
            return str(rule.get("level") or severity.get("default") or "warning")
    return str(severity.get("default") or "warning")


def _should_emit(store: AgenticStore, monitor: dict[str, Any], compiled_rule: dict[str, Any], event_payload: dict[str, Any]) -> bool:
    rule_state = store.get_rule_state(compiled_rule["rule_id"])
    dedupe_fields = event_payload["context_json"].get("dedupe_by") or []
    dedupe_values = [str(event_payload["key_fields"].get(field, "")) for field in dedupe_fields]
    dedupe_key = "|".join(dedupe_values)
    if rule_state is not None and rule_state.get("cooldown_until"):
        cooldown_until = _parse_time(str(rule_state["cooldown_until"]))
        if cooldown_until is not None and datetime.now(timezone.utc) < cooldown_until:
            return False
    if dedupe_key:
        recent = store.list_focus_events(limit=10, monitor_id=monitor["monitor_id"])
        for item in recent:
            if item["rule_id"] != compiled_rule["rule_id"]:
                continue
            existing_key = "|".join(str(item["key_fields"].get(field, "")) for field in dedupe_fields)
            if existing_key == dedupe_key:
                triggered_at = _parse_time(item["triggered_at"])
                if triggered_at is None:
                    continue
                cooldown_sec = int(monitor.get("cooldown_sec", 300))
                if datetime.now(timezone.utc) - triggered_at < timedelta(seconds=cooldown_sec):
                    return False
    return True


def process_signal_rows(
    store: AgenticStore,
    *,
    monitor: dict[str, Any],
    compiled_rule: dict[str, Any],
    result_rows: list[dict[str, Any]],
    run_id: str,
    artifact_ids: list[str],
) -> dict[str, Any]:
    signal = store.add_signal(
        {
            "monitor_id": monitor["monitor_id"],
            "rule_id": compiled_rule["rule_id"],
            "result_rows": result_rows,
            "reason_summary": f"{len(result_rows)} rows matched",
            "run_id": run_id,
            "artifact_ids": artifact_ids,
        }
    )
    generated_events: list[dict[str, Any]] = []
    promotion_rule = monitor["validation"].get("promotion_rule") or {}
    event_spec = monitor["validation"].get("event_extraction") or {}
    suppression_rule = monitor["validation"].get("suppression_rule") or {}
    for row in result_rows:
        if not _eval_condition_tree(promotion_rule.get("condition_tree"), row, result_rows):
            continue
        key_fields = {field: row.get(field) for field in event_spec.get("key_field_mapping") or []}
        payload = {
            "monitor_id": monitor["monitor_id"],
            "rule_id": compiled_rule["rule_id"],
            "severity": _compute_severity(event_spec, row),
            "title": _render_template(str(event_spec.get("event_title_mapping") or compiled_rule["title_template"]), row),
            "summary": _render_template(str(event_spec.get("event_summary_mapping") or compiled_rule["summary_template"]), row),
            "key_fields": key_fields,
            "sample_rows": result_rows[: int(event_spec.get("sample_row_limit", 5))],
            "run_id": run_id,
            "artifact_ids": artifact_ids,
            "context_json": {
                "intent_text": monitor.get("intent_text"),
                "template_id": monitor.get("template_id"),
                "source_kind": (monitor.get("source") or {}).get("kind"),
                "dedupe_by": suppression_rule.get("dedupe_by") or [],
            },
        }
        if _should_emit(store, monitor, compiled_rule, payload):
            event = store.add_focus_event(payload)
            generated_events.append(event)
            cooldown_sec = int(monitor.get("cooldown_sec", 300))
            triggered_at = _parse_time(event["triggered_at"]) or datetime.now(timezone.utc)
            store.upsert_rule_state(
                compiled_rule["rule_id"],
                monitor["monitor_id"],
                {
                    "last_triggered_at": event["triggered_at"],
                    "cooldown_until": (triggered_at + timedelta(seconds=cooldown_sec)).isoformat().replace("+00:00", "Z"),
                    "consecutive_hits": (store.get_rule_state(compiled_rule["rule_id"]) or {}).get("consecutive_hits", 0) + 1,
                },
            )
    return {"signal": signal, "focus_events": generated_events}


def _run_query_table(session: Any, query_sql: str) -> pa.Table:
    result_df = session.sql(query_sql)
    return result_df.to_arrow()


def _materialize_snapshot_artifact(index: ArtifactIndex, run_id: str, run_dir: pathlib.Path, table: pa.Table, name: str) -> str | None:
    if table.num_rows == 0 and not table.schema.names:
        return None
    cli_impl = importlib.import_module("velaria.cli")
    path = run_dir / "artifacts" / f"{name}.parquet"
    pq.write_table(table, path)
    artifact = cli_impl._table_artifact(path, table, [name, "snapshot"])
    artifact["artifact_id"] = cli_impl._new_artifact_id()
    artifact["run_id"] = run_id
    artifact["created_at"] = cli_impl._utc_now()
    index.insert_artifact(artifact)
    return artifact["artifact_id"]


def _load_snapshot_table(snapshot_artifact: dict[str, Any] | None) -> pa.Table:
    if snapshot_artifact is None:
        return pa.table({})
    cli_impl = importlib.import_module("velaria.cli")
    path = pathlib.Path(cli_impl._path_from_uri(snapshot_artifact["uri"]))
    if not path.exists():
        return pa.table({})
    return pq.read_table(path)


def execute_monitor_once(store: AgenticStore, monitor_id: str) -> dict[str, Any]:
    velaria_mod = importlib.import_module("velaria")
    cli_impl = importlib.import_module("velaria.cli")
    Session = velaria_mod.Session
    monitor = store.get_monitor(monitor_id)
    if monitor is None:
        raise FileNotFoundError(f"monitor not found: {monitor_id}")
    source_binding = monitor["source"]
    now = _utc_now()
    run_id, run_dir = create_run(
        "focus-event-monitor",
        {
            "monitor_id": monitor_id,
            "execution_mode": monitor["execution_mode"],
            "source": source_binding,
        },
        None,
        run_name=monitor["name"],
        description=f"focus event monitor run for {monitor['name']}",
        tags=monitor.get("tags") or [],
    )
    write_inputs(run_id, {"monitor": monitor})
    index = ArtifactIndex()
    try:
        index.upsert_run(read_run(run_id))
        store.upsert_monitor_state(monitor_id, {"status": "running", "last_run_at": now, "last_error": None})
        session = Session()
        artifacts: list[str] = []
        current_end: datetime | None = None
        if source_binding["kind"] == "external_event":
            source_id = source_binding.get("source_id") or source_binding.get("binding")
            source = store.get_source(str(source_id))
            if source is None:
                raise FileNotFoundError(f"source not found: {source_id}")
            current_rows = store.read_external_events(source["source_id"])
            if monitor["execution_mode"] == "stream":
                spec = monitor["validation"].get("execution_spec") or {}
                window = (spec.get("window") or {}) if isinstance(spec, dict) else {}
                size_seconds = _parse_duration_seconds(window.get("size") or "60s")
                slide_seconds = _parse_duration_seconds(window.get("slide") or size_seconds)
                state = store.get_monitor_state(monitor_id) or {}
                last_end = _parse_time(state.get("last_emitted_window_end"))
                now_dt = datetime.now(timezone.utc)
                if last_end is None:
                    aligned_epoch = int(now_dt.timestamp()) // slide_seconds * slide_seconds
                    current_end = datetime.fromtimestamp(aligned_epoch + slide_seconds, tz=timezone.utc)
                    window_start = current_end - timedelta(seconds=size_seconds)
                else:
                    current_end = last_end + timedelta(seconds=slide_seconds)
                    window_start = current_end - timedelta(seconds=size_seconds)
                current_rows = store.read_external_events(
                    str(source["source_id"]),
                    start_time=window_start.isoformat().replace("+00:00", "Z"),
                    end_time=current_end.isoformat().replace("+00:00", "Z"),
                    time_field="ingested_at",
                )
            current_table = _jsonl_to_table(current_rows) if current_rows else _empty_external_event_table(source)
        else:
            payload = {"input_path": source_binding["path"], "input_type": source_binding.get("input_type", "auto")}
            current_df = __import__("velaria_service")._load_dataframe(session, payload)  # type: ignore[attr-defined]
            current_table = current_df.to_arrow()
        current_artifact_id = _materialize_snapshot_artifact(index, run_id, run_dir, current_table, "current_snapshot")
        if current_artifact_id:
            artifacts.append(current_artifact_id)
        if monitor["execution_mode"] != "batch" and current_table.num_rows == 0:
            explain = {
                "monitor_id": monitor_id,
                "execution_mode": monitor["execution_mode"],
                "signal_rows": 0,
                "focus_events": 0,
                "empty_window": True,
            }
            write_explain(run_id, explain)
            finalized = finalize_run(
                run_id,
                "succeeded",
                details={
                    "focus_event_count": 0,
                    "signal_ids": [],
                    "empty_window": True,
                },
            )
            index.upsert_run(finalized)
            state_updates = {"status": "idle", "last_success_at": now, "last_snapshot_id": current_artifact_id, "last_error": None}
            if current_end is not None:
                state_updates["last_emitted_window_end"] = current_end.isoformat().replace("+00:00", "Z")
            store.upsert_monitor_state(monitor_id, state_updates)
            return {
                "run_id": run_id,
                "signals": [],
                "focus_events": [],
                "artifacts": [index.get_artifact(artifact_id) for artifact_id in artifacts if index.get_artifact(artifact_id) is not None],
            }
        if monitor["execution_mode"] == "batch":
            previous_artifact = None
            state = store.get_monitor_state(monitor_id)
            last_snapshot_id = state.get("last_snapshot_id") if state else None
            if last_snapshot_id:
                previous_artifact = index.get_artifact(last_snapshot_id)
            previous_table = _load_snapshot_table(previous_artifact)
            previous_artifact_id = _materialize_snapshot_artifact(index, run_id, run_dir, previous_table, "previous_snapshot")
            if previous_artifact_id:
                artifacts.append(previous_artifact_id)
            session.create_temp_view("current_snapshot", session.create_dataframe_from_arrow(current_table))
            session.create_temp_view("previous_snapshot", session.create_dataframe_from_arrow(previous_table))
        else:
            input_view_name = f"input_table_{monitor_id}_{run_id[-8:]}"
            session.create_temp_view(input_view_name, session.create_dataframe_from_arrow(current_table))
        generated_events: list[dict[str, Any]] = []
        signal_records: list[dict[str, Any]] = []
        for compiled_rule in (monitor["compiled_rules"] or []):
            compiled_rule = dict(compiled_rule)
            query_sql = compiled_rule["sql"]
            if monitor["execution_mode"] != "batch":
                query_sql = query_sql.replace("input_table", input_view_name)
            result_table = _run_query_table(session, query_sql)
            result_path = run_dir / "artifacts" / f"result_{compiled_rule['rule_id']}.parquet"
            pq.write_table(result_table, result_path)
            result_artifact = cli_impl._table_artifact(result_path, result_table, ["result", "focus-event-monitor", compiled_rule["rule_id"]])
            result_artifact["artifact_id"] = cli_impl._new_artifact_id()
            result_artifact["run_id"] = run_id
            result_artifact["created_at"] = cli_impl._utc_now()
            index.insert_artifact(result_artifact)
            rule_artifact_ids = [*artifacts, result_artifact["artifact_id"]]
            artifacts.append(result_artifact["artifact_id"])
            result_rows = coerce_result_rows(result_table.to_pylist())
            emitted = process_signal_rows(
                store,
                monitor=monitor,
                compiled_rule=compiled_rule,
                result_rows=result_rows,
                run_id=run_id,
                artifact_ids=rule_artifact_ids,
            )
            signal_records.append(emitted["signal"])
            generated_events.extend(emitted["focus_events"])
        explain = {
            "monitor_id": monitor_id,
            "execution_mode": monitor["execution_mode"],
            "signal_rows": len(signal_records),
            "focus_events": len(generated_events),
        }
        write_explain(run_id, explain)
        finalized = finalize_run(
            run_id,
            "succeeded",
            details={
                "focus_event_count": len(generated_events),
                "signal_ids": [signal["signal_id"] for signal in signal_records],
            },
        )
        index.upsert_run(finalized)
        state_updates = {"status": "idle", "last_success_at": now, "last_snapshot_id": current_artifact_id, "last_error": None}
        if monitor["execution_mode"] == "stream":
            if current_end is not None:
                state_updates["last_emitted_window_end"] = current_end.isoformat().replace("+00:00", "Z")
        store.upsert_monitor_state(monitor_id, state_updates)
        return {
            "run_id": run_id,
            "signals": signal_records,
            "focus_events": generated_events,
            "artifacts": [index.get_artifact(artifact_id) for artifact_id in artifacts if index.get_artifact(artifact_id) is not None],
        }
    except Exception as exc:
        append_stderr(run_id, f"{exc}\n")
        finalized = finalize_run(run_id, "failed", error=str(exc), details={"monitor_id": monitor_id})
        index.upsert_run(finalized)
        store.upsert_monitor_state(monitor_id, {"status": "error", "last_error": str(exc)})
        raise
    finally:
        index.close()
