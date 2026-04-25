from __future__ import annotations

import copy
from typing import Any

from .agentic_templates import get_template


ALLOWED_COMPARISONS = {"==", "!=", ">", ">=", "<", "<=", "in", "not_in"}
ALLOWED_EXECUTION_MODES = {"batch", "stream"}
ALLOWED_SOURCE_KINDS = {"saved_dataset", "local_file", "bitable", "external_event"}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _as_dict(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _as_list(value: Any, *, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    return value


def _default_event_spec(name: str) -> dict[str, Any]:
    return {
        "title": name,
        "summary": "{reason_summary}",
        "severity": {"default": "warning", "rules": []},
        "key_fields": [],
        "sample_rows": 5,
    }


def compile_template_rule(
    *,
    template_id: str,
    template_params: dict[str, Any],
    source: dict[str, Any],
    execution_mode: str,
    name: str,
) -> dict[str, Any]:
    template = get_template(template_id)
    params = dict(template_params or {})
    mode = execution_mode.strip().lower()
    _require(mode in ALLOWED_EXECUTION_MODES, f"unsupported execution_mode: {execution_mode}")
    if template_id == "threshold":
        field = str(params["value_field"])
        threshold = params["threshold"]
        comparison = str(params.get("comparison") or ">=")
        _require(comparison in {">", ">=", "<", "<="}, "unsupported threshold comparison")
        return {
            "version": "v1",
            "name": name,
            "source": source,
            "execution": {"mode": mode} if mode == "batch" else {
                "mode": "stream",
                "window": {
                    "kind": "tumbling",
                    "time_semantics": "processing_time",
                    "size": "60s",
                },
            },
            "signal": {"sql": f"SELECT * FROM input_table WHERE {field} {comparison} {threshold}"},
            "promote": {"when": {"min_rows": 1}},
            "event": {
                "title": f"{field} threshold reached",
                "summary": f"{field} {comparison} {threshold}",
                "severity": {"default": template["severity_default"], "rules": []},
                "key_fields": [field],
                "sample_rows": 5,
            },
            "suppress": {"cooldown": "300s", "dedupe_by": [field]},
        }
    if template_id == "change":
        key_field = str(params["key_field"])
        value_field = str(params["value_field"])
        threshold = params["threshold"]
        return {
            "version": "v1",
            "name": name,
            "source": source,
            "execution": {
                "mode": "batch",
                "compare": {"current": "current_snapshot", "previous": "previous_snapshot"},
            },
            "signal": {
                "sql": (
                    "SELECT c.{key_field}, c.{value_field} AS current_value, "
                    "p.{value_field} AS previous_value, "
                    "c.{value_field} - p.{value_field} AS delta "
                    "FROM current_snapshot c "
                    "JOIN previous_snapshot p ON c.{key_field} = p.{key_field} "
                    "WHERE ABS(c.{value_field} - p.{value_field}) >= {threshold}"
                ).format(key_field=key_field, value_field=value_field, threshold=threshold)
            },
            "promote": {"when": {"min_rows": 1}},
            "event": {
                "title": f"{key_field} delta detected",
                "summary": "delta={delta}",
                "severity": {"default": template["severity_default"], "rules": [{"when": f"ABS(delta) >= {threshold}", "level": "warning"}]},
                "key_fields": [key_field, "delta"],
                "sample_rows": 5,
            },
            "suppress": {"cooldown": "300s", "dedupe_by": [key_field]},
        }
    if template_id == "ranking":
        score_field = str(params["score_field"])
        top_k = int(params["top_k"])
        return {
            "version": "v1",
            "name": name,
            "source": source,
            "execution": {"mode": mode},
            "signal": {"sql": f"SELECT * FROM input_table ORDER BY {score_field} DESC LIMIT {top_k}"},
            "promote": {"when": {"min_rows": 1}},
            "event": {
                "title": f"top {top_k} by {score_field}",
                "summary": f"score field {score_field}",
                "severity": {"default": template["severity_default"], "rules": []},
                "key_fields": [score_field],
                "sample_rows": top_k,
            },
            "suppress": {"cooldown": "300s", "dedupe_by": [score_field]},
        }
    if template_id == "window_count":
        count_threshold = int(params["count_threshold"])
        group_by = [str(item) for item in params.get("group_by") or []]
        select = ", ".join(group_by + ["COUNT(*) AS cnt"]) if group_by else "COUNT(*) AS cnt"
        group = f" GROUP BY {', '.join(group_by)}" if group_by else ""
        return {
            "version": "v1",
            "name": name,
            "source": source,
            "execution": {
                "mode": "stream",
                "window": {"kind": "tumbling", "time_semantics": "processing_time", "size": "60s"},
            },
            "signal": {"sql": f"SELECT {select} FROM input_table{group} HAVING cnt >= {count_threshold}"},
            "promote": {"when": {"min_rows": 1}},
            "event": {
                "title": "window count threshold reached",
                "summary": "cnt={cnt}",
                "severity": {"default": template["severity_default"], "rules": [{"when": f"cnt >= {count_threshold}", "level": "warning"}]},
                "key_fields": [*group_by, "cnt"],
                "sample_rows": 5,
            },
            "suppress": {"cooldown": "300s", "dedupe_by": group_by or ["cnt"]},
        }
    raise ValueError(f"unsupported template_id: {template_id}")


def validate_condition_tree(condition: Any, *, name: str) -> None:
    if isinstance(condition, dict):
        if "min_rows" in condition:
            _require(int(condition["min_rows"]) >= 0, f"{name}.min_rows must be >= 0")
            return
        keys = set(condition)
        control_keys = keys & {"all", "any", "not"}
        if control_keys:
            if "not" in condition:
                validate_condition_tree(condition["not"], name=f"{name}.not")
                return
            branch_key = "all" if "all" in condition else "any"
            for idx, item in enumerate(_as_list(condition[branch_key], name=f"{name}.{branch_key}")):
                validate_condition_tree(item, name=f"{name}.{branch_key}[{idx}]")
            return
        if {"field", "op"} <= keys:
            _require(str(condition["op"]) in ALLOWED_COMPARISONS, f"{name}.op must be one of {sorted(ALLOWED_COMPARISONS)}")
            return
    raise ValueError(f"invalid condition tree at {name}")


def parse_rule_spec(raw: dict[str, Any]) -> dict[str, Any]:
    spec = copy.deepcopy(_as_dict(raw, name="rule spec"))
    _require(str(spec.get("version") or "") == "v1", "rule spec version must be v1")
    _require(bool(spec.get("name")), "rule spec name is required")
    source = _as_dict(spec.get("source"), name="source")
    _require(str(source.get("kind") or "") in ALLOWED_SOURCE_KINDS, "source.kind is invalid")
    _require(bool(source.get("binding")), "source.binding is required")
    execution = _as_dict(spec.get("execution"), name="execution")
    mode = str(execution.get("mode") or "").lower()
    _require(mode in ALLOWED_EXECUTION_MODES, "execution.mode is invalid")
    if mode == "stream":
        window = _as_dict(execution.get("window"), name="execution.window")
        _require(str(window.get("kind") or "") in {"tumbling", "sliding"}, "stream window.kind is invalid")
        _require(str(window.get("time_semantics") or "") == "processing_time", "stream time_semantics must be processing_time in v1")
        _require(bool(window.get("size")), "stream window.size is required")
        if "slide" in window:
            _require(bool(window.get("slide")), "stream window.slide must not be empty")
    signal = _as_dict(spec.get("signal"), name="signal")
    _require(bool(signal.get("sql")), "signal.sql is required")
    promote = _as_dict(spec.get("promote"), name="promote")
    validate_condition_tree(promote.get("when"), name="promote.when")
    event = _as_dict(spec.get("event"), name="event")
    _require(bool(event.get("title")), "event.title is required")
    _require(bool(event.get("summary")), "event.summary is required")
    suppress = _as_dict(spec.get("suppress"), name="suppress")
    _require(bool(suppress.get("cooldown")), "suppress.cooldown is required")
    _require(isinstance(suppress.get("dedupe_by"), list), "suppress.dedupe_by must be a list")
    event.setdefault("severity", {"default": "warning", "rules": []})
    event.setdefault("key_fields", [])
    event.setdefault("sample_rows", 5)
    spec["execution"]["mode"] = mode
    return spec


def compile_rule_spec(raw: dict[str, Any]) -> dict[str, Any]:
    spec = parse_rule_spec(raw)
    mode = spec["execution"]["mode"]
    input_table = "current_snapshot" if mode == "batch" else "input_table"
    signal_sql = str(spec["signal"]["sql"]).replace("input_stream", input_table).replace("input_table", input_table)
    compiled_rule = {
        "rule_id": f"rule_{spec['name'].replace(' ', '_').lower()}",
        "template_id": spec.get("template_id"),
        "query_mode": mode,
        "sql": signal_sql,
        "title_template": spec["event"]["title"],
        "summary_template": spec["event"]["summary"],
        "severity": spec["event"].get("severity", {}).get("default", "warning"),
        "output_mapping": {
            "key_fields": list(spec["event"].get("key_fields") or []),
            "sample_row_limit": int(spec["event"].get("sample_rows", 5)),
        },
        "validation_status": "valid",
        "validation_error": None,
    }
    execution_mode = spec["execution"]["mode"]
    if execution_mode == "stream":
        execution = {
            "mode": "stream",
            "source_table": "input_table",
            "query_sql": signal_sql,
            "trigger_interval_ms": 1000,
            "checkpoint_delivery_mode": "at-least-once",
            "output_mode": "closed-window",
            "window": dict(spec["execution"]["window"]),
            "late_data_policy": "drop_and_count",
            "event_extraction_spec": {
                "signal_fields": [],
                "focus_promotion_condition": spec["promote"]["when"],
                "event_title_mapping": spec["event"]["title"],
                "event_summary_mapping": spec["event"]["summary"],
                "key_field_mapping": list(spec["event"].get("key_fields") or []),
                "sample_row_limit": int(spec["event"].get("sample_rows", 5)),
            },
        }
    else:
        execution = {
            "mode": "batch",
            "snapshot_mode": "compare" if "compare" in spec["execution"] else "single",
            "input_binding": spec["source"]["binding"],
            "comparison_mode": dict(spec["execution"].get("compare") or {}),
            "schedule": None,
        }
    return {
        "rule_spec": spec,
        "compiled_rules": [compiled_rule],
        "execution_mode": execution_mode,
        "execution_spec": execution,
        "promotion_rule": {
            "condition_tree": spec["promote"]["when"],
            "min_rows": int(spec["promote"]["when"].get("min_rows", 1)) if isinstance(spec["promote"]["when"], dict) else 1,
            "grouping_key": list(spec["event"].get("key_fields") or []),
            "promotion_mode": "row_based",
        },
        "event_extraction": {
            "signal_fields": [],
            "focus_promotion_condition": spec["promote"]["when"],
            "event_title_mapping": spec["event"]["title"],
            "event_summary_mapping": spec["event"]["summary"],
            "key_field_mapping": list(spec["event"].get("key_fields") or []),
            "sample_row_limit": int(spec["event"].get("sample_rows", 5)),
            "severity": spec["event"].get("severity", {"default": "warning", "rules": []}),
        },
        "suppression_rule": {
            "cooldown": spec["suppress"]["cooldown"],
            "dedupe_by": list(spec["suppress"].get("dedupe_by") or []),
            "reopen_policy": "new_after_cooldown",
        },
    }
