from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from velaria.agentic_dsl import compile_rule_spec, compile_template_rule, parse_rule_spec
from velaria.agentic_search import search_datasets, search_events, search_fields, search_templates
from velaria.agentic_store import AgenticStore

from ._helpers import (
    _dataset_docs_from_store,
    _event_docs_from_store,
    _field_docs_from_store,
    _normalize_path,
)


def _normalize_monitor_create_payload(payload: dict[str, Any], store: AgenticStore | None = None) -> dict[str, Any]:
    source = dict(payload.get("source") or {})
    if not source:
        raise ValueError("source is required")
    if "source_id" not in source and payload.get("source_id"):
        source["source_id"] = payload["source_id"]
    if "kind" not in source and payload.get("source_kind"):
        source["kind"] = payload["source_kind"]
    if "path" not in source and payload.get("input_path"):
        source["path"] = str(_normalize_path(str(payload["input_path"])))
    if "input_type" not in source and payload.get("input_type"):
        source["input_type"] = payload["input_type"]
    if "kind" not in source:
        raise ValueError("source.kind is required")
    if "source_id" in source and "binding" not in source:
        source["binding"] = source["source_id"]
    if payload.get("dsl"):
        compiled = compile_rule_spec(dict(payload["dsl"]))
        name = str(payload.get("name") or compiled["rule_spec"]["name"])
    else:
        template_id = str(payload.get("template_id") or "")
        if not template_id:
            raise ValueError("template_id or dsl is required")
        template_params = dict(payload.get("template_params") or {})
        name = str(payload.get("name") or template_id)
        compiled = compile_rule_spec(
            compile_template_rule(
                template_id=template_id,
                template_params=template_params,
                source={"kind": source["kind"], "binding": source.get("binding") or source.get("path") or source.get("source_id")},
                execution_mode=str(payload.get("execution_mode") or "batch"),
                name=name,
            )
        )
    return {
        "name": name,
        "intent_text": payload.get("intent_text"),
        "source": source,
        "template_id": payload.get("template_id"),
        "template_params": template_params if not payload.get("dsl") else dict(payload.get("template_params") or {}),
        "compiled_rules": compiled["compiled_rules"],
        "execution_mode": compiled["execution_mode"],
        "interval_sec": int(payload.get("interval_sec", 60)),
        "cooldown_sec": int(payload.get("cooldown_sec", 300)),
        "tags": payload.get("tags") or [],
        "rule_spec": compiled["rule_spec"],
        "validation": {
            "status": "pending",
            "execution_spec": compiled["execution_spec"],
            "promotion_rule": compiled["promotion_rule"],
            "event_extraction": compiled["event_extraction"],
            "suppression_rule": compiled["suppression_rule"],
        },
        "enabled": bool(payload.get("enabled", False)),
    }


def _validate_monitor_payload(store: AgenticStore, monitor: dict[str, Any]) -> dict[str, Any]:
    validation = dict(monitor.get("validation") or {})
    errors: list[str] = []
    source = monitor.get("source") or {}
    if source.get("kind") == "external_event":
        source_id = source.get("source_id") or source.get("binding")
        source_meta = store.get_source(str(source_id)) if source_id else None
        if not source_id or source_meta is None:
            errors.append("external_event source not found")
        else:
            available_columns = {
                "event_time",
                "event_type",
                "source_key",
                "payload_json",
                "ingested_at",
                "source_id",
                *list((source_meta.get("schema_binding") or {}).get("field_mappings", {}).keys()),
            }
            template_params = dict(monitor.get("template_params") or {})
            for field_name in template_params.get("group_by") or []:
                normalized = str(field_name).strip()
                if normalized and normalized not in available_columns:
                    errors.append(
                        f"invalid group_by field: {normalized}; available columns: {', '.join(sorted(available_columns))}"
                    )
    elif source.get("kind") in {"saved_dataset", "local_file", "bitable"}:
        path = source.get("path")
        if not path:
            errors.append("source.path is required")
        elif not Path(str(path)).exists():
            errors.append(f"source path not found: {path}")
    try:
        parse_rule_spec(dict(monitor["rule_spec"]))
    except Exception as exc:
        errors.append(str(exc))
    compiled_rules = monitor.get("compiled_rules") or []
    if not compiled_rules:
        errors.append("compiled_rules must not be empty")
    if monitor.get("execution_mode") == "stream":
        window = (((validation.get("execution_spec") or {}).get("window")) or {})
        if not window:
            errors.append("stream execution requires window config")
    validation["status"] = "valid" if not errors else "invalid"
    validation["errors"] = errors
    return validation


def _monitor_payload_for_response(store: AgenticStore, monitor: dict[str, Any]) -> dict[str, Any]:
    response = dict(monitor)
    response["state"] = store.get_monitor_state(monitor["monitor_id"]) or {
        "monitor_id": monitor["monitor_id"],
        "status": "idle",
    }
    return response


def _monitor_from_intent_payload(store: AgenticStore, payload: dict[str, Any]) -> dict[str, Any]:
    query_text = str(payload.get("intent_text") or payload.get("query_text") or "").strip()
    if not query_text:
        raise ValueError("intent_text is required")
    top_k = int(payload.get("top_k", 5))
    grounding_payload = {
        "template_hits": search_templates(query_text, top_k=top_k),
        "event_hits": search_events(query_text, _event_docs_from_store(store), top_k=top_k),
        "dataset_hits": search_datasets(query_text, _dataset_docs_from_store(store), top_k=top_k),
        "field_hits": search_fields(query_text, _field_docs_from_store(store), top_k=top_k),
    }
    bundle = store.save_grounding_bundle(query_text, grounding_payload)
    template_hits = grounding_payload["template_hits"]
    if not template_hits:
        raise ValueError("no template candidates found for intent")
    template_id = str(payload.get("template_id") or template_hits[0]["target_id"])
    template_params = dict(payload.get("template_params") or {})
    normalized = _normalize_monitor_create_payload(
        {
            "name": payload.get("name") or query_text,
            "intent_text": query_text,
            "template_id": template_id,
            "template_params": template_params,
            "source": payload.get("source"),
            "execution_mode": payload.get("execution_mode", "batch"),
            "interval_sec": payload.get("interval_sec", 60),
            "cooldown_sec": payload.get("cooldown_sec", 300),
            "tags": payload.get("tags") or [],
        },
        store,
    )
    normalized["grounding_bundle_id"] = bundle["bundle_id"]
    normalized["validation"]["grounding_bundle_id"] = bundle["bundle_id"]
    return {"monitor": normalized, "grounding_bundle": bundle, "grounding_payload": grounding_payload}
