from __future__ import annotations

import argparse
import json
from typing import Any

from velaria.agentic_store import AgenticStore
from velaria.agentic_dsl import compile_rule_spec, compile_template_rule
from velaria.agentic_search import search_datasets, search_events, search_fields, search_templates
from velaria.agentic_runtime import execute_monitor_once

from velaria.cli._common import (
    CliStructuredError,
    CliUsageError,
    _emit_json,
)


def _search_templates_cli(args: argparse.Namespace) -> int:
    return _emit_json(
        {
            "ok": True,
            "hits": search_templates(args.query_text, top_k=args.top_k),
            "query_text": args.query_text,
            "retrieval_mode": "hybrid",
        }
    )


def _search_events_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        docs = [
            {
                "doc_id": event["event_id"],
                "target_kind": "event",
                "title": event["title"],
                "summary": event["summary"],
                "body": json.dumps(event.get("key_fields") or {}, ensure_ascii=False),
                "tags": [event["severity"]],
                "metadata": {"event_id": event["event_id"], "monitor_id": event["monitor_id"]},
            }
            for event in store.list_focus_events(limit=200)
        ]
        return _emit_json(
            {
                "ok": True,
                "hits": search_events(args.query_text, docs, top_k=args.top_k),
                "query_text": args.query_text,
                "retrieval_mode": "hybrid",
            }
        )


def _search_datasets_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        docs = []
        for source in store.list_sources():
            if source["kind"] not in {"saved_dataset", "local_file", "bitable"}:
                continue
            path = source["spec"].get("path") or source["spec"].get("input_path") or source.get("event_log_path")
            docs.append(
                {
                    "doc_id": source["source_id"],
                    "target_kind": "dataset",
                    "title": source["name"],
                    "summary": str(path or source["kind"]),
                    "body": json.dumps(source.get("metadata") or {}, ensure_ascii=False),
                    "tags": [source["kind"]],
                    "metadata": {"source_id": source["source_id"], "kind": source["kind"]},
                }
            )
        return _emit_json(
            {
                "ok": True,
                "hits": search_datasets(args.query_text, docs, top_k=args.top_k),
                "query_text": args.query_text,
                "retrieval_mode": "hybrid",
            }
        )


def _search_fields_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        docs = []
        for source in store.list_sources():
            schema_binding = source.get("schema_binding") or {}
            for name in (schema_binding.get("field_mappings") or {}).keys():
                docs.append(
                    {
                        "doc_id": f"{source['source_id']}:{name}",
                        "target_kind": "field",
                        "title": name,
                        "summary": f"field from {source['name']}",
                        "body": source["kind"],
                        "tags": [source["kind"]],
                        "metadata": {"source_id": source["source_id"], "field": name},
                    }
                )
        return _emit_json(
            {
                "ok": True,
                "hits": search_fields(args.query_text, docs, top_k=args.top_k),
                "query_text": args.query_text,
                "retrieval_mode": "hybrid",
            }
        )


def _grounding_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        payload = {
            "template_hits": search_templates(args.query_text, top_k=args.top_k),
            "event_hits": search_events(args.query_text, [
                {
                    "doc_id": event["event_id"],
                    "target_kind": "event",
                    "title": event["title"],
                    "summary": event["summary"],
                    "body": json.dumps(event.get("key_fields") or {}, ensure_ascii=False),
                    "tags": [event["severity"]],
                    "metadata": {"event_id": event["event_id"], "monitor_id": event["monitor_id"]},
                }
                for event in store.list_focus_events(limit=200)
            ], top_k=args.top_k),
            "dataset_hits": [],
            "field_hits": [],
        }
        bundle = store.save_grounding_bundle(args.query_text, payload)
        return _emit_json({"ok": True, **bundle})


def _monitor_create_from_intent_cli(args: argparse.Namespace) -> int:
    from velaria_service import _monitor_from_intent_payload
    with AgenticStore() as store:
        generated = _monitor_from_intent_payload(
            store,
            {
                "name": args.name,
                "intent_text": args.intent_text,
                "source": json.loads(args.source),
                "template_id": args.template_id,
                "template_params": json.loads(args.template_params or "{}"),
                "execution_mode": args.execution_mode,
                "interval_sec": args.interval_sec,
                "cooldown_sec": args.cooldown_sec,
                "tags": args.tags or [],
                "top_k": args.top_k,
            },
        )
        monitor = store.upsert_monitor(generated["monitor"])
        return _emit_json(
            {
                "ok": True,
                "monitor": monitor,
                "grounding_bundle": generated["grounding_bundle"],
                "grounding_payload": generated["grounding_payload"],
            }
        )


def _source_create_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        source = store.upsert_source(
            {
                "source_id": args.source_id,
                "kind": args.kind,
                "name": args.name or args.source_id or args.kind,
                "spec": {"path": args.path} if args.path else {},
                "schema_binding": json.loads(args.schema_binding) if args.schema_binding else {},
            }
        )
        return _emit_json({"ok": True, "source": source})


def _source_list_cli(_args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        return _emit_json({"ok": True, "sources": store.list_sources()})


def _source_ingest_cli(args: argparse.Namespace) -> int:
    payload = json.loads(args.payload)
    with AgenticStore() as store:
        row = store.append_external_event(args.source_id, payload)
        return _emit_json({"ok": True, "observation": row})


def _monitor_normalize_cli(args: argparse.Namespace) -> dict[str, Any]:
    if args.dsl:
        dsl = json.loads(args.dsl)
        compiled = compile_rule_spec(dsl)
        return {
            "name": args.name or dsl["name"],
            "intent_text": args.intent_text,
            "source": json.loads(args.source),
            "template_id": None,
            "template_params": {},
            "compiled_rules": compiled["compiled_rules"],
            "execution_mode": compiled["execution_mode"],
            "interval_sec": args.interval_sec,
            "cooldown_sec": args.cooldown_sec,
            "tags": args.tags or [],
            "rule_spec": compiled["rule_spec"],
            "validation": {
                "status": "pending",
                "execution_spec": compiled["execution_spec"],
                "promotion_rule": compiled["promotion_rule"],
                "event_extraction": compiled["event_extraction"],
                "suppression_rule": compiled["suppression_rule"],
            },
            "enabled": False,
        }
    if not args.template_id:
        raise CliUsageError("template_id or dsl is required", phase="argument_parse")
    source = json.loads(args.source)
    compiled = compile_rule_spec(
        compile_template_rule(
            template_id=args.template_id,
            template_params=json.loads(args.template_params or "{}"),
            source={"kind": source["kind"], "binding": source.get("binding") or source.get("source_id") or source.get("path")},
            execution_mode=args.execution_mode,
            name=args.name or args.template_id,
        )
    )
    return {
        "name": args.name or args.template_id,
        "intent_text": args.intent_text,
        "source": source,
        "template_id": args.template_id,
        "template_params": json.loads(args.template_params or "{}"),
        "compiled_rules": compiled["compiled_rules"],
        "execution_mode": compiled["execution_mode"],
        "interval_sec": args.interval_sec,
        "cooldown_sec": args.cooldown_sec,
        "tags": args.tags or [],
        "rule_spec": compiled["rule_spec"],
        "validation": {
            "status": "pending",
            "execution_spec": compiled["execution_spec"],
            "promotion_rule": compiled["promotion_rule"],
            "event_extraction": compiled["event_extraction"],
            "suppression_rule": compiled["suppression_rule"],
        },
        "enabled": False,
    }


def _monitor_create_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        monitor = store.upsert_monitor(_monitor_normalize_cli(args))
        return _emit_json({"ok": True, "monitor": monitor})


def _monitor_list_cli(_args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        return _emit_json({"ok": True, "monitors": store.list_monitors()})


def _monitor_show_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        monitor = store.get_monitor(args.monitor_id)
        if monitor is None:
            raise CliStructuredError(f"monitor not found: {args.monitor_id}", error_type="file_not_found", phase="monitor_lookup")
        return _emit_json({"ok": True, "monitor": monitor, "state": store.get_monitor_state(args.monitor_id)})


def _monitor_validate_cli(args: argparse.Namespace) -> int:
    from velaria_service import _validate_monitor_payload
    with AgenticStore() as store:
        monitor = store.get_monitor(args.monitor_id)
        if monitor is None:
            raise CliStructuredError(f"monitor not found: {args.monitor_id}", error_type="file_not_found", phase="monitor_lookup")
        monitor["validation"] = _validate_monitor_payload(store, monitor)
        monitor = store.upsert_monitor(monitor)
        return _emit_json({"ok": True, "monitor": monitor})


def _monitor_enable_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        monitor = store.set_monitor_enabled(args.monitor_id, True)
        return _emit_json({"ok": True, "monitor": monitor})


def _monitor_disable_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        monitor = store.set_monitor_enabled(args.monitor_id, False)
        return _emit_json({"ok": True, "monitor": monitor})


def _monitor_run_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        result = execute_monitor_once(store, args.monitor_id)
        return _emit_json({"ok": True, **result})


def _monitor_delete_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        deleted = store.delete_monitor(args.monitor_id)
        if not deleted:
            raise CliStructuredError(f"monitor not found: {args.monitor_id}", error_type="file_not_found", phase="monitor_lookup")
        return _emit_json({"ok": True, "monitor_id": args.monitor_id, "deleted": True})


def _focus_events_poll_cli(args: argparse.Namespace) -> int:
    with AgenticStore() as store:
        payload = store.poll_focus_events(consumer_id=args.consumer_id, limit=args.limit, after_event_id=args.after_event_id)
        return _emit_json({"ok": True, **payload})


def _focus_events_update_cli(args: argparse.Namespace, *, status_name: str) -> int:
    with AgenticStore() as store:
        event = store.update_focus_event_status(args.event_id, status_name)
        return _emit_json({"ok": True, "focus_event": event})


def register(subparsers: argparse._SubParsersAction) -> None:
    source_parser = subparsers.add_parser("source", help="Agentic source commands.")
    source_subparsers = source_parser.add_subparsers(dest="source_command", required=True)

    source_create = source_subparsers.add_parser("create", help="Create a source.")
    source_create.add_argument("--source-id")
    source_create.add_argument("--kind", required=True)
    source_create.add_argument("--name")
    source_create.add_argument("--path")
    source_create.add_argument("--schema-binding")

    source_list = source_subparsers.add_parser("list", help="List sources.")

    source_ingest = source_subparsers.add_parser("ingest", help="Append an external observation payload.")
    source_ingest.add_argument("--source-id", required=True)
    source_ingest.add_argument("--payload", required=True, help="JSON payload")

    search_parser = subparsers.add_parser("search", help="Agentic search commands.")
    search_subparsers = search_parser.add_subparsers(dest="search_command", required=True)
    for name in ("templates", "events", "datasets", "fields"):
        p = search_subparsers.add_parser(name, help=f"Search {name}.")
        p.add_argument("--query-text", required=True)
        p.add_argument("--top-k", type=int, default=10)

    grounding = subparsers.add_parser("grounding", help="Build a grounding bundle.")
    grounding.add_argument("--query-text", required=True)
    grounding.add_argument("--top-k", type=int, default=5)

    monitor_parser = subparsers.add_parser("monitor", help="Agentic monitor commands.")
    monitor_subparsers = monitor_parser.add_subparsers(dest="monitor_command", required=True)

    monitor_create = monitor_subparsers.add_parser("create", help="Create a monitor from template or DSL.")
    monitor_create.add_argument("--name")
    monitor_create.add_argument("--intent-text")
    monitor_create.add_argument("--source", required=True, help="JSON object for source binding")
    monitor_create.add_argument("--template-id")
    monitor_create.add_argument("--template-params")
    monitor_create.add_argument("--dsl", help="JSON rule spec")
    monitor_create.add_argument("--execution-mode", default="batch")
    monitor_create.add_argument("--interval-sec", type=int, default=60)
    monitor_create.add_argument("--cooldown-sec", type=int, default=300)
    monitor_create.add_argument("--tags", nargs="*")

    monitor_from_intent = monitor_subparsers.add_parser("create-from-intent", help="Create a monitor from natural-language intent.")
    monitor_from_intent.add_argument("--name")
    monitor_from_intent.add_argument("--intent-text", required=True)
    monitor_from_intent.add_argument("--source", required=True, help="JSON object for source binding")
    monitor_from_intent.add_argument("--template-id")
    monitor_from_intent.add_argument("--template-params")
    monitor_from_intent.add_argument("--execution-mode", default="batch")
    monitor_from_intent.add_argument("--interval-sec", type=int, default=60)
    monitor_from_intent.add_argument("--cooldown-sec", type=int, default=300)
    monitor_from_intent.add_argument("--tags", nargs="*")
    monitor_from_intent.add_argument("--top-k", type=int, default=5)

    monitor_list = monitor_subparsers.add_parser("list", help="List monitors.")

    monitor_show = monitor_subparsers.add_parser("show", help="Show a monitor.")
    monitor_show.add_argument("--monitor-id", required=True)

    monitor_validate = monitor_subparsers.add_parser("validate", help="Validate a monitor.")
    monitor_validate.add_argument("--monitor-id", required=True)

    monitor_enable = monitor_subparsers.add_parser("enable", help="Enable a monitor.")
    monitor_enable.add_argument("--monitor-id", required=True)

    monitor_disable = monitor_subparsers.add_parser("disable", help="Disable a monitor.")
    monitor_disable.add_argument("--monitor-id", required=True)

    monitor_run = monitor_subparsers.add_parser("run", help="Run a monitor once.")
    monitor_run.add_argument("--monitor-id", required=True)

    monitor_delete = monitor_subparsers.add_parser("delete", help="Delete a monitor.")
    monitor_delete.add_argument("--monitor-id", required=True)

    focus_events = subparsers.add_parser("focus-events", help="FocusEvent commands.")
    focus_subparsers = focus_events.add_subparsers(dest="focus_events_command", required=True)

    focus_poll = focus_subparsers.add_parser("poll", help="Poll focus events.")
    focus_poll.add_argument("--consumer-id", required=True)
    focus_poll.add_argument("--limit", type=int, default=20)
    focus_poll.add_argument("--after-event-id")

    focus_consume = focus_subparsers.add_parser("consume", help="Mark focus event consumed.")
    focus_consume.add_argument("--event-id", required=True)

    focus_archive = focus_subparsers.add_parser("archive", help="Mark focus event archived.")
    focus_archive.add_argument("--event-id", required=True)
