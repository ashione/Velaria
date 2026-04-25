from __future__ import annotations

import json
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import velaria.cli as cli_impl
from velaria import (
    Session,
    __version__,
)
from velaria.workspace import (
    ArtifactIndex,
    append_stderr,
    create_run,
    finalize_run,
    read_run,
    write_inputs,
)
from velaria.agentic_store import AgenticStore
from velaria.agentic_runtime import execute_monitor_once
from velaria.agentic_runtime import coerce_result_rows, process_signal_rows

from ._helpers import (
    ApiRouteNotFoundError,
    _api_parts,
    _build_run_response,
    _default_embedding_model,
    _default_embedding_provider,
    _enrich_run,
    _error_response,
    _json_dumps,
    _load_dataframe,
    _normalize_path,
    _parse_csv_list,
    _parse_path_list,
    _parse_string_list,
    _preview_from_dataframe,
    _register_artifacts,
    _resolve_auto_input_payload,
    _safe_sql_identifier,
    _timestamp_suffix,
    _update_run_progress,
    _BITABLE_PAGE_SIZE,
    _BITABLE_TIMEOUT_SECONDS,
)
from .import_handlers import (
    _execute_bitable_import,
    _finalize_bitable_import_run,
    _preview_bitable_import,
)
from .analysis_handlers import _execute_file_sql
from .dataset_handlers import (
    _execute_embedding_build,
    _execute_hybrid_search,
    _execute_keyword_search,
)
from ._helpers import _execute_keyword_index_build
from .search_handlers import (
    handle_grounding,
    handle_search_datasets,
    handle_search_events,
    handle_search_fields,
    handle_search_templates,
)
from .agentic_handlers import (
    _monitor_from_intent_payload,
    _monitor_payload_for_response,
    _normalize_monitor_create_payload,
    _validate_monitor_payload,
)


try:
    from velaria.agentic_runtime import _parse_time
except ImportError:
    def _parse_time(raw: str | None) -> datetime | None:  # type: ignore[misc]
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None


@dataclass
class VelariaService:
    host: str
    port: int
    _embedding_provider_cache: dict[tuple[str, str | None], tuple[Any, str]] = field(default_factory=dict)
    _embedding_provider_lock: threading.Lock = field(default_factory=threading.Lock)
    _scheduler_stop: threading.Event = field(default_factory=threading.Event)
    _scheduler_thread: threading.Thread | None = None
    _realtime_runners: dict[str, Any] = field(default_factory=dict)
    _realtime_runners_lock: threading.Lock = field(default_factory=threading.Lock)

    def get_embedding_provider(self, provider_name: str, model_name: str | None):
        cache_key = (provider_name.strip().lower(), model_name or None)
        with self._embedding_provider_lock:
            cached = self._embedding_provider_cache.get(cache_key)
            if cached is not None:
                return cached
            created = cli_impl._make_embedding_provider(provider_name, model_name)
            self._embedding_provider_cache[cache_key] = created
            return created

    def _run_scheduler(self) -> None:
        while not self._scheduler_stop.is_set():
            try:
                with AgenticStore() as store:
                    now_dt = datetime.now(timezone.utc)
                    for monitor in store.list_monitors():
                        if not monitor.get("enabled"):
                            continue
                        source = monitor.get("source") or {}
                        if monitor.get("execution_mode") == "stream" and source.get("kind") == "external_event":
                            continue
                        state = store.get_monitor_state(monitor["monitor_id"]) or {}
                        last_run = _parse_time(state.get("last_run_at"))
                        if last_run is not None:
                            interval = max(1, int(monitor.get("interval_sec", 60)))
                            if now_dt - last_run < timedelta(seconds=interval):
                                continue
                        try:
                            execute_monitor_once(store, monitor["monitor_id"])
                        except Exception:
                            continue
            except Exception:
                pass
            self._scheduler_stop.wait(1.0)

    def _push_realtime_observation(self, source_id: str, observation: dict[str, Any]) -> None:
        with self._realtime_runners_lock:
            runners = [runner for runner in self._realtime_runners.values() if runner["source_id"] == source_id]
        if not runners:
            return
        row = {}
        for key in runners[0]["columns"]:
            value = observation.get(key)
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False, sort_keys=True)
            row[key] = value
        table = pa.table({key: [value] for key, value in row.items()})
        for runner in runners:
            runner["source"].push_arrow(table)

    def _process_realtime_sink_batch(self, runner: dict[str, Any], batch: pa.Table) -> None:
        with AgenticStore() as store:
            monitor = store.get_monitor(runner["monitor_id"])
            if monitor is None:
                return
            compiled_rule = dict(runner["compiled_rule"])
            result_rows = coerce_result_rows(batch.to_pylist())
            if not result_rows:
                return
            run_id, run_dir = create_run(
                "focus-event-realtime-batch",
                {"monitor_id": runner["monitor_id"], "execution_mode": "stream"},
                __version__,
                run_name=f"{monitor['name']}-realtime",
                description=f"Realtime focus-event batch for {monitor['name']}",
                tags=monitor.get("tags") or [],
            )
            write_inputs(run_id, {"monitor": monitor, "mode": "realtime-stream"})
            index = ArtifactIndex()
            try:
                index.upsert_run(read_run(run_id))
                path = Path(run_dir) / "artifacts" / "result.parquet"
                pq.write_table(batch, path)
                artifact = cli_impl._table_artifact(path, batch, ["result", "focus-event-realtime"])
                artifact["artifact_id"] = cli_impl._new_artifact_id()
                artifact["run_id"] = run_id
                artifact["created_at"] = cli_impl._utc_now()
                index.insert_artifact(artifact)
                emitted = process_signal_rows(
                    store,
                    monitor=monitor,
                    compiled_rule=compiled_rule,
                    result_rows=result_rows,
                    run_id=run_id,
                    artifact_ids=[artifact["artifact_id"]],
                )
                finalized = finalize_run(
                    run_id,
                    "succeeded",
                    details={
                        "focus_event_count": len(emitted["focus_events"]),
                        "signal_id": emitted["signal"]["signal_id"],
                    },
                )
                index.upsert_run(finalized)
            except Exception as exc:
                append_stderr(run_id, traceback.format_exc())
                finalized = finalize_run(run_id, "failed", error=str(exc), details={"monitor_id": runner["monitor_id"]})
                index.upsert_run(finalized)
            finally:
                index.close()

    def _start_realtime_runner(self, monitor_id: str) -> None:
        with AgenticStore() as store:
            monitor = store.get_monitor(monitor_id)
            if monitor is None:
                raise FileNotFoundError(f"monitor not found: {monitor_id}")
            source_binding = monitor.get("source") or {}
            if monitor.get("execution_mode") != "stream" or source_binding.get("kind") != "external_event":
                return
            source_id = str(source_binding.get("source_id") or source_binding.get("binding"))
            source_meta = store.get_source(source_id)
            if source_meta is None:
                raise FileNotFoundError(f"source not found: {source_id}")
            if any(runner["monitor_id"] == monitor_id for runner in self._realtime_runners.values()):
                return
            columns = [
                "event_time",
                "event_type",
                "source_key",
                "payload_json",
                "ingested_at",
                "source_id",
                *list((source_meta.get("schema_binding") or {}).get("field_mappings", {}).keys()),
            ]
        import velaria
        created_runner_ids: list[str] = []
        for compiled_rule in monitor["compiled_rules"] or []:
            session = velaria.Session()
            source = session.create_realtime_stream_source(columns)
            stream_df = session.read_realtime_stream_source(source)
            runner_id = f"{monitor_id}:{compiled_rule['rule_id']}"
            view_name = _safe_sql_identifier(
                f"input_table_{monitor_id}_{compiled_rule['rule_id']}_{uuid.uuid4().hex[:8]}"
            )
            session.create_temp_view(view_name, stream_df)
            sink = session.create_realtime_stream_sink()
            query_df = session.stream_sql(str(compiled_rule["sql"]).replace("input_table", view_name))
            query = query_df.write_stream_queue_sink(sink, trigger_interval_ms=50)
            stop_event = threading.Event()
            runner = {
                "runner_id": runner_id,
                "monitor_id": monitor_id,
                "rule_id": compiled_rule["rule_id"],
                "compiled_rule": dict(compiled_rule),
                "source_id": source_id,
                "columns": columns,
                "session": session,
                "source": source,
                "sink": sink,
                "query": query,
                "stop_event": stop_event,
            }

            def _query_worker(local_runner: dict[str, Any] = runner) -> None:
                try:
                    local_runner["query"].start()
                    with AgenticStore() as inner_store:
                        inner_store.upsert_monitor_state(
                            monitor_id,
                            {
                                "status": "running",
                                "stream_query_id": local_runner["query"].progress()["query_id"],
                                "last_error": None,
                            },
                        )
                    local_runner["query"].await_termination()
                except Exception as exc:
                    with AgenticStore() as inner_store:
                        inner_store.upsert_monitor_state(monitor_id, {"status": "error", "last_error": str(exc)})

            def _sink_worker(local_runner: dict[str, Any] = runner) -> None:
                while not local_runner["stop_event"].is_set():
                    batch = local_runner["sink"].poll_arrow()
                    if batch is None:
                        time.sleep(0.05)
                        continue
                    try:
                        self._process_realtime_sink_batch(local_runner, batch)
                    except Exception as exc:
                        with AgenticStore() as inner_store:
                            inner_store.upsert_monitor_state(monitor_id, {"status": "error", "last_error": str(exc)})
                        break

            query_thread = threading.Thread(target=_query_worker, daemon=True)
            sink_thread = threading.Thread(target=_sink_worker, daemon=True)
            runner["query_thread"] = query_thread
            runner["sink_thread"] = sink_thread
            with self._realtime_runners_lock:
                self._realtime_runners[runner_id] = runner
            with AgenticStore() as inner_store:
                inner_store.upsert_monitor_state(
                    monitor_id,
                    {
                        "status": "running",
                        "last_error": None,
                    },
                )
            created_runner_ids.append(runner_id)
            query_thread.start()
            sink_thread.start()

    def _stop_realtime_runner(self, monitor_id: str) -> None:
        with self._realtime_runners_lock:
            runner_ids = [runner_id for runner_id, runner in self._realtime_runners.items() if runner["monitor_id"] == monitor_id]
            runners = [self._realtime_runners.pop(runner_id) for runner_id in runner_ids]
        if not runners:
            return
        for runner in runners:
            runner["stop_event"].set()
            try:
                runner["source"].close()
            except Exception:
                pass
            try:
                runner["query"].stop()
            except Exception:
                pass
            for key in ("query_thread", "sink_thread"):
                thread = runner.get(key)
                if thread is not None:
                    thread.join(timeout=2)
        with AgenticStore() as store:
            store.upsert_monitor_state(monitor_id, {"status": "idle"})

    def build_handler(self):
        service = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "VelariaService/0.1"

            def _send_json(self, status: int, payload: dict[str, Any]) -> None:
                body = _json_dumps(payload)
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS")
                self.end_headers()
                self.wfile.write(body)

            def _read_json(self) -> dict[str, Any]:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length > 0 else b"{}"
                if not raw:
                    return {}
                return json.loads(raw.decode("utf-8"))

            def do_OPTIONS(self) -> None:  # noqa: N802
                self.send_response(HTTPStatus.NO_CONTENT)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS")
                self.end_headers()

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path
                try:
                    if path == "/health":
                        self._send_json(
                            HTTPStatus.OK,
                            {
                                "ok": True,
                                "service": "velaria-service",
                                "version": __version__,
                                "port": self.server.server_port,
                            },
                        )
                        return
                    parts = _api_parts(path)
                    if parts is None:
                        raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                    if parts == ("external-events", "sources"):
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, {"ok": True, "sources": store.list_sources()})
                            return
                    if parts == ("monitors",):
                        with AgenticStore() as store:
                            monitors = [_monitor_payload_for_response(store, monitor) for monitor in store.list_monitors()]
                            self._send_json(HTTPStatus.OK, {"ok": True, "monitors": monitors})
                            return
                    if len(parts) == 2 and parts[0] == "monitors":
                        with AgenticStore() as store:
                            monitor = store.get_monitor(parts[1])
                            if monitor is None:
                                raise FileNotFoundError(f"monitor not found: {parts[1]}")
                            self._send_json(HTTPStatus.OK, {"ok": True, "monitor": _monitor_payload_for_response(store, monitor)})
                            return
                    if parts == ("focus-events",):
                        query = parse_qs(parsed.query)
                        limit = int(query.get("limit", ["50"])[0])
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, {"ok": True, "focus_events": store.list_focus_events(limit=limit)})
                            return
                    if len(parts) == 2 and parts[0] == "focus-events":
                        with AgenticStore() as store:
                            event = store.get_focus_event(parts[1])
                            if event is None:
                                raise FileNotFoundError(f"focus event not found: {parts[1]}")
                            self._send_json(HTTPStatus.OK, {"ok": True, "focus_event": event})
                            return
                    if parts == ("signals",):
                        query = parse_qs(parsed.query)
                        limit = int(query.get("limit", ["50"])[0])
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, {"ok": True, "signals": store.list_signals(limit=limit)})
                            return
                    if len(parts) == 2 and parts[0] == "signals":
                        with AgenticStore() as store:
                            signal = store.get_signal(parts[1])
                            if signal is None:
                                raise FileNotFoundError(f"signal not found: {parts[1]}")
                            self._send_json(HTTPStatus.OK, {"ok": True, "signal": signal})
                            return
                    if parts == ("runs",):
                        query = parse_qs(parsed.query)
                        limit = int(query.get("limit", ["20"])[0])
                        index = ArtifactIndex()
                        try:
                            runs = index.list_runs(limit=limit)
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    "runs": runs,
                                },
                            )
                            return
                        finally:
                            index.close()
                    if len(parts) == 3 and parts[0] == "runs" and parts[2] == "result":
                        run_id = parts[1]
                        query = parse_qs(parsed.query)
                        limit = int(query.get("limit", ["20"])[0])
                        index = ArtifactIndex()
                        try:
                            run = index.get_run(run_id)
                            if run is None:
                                raise FileNotFoundError(f"run not found: {run_id}")
                            artifact = cli_impl._find_run_result_artifact(index, run_id)
                            preview = cli_impl._read_preview_for_artifact(artifact, limit=limit)
                            if artifact.get("preview_json") is None:
                                index.update_artifact_preview(artifact["artifact_id"], preview)
                                artifact = index.get_artifact(artifact["artifact_id"]) or artifact
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    "run": _enrich_run(index, run),
                                    "artifact": artifact,
                                    "preview": preview,
                                },
                            )
                            return
                        finally:
                            index.close()
                    if len(parts) == 2 and parts[0] == "runs":
                        run_id = parts[1]
                        index = ArtifactIndex()
                        try:
                            run = index.get_run(run_id)
                            if run is None:
                                raise FileNotFoundError(f"run not found: {run_id}")
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    "run": _enrich_run(index, run),
                                    "artifacts": index.list_artifacts(run_id=run_id, limit=100),
                                },
                            )
                            return
                        finally:
                            index.close()
                    if len(parts) == 3 and parts[0] == "artifacts" and parts[2] == "preview":
                        artifact_id = parts[1]
                        query = parse_qs(parsed.query)
                        limit = int(query.get("limit", ["20"])[0])
                        index = ArtifactIndex()
                        try:
                            artifact = index.get_artifact(artifact_id)
                            if artifact is None:
                                raise FileNotFoundError(f"artifact not found: {artifact_id}")
                            preview = cli_impl._read_preview_for_artifact(artifact, limit=limit)
                            if artifact.get("preview_json") is None:
                                index.update_artifact_preview(artifact_id, preview)
                                artifact = index.get_artifact(artifact_id) or artifact
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    "artifact": artifact,
                                    "preview": preview,
                                },
                            )
                            return
                        finally:
                            index.close()
                    raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                except Exception as exc:
                    status, payload = _error_response(exc)
                    self._send_json(status, payload)

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path
                try:
                    payload = self._read_json()
                    parts = _api_parts(path)
                    if parts is None:
                        raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                    if parts == ("external-events", "sources"):
                        with AgenticStore() as store:
                            source = store.upsert_source(
                                {
                                    "source_id": payload.get("source_id"),
                                    "kind": "external_event",
                                    "name": payload.get("name") or payload.get("source_id") or "external-event-source",
                                    "spec": dict(payload.get("spec") or {}),
                                    "schema_binding": dict(payload.get("schema_binding") or {}),
                                    "auth": dict(payload.get("auth") or {}),
                                    "metadata": dict(payload.get("metadata") or {}),
                                }
                            )
                            self._send_json(HTTPStatus.CREATED, {"ok": True, "source": source})
                            return
                    if len(parts) == 4 and parts[:2] == ("external-events", "sources") and parts[3] == "ingest":
                        source_id = parts[2]
                        with AgenticStore() as store:
                            event = store.append_external_event(source_id, payload)
                        service._push_realtime_observation(source_id, event)
                        self._send_json(HTTPStatus.ACCEPTED, {"ok": True, "observation": event})
                        return
                    if parts == ("search", "templates"):
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, handle_search_templates(store, payload))
                            return
                    if parts == ("search", "events"):
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, handle_search_events(store, payload))
                            return
                    if parts == ("search", "datasets"):
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, handle_search_datasets(store, payload))
                            return
                    if parts == ("search", "fields"):
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, handle_search_fields(store, payload))
                            return
                    if parts == ("grounding",):
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, handle_grounding(store, payload))
                            return
                    if parts == ("monitors", "from-intent"):
                        with AgenticStore() as store:
                            generated = _monitor_from_intent_payload(store, payload)
                            monitor = store.upsert_monitor(generated["monitor"])
                            self._send_json(
                                HTTPStatus.CREATED,
                                {
                                    "ok": True,
                                    "monitor": _monitor_payload_for_response(store, monitor),
                                    "grounding_bundle": generated["grounding_bundle"],
                                    "grounding_payload": generated["grounding_payload"],
                                },
                            )
                            return
                    if parts == ("monitors",):
                        with AgenticStore() as store:
                            monitor_payload = _normalize_monitor_create_payload(payload, store)
                            monitor = store.upsert_monitor(monitor_payload)
                            self._send_json(HTTPStatus.CREATED, {"ok": True, "monitor": _monitor_payload_for_response(store, monitor)})
                            return
                    if len(parts) == 3 and parts[0] == "monitors" and parts[2] == "validate":
                        with AgenticStore() as store:
                            monitor = store.get_monitor(parts[1])
                            if monitor is None:
                                raise FileNotFoundError(f"monitor not found: {parts[1]}")
                            validation = _validate_monitor_payload(store, monitor)
                            monitor["validation"] = validation
                            monitor = store.upsert_monitor(monitor)
                            self._send_json(HTTPStatus.OK, {"ok": True, "monitor": _monitor_payload_for_response(store, monitor)})
                            return
                    if len(parts) == 3 and parts[0] == "monitors" and parts[2] == "enable":
                        with AgenticStore() as store:
                            monitor = store.get_monitor(parts[1])
                            if monitor is None:
                                raise FileNotFoundError(f"monitor not found: {parts[1]}")
                            validation = _validate_monitor_payload(store, monitor)
                            if validation["status"] != "valid":
                                monitor["validation"] = validation
                                monitor = store.upsert_monitor(monitor)
                                raise ValueError("monitor validation failed")
                            monitor["validation"] = validation
                            store.upsert_monitor(monitor)
                            enabled = store.set_monitor_enabled(parts[1], True)
                        service._start_realtime_runner(parts[1])
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, {"ok": True, "monitor": _monitor_payload_for_response(store, enabled)})
                            return
                    if len(parts) == 3 and parts[0] == "monitors" and parts[2] == "disable":
                        service._stop_realtime_runner(parts[1])
                        with AgenticStore() as store:
                            monitor = store.set_monitor_enabled(parts[1], False)
                            self._send_json(HTTPStatus.OK, {"ok": True, "monitor": _monitor_payload_for_response(store, monitor)})
                            return
                    if len(parts) == 3 and parts[0] == "monitors" and parts[2] == "run":
                        with AgenticStore() as store:
                            result = execute_monitor_once(store, parts[1])
                            self._send_json(HTTPStatus.OK, {"ok": True, **result})
                            return
                    if parts == ("focus-events", "poll"):
                        consumer_id = str(payload.get("consumer_id") or "default")
                        with AgenticStore() as store:
                            result = store.poll_focus_events(consumer_id=consumer_id, limit=int(payload.get("limit", 20)), after_event_id=payload.get("after_event_id"))
                            self._send_json(HTTPStatus.OK, {"ok": True, **result})
                            return
                    if len(parts) == 3 and parts[0] == "focus-events" and parts[2] in {"consume", "archive"}:
                        with AgenticStore() as store:
                            status_name = "consumed" if parts[2] == "consume" else "archived"
                            event = store.update_focus_event_status(parts[1], status_name)
                            self._send_json(HTTPStatus.OK, {"ok": True, "focus_event": event})
                            return
                    if parts == ("import", "preview"):
                        if str(payload.get("input_type") or "auto").lower() == "bitable":
                            preview_payload = _preview_bitable_import(payload)
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    **preview_payload,
                                },
                            )
                            return
                        import velaria_service as _pkg
                        session = _pkg.Session()
                        resolved_input_type, _ = _pkg._resolve_auto_input_payload(session, payload)
                        df = _pkg._load_dataframe(session, payload)
                        preview = _pkg._preview_from_dataframe(df, limit=int(payload.get("limit", 50)))
                        self._send_json(
                            HTTPStatus.OK,
                            {
                                "ok": True,
                                "dataset": {
                                    "name": payload.get("dataset_name")
                                    or Path(payload["input_path"]).stem,
                                    "source_type": resolved_input_type,
                                    "source_path": str(_normalize_path(payload["input_path"])),
                                },
                                "preview": preview,
                            },
                        )
                        return
                    if parts == ("runs", "bitable-import"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        request_app_secret = payload.get("app_secret") or payload.get("bitable_app_secret")
                        embedding_config = payload.get("embedding_config")
                        keyword_index_config = payload.get("keyword_index_config")
                        action_args = {
                            "bitable_url": payload.get("bitable_url") or payload.get("input_path"),
                            "app_id": payload.get("app_id") or payload.get("bitable_app_id"),
                            "dataset_name": payload.get("dataset_name"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                            "timeout_seconds": int(payload.get("timeout_seconds", _BITABLE_TIMEOUT_SECONDS)),
                            "page_size": int(payload.get("page_size", _BITABLE_PAGE_SIZE)),
                            "embedding_config": embedding_config,
                            "keyword_index_config": keyword_index_config,
                        }
                        worker_args = {
                            **action_args,
                            "app_secret": request_app_secret,
                        }
                        run_id, run_dir = create_run(
                            "bitable-import",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name") or f"bitable-import-{_timestamp_suffix()}",
                            description=payload.get("description") or "Bitable import run",
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "bitable-import",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            created = read_run(run_id)
                            index.upsert_run(created)
                            worker = threading.Thread(
                                target=_finalize_bitable_import_run,
                                kwargs={
                                    "run_id": run_id,
                                    "run_dir": run_dir,
                                    "action_args": worker_args,
                                },
                                daemon=True,
                            )
                            worker.start()
                            self._send_json(
                                HTTPStatus.ACCEPTED,
                                {
                                    "ok": True,
                                    "run_id": run_id,
                                    "run": _enrich_run(index, created),
                                    "run_dir": str(run_dir),
                                },
                            )
                            return
                        finally:
                            index.close()
                    if parts == ("runs", "file-sql"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        action_args = {
                            "input_path": str(_normalize_path(payload["input_path"])),
                            "input_type": payload.get("input_type", "auto"),
                            "delimiter": payload.get("delimiter", ","),
                            "line_mode": payload.get("line_mode", "split"),
                            "regex_pattern": payload.get("regex_pattern"),
                            "mappings": payload.get("mappings"),
                            "columns": payload.get("columns"),
                            "json_format": payload.get("json_format", "json_lines"),
                            "table": payload.get("table", "input_table"),
                            "query": payload["query"],
                            "output_path": payload.get("output_path"),
                        }
                        run_id, run_dir = create_run(
                            "file-sql",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "file-sql",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            result = _execute_file_sql(action_args | {"preview_limit": payload.get("preview_limit", 50)}, run_id, run_dir)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded")
                            index.upsert_run(finalized)
                            self._send_json(HTTPStatus.OK, _build_run_response(index, finalized, run_dir=run_dir, result=result, artifacts=created_artifacts))
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    if parts == ("runs", "embedding-build"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        provider_name = payload.get("provider") or _default_embedding_provider()
                        resolved_model_name = _default_embedding_model(provider_name, payload.get("model"))
                        action_args = {
                            "input_path": str(_normalize_path(payload["input_path"])),
                            "input_type": payload.get("input_type", "auto"),
                            "delimiter": payload.get("delimiter", ","),
                            "line_mode": payload.get("line_mode", "split"),
                            "regex_pattern": payload.get("regex_pattern"),
                            "mappings": payload.get("mappings"),
                            "columns": payload.get("columns"),
                            "json_format": payload.get("json_format", "json_lines"),
                            "text_columns": payload.get("text_columns"),
                            "provider": provider_name,
                            "model": resolved_model_name,
                            "template_version": payload.get("template_version", "text-v1"),
                            "vector_column": payload.get("vector_column", "embedding"),
                            "doc_id_field": payload.get("doc_id_field", "doc_id"),
                            "source_updated_at_field": payload.get("source_updated_at_field", "source_updated_at"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                            "sheet_name": payload.get("sheet_name", 0),
                            "date_format": payload.get("date_format", "%Y-%m-%d"),
                        }
                        run_id, run_dir = create_run(
                            "embedding-build",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "embedding-build",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            provider, resolved_model = service.get_embedding_provider(
                                str(provider_name),
                                resolved_model_name,
                            )
                            result = _execute_embedding_build(action_args, run_dir, provider=provider, resolved_model=resolved_model)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded")
                            index.upsert_run(finalized)
                            self._send_json(
                                HTTPStatus.OK,
                                _build_run_response(
                                    index,
                                    finalized,
                                    run_dir=run_dir,
                                    result=result,
                                    artifacts=created_artifacts,
                                ),
                            )
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    if parts == ("runs", "keyword-index-build"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        dataset_paths = _parse_path_list(payload.get("dataset_paths"))
                        if payload.get("dataset_path"):
                            dataset_paths = [payload["dataset_path"], *dataset_paths]
                        action_args = {
                            "dataset_paths": [str(_normalize_path(path)) for path in dataset_paths],
                            "input_path": str(_normalize_path(payload["input_path"])) if payload.get("input_path") else None,
                            "input_type": payload.get("input_type", "auto"),
                            "delimiter": payload.get("delimiter", ","),
                            "line_mode": payload.get("line_mode", "split"),
                            "regex_pattern": payload.get("regex_pattern"),
                            "mappings": payload.get("mappings"),
                            "columns": payload.get("columns"),
                            "json_format": payload.get("json_format", "json_lines"),
                            "text_columns": payload.get("text_columns"),
                            "analyzer": payload.get("analyzer", "jieba"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                        }
                        run_id, run_dir = create_run(
                            "keyword-index-build",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "keyword-index-build",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            result = _execute_keyword_index_build(action_args, run_dir, run_id=run_id)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded", details=result.get("payload"))
                            index.upsert_run(finalized)
                            self._send_json(
                                HTTPStatus.OK,
                                _build_run_response(
                                    index,
                                    finalized,
                                    run_dir=run_dir,
                                    result=result,
                                    artifacts=created_artifacts,
                                ),
                            )
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    if parts == ("runs", "keyword-search"):
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        action_args = {
                            "index_path": str(_normalize_path(str(payload["index_path"])))
                            if payload.get("index_path")
                            else None,
                            "query_text": payload.get("query_text"),
                            "top_k": int(payload.get("top_k", 10)),
                            "where_sql": payload.get("where_sql"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                        }
                        run_id, run_dir = create_run(
                            "keyword-search",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "keyword-search",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            result = _execute_keyword_search(action_args, run_dir)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded")
                            index.upsert_run(finalized)
                            self._send_json(
                                HTTPStatus.OK,
                                _build_run_response(
                                    index,
                                    finalized,
                                    run_dir=run_dir,
                                    result=result,
                                    artifacts=created_artifacts,
                                ),
                            )
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    if parts in {("search", "hybrid"), ("runs", "hybrid-search")}:
                        tags = payload.get("tags") or []
                        if isinstance(tags, str):
                            tags = _parse_csv_list(tags)
                        provider_name = payload.get("provider") or _default_embedding_provider()
                        resolved_model_name = _default_embedding_model(provider_name, payload.get("model"))
                        action_args = {
                            "dataset_path": str(_normalize_path(str(payload["dataset_path"])))
                            if payload.get("dataset_path")
                            else None,
                            "query_text": payload["query_text"],
                            "provider": provider_name,
                            "model": resolved_model_name,
                            "template_version": payload.get("template_version", "text-v1"),
                            "top_k": int(payload.get("top_k", 10)),
                            "metric": payload.get("metric", "cosine"),
                            "where_sql": payload.get("where_sql"),
                            "vector_column": payload.get("vector_column", "embedding"),
                            "output_path": payload.get("output_path"),
                            "preview_limit": int(payload.get("preview_limit", 50)),
                        }
                        run_id, run_dir = create_run(
                            "hybrid-search",
                            action_args,
                            __version__,
                            run_name=payload.get("run_name"),
                            description=payload.get("description"),
                            tags=tags,
                        )
                        write_inputs(
                            run_id,
                            {
                                "action": "hybrid-search",
                                "action_args": action_args,
                                "run_name": payload.get("run_name"),
                                "description": payload.get("description"),
                                "tags": tags,
                            },
                        )
                        index = ArtifactIndex()
                        try:
                            index.upsert_run(read_run(run_id))
                            provider, resolved_model = service.get_embedding_provider(
                                str(provider_name),
                                resolved_model_name,
                            )
                            result = _execute_hybrid_search(action_args, run_dir, provider=provider, resolved_model=resolved_model)
                            created_artifacts = _register_artifacts(index, run_id, result.get("artifacts", []))
                            finalized = finalize_run(run_id, "succeeded")
                            index.upsert_run(finalized)
                            self._send_json(HTTPStatus.OK, _build_run_response(index, finalized, run_dir=run_dir, result=result, artifacts=created_artifacts))
                            return
                        except Exception as exc:
                            append_stderr(run_id, traceback.format_exc())
                            finalized = finalize_run(
                                run_id,
                                "failed",
                                error=str(exc),
                                details=cli_impl._error_payload_from_exception(exc),
                            )
                            index.upsert_run(finalized)
                            raise
                        finally:
                            index.close()
                    raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                except Exception as exc:
                    status, payload = _error_response(exc)
                    self._send_json(status, payload)

            def do_PATCH(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path
                try:
                    payload = self._read_json()
                    parts = _api_parts(path)
                    if parts is None:
                        raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                    if len(parts) == 2 and parts[0] == "monitors":
                        with AgenticStore() as store:
                            monitor = store.get_monitor(parts[1])
                            if monitor is None:
                                raise FileNotFoundError(f"monitor not found: {parts[1]}")
                            if "name" in payload:
                                monitor["name"] = payload["name"]
                            if "intent_text" in payload:
                                monitor["intent_text"] = payload["intent_text"]
                            if "interval_sec" in payload:
                                monitor["interval_sec"] = int(payload["interval_sec"])
                            if "cooldown_sec" in payload:
                                monitor["cooldown_sec"] = int(payload["cooldown_sec"])
                            if "tags" in payload:
                                monitor["tags"] = list(payload["tags"] or [])
                            requested_enabled = None
                            if "enabled" in payload:
                                requested_enabled = bool(payload["enabled"])
                            if "dsl" in payload or "template_id" in payload:
                                normalized = _normalize_monitor_create_payload({**monitor, **payload, "source": payload.get("source") or monitor["source"]}, store)
                                monitor.update(normalized)
                            if requested_enabled is not None:
                                if requested_enabled:
                                    validation = _validate_monitor_payload(store, monitor)
                                    monitor["validation"] = validation
                                    if validation["status"] != "valid":
                                        store.upsert_monitor(monitor)
                                        raise ValueError("monitor validation failed")
                                monitor["enabled"] = requested_enabled
                            updated = store.upsert_monitor(monitor)
                        if requested_enabled is True:
                            service._start_realtime_runner(parts[1])
                        elif requested_enabled is False:
                            service._stop_realtime_runner(parts[1])
                        with AgenticStore() as store:
                            self._send_json(HTTPStatus.OK, {"ok": True, "monitor": _monitor_payload_for_response(store, updated)})
                            return
                    raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                except Exception as exc:
                    status, payload = _error_response(exc)
                    self._send_json(status, payload)

            def do_DELETE(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path
                try:
                    parts = _api_parts(path)
                    if parts is None:
                        raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                    if len(parts) == 2 and parts[0] == "monitors":
                        service._stop_realtime_runner(parts[1])
                        with AgenticStore() as store:
                            deleted = store.delete_monitor(parts[1])
                            if not deleted:
                                raise FileNotFoundError(f"monitor not found: {parts[1]}")
                            self._send_json(HTTPStatus.OK, {"ok": True, "monitor_id": parts[1], "deleted": True})
                            return
                    if len(parts) == 2 and parts[0] == "runs":
                        run_id = parts[1]
                        index = ArtifactIndex()
                        try:
                            deleted = index.delete_run(run_id, delete_files=True)
                            self._send_json(
                                HTTPStatus.OK,
                                {
                                    "ok": True,
                                    **deleted,
                                },
                            )
                            return
                        finally:
                            index.close()
                    raise ApiRouteNotFoundError(f"unknown endpoint: {path}")
                except Exception as exc:
                    status, payload = _error_response(exc)
                    self._send_json(status, payload)

            def log_message(self, format: str, *args) -> None:  # noqa: A003
                return

        return Handler

    def serve_forever(self) -> None:
        server = ThreadingHTTPServer((self.host, self.port), self.build_handler())
        self._scheduler_stop.clear()
        self._scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self._scheduler_thread.start()
        try:
            with AgenticStore() as store:
                for monitor in store.list_monitors():
                    source = monitor.get("source") or {}
                    if monitor.get("enabled") and monitor.get("execution_mode") == "stream" and source.get("kind") == "external_event":
                        self._start_realtime_runner(monitor["monitor_id"])
        except Exception:
            pass
        print(
            json.dumps(
                {
                    "event": "ready",
                    "host": self.host,
                    "port": server.server_port,
                    "service": "velaria-service",
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        try:
            server.serve_forever()
        finally:
            self._scheduler_stop.set()
            if self._scheduler_thread is not None:
                self._scheduler_thread.join(timeout=2)
            with self._realtime_runners_lock:
                runner_ids = list(self._realtime_runners.keys())
            for monitor_id in runner_ids:
                self._stop_realtime_runner(monitor_id)
            server.server_close()
