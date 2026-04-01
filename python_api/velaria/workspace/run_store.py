from __future__ import annotations

import json
import pathlib
import secrets
import tempfile
from datetime import datetime, timezone
from typing import Any

from .paths import ensure_dirs, get_runs_dir
from .types import RunRecord


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{secrets.token_hex(4)}"


def get_run_dir(run_id: str) -> pathlib.Path:
    return get_runs_dir() / run_id


def get_run_file(run_id: str) -> pathlib.Path:
    return get_run_dir(run_id) / "run.json"


def _write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=str(path.parent),
        delete=False,
    ) as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
        tmp_path = pathlib.Path(handle.name)
    tmp_path.replace(path)


def read_run(run_id: str) -> dict[str, Any]:
    with get_run_file(run_id).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def update_run(run_id: str, **updates: Any) -> dict[str, Any]:
    payload = read_run(run_id)
    for key, value in updates.items():
        if value is not None:
            payload[key] = value
    _write_json(get_run_file(run_id), payload)
    return payload


def create_run(
    action: str,
    args: dict[str, Any],
    velaria_version: str | None,
    run_name: str | None = None,
) -> tuple[str, pathlib.Path]:
    ensure_dirs()
    run_id = _make_run_id()
    run_dir = get_run_dir(run_id)
    artifacts_dir = run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "stdout.log").touch()
    (run_dir / "stderr.log").touch()
    (run_dir / "progress.jsonl").touch()
    record = RunRecord(
        run_id=run_id,
        created_at=utc_now(),
        action=action,
        cli_args=args,
        velaria_version=velaria_version,
        run_dir=str(run_dir),
        run_name=run_name,
    )
    _write_json(run_dir / "run.json", record.to_dict())
    return run_id, run_dir


def write_inputs(run_id: str, payload: dict[str, Any]) -> pathlib.Path:
    path = get_run_dir(run_id) / "inputs.json"
    _write_json(path, payload)
    return path


def write_explain(run_id: str, payload: dict[str, Any]) -> pathlib.Path:
    path = get_run_dir(run_id) / "explain.json"
    _write_json(path, payload)
    return path


def append_progress_snapshot(run_id: str, snapshot_json: str) -> pathlib.Path:
    path = get_run_dir(run_id) / "progress.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(snapshot_json.rstrip("\n"))
        handle.write("\n")
    return path


def _append_log(run_id: str, filename: str, message: str) -> pathlib.Path:
    path = get_run_dir(run_id) / filename
    with path.open("a", encoding="utf-8") as handle:
        handle.write(message)
    return path


def append_stdout(run_id: str, message: str) -> pathlib.Path:
    return _append_log(run_id, "stdout.log", message)


def append_stderr(run_id: str, message: str) -> pathlib.Path:
    return _append_log(run_id, "stderr.log", message)


def finalize_run(
    run_id: str,
    status: str,
    finished_at: str | None = None,
    error: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    updates: dict[str, Any] = {
        "status": status,
        "finished_at": finished_at or utc_now(),
        "error": error,
    }
    if details:
        updates["details"] = details
    return update_run(run_id, **updates)
