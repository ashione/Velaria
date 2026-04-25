"""Local Velaria functions exposed to agent runtimes."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import pathlib
import re
import shlex
import unicodedata
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable


JsonDict = dict[str, Any]


@dataclass(frozen=True)
class LocalFunction:
    name: str
    description: str
    input_schema: JsonDict
    handler: Callable[[JsonDict], JsonDict]


def _tool_schema(properties: JsonDict, required: list[str] | None = None) -> JsonDict:
    return {
        "type": "object",
        "properties": properties,
        "required": required or [],
        "additionalProperties": False,
    }


def _table_preview(table: Any, limit: int) -> JsonDict:
    row_limit = max(0, min(int(limit), 200))
    return {
        "schema": list(table.schema.names),
        "rows": table.slice(0, min(table.num_rows, row_limit)).to_pylist(),
        "row_count": int(table.num_rows),
    }


def _velaria_read(args: JsonDict) -> JsonDict:
    from velaria import Session
    from velaria import read_excel

    original_path = str(args["path"])
    path, source_metadata = _materialize_source_path(original_path)
    limit = int(args.get("limit") or 20)
    session = Session()
    try:
        if _is_excel_path(path):
            table = read_excel(session, path).to_arrow()
        else:
            table = session.read(path).to_arrow()
    except Exception as exc:
        if not _is_csv_path(path):
            raise
        normalized = _normalize_source_for_sql(path, limit=limit)
        path = str(normalized["normalized_path"])
        source_metadata = {
            **source_metadata,
            "original_source_path": original_path,
            "normalization": normalized,
            "read_fallback_error": str(exc),
        }
        table = session.read(path).to_arrow()
    return {
        "source_path": path,
        **source_metadata,
        **_table_preview(table, limit),
    }


def _velaria_schema(args: JsonDict) -> JsonDict:
    path = str(args.get("path") or "")
    if not path:
        return {"schema": [], "table_name": str(args.get("table_name") or "input_table")}
    preview = _velaria_read({"path": path, "limit": 0})
    return {
        "schema": preview["schema"],
        "table_name": str(args.get("table_name") or "input_table"),
        "source_path": preview.get("source_path", path),
        **({"source_url": preview["source_url"]} if preview.get("source_url") else {}),
    }


def _prepare_session_with_source(args: JsonDict):
    from velaria import Session
    from velaria import read_excel

    session = Session()
    raw_source_path = str(args.get("source_path") or args.get("path") or "")
    table_name = str(args.get("table_name") or "input_table")
    if raw_source_path:
        source_path, _metadata = _materialize_source_path(raw_source_path)
        if _is_excel_path(source_path):
            df = read_excel(session, source_path)
        else:
            df = session.read(source_path)
        session.create_temp_view(table_name, df)
    return session


def _prepare_query_source(args: JsonDict, query: str) -> tuple[str, JsonDict, str, str]:
    original_source_path = str(args.get("source_path") or args.get("path") or "")
    source_path, source_metadata = _materialize_source_path(original_source_path) if original_source_path else ("", {})
    table_name = _resolve_query_table_name(str(args.get("table_name") or "input_table"), query)
    if source_path and _should_normalize_source(args, source_path, query):
        normalized = _normalize_source_for_sql(
            source_path,
            delimiter=str(args.get("delimiter") or ","),
            limit=int(args.get("limit") or 50),
        )
        source_path = str(normalized["normalized_path"])
        source_metadata = {
            **source_metadata,
            "normalization": normalized,
            "original_source_path": original_source_path or source_path,
        }
        query = _rewrite_query_columns(query, normalized.get("column_mapping") or {})
        table_name = _resolve_query_table_name(table_name, query)
    return source_path, source_metadata, table_name, query


def _velaria_sql(args: JsonDict) -> JsonDict:
    query = str(args["query"])
    limit = int(args.get("limit") or 50)
    source_path, source_metadata, table_name, query = _prepare_query_source(args, query)
    session = _prepare_session_with_source({**args, "source_path": source_path, "table_name": table_name})
    table = session.sql(query).to_arrow()
    return {
        "query": query,
        "source_path": source_path,
        **source_metadata,
        "table_name": table_name,
        **_table_preview(table, limit),
    }


def _velaria_explain(args: JsonDict) -> JsonDict:
    query = str(args["query"])
    source_path, source_metadata, table_name, query = _prepare_query_source(args, query)
    session = _prepare_session_with_source({**args, "source_path": source_path, "table_name": table_name})
    if hasattr(session, "explain_sql"):
        explain = session.explain_sql(query)
    else:
        explain = session.sql(query).explain()
    return {
        "explain": str(explain),
        "query": query,
        "source_path": source_path,
        **source_metadata,
        "table_name": table_name,
    }


def _velaria_dataset_import(args: JsonDict) -> JsonDict:
    from velaria.agentic_store import AgenticStore

    original_path = str(args["path"])
    path, source_metadata = _materialize_source_path(original_path)
    input_type = str(args.get("input_type") or "auto")
    table_name = str(args.get("table_name") or "input_table")
    limit = int(args.get("limit") or 20)
    schema_binding = args.get("schema_binding") if isinstance(args.get("schema_binding"), dict) else {}
    metadata = args.get("metadata") if isinstance(args.get("metadata"), dict) else {}

    preview = _velaria_read({"path": path, "limit": limit})
    path = str(preview.get("source_path") or path)
    if isinstance(preview.get("normalization"), dict):
        source_metadata = {**source_metadata, "normalization": preview["normalization"]}
    with AgenticStore() as store:
        source = store.upsert_source(
            {
                "source_id": args.get("source_id"),
                "kind": str(args.get("kind") or "local_file"),
                "name": str(args.get("name") or pathlib.Path(path).name),
                "spec": {
                    "path": path,
                    "input_path": path,
                    **source_metadata,
                    "input_type": input_type,
                    "table_name": table_name,
                },
                "schema_binding": {
                    "table_name": table_name,
                    "fields": preview["schema"],
                    **schema_binding,
                },
                "metadata": {
                    "source_path": path,
                    **source_metadata,
                    "input_type": input_type,
                    "row_count": preview["row_count"],
                    **metadata,
                },
            }
        )
    return {
        "source": source,
        "source_id": source["source_id"],
        "source_path": path,
        **source_metadata,
        "input_type": input_type,
        "table_name": table_name,
        "schema": preview["schema"],
        "row_count": preview["row_count"],
        "rows": preview["rows"],
    }


def _velaria_dataset_download(args: JsonDict) -> JsonDict:
    url = str(args["url"])
    if not _is_url(url):
        return {"ok": False, "error": "url must be an http(s) URL"}
    target = _download_source_url(url)
    limit = int(args.get("limit") or 20)
    result: JsonDict = {
        "source_url": url,
        "source_path": str(target),
        "file_name": target.name,
        "size_bytes": target.stat().st_size if target.exists() else 0,
    }
    if bool(args.get("preview", True)):
        preview = _velaria_read({"path": str(target), "limit": limit})
        result["source_path"] = str(preview.get("source_path") or result["source_path"])
        if isinstance(preview.get("normalization"), dict):
            result["normalization"] = preview["normalization"]
        result.update(
            {
                "schema": preview.get("schema") or [],
                "row_count": preview.get("row_count", 0),
                "rows": preview.get("rows") or [],
            }
        )
    return result


def _velaria_dataset_process(args: JsonDict) -> JsonDict:
    raw_source_path = str(args.get("source_path") or args.get("path") or "")
    if not raw_source_path:
        return {"ok": False, "error": "source_path or path is required"}
    source_path, source_metadata = _materialize_source_path(raw_source_path)
    query = str(args["query"])
    table_name = _resolve_query_table_name(str(args.get("table_name") or "input_table"), query)
    input_type = str(args.get("input_type") or "auto")
    delimiter = str(args.get("delimiter") or ",")
    preview_limit = max(0, min(int(args.get("limit") or 50), 200))
    save_run = bool(args.get("save_run", True))
    if _is_excel_path(source_path):
        source_path = _convert_excel_to_csv(source_path)
        input_type = "csv"
    if _should_normalize_source(args, source_path, query):
        normalized = _normalize_source_for_sql(source_path, delimiter=delimiter, limit=preview_limit)
        source_path = str(normalized["normalized_path"])
        source_metadata = {
            **source_metadata,
            "normalization": normalized,
            "original_source_path": raw_source_path,
        }
        query = _rewrite_query_columns(query, normalized.get("column_mapping") or {})
        input_type = "csv"
        table_name = _resolve_query_table_name(table_name, query)
    if save_run:
        argv = [
            "run",
            "start",
        ]
        if args.get("run_name"):
            argv.extend(["--run-name", str(args["run_name"])])
        if args.get("description"):
            argv.extend(["--description", str(args["description"])])
        for tag in args.get("tags") or []:
            argv.extend(["--tag", str(tag)])
        if args.get("timeout_ms"):
            argv.extend(["--timeout-ms", str(args["timeout_ms"])])
        argv.extend(
            [
                "--",
                "file-sql",
                "--csv",
                source_path,
                "--table",
                table_name,
                "--input-type",
                input_type,
                "--delimiter",
                delimiter,
                "--query",
                query,
            ]
        )
        if args.get("output_path"):
            argv.extend(["--output-path", str(args["output_path"])])
        result = _execute_cli_argv(argv)
        result.update(
            {
                "source_path": source_path,
                **source_metadata,
                "table_name": table_name,
                "query": query,
                "input_type": input_type,
            }
        )
        payload = result.get("payload") if isinstance(result.get("payload"), dict) else {}
        run_result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        if run_result:
            _merge_dataset_result(result, run_result, preview_limit)
        return result

    from velaria.cli.file_sql import _execute_csv_sql

    output_path = pathlib.Path(str(args["output_path"])) if args.get("output_path") else None
    executed = _execute_csv_sql(
        input_path=pathlib.Path(source_path),
        table=table_name,
        query=query,
        input_type=input_type,
        delimiter=delimiter,
        output_path=output_path,
    )
    payload = dict(executed["payload"])
    _merge_dataset_result(payload, payload, preview_limit)
    return {
        "source_path": source_path,
        **source_metadata,
        "table_name": table_name,
        "query": query,
        "input_type": input_type,
        "schema": payload.get("schema") or [],
        "row_count": payload.get("row_count", len(payload.get("rows") or [])),
        "rows": payload.get("rows") or [],
        "artifacts": executed.get("artifacts") or [],
    }


def _materialize_source_path(raw_path: str) -> tuple[str, JsonDict]:
    if not _is_url(raw_path):
        return raw_path, {}
    target = _download_source_url(raw_path)
    return str(target), {"source_url": raw_path}


def _is_url(value: str) -> bool:
    parsed = urllib.parse.urlparse(value)
    return parsed.scheme in {"http", "https"}


def _download_source_url(url: str) -> pathlib.Path:
    parsed = urllib.parse.urlparse(url)
    basename = pathlib.Path(urllib.parse.unquote(parsed.path)).name
    if not basename:
        suffix = ""
        basename = "source"
    else:
        suffix = pathlib.Path(basename).suffix
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    stem = pathlib.Path(basename).stem or "source"
    filename = f"{stem}-{digest}{suffix}"
    target_dir = _runtime_import_dir() / "downloads"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / filename
    if not target.exists():
        urllib.request.urlretrieve(url, target)
    return target


def _runtime_import_dir() -> pathlib.Path:
    root = (
        os.environ.get("VELARIA_RUNTIME_WORKSPACE")
        or os.environ.get("VELARIA_WORKSPACE")
        or str(pathlib.Path.home() / ".velaria")
    )
    path = pathlib.Path(root).expanduser() / "imports"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _is_excel_path(path: str) -> bool:
    return pathlib.Path(path).suffix.lower() in {".xlsx", ".xlsm", ".xls"}


def _is_csv_path(path: str) -> bool:
    return pathlib.Path(path).suffix.lower() == ".csv"


def _convert_excel_to_csv(path: str) -> str:
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError("pandas is required to convert Excel inputs for Velaria SQL") from exc
    source = pathlib.Path(path)
    target_dir = _runtime_import_dir() / "converted"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{source.stem}.csv"
    frame = pd.read_excel(str(source))
    frame.to_csv(target, index=False)
    return str(target)


def _velaria_dataset_normalize(args: JsonDict) -> JsonDict:
    raw_source_path = str(args.get("source_path") or args.get("path") or "")
    if not raw_source_path:
        return {"ok": False, "error": "source_path or path is required"}
    source_path, source_metadata = _materialize_source_path(raw_source_path)
    normalized = _normalize_source_for_sql(
        source_path,
        delimiter=str(args.get("delimiter") or ","),
        encoding=str(args.get("encoding") or "auto"),
        rename_columns=bool(args.get("rename_columns", True)),
        output_path=str(args.get("output_path") or ""),
        limit=int(args.get("limit") or 20),
    )
    return {
        "source_path": str(normalized["normalized_path"]),
        **source_metadata,
        "original_source_path": source_path,
        "table_name": str(args.get("table_name") or "input_table"),
        **normalized,
    }


def _normalize_source_for_sql(
    source_path: str,
    *,
    delimiter: str = ",",
    encoding: str = "auto",
    rename_columns: bool = True,
    output_path: str = "",
    limit: int = 20,
) -> JsonDict:
    frame, detected_encoding = _read_source_frame(source_path, delimiter=delimiter, encoding=encoding)
    original_columns = [str(column) for column in frame.columns]
    mapping = _sql_safe_column_mapping(original_columns) if rename_columns else {name: name for name in original_columns}
    frame = frame.rename(columns=mapping)
    target = pathlib.Path(output_path).expanduser() if output_path else _normalized_output_path(source_path, mapping)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False, encoding="utf-8")
    preview_rows = frame.head(max(0, min(int(limit), 200))).to_dict(orient="records")
    return {
        "normalized_path": str(target),
        "encoding": detected_encoding,
        "schema": [str(column) for column in frame.columns],
        "original_schema": original_columns,
        "column_mapping": mapping,
        "row_count": int(len(frame)),
        "rows": preview_rows,
    }


def _read_source_frame(source_path: str, *, delimiter: str, encoding: str) -> tuple[Any, str]:
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError("pandas is required to normalize tabular datasets") from exc
    if _is_excel_path(source_path):
        return pd.read_excel(source_path), "excel"
    encodings = [encoding] if encoding and encoding != "auto" else [
        "utf-8",
        "utf-8-sig",
        "gb18030",
        "gbk",
        "big5",
        "latin1",
    ]
    last_error: Exception | None = None
    for candidate in encodings:
        try:
            return pd.read_csv(source_path, sep=delimiter, encoding=candidate), candidate
        except UnicodeDecodeError as exc:
            last_error = exc
        except Exception as exc:
            last_error = exc
            if candidate not in {"latin1"}:
                continue
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"failed to normalize source: {source_path}")


def _normalized_output_path(source_path: str, mapping: dict[str, str]) -> pathlib.Path:
    source = pathlib.Path(source_path)
    digest_input = json.dumps(mapping, ensure_ascii=False, sort_keys=True) + str(source.resolve())
    digest = hashlib.sha1(digest_input.encode("utf-8")).hexdigest()[:12]
    return _runtime_import_dir() / "normalized" / f"{source.stem}-normalized-{digest}.csv"


def _sql_safe_column_mapping(columns: list[str]) -> dict[str, str]:
    used: set[str] = set()
    mapping: dict[str, str] = {}
    for index, original in enumerate(columns, start=1):
        candidate = _ascii_identifier(original)
        if not candidate:
            candidate = f"col_{index}"
        candidate = _unique_identifier(candidate, used)
        used.add(candidate)
        mapping[original] = candidate
    return mapping


def _ascii_identifier(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii").lower()
    ascii_text = re.sub(r"[^a-z0-9_]+", "_", ascii_text).strip("_")
    if ascii_text and ascii_text[0].isdigit():
        ascii_text = f"col_{ascii_text}"
    return ascii_text


def _unique_identifier(candidate: str, used: set[str]) -> str:
    if candidate not in used:
        return candidate
    suffix = 2
    while f"{candidate}_{suffix}" in used:
        suffix += 1
    return f"{candidate}_{suffix}"


def _should_normalize_source(args: JsonDict, source_path: str, query: str) -> bool:
    if args.get("normalize") is True:
        return True
    if args.get("normalize") is False or args.get("auto_normalize") is False:
        return False
    if not _is_csv_path(source_path):
        return False
    if _contains_non_ascii(query):
        return True
    return not _looks_utf8_decodable(source_path)


def _contains_non_ascii(text: str) -> bool:
    return any(ord(ch) > 127 for ch in text)


def _looks_utf8_decodable(path: str) -> bool:
    try:
        with open(path, "rb") as handle:
            handle.read(65536).decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False
    except Exception:
        return True


def _rewrite_query_columns(query: str, mapping: dict[str, str]) -> str:
    rewritten = query
    for original, normalized in sorted(mapping.items(), key=lambda item: len(item[0]), reverse=True):
        if original and original != normalized:
            rewritten = rewritten.replace(original, normalized)
    return rewritten


def _merge_dataset_result(target: JsonDict, result: JsonDict, limit: int) -> None:
    rows = result.get("rows") if isinstance(result.get("rows"), list) else []
    target["schema"] = result.get("schema") or target.get("schema") or []
    target["row_count"] = result.get("row_count", len(rows))
    target["rows"] = rows[:limit]


def _query_references_table(query: str, table_name: str) -> bool:
    import re

    return re.search(rf"\b{re.escape(table_name)}\b", query, flags=re.IGNORECASE) is not None


def _resolve_query_table_name(configured_table: str, query: str) -> str:
    configured = configured_table or "input_table"
    if configured != "input_table" and _query_references_table(query, "input_table"):
        return "input_table"
    referenced = _first_query_table_name(query)
    if referenced and _is_sql_identifier(referenced):
        return referenced
    return configured


def _first_query_table_name(query: str) -> str:
    match = re.search(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)\b", query, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def _is_sql_identifier(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value))


def _velaria_cli_run(args: JsonDict) -> JsonDict:
    command = args.get("command")
    argv = args.get("argv")
    if command:
        argv = shlex.split(str(command))
    if not isinstance(argv, list) or not all(isinstance(v, str) for v in argv):
        return {"ok": False, "error": "argv must be a list of strings or command must be a string"}
    if not argv:
        return {"ok": False, "error": "argv is required"}
    if argv[0] in {"-i", "--interactive", "ai"}:
        return {"ok": False, "error": f"interactive/ai command is not allowed through velaria_cli_run: {argv[0]}"}

    return _execute_cli_argv(list(argv))


def _execute_cli_argv(argv: list[str]) -> JsonDict:
    from velaria.cli import main as cli_main

    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_cli_output(stdout, stderr):
        exit_code = cli_main(argv)
    result = {
        "ok": exit_code == 0,
        "exit_code": exit_code,
        "argv": argv,
        "stdout": stdout.getvalue(),
        "stderr": stderr.getvalue(),
    }
    _merge_cli_metadata(result)
    return result


def _merge_cli_metadata(result: JsonDict) -> None:
    stdout = str(result.get("stdout") or "").strip()
    if not stdout:
        return
    try:
        payload = json.loads(stdout)
    except Exception:
        lines = [line for line in stdout.splitlines() if line.strip()]
        if not lines:
            return
        try:
            payload = json.loads(lines[-1])
        except Exception:
            return
    if not isinstance(payload, dict):
        return
    result["payload"] = payload
    for key in ("run_id", "artifact_id", "artifacts", "artifact"):
        if key in payload:
            result[key] = payload[key]


@contextlib.contextmanager
def redirect_cli_output(stdout: io.StringIO, stderr: io.StringIO):
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        yield


def _velaria_artifact_preview(args: JsonDict) -> JsonDict:
    from velaria.cli._common import _read_preview_for_artifact
    from velaria.workspace import ArtifactIndex

    artifact_id = str(args["artifact_id"])
    limit = int(args.get("limit") or 20)
    index = ArtifactIndex()
    try:
        artifact = index.get_artifact(artifact_id)
        if artifact is None:
            return {"ok": False, "error": f"artifact not found: {artifact_id}"}
        return {
            "ok": True,
            "artifact_id": artifact_id,
            "artifact": artifact,
            "preview": _read_preview_for_artifact(artifact, limit=limit),
        }
    finally:
        index.close()


def _skill_path() -> pathlib.Path | None:
    configured = os.environ.get("VELARIA_SKILL_PATH")
    if configured:
        path = pathlib.Path(configured).expanduser()
        if path.exists():
            return path
    configured_dir = os.environ.get("VELARIA_SKILL_DIR")
    if configured_dir:
        path = pathlib.Path(configured_dir).expanduser() / "velaria_python_local" / "SKILL.md"
        if path.exists():
            return path
    path = pathlib.Path.home() / ".velaria" / "skills" / "velaria_python_local" / "SKILL.md"
    if path.exists():
        return path
    return None


def load_velaria_skill_text() -> str:
    path = _skill_path()
    if path is None:
        return (
            "# Velaria Local Python Skill\n\n"
            "Use Velaria through DataflowSession/Session, session.read(...), "
            "session.sql(...), Velaria local functions, and workspace runs/artifacts."
        )
    return path.read_text(encoding="utf-8")


def velaria_agent_instructions() -> str:
    tool_catalog = "\n".join(
        f"- `{tool['name']}`: {tool['description']}" for tool in tool_definitions()
    )
    return (
        "You are Velaria Agent running inside the Velaria CLI runtime wrapper. "
        "Codex or Claude may provide the underlying agent runtime, but your "
        "product identity, tool choices, and user-facing behavior are Velaria. "
        "Do not describe yourself as a generic Codex or Claude coding agent "
        "when handling Velaria data work.\n\n"
        "Velaria is a local-first C++20 dataflow engine with a supported Python "
        "ecosystem layer for local file ingress, SQL execution, workspace runs, "
        "artifacts, and agent-assisted analysis. Prefer the registered Velaria "
        "local functions/MCP tools for Velaria operations. Use "
        "`velaria_dataset_download` for HTTP(S) dataset localization, "
        "`velaria_dataset_import` for dataset registration/import, and "
        "`velaria_dataset_normalize` for encoding/column-name normalization, "
        "and `velaria_dataset_process` for dataset SQL processing before "
        "falling back to raw shell or generic CLI commands.\n\n"
        "Default workflow policy for data tasks: when the user asks to inspect, "
        "import, clean, summarize, query, transform, or save datasets, first get "
        "the data into a Velaria-processable local format such as CSV, JSON, "
        "Arrow, Parquet, or supported Excel, then call the Velaria local "
        "functions. Use `velaria_read`/`velaria_schema` to inspect local files, "
        "`velaria_dataset_import` to register datasets, `velaria_sql` for "
        "ad-hoc queries, `velaria_dataset_process` to save runs/artifacts, and "
        "`velaria_artifact_preview` to inspect saved outputs. If a dataset has "
        "non-UTF-8 bytes, non-ASCII column names, or SQL identifier errors such "
        "as invalid token bytes, call `velaria_dataset_normalize` and then query "
        "the returned SQL-safe schema/column_mapping. For HTTP(S) URLs, "
        "call `velaria_dataset_download` or pass the URL directly to "
        "`velaria_dataset_import`, `velaria_read`, `velaria_sql`, or "
        "`velaria_dataset_process`; these functions localize the remote file "
        "inside the Velaria runtime workspace. Do not write curl, wget, Python "
        "download code, or generic shell download steps before trying the "
        "Velaria function. Do not probe `velaria_cli.py --help` or inspect the "
        "generic CLI before using the registered Velaria tools for ordinary "
        "dataset work. Do not use web search to discover Velaria tools. If "
        "direct `mcp__velaria__.*` tools are not visible because the runtime "
        "deferred them, use `tool_search` with a Velaria-specific query and then "
        "call the returned `mcp__velaria__` tool. Use web search only when the "
        "user asks you to find an external data source or current external facts. "
        "If a source is an `.xls` or `.xlsx` URL, Velaria can download it and "
        "convert it to CSV for SQL processing. If Velaria cannot handle the "
        "requested workflow, explain that boundary and then use the best "
        "available fallback.\n\n"
        "Velaria SQL v1 supports SELECT projection/aliases, WHERE, GROUP BY, "
        "ORDER BY, LIMIT, minimal JOIN, INSERT INTO ... VALUES, INSERT INTO ... "
        "SELECT, CREATE TABLE, CREATE SOURCE TABLE, and CREATE SINK TABLE. Do "
        "not generate CTEs, subqueries, HAVING, stored procedures, or broad ANSI "
        "window SQL. Built-in scalar functions include YEAR, MONTH, DAY, "
        "ISO_YEAR, ISO_WEEK, WEEK, and YEARWEEK; GROUP BY may use supported "
        "scalar expressions directly. SOURCE TABLE is read-only; SINK TABLE "
        "can be written but must not be used as a query input.\n\n"
        "Python and CLI commands in this repository must be run through uv. The "
        "repository-visible CLI entry is `uv run --project python python "
        "python/velaria_cli.py ...`.\n\n"
        "Available Velaria local functions:\n"
        f"{tool_catalog}\n\n"
        "The full Velaria usage skill is available as an MCP resource at "
        "`velaria://skills/velaria-python-local`. Load that resource only when "
        "you need detailed workflow guidance, examples, parameters, or boundary "
        "rules for a Velaria task."
    )


def local_function_registry() -> dict[str, LocalFunction]:
    return {
        "velaria_read": LocalFunction(
            "velaria_read",
            "Read a local data file or HTTP(S) dataset URL with Velaria and return schema plus preview rows. URL inputs are downloaded into the Velaria runtime workspace first.",
            _tool_schema(
                {
                    "path": {
                        "type": "string",
                        "description": "Local file path or HTTP(S) URL to a CSV, JSON, Arrow, Parquet, or supported Excel dataset.",
                    },
                    "limit": {"type": "integer", "default": 20, "description": "Maximum preview rows to return."},
                },
                ["path"],
            ),
            _velaria_read,
        ),
        "velaria_schema": LocalFunction(
            "velaria_schema",
            "Inspect the schema for a local data file, HTTP(S) dataset URL, or the named working table.",
            _tool_schema(
                {
                    "path": {
                        "type": "string",
                        "description": "Optional local file path or HTTP(S) URL. If omitted, returns the named working table schema placeholder.",
                    },
                    "table_name": {"type": "string", "default": "input_table"},
                }
            ),
            _velaria_schema,
        ),
        "velaria_sql": LocalFunction(
            "velaria_sql",
            "Execute Velaria SQL. Provide source_path as a local file path or HTTP(S) dataset URL to register it as input_table first.",
            _tool_schema(
                {
                    "query": {"type": "string"},
                    "source_path": {
                        "type": "string",
                        "description": "Optional local file path or HTTP(S) URL to register as the query input table.",
                    },
                    "table_name": {"type": "string", "default": "input_table"},
                    "limit": {"type": "integer", "default": 50},
                },
                ["query"],
            ),
            _velaria_sql,
        ),
        "velaria_explain": LocalFunction(
            "velaria_explain",
            "Explain a Velaria SQL query without returning full results.",
            _tool_schema(
                {
                    "query": {"type": "string"},
                    "source_path": {
                        "type": "string",
                        "description": "Optional local file path or HTTP(S) URL to register before explaining the query.",
                    },
                    "table_name": {"type": "string", "default": "input_table"},
                },
                ["query"],
            ),
            _velaria_explain,
        ),
        "velaria_dataset_import": LocalFunction(
            "velaria_dataset_import",
            "Import/register a local file or HTTP(S) dataset URL as a Velaria source and return dataset schema plus preview metadata. Prefer this for user-provided dataset URLs before writing custom download code.",
            _tool_schema(
                {
                    "path": {
                        "type": "string",
                        "description": "Local file path or HTTP(S) URL to import. URL inputs are downloaded into the Velaria runtime workspace first.",
                    },
                    "source_id": {"type": "string"},
                    "name": {"type": "string"},
                    "kind": {"type": "string", "default": "local_file"},
                    "input_type": {"type": "string", "default": "auto"},
                    "table_name": {"type": "string", "default": "input_table"},
                    "limit": {"type": "integer", "default": 20},
                    "schema_binding": {"type": "object"},
                    "metadata": {"type": "object"},
                },
                ["path"],
            ),
            _velaria_dataset_import,
        ),
        "velaria_dataset_download": LocalFunction(
            "velaria_dataset_download",
            "Download/localize an HTTP(S) dataset URL into the Velaria runtime workspace and optionally return schema plus preview rows. Use this first when the user asks to fetch or download a dataset.",
            _tool_schema(
                {
                    "url": {
                        "type": "string",
                        "description": "HTTP(S) URL for the dataset to download into the Velaria runtime workspace.",
                    },
                    "preview": {
                        "type": "boolean",
                        "default": True,
                        "description": "Whether to read the downloaded file with Velaria and return schema plus preview rows.",
                    },
                    "limit": {"type": "integer", "default": 20, "description": "Maximum preview rows to return."},
                },
                ["url"],
            ),
            _velaria_dataset_download,
        ),
        "velaria_dataset_normalize": LocalFunction(
            "velaria_dataset_normalize",
            "Normalize a CSV or supported Excel dataset into a Velaria-processable UTF-8 CSV with SQL-safe column names. Use this after encoding errors, invalid token bytes, or non-ASCII column names.",
            _tool_schema(
                {
                    "source_path": {
                        "type": "string",
                        "description": "Local file path or HTTP(S) URL to normalize.",
                    },
                    "path": {
                        "type": "string",
                        "description": "Alias for source_path.",
                    },
                    "encoding": {
                        "type": "string",
                        "default": "auto",
                        "description": "CSV encoding. auto tries UTF-8, UTF-8 BOM, GB18030, GBK, Big5, and Latin-1.",
                    },
                    "delimiter": {"type": "string", "default": ","},
                    "rename_columns": {
                        "type": "boolean",
                        "default": True,
                        "description": "Rename columns to SQL-safe ASCII identifiers and return column_mapping.",
                    },
                    "output_path": {"type": "string"},
                    "table_name": {"type": "string", "default": "input_table"},
                    "limit": {"type": "integer", "default": 20},
                },
            ),
            _velaria_dataset_normalize,
        ),
        "velaria_dataset_process": LocalFunction(
            "velaria_dataset_process",
            "Process a Velaria local file or HTTP(S) dataset URL with SQL, optionally saving a workspace run and result artifact. URL inputs are downloaded into the Velaria runtime workspace first. The function aligns table_name with the SQL FROM table and auto-normalizes CSV sources when SQL contains non-ASCII identifiers or the file is not UTF-8.",
            _tool_schema(
                {
                    "source_path": {
                        "type": "string",
                        "description": "Local file path or HTTP(S) URL to process.",
                    },
                    "path": {
                        "type": "string",
                        "description": "Alias for source_path; local file path or HTTP(S) URL to process.",
                    },
                    "query": {"type": "string"},
                    "table_name": {
                        "type": "string",
                        "default": "input_table",
                        "description": "Working SQL table name. Omit this unless the query uses a different table; default queries should use input_table.",
                    },
                    "input_type": {"type": "string", "default": "auto"},
                    "delimiter": {"type": "string", "default": ","},
                    "output_path": {"type": "string"},
                    "save_run": {"type": "boolean", "default": True},
                    "run_name": {"type": "string"},
                    "description": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "timeout_ms": {"type": "integer"},
                    "limit": {"type": "integer", "default": 50},
                    "normalize": {"type": "boolean"},
                    "auto_normalize": {"type": "boolean", "default": True},
                },
                ["query"],
            ),
            _velaria_dataset_process,
        ),
        "velaria_cli_run": LocalFunction(
            "velaria_cli_run",
            "Run a non-interactive Velaria CLI command and capture stdout/stderr.",
            _tool_schema(
                {
                    "argv": {"type": "array", "items": {"type": "string"}},
                    "command": {"type": "string"},
                }
            ),
            _velaria_cli_run,
        ),
        "velaria_artifact_preview": LocalFunction(
            "velaria_artifact_preview",
            "Preview a workspace artifact by artifact_id.",
            _tool_schema(
                {
                    "artifact_id": {"type": "string"},
                    "limit": {"type": "integer", "default": 20},
                },
                ["artifact_id"],
            ),
            _velaria_artifact_preview,
        ),
    }


def tool_definitions() -> list[JsonDict]:
    return [
        {
            "name": fn.name,
            "description": fn.description,
            "input_schema": fn.input_schema,
            "annotations": _tool_annotations(fn.name),
        }
        for fn in local_function_registry().values()
    ]


def _tool_annotations(name: str) -> JsonDict:
    read_only = name in {
        "velaria_read",
        "velaria_schema",
        "velaria_explain",
        "velaria_sql",
        "velaria_artifact_preview",
    }
    idempotent = name not in {"velaria_dataset_process", "velaria_cli_run"}
    open_world = name in {
        "velaria_read",
        "velaria_schema",
        "velaria_sql",
        "velaria_explain",
        "velaria_dataset_import",
        "velaria_dataset_download",
        "velaria_dataset_normalize",
        "velaria_dataset_process",
    }
    return {
        "readOnlyHint": read_only,
        "destructiveHint": False,
        "idempotentHint": idempotent,
        "openWorldHint": open_world,
    }


def execute_local_function(name: str, arguments: JsonDict | None = None) -> JsonDict:
    registry = local_function_registry()
    fn = registry.get(name)
    if fn is None:
        return {"ok": False, "error": f"unknown Velaria local function: {name}"}
    try:
        result = fn.handler(arguments or {})
        if "ok" not in result:
            result = {"ok": True, **result}
        result.setdefault("function", name)
        return result
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "error_type": type(exc).__name__,
        }


def json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)
