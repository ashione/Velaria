"""Local Velaria functions exposed to agent runtimes."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import pathlib
import shlex
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
    if _is_excel_path(path):
        table = read_excel(session, path).to_arrow()
    else:
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


def _velaria_sql(args: JsonDict) -> JsonDict:
    query = str(args["query"])
    limit = int(args.get("limit") or 50)
    original_source_path = str(args.get("source_path") or args.get("path") or "")
    source_path, source_metadata = _materialize_source_path(original_source_path) if original_source_path else ("", {})
    table_name = str(args.get("table_name") or "input_table")
    session = _prepare_session_with_source(args)
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
    session = _prepare_session_with_source(args)
    if hasattr(session, "explain_sql"):
        explain = session.explain_sql(query)
    else:
        explain = session.sql(query).explain()
    return {"explain": str(explain)}


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
    table_name = str(args.get("table_name") or "input_table")
    input_type = str(args.get("input_type") or "auto")
    delimiter = str(args.get("delimiter") or ",")
    preview_limit = max(0, min(int(args.get("limit") or 50), 200))
    save_run = bool(args.get("save_run", True))
    if _is_excel_path(source_path):
        source_path = _convert_excel_to_csv(source_path)
        input_type = "csv"
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


def _merge_dataset_result(target: JsonDict, result: JsonDict, limit: int) -> None:
    rows = result.get("rows") if isinstance(result.get("rows"), list) else []
    target["schema"] = result.get("schema") or target.get("schema") or []
    target["row_count"] = result.get("row_count", len(rows))
    target["rows"] = rows[:limit]


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
        "`velaria_dataset_process` for dataset SQL processing before falling "
        "back to raw shell or generic CLI commands.\n\n"
        "Default workflow policy for data tasks: when the user asks to inspect, "
        "import, clean, summarize, query, transform, or save datasets, first get "
        "the data into a Velaria-processable local format such as CSV, JSON, "
        "Arrow, Parquet, or supported Excel, then call the Velaria local "
        "functions. Use `velaria_read`/`velaria_schema` to inspect local files, "
        "`velaria_dataset_import` to register datasets, `velaria_sql` for "
        "ad-hoc queries, `velaria_dataset_process` to save runs/artifacts, and "
        "`velaria_artifact_preview` to inspect saved outputs. For HTTP(S) URLs, "
        "call `velaria_dataset_download` or pass the URL directly to "
        "`velaria_dataset_import`, `velaria_read`, `velaria_sql`, or "
        "`velaria_dataset_process`; these functions localize the remote file "
        "inside the Velaria runtime workspace. Do not write curl, wget, Python "
        "download code, or generic shell download steps before trying the "
        "Velaria function. If a source is an `.xls` or `.xlsx` URL, Velaria can "
        "download it and convert it to CSV for SQL processing. If Velaria cannot "
        "handle the requested workflow, explain that boundary and then use the "
        "best available fallback.\n\n"
        "Velaria SQL v1 supports SELECT projection/aliases, WHERE, GROUP BY, "
        "ORDER BY, LIMIT, minimal JOIN, INSERT INTO ... VALUES, INSERT INTO ... "
        "SELECT, CREATE TABLE, CREATE SOURCE TABLE, and CREATE SINK TABLE. Do "
        "not generate CTEs, subqueries, HAVING, stored procedures, or broad ANSI "
        "window SQL. SOURCE TABLE is read-only; SINK TABLE can be written but "
        "must not be used as a query input.\n\n"
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
        "velaria_dataset_process": LocalFunction(
            "velaria_dataset_process",
            "Process a Velaria local file or HTTP(S) dataset URL with SQL, optionally saving a workspace run and result artifact. URL inputs are downloaded into the Velaria runtime workspace first.",
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
                    "table_name": {"type": "string", "default": "input_table"},
                    "input_type": {"type": "string", "default": "auto"},
                    "delimiter": {"type": "string", "default": ","},
                    "output_path": {"type": "string"},
                    "save_run": {"type": "boolean", "default": True},
                    "run_name": {"type": "string"},
                    "description": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "timeout_ms": {"type": "integer"},
                    "limit": {"type": "integer", "default": 50},
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
