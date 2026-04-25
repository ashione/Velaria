"""Velaria local app service -- modular package.

Re-exports the public surface so that ``import velaria_service`` or
``importlib.import_module("velaria_service")`` keeps working exactly as
before the single-file-to-package refactor.
"""
from __future__ import annotations

import argparse
from http.server import ThreadingHTTPServer  # noqa: F401 – re-exported for tests

import velaria.cli as cli_impl  # noqa: F401 – re-exported for tests
from velaria import (  # noqa: F401 – re-exported for tests
    BitableClient,
    Session,
)

# Re-export VelariaService and handler from _router
from ._router import VelariaService  # noqa: F401

# Re-export helpers that external code (tests, cli.py, agentic_runtime.py) references
from ._helpers import (  # noqa: F401
    ApiRouteNotFoundError,
    _api_parts,
    _build_run_response,
    _default_bitable_dataset_name,
    _default_embedding_model,
    _default_embedding_provider,
    _directory_artifact,
    _enrich_run,
    _error_response,
    _execute_keyword_index_build,
    _json_dumps,
    _load_arrow_table,
    _load_arrow_tables,
    _load_dataframe,
    _normalize_bitable_rows,
    _normalize_bitable_value,
    _normalize_path,
    _parse_csv_list,
    _parse_mappings,
    _parse_path_list,
    _parse_string_list,
    _preview_from_dataframe,
    _register_artifacts,
    _resolve_auto_input_payload,
    _resolve_bitable_credentials,
    _resolve_json_columns,
    _run_duration_ms,
    _safe_sql_identifier,
    _score_semantics,
    _timestamp_suffix,
    _update_run_progress,
    _ARROW_SUFFIXES,
    _BITABLE_PAGE_SIZE,
    _BITABLE_TIMEOUT_SECONDS,
    _EXCEL_SUFFIXES,
    _PARQUET_SUFFIXES,
    _dataset_docs_from_store,
    _event_docs_from_store,
    _field_docs_from_store,
)

# Re-export import handlers
from .import_handlers import (  # noqa: F401
    _execute_bitable_import,
    _finalize_bitable_import_run,
    _preview_bitable_import,
    _register_artifacts_preview_table,
)

# Re-export analysis handlers
from .analysis_handlers import _execute_file_sql  # noqa: F401

# Re-export dataset handlers
from .dataset_handlers import (  # noqa: F401
    _execute_embedding_build,
    _execute_hybrid_search,
    _execute_keyword_search,
)

# Re-export search handlers
from .search_handlers import (  # noqa: F401
    handle_grounding,
    handle_search_datasets,
    handle_search_events,
    handle_search_fields,
    handle_search_templates,
)

# Re-export agentic handlers
from .agentic_handlers import (  # noqa: F401
    _monitor_from_intent_payload,
    _monitor_payload_for_response,
    _normalize_monitor_create_payload,
    _validate_monitor_payload,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Velaria local app service.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=37491)
    args = parser.parse_args()
    service = VelariaService(host=args.host, port=args.port)
    service.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
