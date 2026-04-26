from __future__ import annotations

import argparse
from typing import Any

from velaria.agentic_store import AgenticStore
from velaria.cli._common import _emit_json
from velaria.workspace import ArtifactIndex


_DATASET_SOURCE_KINDS = {"saved_dataset", "local_file", "bitable"}


def _datasets_list(args: argparse.Namespace) -> int:
    limit = max(0, int(args.limit))
    source_items = _dataset_sources()
    artifact_items = _dataset_artifacts(limit=max(limit, 50))
    datasets = [*source_items, *artifact_items][:limit]
    return _emit_json(
        {
            "ok": True,
            "datasets": datasets,
            "sources_count": len(source_items),
            "artifacts_count": len(artifact_items),
        }
    )


def _dataset_sources() -> list[dict[str, Any]]:
    with AgenticStore() as store:
        sources = [
            source
            for source in store.list_sources()
            if source.get("kind") in _DATASET_SOURCE_KINDS
        ]
    items = []
    for source in sources:
        spec = source.get("spec") if isinstance(source.get("spec"), dict) else {}
        metadata = source.get("metadata") if isinstance(source.get("metadata"), dict) else {}
        path = spec.get("path") or spec.get("input_path") or source.get("event_log_path") or ""
        items.append(
            {
                "dataset_id": f"source:{source.get('source_id')}",
                "origin": "source",
                "source_id": source.get("source_id"),
                "name": source.get("name") or source.get("source_id") or "",
                "kind": source.get("kind") or "",
                "path_or_uri": path,
                "created_at": source.get("created_at"),
                "updated_at": source.get("updated_at"),
                "schema": _source_schema(source, metadata),
                "metadata": metadata,
            }
        )
    return items


def _dataset_artifacts(*, limit: int) -> list[dict[str, Any]]:
    index = ArtifactIndex()
    try:
        artifacts = index.list_artifacts(limit=limit)
    finally:
        index.close()
    items = []
    for artifact in artifacts:
        if not _is_dataset_artifact(artifact):
            continue
        created_at = artifact.get("created_at")
        items.append(
            {
                "dataset_id": f"artifact:{artifact.get('artifact_id')}",
                "origin": "artifact",
                "artifact_id": artifact.get("artifact_id"),
                "run_id": artifact.get("run_id"),
                "name": artifact.get("artifact_id") or "",
                "kind": "artifact",
                "path_or_uri": artifact.get("uri") or "",
                "created_at": created_at,
                "updated_at": created_at,
                "format": artifact.get("format"),
                "row_count": artifact.get("row_count"),
                "schema": _artifact_schema(artifact),
            }
        )
    return items


def _is_dataset_artifact(artifact: dict[str, Any]) -> bool:
    tags = artifact.get("tags_json")
    if isinstance(tags, list) and "result" in tags:
        return True
    return artifact.get("type") == "table"


def _source_schema(source: dict[str, Any], metadata: dict[str, Any]) -> list[Any]:
    schema = metadata.get("schema")
    if isinstance(schema, list):
        return schema
    binding = source.get("schema_binding")
    if isinstance(binding, dict):
        mappings = binding.get("field_mappings")
        if isinstance(mappings, dict):
            return list(mappings)
    return []


def _artifact_schema(artifact: dict[str, Any]) -> list[Any]:
    schema = artifact.get("schema_json")
    if isinstance(schema, list):
        return schema
    return []


def register(subparsers: argparse._SubParsersAction) -> None:
    datasets_parser = subparsers.add_parser("datasets", help="Dataset inventory commands.")
    datasets_subparsers = datasets_parser.add_subparsers(dest="datasets_command", required=True)

    list_parser = datasets_subparsers.add_parser("list", help="List available datasets.")
    list_parser.add_argument("--limit", type=int, default=50)
