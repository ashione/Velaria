from __future__ import annotations

import argparse
from typing import Any

from velaria.workspace import ArtifactIndex
from velaria.workspace.types import PREVIEW_LIMIT_ROWS

from velaria.cli._common import (
    CliStructuredError,
    _emit_json,
    _read_preview_for_artifact,
)


def _artifacts_list(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        return _emit_json(
            {
                "ok": True,
                "artifacts": index.list_artifacts(limit=args.limit, run_id=args.run_id),
            }
        )
    finally:
        index.close()


def _artifacts_preview(args: argparse.Namespace) -> int:
    index = ArtifactIndex()
    try:
        artifact = index.get_artifact(args.artifact_id)
        if artifact is None:
            raise CliStructuredError(
                f"artifact not found: {args.artifact_id}",
                error_type="file_not_found",
                phase="artifact_lookup",
                details={"artifact_id": args.artifact_id},
            )
        preview = _read_preview_for_artifact(artifact, limit=args.limit)
        if artifact.get("preview_json") is None:
            index.update_artifact_preview(args.artifact_id, preview)
            artifact = index.get_artifact(args.artifact_id) or artifact
        return _emit_json(
            {
                "ok": True,
                "artifact_id": args.artifact_id,
                "preview": preview,
                "artifact": artifact,
            }
        )
    finally:
        index.close()


def register(subparsers: argparse._SubParsersAction) -> None:
    artifacts_parser = subparsers.add_parser("artifacts", help="Artifact index commands.")
    artifacts_subparsers = artifacts_parser.add_subparsers(dest="artifacts_command", required=True)

    artifacts_list = artifacts_subparsers.add_parser("list", help="List artifacts.")
    artifacts_list.add_argument("--run-id")
    artifacts_list.add_argument("--limit", type=int, default=50)

    artifacts_preview = artifacts_subparsers.add_parser("preview", help="Preview an artifact.")
    artifacts_preview.add_argument("--artifact-id", required=True)
    artifacts_preview.add_argument("--limit", type=int, default=PREVIEW_LIMIT_ROWS)
