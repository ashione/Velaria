from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None

PREVIEW_LIMIT_ROWS = 50
PREVIEW_LIMIT_BYTES = 200 * 1024


@dataclass
class RunRecord:
    run_id: str
    created_at: str
    action: str
    cli_args: dict[str, Any]
    velaria_version: str | None
    run_dir: str
    status: str = "running"
    finished_at: str | None = None
    run_name: str | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ArtifactRecord:
    artifact_id: str
    run_id: str
    created_at: str
    type: str
    uri: str
    format: str
    row_count: int | None = None
    schema_json: JsonValue = None
    preview_json: JsonValue = None
    tags_json: list[str] | None = None
    path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
