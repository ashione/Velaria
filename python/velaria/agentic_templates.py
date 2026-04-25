from __future__ import annotations

from typing import Any


def builtin_templates() -> list[dict[str, Any]]:
    return [
        {
            "template_id": "threshold",
            "name": "Threshold Trigger",
            "category": "threshold",
            "description": "Trigger when a numeric field crosses a threshold.",
            "applies_to": ["saved_dataset", "local_file", "bitable", "external_event"],
            "required_fields": ["value_field"],
            "parameter_defs": [
                {"name": "value_field", "kind": "column", "required": True, "multi": False},
                {"name": "threshold", "kind": "number", "required": True, "multi": False},
                {"name": "comparison", "kind": "enum", "required": False, "multi": False, "allowed_values": [">", ">=", "<", "<="], "default_value": ">="},
            ],
            "severity_default": "warning",
            "tags": ["monitor", "threshold"],
            "version": "v1",
        },
        {
            "template_id": "change",
            "name": "Snapshot Delta",
            "category": "change",
            "description": "Compare current and previous snapshots by key and delta field.",
            "applies_to": ["saved_dataset", "local_file", "bitable"],
            "required_fields": ["key_field", "value_field"],
            "parameter_defs": [
                {"name": "key_field", "kind": "column", "required": True, "multi": False},
                {"name": "value_field", "kind": "column", "required": True, "multi": False},
                {"name": "threshold", "kind": "number", "required": True, "multi": False},
            ],
            "severity_default": "warning",
            "tags": ["monitor", "change"],
            "version": "v1",
        },
        {
            "template_id": "ranking",
            "name": "Top K Ranking",
            "category": "ranking",
            "description": "Emit top-ranked rows by a score field.",
            "applies_to": ["saved_dataset", "local_file", "bitable", "external_event"],
            "required_fields": ["score_field"],
            "parameter_defs": [
                {"name": "score_field", "kind": "column", "required": True, "multi": False},
                {"name": "top_k", "kind": "number", "required": True, "multi": False},
            ],
            "severity_default": "info",
            "tags": ["monitor", "ranking"],
            "version": "v1",
        },
        {
            "template_id": "window_count",
            "name": "Windowed Count",
            "category": "window",
            "description": "Count events in each closed window grouped by key fields.",
            "applies_to": ["external_event"],
            "required_fields": [],
            "parameter_defs": [
                {"name": "group_by", "kind": "column", "required": False, "multi": True},
                {"name": "count_threshold", "kind": "number", "required": True, "multi": False},
            ],
            "severity_default": "warning",
            "tags": ["monitor", "window", "stream"],
            "version": "v1",
        },
    ]


def get_template(template_id: str) -> dict[str, Any]:
    for template in builtin_templates():
        if template["template_id"] == template_id:
            return template
    raise KeyError(template_id)
