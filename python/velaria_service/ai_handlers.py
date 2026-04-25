"""AI-related HTTP handlers for the Velaria service."""
from __future__ import annotations

from typing import Any

from ._helpers import get_ai_config
from velaria.ai_provider import create_provider


def handle_generate_sql(payload: dict[str, Any]) -> dict[str, Any]:
    """Handle ``POST /api/v1/ai/generate-sql``.

    Required payload keys:
        - ``prompt``: natural-language description of the desired query.
        - ``schema``: list of column names.
        - ``table_name``: name of the table to query.

    Optional payload keys:
        - ``sample_rows``: list of dicts representing example rows.
        - ``provider``, ``api_key``, ``base_url``, ``model``: overrides for
          the AI provider configuration (falls back to
          ``~/.velaria/config.json``).
    """
    prompt = payload.get("prompt")
    if not prompt:
        return {"ok": False, "error": "prompt is required", "error_type": "validation_error"}

    schema = payload.get("schema")
    if not schema or not isinstance(schema, list):
        return {"ok": False, "error": "schema (list of column names) is required", "error_type": "validation_error"}

    table_name = str(payload.get("table_name") or "input_table")
    sample_rows = payload.get("sample_rows")

    # Merge: request-level overrides > ~/.velaria/config.json defaults
    config = get_ai_config()
    for key in ("provider", "api_key", "base_url", "model"):
        if payload.get(key):
            config[key] = payload[key]

    if not config.get("api_key"):
        return {
            "ok": False,
            "error": (
                "AI provider API key is not configured. "
                "Set it in ~/.velaria/config.json (aiApiKey) or pass api_key in the request."
            ),
            "error_type": "configuration_error",
        }

    try:
        provider = create_provider(config)
    except Exception as exc:
        return {"ok": False, "error": f"failed to create AI provider: {exc}", "error_type": "ai_provider_error"}

    result = provider.generate_sql(
        prompt=str(prompt),
        schema=list(schema),
        table_name=table_name,
        sample_rows=sample_rows,
    )

    # If the provider returned an error envelope, propagate it.
    if result.get("ok") is False:
        return result

    return {
        "ok": True,
        "sql": result.get("sql", ""),
        "explanation": result.get("explanation", ""),
        "provider": config.get("provider", "openai"),
        "model": config.get("model", ""),
    }
