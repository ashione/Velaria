"""AI provider abstraction for SQL generation.

Zero external dependencies -- uses only ``urllib.request``, ``json`` and
``ssl`` from the standard library so that the core provider layer stays
self-contained and does not pull in any pip packages.
"""
from __future__ import annotations

import json
import ssl
import urllib.request
from typing import Any


_SYSTEM_PROMPT = (
    "You are a data analysis assistant for Velaria. "
    "Generate SQL queries based on natural language descriptions.\n\n"
    "Available SQL syntax (Velaria SQL v1):\n"
    "- SELECT with projection, aliases, WHERE (AND/OR), GROUP BY, ORDER BY, LIMIT\n"
    "- Minimal JOIN, UNION / UNION ALL\n"
    "- INSERT INTO ... VALUES, INSERT INTO ... SELECT\n"
    "- Builtins: LOWER, UPPER, TRIM, LTRIM, RTRIM, LENGTH, LEN, CHAR_LENGTH, "
    "CHARACTER_LENGTH, REVERSE, CONCAT, CONCAT_WS, LEFT, RIGHT, SUBSTR/SUBSTRING, "
    "POSITION, REPLACE, ABS, CEIL, FLOOR, ROUND, YEAR, MONTH, DAY\n\n"
    "NOT supported: CTE, subqueries, HAVING, WINDOW functions, stored procedures\n\n"
    'Respond ONLY with a JSON object: {"sql": "<the SQL query>", "explanation": "<brief explanation>"}\n'
    "Do not include markdown code blocks or any other text outside the JSON."
)

_TIMEOUT_SECONDS = 30


def _build_user_message(
    prompt: str,
    schema: list[str],
    table_name: str,
    sample_rows: list[dict[str, Any]] | None = None,
) -> str:
    parts: list[str] = [
        f"Table: {table_name}",
        f"Columns: {', '.join(schema)}",
    ]
    if sample_rows:
        parts.append(f"Sample rows (first {len(sample_rows)}):")
        for row in sample_rows[:5]:
            parts.append(f"  {json.dumps(row, ensure_ascii=False)}")
    parts.append("")
    parts.append(f"Request: {prompt}")
    return "\n".join(parts)


def _extract_sql_result(raw_text: str) -> dict[str, Any]:
    """Parse a JSON object out of *raw_text*, tolerating optional markdown
    code fences that some models wrap their output in."""
    text = raw_text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # Drop leading ```json / ``` and trailing ```
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    parsed = json.loads(text)
    return {
        "sql": str(parsed.get("sql") or ""),
        "explanation": str(parsed.get("explanation") or ""),
    }


def _make_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    return ctx


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class AiProvider:
    """Base class for LLM providers."""

    def generate_sql(
        self,
        prompt: str,
        schema: list[str],
        table_name: str,
        sample_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Returns ``{"sql": "...", "explanation": "..."}``."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# OpenAI-compatible provider (OpenAI, DeepSeek, Ollama, etc.)
# ---------------------------------------------------------------------------


class OpenAiCompatibleProvider(AiProvider):
    """Works with OpenAI, DeepSeek, Ollama, any OpenAI-compatible API."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4o-mini",
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model

    def generate_sql(
        self,
        prompt: str,
        schema: list[str],
        table_name: str,
        sample_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        user_message = _build_user_message(prompt, schema, table_name, sample_rows)
        request_body = json.dumps(
            {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
            },
            ensure_ascii=False,
        ).encode("utf-8")

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        req = urllib.request.Request(url, data=request_body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS, context=_make_ssl_context()) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            raw_content = data["choices"][0]["message"]["content"]
            return _extract_sql_result(raw_content)
        except Exception as exc:
            return {"ok": False, "error": str(exc), "error_type": "ai_provider_error"}


# ---------------------------------------------------------------------------
# Anthropic Claude provider
# ---------------------------------------------------------------------------


class ClaudeProvider(AiProvider):
    """Anthropic Claude Messages API."""

    _API_URL = "https://api.anthropic.com/v1/messages"

    def __init__(
        self,
        api_key: str,
        model: str = "claude-sonnet-4-20250514",
    ) -> None:
        self.api_key = api_key
        self.model = model

    def generate_sql(
        self,
        prompt: str,
        schema: list[str],
        table_name: str,
        sample_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        user_message = _build_user_message(prompt, schema, table_name, sample_rows)
        request_body = json.dumps(
            {
                "model": self.model,
                "system": _SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": user_message},
                ],
                "max_tokens": 1024,
            },
            ensure_ascii=False,
        ).encode("utf-8")

        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
        }

        req = urllib.request.Request(self._API_URL, data=request_body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS, context=_make_ssl_context()) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            # Claude Messages API returns content as a list of blocks.
            raw_content = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    raw_content += block["text"]
            return _extract_sql_result(raw_content)
        except Exception as exc:
            return {"ok": False, "error": str(exc), "error_type": "ai_provider_error"}


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_provider(config: dict[str, Any]) -> AiProvider:
    """Create a provider from a config dict.

    Expected keys: ``provider``, ``api_key``, ``base_url``, ``model``.
    """
    provider_name = str(config.get("provider") or "openai").strip().lower()
    api_key = str(config.get("api_key") or "")
    base_url = str(config.get("base_url") or "https://api.openai.com/v1")
    model = str(config.get("model") or "")

    if provider_name in {"claude", "anthropic"}:
        return ClaudeProvider(
            api_key=api_key,
            model=model or "claude-sonnet-4-20250514",
        )

    # Default: OpenAI-compatible (covers openai, deepseek, ollama, etc.)
    return OpenAiCompatibleProvider(
        api_key=api_key,
        base_url=base_url,
        model=model or "gpt-4o-mini",
    )
