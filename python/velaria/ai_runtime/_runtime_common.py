from __future__ import annotations

import os
import uuid
from typing import Any


def coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def normalize_proxy_env(proxy_env: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in proxy_env.items():
        key_text = str(key).strip()
        value_text = str(value).strip()
        if not key_text or not value_text:
            continue
        key_lower = key_text.lower()
        if key_lower in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}:
            normalized[key_lower] = value_text
    return normalized


def proxy_env_from_process(configured: dict[str, str]) -> dict[str, str]:
    merged = dict(configured)
    for key in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        if key not in merged:
            value = os.environ.get(key) or os.environ.get(key.upper())
            if value:
                merged[key] = value
    return merged


def apply_proxy_env(env: dict[str, str], configured: dict[str, str]) -> None:
    proxies = proxy_env_from_process(configured)
    if not proxies:
        return
    proxies.setdefault("no_proxy", "127.0.0.1,localhost,::1")
    for key, value in proxies.items():
        env[key] = value
        env[key.upper()] = value


def is_uuid_text(value: str) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except (TypeError, ValueError):
        return False
