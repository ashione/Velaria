"""Helpers for reading and grouping Feishu Bitable data."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from typing import Dict, Iterable, List, Optional


class BitableClientError(RuntimeError):
    """Raised when Feishu Bitable API returns a non-zero business error."""


class BitableClient:
    """Minimal Bitable API client for app_access_token + record listing."""

    _AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    _RECORD_SEARCH_TEMPLATE = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}"
        "/tables/{table_id}/records/search"
    )

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        request_timeout_seconds: int = 10,
    ):
        self.app_id = app_id
        self.app_secret = app_secret
        self.request_timeout_seconds = request_timeout_seconds
        self._cached_access_token: Optional[str] = None
        self._cached_access_token_expire_at = 0.0

    def parse_bitable_url(self, bitable_url: str) -> tuple[str, str, Optional[str]]:
        parsed = urllib.parse.urlparse(bitable_url)
        path = parsed.path.strip("/").split("/")
        if len(path) < 4 or path[0] != "base" or path[2] != "tables":
            raise ValueError(f"Unsupported Bitable URL format: {bitable_url}")

        app_token = path[1]
        table_id = path[3]
        query = urllib.parse.parse_qs(parsed.query)
        view_id = query.get("view", [None])[0]
        return app_token, table_id, view_id

    def _now(self) -> float:
        return time.time()

    def _require_token(self) -> str:
        if self._cached_access_token and self._now() < self._cached_access_token_expire_at:
            return self._cached_access_token

        body = json.dumps({"app_id": self.app_id, "app_secret": self.app_secret}).encode(
            "utf-8"
        )
        data = self._request("POST", self._AUTH_URL, body=body)
        if not isinstance(data, dict) or data.get("code") != 0:
            code = "?" if not isinstance(data, dict) else data.get("code")
            msg = "" if not isinstance(data, dict) else data.get("msg")
            raise BitableClientError(f"token request failed: code={code} msg={msg}")

        token = data["app_access_token"]
        expires_in = int(data.get("expire", 7200))
        self._cached_access_token = token
        self._cached_access_token_expire_at = self._now() + max(1, expires_in - 120)
        return token

    def _request(
        self,
        method: str,
        url: str,
        body: Optional[bytes] = None,
    ) -> dict:
        headers = {"Content-Type": "application/json"}
        if self._cached_access_token is not None:
            headers["Authorization"] = f"Bearer {self._cached_access_token}"

        request = urllib.request.Request(
            url,
            data=body,
            method=method,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.request_timeout_seconds
            ) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise BitableClientError(f"HTTP request failed ({exc.code}): {detail}") from exc

        return json.loads(payload.decode("utf-8"))

    def _request_with_token(self, method: str, url: str, body: Optional[dict] = None) -> dict:
        token = self._require_token()
        request_body = None
        if body is not None:
            request_body = json.dumps(body).encode("utf-8")
            request = urllib.request.Request(
                url,
                data=request_body,
                method=method,
            )
            request.add_header("Content-Type", "application/json")
            request.add_header("Authorization", f"Bearer {token}")
            try:
                with urllib.request.urlopen(request, timeout=self.request_timeout_seconds) as response:
                    payload = response.read()
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise BitableClientError(
                    f"HTTP request failed ({exc.code}): {detail}"
                ) from exc
            return json.loads(payload.decode("utf-8"))
        request = urllib.request.Request(url, method=method)
        request.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(request, timeout=self.request_timeout_seconds) as response:
            payload = response.read()
        return json.loads(payload.decode("utf-8"))

    def _normalize_row(self, record: dict) -> Dict[str, object]:
        if isinstance(record, dict) and "fields" in record:
            return dict(record.get("fields") or {})
        if (
            isinstance(record, dict)
            and "record" in record
            and isinstance(record["record"], dict)
            and "fields" in record["record"]
        ):
            return dict(record["record"]["fields"] or {})
        return {}

    def list_records(
        self,
        app_token: str,
        table_id: str,
        *,
        view_id: Optional[str] = None,
        page_size: int = 100,
    ) -> List[Dict[str, object]]:
        endpoint = self._RECORD_SEARCH_TEMPLATE.format(
            app_token=urllib.parse.quote(app_token),
            table_id=urllib.parse.quote(table_id),
        )

        all_records: List[Dict[str, object]] = []
        page_token: Optional[str] = None
        while True:
            request_body: Dict[str, object] = {"page_size": page_size}
            if view_id:
                request_body["view_id"] = view_id
            if page_token:
                request_body["page_token"] = page_token

            data = self._request_with_token("POST", endpoint, request_body)
            if not isinstance(data, dict) or data.get("code") != 0:
                code = "?" if not isinstance(data, dict) else data.get("code")
                msg = "" if not isinstance(data, dict) else data.get("msg")
                raise BitableClientError(f"list records failed: code={code} msg={msg}")

            payload = data.get("data") or {}
            raw_items = payload.get("items", [])
            if not isinstance(raw_items, list):
                raise BitableClientError("unexpected data format: items is not list")

            for item in raw_items:
                all_records.append(self._normalize_row(item))

            has_more = bool(payload.get("has_more"))
            page_token = payload.get("page_token")
            if not has_more or not page_token:
                break

        return all_records

    def list_records_from_url(self, bitable_url: str, *, page_size: int = 100) -> List[Dict[str, object]]:
        app_token, table_id, view_id = self.parse_bitable_url(bitable_url)
        return self.list_records(app_token, table_id, view_id=view_id, page_size=page_size)


def group_rows_by_field(rows: Iterable[Dict[str, object]], field_name: str) -> Dict[str, List[Dict[str, object]]]:
    grouped: Dict[str, List[Dict[str, object]]] = {}
    for row in rows:
        raw_value = row.get(field_name)
        key = ""
        if raw_value is None:
            key = ""
        elif isinstance(raw_value, str):
            key = raw_value
        elif isinstance(raw_value, (list, tuple)) and raw_value:
            if isinstance(raw_value[0], str):
                key = raw_value[0]
            else:
                key = str(raw_value[0])
        elif isinstance(raw_value, dict):
            if "name" in raw_value:
                key = str(raw_value["name"])
            else:
                key = json.dumps(raw_value, ensure_ascii=False, sort_keys=True)
        else:
            key = str(raw_value)

        rows_for_owner = grouped.setdefault(key, [])
        rows_for_owner.append(row)

    return grouped


def group_rows_count_by_field(rows: Iterable[Dict[str, object]], field_name: str) -> Dict[str, int]:
    output: Dict[str, int] = defaultdict(int)
    for key, grouped in group_rows_by_field(rows, field_name).items():
        output[key] = len(grouped)
    return dict(output)

