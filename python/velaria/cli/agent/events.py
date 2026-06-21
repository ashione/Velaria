from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class VelariaAgentEvent:
    type: str
    content: str = ""
    session_id: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": self.type,
            "content": self.content,
        }
        if self.session_id:
            payload["session_id"] = self.session_id
        if self.data:
            payload["data"] = self.data
        return payload
