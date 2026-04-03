from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class Finding:
    """Machine-readable validation finding."""

    code: str
    severity: str
    message: str
    node_id: str | None = None

    def render(self) -> str:
        location = f" node={self.node_id}" if self.node_id else ""
        return f"[{self.severity}] {self.code}:{location} {self.message}"
