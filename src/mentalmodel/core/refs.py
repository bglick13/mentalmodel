from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class Ref:
    """Reference to another node's output port."""

    target: str
    port: str = "default"
