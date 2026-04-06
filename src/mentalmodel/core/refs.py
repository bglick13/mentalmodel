from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias


@dataclass(slots=True, frozen=True)
class Ref:
    """Reference to another node's output port."""

    target: str
    port: str = "default"


@dataclass(slots=True, frozen=True)
class BlockRef:
    """Reference to a logical block input bound by a Use node."""

    logical_name: str
    port: str = "default"


InputRef: TypeAlias = Ref | BlockRef
