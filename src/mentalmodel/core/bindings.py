from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(slots=True, frozen=True)
class InputBindingSource:
    """Resolved source for one bound handler input."""

    kind: Literal["node_output", "loop_item", "loop_state"]
    key: str

    @classmethod
    def node_output(cls, node_id: str) -> InputBindingSource:
        return cls(kind="node_output", key=node_id)

    @classmethod
    def loop_item(cls, logical_name: str) -> InputBindingSource:
        return cls(kind="loop_item", key=logical_name)

    @classmethod
    def loop_state(cls, logical_name: str) -> InputBindingSource:
        return cls(kind="loop_state", key=logical_name)
