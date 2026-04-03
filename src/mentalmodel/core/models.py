from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from mentalmodel.core.interfaces import JsonValue

OutputT = TypeVar("OutputT")
StateT = TypeVar("StateT")
DetailT = TypeVar("DetailT", bound=JsonValue)


@dataclass(slots=True)
class ActorResult(Generic[OutputT, StateT]):
    """Runtime result for an actor execution."""

    output: OutputT
    next_state: StateT | None = None
    observations: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(slots=True)
class InvariantResult(Generic[DetailT]):
    """Runtime result for an invariant evaluation."""

    passed: bool
    details: Mapping[str, DetailT] = field(default_factory=dict)
