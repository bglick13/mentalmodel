from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from mentalmodel.core.interfaces import NamedPrimitive

ChildT = TypeVar("ChildT", bound="NamedPrimitive", covariant=True)
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


@dataclass(slots=True, frozen=True)
class BlockInput(Generic[InputT]):
    """Logical input declared on a reusable block."""

    required: bool = True


@dataclass(slots=True, frozen=True)
class BlockOutput(Generic[OutputT]):
    """Logical output exported from a reusable block."""

    source_node_id: str


@dataclass(slots=True, frozen=True)
class BlockDefaults:
    """Reusable defaults applied when a block is instantiated."""

    runtime_context: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class Block(Generic[ChildT]):
    """Reusable semantic fragment with declared logical inputs and outputs."""

    name: str
    inputs: Mapping[str, BlockInput[object]] = field(default_factory=dict)
    outputs: Mapping[str, BlockOutput[object]] = field(default_factory=dict)
    children: Sequence[ChildT] = field(default_factory=tuple)
    defaults: BlockDefaults | None = None
    description: str | None = None
