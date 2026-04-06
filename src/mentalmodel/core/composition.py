from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

from mentalmodel.core.interfaces import JoinReducer
from mentalmodel.core.refs import InputRef
from mentalmodel.environment import ResourceKey
from mentalmodel.ir.graph import IRFragment, IRNode

if TYPE_CHECKING:
    from mentalmodel.core.interfaces import NamedPrimitive
    from mentalmodel.ir.lowering import LoweringContext

ChildT = TypeVar("ChildT", bound="NamedPrimitive", covariant=True)
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


@dataclass(slots=True)
class Parallel(Generic[ChildT]):
    """Semantic grouping for fanout work."""

    name: str
    children: Sequence[ChildT] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)

    def lower(self, ctx: LoweringContext) -> IRFragment:
        node = IRNode(
            node_id=self.name,
            kind="parallel",
            label=self.name,
            metadata=dict(self.metadata),
        )
        return ctx.lower_container(primitive=self, node=node, children=self.children)


@dataclass(slots=True)
class Join(Generic[InputT, OutputT]):
    """Explicit merge point for prior values."""

    name: str
    inputs: list[InputRef] = field(default_factory=list)
    reducer: JoinReducer[InputT, OutputT] | None = None
    resources: tuple[ResourceKey[object], ...] = ()
    metadata: dict[str, str] = field(default_factory=dict)

    def lower(self, ctx: LoweringContext) -> IRFragment:
        metadata = dict(self.metadata)
        if self.reducer is not None:
            metadata.setdefault("reducer", type(self.reducer).__name__)
        node = IRNode(
            node_id=self.name,
            kind="join",
            label=self.name,
            metadata=metadata,
        )
        return ctx.lower_leaf(primitive=self, node=node, inputs=self.inputs)
