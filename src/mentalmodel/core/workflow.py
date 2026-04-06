from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

from mentalmodel.ir.graph import IRFragment, IRNode

if TYPE_CHECKING:
    from mentalmodel.core.interfaces import NamedPrimitive
    from mentalmodel.ir.lowering import LoweringContext

ChildT = TypeVar("ChildT", bound="NamedPrimitive", covariant=True)


@dataclass(slots=True)
class Workflow(Generic[ChildT]):
    """Top-level semantic container for a program."""

    name: str
    children: Sequence[ChildT] = field(default_factory=list)
    description: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def lower(self, ctx: LoweringContext) -> IRFragment:
        metadata = dict(self.metadata)
        if self.description:
            metadata["description"] = self.description
        node = IRNode(
            node_id=self.name,
            kind="workflow",
            label=self.name,
            metadata=metadata,
        )
        return ctx.lower_container(primitive=self, node=node, children=self.children)
