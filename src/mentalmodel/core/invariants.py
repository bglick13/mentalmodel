from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

from mentalmodel.core.interfaces import InvariantChecker, JsonValue
from mentalmodel.core.refs import InputRef
from mentalmodel.environment import ResourceKey
from mentalmodel.ir.graph import IRFragment, IRNode

if TYPE_CHECKING:
    from mentalmodel.ir.lowering import LoweringContext

InputT = TypeVar("InputT")
DetailT = TypeVar("DetailT", bound=JsonValue)


@dataclass(slots=True)
class Invariant(Generic[InputT, DetailT]):
    """Constraint attached to explicit program values."""

    name: str
    checker: InvariantChecker[InputT, DetailT]
    inputs: list[InputRef] = field(default_factory=list)
    resources: tuple[ResourceKey[object], ...] = ()
    severity: str = "error"
    metadata: dict[str, str] = field(default_factory=dict)

    def lower(self, ctx: LoweringContext) -> IRFragment:
        metadata = dict(self.metadata)
        metadata.setdefault("severity", self.severity)
        metadata.setdefault("checker", type(self.checker).__name__)
        node = IRNode(
            node_id=self.name,
            kind="invariant",
            label=self.name,
            metadata=metadata,
        )
        return ctx.lower_leaf(primitive=self, node=node, inputs=self.inputs)
