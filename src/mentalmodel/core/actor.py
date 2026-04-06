from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

from mentalmodel.core.interfaces import ActorHandler
from mentalmodel.core.refs import InputRef
from mentalmodel.environment import ResourceKey
from mentalmodel.ir.graph import IRFragment, IRNode
from mentalmodel.observability.metrics import OutputMetricSpec

if TYPE_CHECKING:
    from mentalmodel.ir.lowering import LoweringContext

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
StateT = TypeVar("StateT")


@dataclass(slots=True)
class Actor(Generic[InputT, OutputT, StateT]):
    """Stateful semantic processing node."""

    name: str
    handler: ActorHandler[InputT, StateT, OutputT]
    inputs: list[InputRef] = field(default_factory=list)
    resources: tuple[ResourceKey[object], ...] = ()
    metrics: list[OutputMetricSpec[OutputT]] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)

    def lower(self, ctx: LoweringContext) -> IRFragment:
        metadata = dict(self.metadata)
        metadata.setdefault("stateful", "true")
        metadata.setdefault("handler", type(self.handler).__name__)
        node = IRNode(
            node_id=self.name,
            kind="actor",
            label=self.name,
            metadata=metadata,
        )
        return ctx.lower_leaf(primitive=self, node=node, inputs=self.inputs)
