from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from mentalmodel.ir.graph import IRFragment

if TYPE_CHECKING:
    from mentalmodel.ir.lowering import LoweringContext
    from mentalmodel.runtime.execution import (
        CompiledExecutionNode,
        ExecutionNodeMetadata,
        MappingInputAdapter,
    )


class PrimitivePlugin(Protocol):
    """Plugin contract for non-core primitives."""

    kind: str
    origin: str
    version: str | None

    def supports(self, primitive: object) -> bool:
        """Return whether the plugin can lower the given primitive."""

    def lower(self, primitive: object, ctx: LoweringContext) -> IRFragment:
        """Lower the primitive into a canonical IR fragment."""


class ExecutablePrimitivePlugin(PrimitivePlugin, Protocol):
    """Plugin contract for primitives that also own runtime execution."""

    def compile(
        self,
        *,
        primitive: object,
        metadata: ExecutionNodeMetadata,
        input_adapter: MappingInputAdapter[object],
    ) -> CompiledExecutionNode:
        """Compile the primitive into an executable runtime node."""
