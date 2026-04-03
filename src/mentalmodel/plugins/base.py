from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from mentalmodel.ir.graph import IRFragment

if TYPE_CHECKING:
    from mentalmodel.ir.lowering import LoweringContext


class PrimitivePlugin(Protocol):
    """Plugin contract for non-core primitives."""

    kind: str

    def supports(self, primitive: object) -> bool:
        """Return whether the plugin can lower the given primitive."""

    def lower(self, primitive: object, ctx: LoweringContext) -> IRFragment:
        """Lower the primitive into a canonical IR fragment."""
