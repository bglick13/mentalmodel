from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from mentalmodel.ir.graph import IRFragment, IRNode

if TYPE_CHECKING:
    from mentalmodel.core.interfaces import NamedPrimitive
    from mentalmodel.ir.lowering import LoweringContext


@dataclass(slots=True)
class RuntimeContext:
    """Extension primitive for runtime/capability grouping."""

    name: str
    runtime: str
    children: Sequence[NamedPrimitive] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)


class RuntimeContextPlugin:
    """Lowers `RuntimeContext` objects into grouping nodes plus child metadata."""

    kind = "runtime_context"

    def supports(self, primitive: object) -> bool:
        return isinstance(primitive, RuntimeContext)

    def lower(self, primitive: object, ctx: LoweringContext) -> IRFragment:
        runtime_context = cast(RuntimeContext, primitive)
        metadata = dict(runtime_context.metadata)
        metadata["runtime_context"] = runtime_context.runtime
        node = IRNode(
            node_id=runtime_context.name,
            kind="runtime_context",
            label=runtime_context.name,
            metadata=metadata,
        )
        child_ctx = ctx.child_context({"runtime_context": runtime_context.runtime})
        fragment = IRFragment()
        lowered_node = child_ctx._apply_metadata(node)
        child_ctx._register_node(lowered_node)
        fragment.nodes.append(lowered_node)
        for child in runtime_context.children:
            child_fragment = child_ctx.lower(child)
            fragment.extend(child_fragment)
            for child_root in child_ctx.fragment_roots(child_fragment):
                fragment.edges.append(
                    child_ctx.make_edge(
                        source_node_id=lowered_node.node_id,
                        source_port="contains",
                        target_node_id=child_root,
                        target_port="contained",
                        kind="contains",
                    )
                )
        return fragment
