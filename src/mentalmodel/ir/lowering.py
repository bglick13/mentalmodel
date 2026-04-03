from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.refs import Ref
from mentalmodel.errors import LoweringError
from mentalmodel.ir.graph import IREdge, IRFragment, IRGraph, IRNode
from mentalmodel.plugins.registry import PluginRegistry, default_registry

if TYPE_CHECKING:
    from mentalmodel.core.workflow import Workflow


@dataclass(slots=True)
class LoweringContext:
    """Shared mutable state while lowering authoring primitives into IR."""

    registry: PluginRegistry = field(default_factory=default_registry)
    inherited_metadata: dict[str, str] = field(default_factory=dict)
    _registered_ids: set[str] = field(default_factory=set)

    def lower(self, primitive: NamedPrimitive) -> IRFragment:
        lower = getattr(primitive, "lower", None)
        if callable(lower):
            return cast(IRFragment, lower(self))
        plugin = self.registry.find_plugin(primitive)
        if plugin is None:
            raise LoweringError(
                f"No lowering strategy found for object of type {type(primitive).__name__}."
            )
        return plugin.lower(primitive, self)

    def child_context(self, metadata: dict[str, str] | None = None) -> LoweringContext:
        merged = dict(self.inherited_metadata)
        if metadata:
            merged.update(metadata)
        return LoweringContext(
            registry=self.registry,
            inherited_metadata=merged,
            _registered_ids=self._registered_ids,
        )

    def lower_container(self, *, node: IRNode, children: Sequence[NamedPrimitive]) -> IRFragment:
        fragment = IRFragment()
        container_node = self._apply_metadata(node)
        self._register_node(container_node)
        fragment.nodes.append(container_node)
        child_ctx = self.child_context()
        for child in children:
            child_fragment = child_ctx.lower(child)
            fragment.extend(child_fragment)
            child_roots = child_ctx.fragment_roots(child_fragment)
            for child_root in child_roots:
                fragment.edges.append(
                    self.make_edge(
                        source_node_id=container_node.node_id,
                        source_port="contains",
                        target_node_id=child_root,
                        target_port="contained",
                        kind="contains",
                    )
                )
        return fragment

    def lower_leaf(self, *, node: IRNode, inputs: list[Ref]) -> IRFragment:
        fragment = IRFragment()
        lowered_node = self._apply_metadata(node)
        self._register_node(lowered_node)
        fragment.nodes.append(lowered_node)
        for ref in inputs:
            fragment.edges.append(
                self.make_edge(
                    source_node_id=ref.target,
                    source_port=ref.port,
                    target_node_id=lowered_node.node_id,
                    target_port="input",
                )
            )
        return fragment

    def make_edge(
        self,
        *,
        source_node_id: str,
        source_port: str,
        target_node_id: str,
        target_port: str,
        kind: str = "data",
    ) -> IREdge:
        return IREdge(
            edge_id=f"{source_node_id}:{source_port}->{target_node_id}:{target_port}:{kind}",
            source_node_id=source_node_id,
            source_port=source_port,
            target_node_id=target_node_id,
            target_port=target_port,
            kind=kind,
        )

    @staticmethod
    def fragment_roots(fragment: IRFragment) -> list[str]:
        if not fragment.nodes:
            return []
        contained_targets = {
            edge.target_node_id for edge in fragment.edges if edge.kind == "contains"
        }
        roots = [node.node_id for node in fragment.nodes if node.node_id not in contained_targets]
        return roots or [fragment.nodes[0].node_id]

    def _register_node(self, node: IRNode) -> None:
        if node.node_id in self._registered_ids:
            raise LoweringError(f"Duplicate node id during lowering: {node.node_id!r}")
        self._registered_ids.add(node.node_id)

    def _apply_metadata(self, node: IRNode) -> IRNode:
        metadata = dict(self.inherited_metadata)
        metadata.update(node.metadata)
        return IRNode(
            node_id=node.node_id,
            kind=node.kind,
            label=node.label,
            metadata=metadata,
        )


def lower_program(
    program: Workflow[NamedPrimitive],
    *,
    registry: PluginRegistry | None = None,
) -> IRGraph:
    """Lower an authoring object into the canonical IR graph."""

    ctx = LoweringContext(registry=registry or default_registry())
    fragment = ctx.lower(program)
    return IRGraph(
        graph_id=program.name,
        nodes=tuple(fragment.nodes),
        edges=tuple(fragment.edges),
    )
