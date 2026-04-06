from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.refs import BlockRef, InputRef, Ref
from mentalmodel.errors import LoweringError
from mentalmodel.ir.graph import IREdge, IRFragment, IRGraph, IRNode
from mentalmodel.ir.provenance import NodeProvenance
from mentalmodel.plugins.registry import PluginRegistry, default_registry

if TYPE_CHECKING:
    from mentalmodel.core.workflow import Workflow


@dataclass(slots=True)
class LoweringContext:
    """Shared mutable state while lowering authoring primitives into IR."""

    registry: PluginRegistry = field(default_factory=default_registry)
    inherited_metadata: dict[str, str] = field(default_factory=dict)
    provenance: NodeProvenance = field(default_factory=NodeProvenance.core)
    namespace_path: tuple[str, ...] = field(default_factory=tuple)
    input_bindings: dict[str, Ref] = field(default_factory=dict)
    _registered_ids: set[str] = field(default_factory=set)
    _lowered_primitives: dict[str, NamedPrimitive] = field(default_factory=dict)

    def lower(self, primitive: NamedPrimitive) -> IRFragment:
        lower = getattr(primitive, "lower", None)
        if callable(lower):
            return cast(IRFragment, lower(self.with_provenance(NodeProvenance.core())))
        plugin = self.registry.find_plugin(primitive)
        if plugin is None:
            raise LoweringError(
                f"No lowering strategy found for object of type {type(primitive).__name__}."
            )
        return plugin.lower(primitive, self.with_provenance(NodeProvenance.from_plugin(plugin)))

    def child_context(
        self,
        metadata: dict[str, str] | None = None,
        *,
        namespace_suffix: str | None = None,
        input_bindings: dict[str, Ref] | None = None,
    ) -> LoweringContext:
        merged = dict(self.inherited_metadata)
        if metadata:
            merged.update(metadata)
        bindings = dict(self.input_bindings)
        if input_bindings is not None:
            bindings.update(input_bindings)
        namespace_path = self.namespace_path
        if namespace_suffix is not None:
            namespace_path = (*namespace_path, namespace_suffix)
        return LoweringContext(
            registry=self.registry,
            inherited_metadata=merged,
            provenance=self.provenance,
            namespace_path=namespace_path,
            input_bindings=bindings,
            _registered_ids=self._registered_ids,
            _lowered_primitives=self._lowered_primitives,
        )

    def with_provenance(self, provenance: NodeProvenance) -> LoweringContext:
        """Return a context that stamps nodes with the provided provenance."""

        return LoweringContext(
            registry=self.registry,
            inherited_metadata=dict(self.inherited_metadata),
            provenance=provenance,
            namespace_path=self.namespace_path,
            input_bindings=dict(self.input_bindings),
            _registered_ids=self._registered_ids,
            _lowered_primitives=self._lowered_primitives,
        )

    def lower_container(
        self,
        *,
        primitive: NamedPrimitive,
        node: IRNode,
        children: Sequence[NamedPrimitive],
    ) -> IRFragment:
        fragment = IRFragment()
        container_node = self.register_container_node(node=node, primitive=primitive)
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

    def lower_leaf(
        self,
        *,
        primitive: NamedPrimitive,
        node: IRNode,
        inputs: Sequence[InputRef],
    ) -> IRFragment:
        fragment = IRFragment()
        lowered_node = self.register_container_node(node=node, primitive=primitive)
        fragment.nodes.append(lowered_node)
        for ref in inputs:
            resolved_ref = self.resolve_input_ref(ref)
            fragment.edges.append(
                self.make_edge(
                    source_node_id=resolved_ref.target,
                    source_port=resolved_ref.port,
                    target_node_id=lowered_node.node_id,
                    target_port=_input_port_for_ref(ref),
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

    def register_container_node(self, *, node: IRNode, primitive: NamedPrimitive) -> IRNode:
        lowered_node = self._apply_metadata(node)
        self._register_node(lowered_node)
        self._lowered_primitives[lowered_node.node_id] = primitive
        return lowered_node

    def _apply_metadata(self, node: IRNode) -> IRNode:
        metadata = dict(self.inherited_metadata)
        metadata.update(node.metadata)
        metadata.update(self.provenance.as_metadata())
        node_id = self.namespaced_name(node.node_id)
        if self.namespace_path:
            metadata.setdefault("logical_node_id", node.node_id)
        return IRNode(
            node_id=node_id,
            kind=node.kind,
            label=node.label,
            metadata=metadata,
        )

    def namespaced_name(self, raw: str) -> str:
        if not self.namespace_path:
            return raw
        return ".".join((*self.namespace_path, raw))

    def resolve_external_ref(self, ref: Ref) -> Ref:
        return Ref(target=self.namespaced_name(ref.target), port=ref.port)

    def resolve_input_ref(self, ref: InputRef) -> Ref:
        if isinstance(ref, BlockRef):
            resolved = self.input_bindings.get(ref.logical_name)
            if resolved is None:
                raise LoweringError(
                    f"Missing bound block input {ref.logical_name!r} in namespace "
                    f"{'.'.join(self.namespace_path) or '<root>'}."
                )
            if resolved.port != ref.port:
                return Ref(target=resolved.target, port=ref.port)
            return resolved
        return self.resolve_external_ref(ref)


def lower_program(
    program: Workflow[NamedPrimitive],
    *,
    registry: PluginRegistry | None = None,
) -> IRGraph:
    """Lower an authoring object into the canonical IR graph."""

    graph, _ = lower_program_with_bindings(program, registry=registry)
    return graph


def lower_program_with_bindings(
    program: Workflow[NamedPrimitive],
    *,
    registry: PluginRegistry | None = None,
) -> tuple[IRGraph, dict[str, NamedPrimitive]]:
    """Lower a program and return the lowered primitive binding map."""

    ctx = LoweringContext(registry=registry or default_registry())
    fragment = ctx.lower(program)
    return IRGraph(
        graph_id=program.name,
        nodes=tuple(fragment.nodes),
        edges=tuple(fragment.edges),
    ), dict(ctx._lowered_primitives)


def _input_port_for_ref(ref: InputRef) -> str:
    if isinstance(ref, BlockRef):
        return ref.logical_name
    return ref.target
