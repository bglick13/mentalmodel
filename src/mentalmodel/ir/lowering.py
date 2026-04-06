from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from mentalmodel.core.bindings import InputBindingSource
from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.refs import BlockRef, InputRef, LoopItemRef, LoopStateRef, Ref
from mentalmodel.environment import ResourceKey, merge_resource_keys
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
    input_bindings: dict[str, InputBindingSource] = field(default_factory=dict)
    inherited_resources: tuple[ResourceKey[object], ...] = field(default_factory=tuple)
    _registered_ids: set[str] = field(default_factory=set)
    _lowered_primitives: dict[str, NamedPrimitive] = field(default_factory=dict)
    _lowered_resources: dict[str, tuple[ResourceKey[object], ...]] = field(
        default_factory=dict
    )

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
        input_bindings: dict[str, InputBindingSource] | None = None,
        inherited_resources: Sequence[ResourceKey[object]] | None = None,
    ) -> LoweringContext:
        merged = dict(self.inherited_metadata)
        if metadata:
            merged.update(metadata)
        bindings = dict(self.input_bindings)
        if input_bindings is not None:
            bindings.update(input_bindings)
        resources = self.inherited_resources
        if inherited_resources is not None:
            resources = merge_resource_keys(resources, tuple(inherited_resources))
        namespace_path = self.namespace_path
        if namespace_suffix is not None:
            namespace_path = (*namespace_path, namespace_suffix)
        return LoweringContext(
            registry=self.registry,
            inherited_metadata=merged,
            provenance=self.provenance,
            namespace_path=namespace_path,
            input_bindings=bindings,
            inherited_resources=resources,
            _registered_ids=self._registered_ids,
            _lowered_primitives=self._lowered_primitives,
            _lowered_resources=self._lowered_resources,
        )

    def with_provenance(self, provenance: NodeProvenance) -> LoweringContext:
        """Return a context that stamps nodes with the provided provenance."""

        return LoweringContext(
            registry=self.registry,
            inherited_metadata=dict(self.inherited_metadata),
            provenance=provenance,
            namespace_path=self.namespace_path,
            input_bindings=dict(self.input_bindings),
            inherited_resources=self.inherited_resources,
            _registered_ids=self._registered_ids,
            _lowered_primitives=self._lowered_primitives,
            _lowered_resources=self._lowered_resources,
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
        resolved_inputs = [self.resolve_input_source(ref) for ref in inputs]
        loop_item_bindings = {
            _input_port_for_ref(ref): source.key
            for ref, source in zip(inputs, resolved_inputs, strict=False)
            if source.kind == "loop_item"
        }
        loop_state_bindings = {
            _input_port_for_ref(ref): source.key
            for ref, source in zip(inputs, resolved_inputs, strict=False)
            if source.kind == "loop_state"
        }
        if loop_item_bindings or loop_state_bindings:
            node = IRNode(
                node_id=node.node_id,
                kind=node.kind,
                label=node.label,
                metadata={
                    **node.metadata,
                    **_synthetic_input_metadata(
                        loop_item_bindings=loop_item_bindings,
                        loop_state_bindings=loop_state_bindings,
                    ),
                },
            )
        lowered_node = self.register_container_node(node=node, primitive=primitive)
        fragment.nodes.append(lowered_node)
        for ref, resolved_ref in zip(inputs, resolved_inputs, strict=False):
            if resolved_ref.kind != "node_output":
                continue
            fragment.edges.append(
                self.make_edge(
                    source_node_id=resolved_ref.key,
                    source_port="default",
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
        resource_keys = merge_resource_keys(
            self.inherited_resources,
            _declared_resources(primitive),
        )
        lowered_node = self._apply_metadata(node, resource_keys=resource_keys)
        self._register_node(lowered_node)
        self._lowered_primitives[lowered_node.node_id] = primitive
        self._lowered_resources[lowered_node.node_id] = resource_keys
        return lowered_node

    def _apply_metadata(
        self,
        node: IRNode,
        *,
        resource_keys: tuple[ResourceKey[object], ...],
    ) -> IRNode:
        metadata = dict(self.inherited_metadata)
        metadata.update(node.metadata)
        metadata.update(self.provenance.as_metadata())
        if resource_keys:
            metadata["resource_keys"] = ",".join(
                key.name for key in resource_keys
            )
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

    def resolve_input_source(self, ref: InputRef) -> InputBindingSource:
        if isinstance(ref, BlockRef):
            resolved_binding = self.input_bindings.get(ref.logical_name)
            if resolved_binding is None:
                raise LoweringError(
                    f"Missing bound block input {ref.logical_name!r} in namespace "
                    f"{'.'.join(self.namespace_path) or '<root>'}."
                )
            return resolved_binding
        if isinstance(ref, Ref):
            resolved_ref = self.resolve_external_ref(ref)
            return InputBindingSource.node_output(resolved_ref.target)
        if isinstance(ref, LoopItemRef):
            if "loop_owner" not in self.inherited_metadata:
                raise LoweringError(
                    "LoopItemRef can only be used inside a StepLoop body."
                )
            return InputBindingSource.loop_item(ref.logical_name)
        if "loop_owner" not in self.inherited_metadata:
            raise LoweringError(
                "LoopStateRef can only be used inside a StepLoop body."
            )
        return InputBindingSource.loop_state(ref.logical_name)


def lower_program(
    program: Workflow[NamedPrimitive],
    *,
    registry: PluginRegistry | None = None,
) -> IRGraph:
    """Lower an authoring object into the canonical IR graph."""

    graph, _, _ = lower_program_with_bindings(program, registry=registry)
    return graph


def lower_program_with_bindings(
    program: Workflow[NamedPrimitive],
    *,
    registry: PluginRegistry | None = None,
) -> tuple[
    IRGraph,
    dict[str, NamedPrimitive],
    dict[str, tuple[ResourceKey[object], ...]],
]:
    """Lower a program and return primitive plus resource bindings."""

    ctx = LoweringContext(registry=registry or default_registry())
    fragment = ctx.lower(program)
    return IRGraph(
        graph_id=program.name,
        nodes=tuple(fragment.nodes),
        edges=tuple(fragment.edges),
    ), dict(ctx._lowered_primitives), dict(ctx._lowered_resources)


def _input_port_for_ref(ref: InputRef) -> str:
    if isinstance(ref, BlockRef):
        return ref.logical_name
    if isinstance(ref, LoopItemRef):
        return ref.logical_name
    if isinstance(ref, LoopStateRef):
        return ref.logical_name
    return ref.target


def _synthetic_input_metadata(
    *,
    loop_item_bindings: dict[str, str],
    loop_state_bindings: dict[str, str],
) -> dict[str, str]:
    metadata: dict[str, str] = {}
    if loop_item_bindings:
        metadata["loop_item_bindings"] = ",".join(
            f"{alias}={logical_name}"
            for alias, logical_name in sorted(loop_item_bindings.items())
        )
    if loop_state_bindings:
        metadata["loop_state_bindings"] = ",".join(
            f"{alias}={logical_name}"
            for alias, logical_name in sorted(loop_state_bindings.items())
        )
    return metadata


def _declared_resources(
    primitive: NamedPrimitive,
) -> tuple[ResourceKey[object], ...]:
    resources = getattr(primitive, "resources", ())
    if not isinstance(resources, tuple):
        resources = tuple(resources)
    return cast(tuple[ResourceKey[object], ...], resources)
