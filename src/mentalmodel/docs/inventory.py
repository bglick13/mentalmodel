from __future__ import annotations

from dataclasses import dataclass, field

from mentalmodel.ir.graph import IRGraph, IRNode
from mentalmodel.ir.provenance import (
    PLUGIN_KIND_METADATA_KEY,
    PLUGIN_ORIGIN_METADATA_KEY,
    PLUGIN_VERSION_METADATA_KEY,
)


@dataclass(slots=True, frozen=True)
class NodeInventoryEntry:
    """Normalized documentation view for a single IR node."""

    node_id: str
    kind: str
    label: str
    runtime_context: str | None
    plugin_kind: str | None
    plugin_origin: str | None
    plugin_version: str | None
    data_dependencies: tuple[str, ...]
    data_dependents: tuple[str, ...]
    bind_dependencies: tuple[str, ...]
    bind_dependents: tuple[str, ...]
    container_parent: str | None
    contained_children: tuple[str, ...]
    block_name: str | None
    block_inputs: tuple[str, ...]
    block_outputs: tuple[str, ...]
    resource_keys: tuple[str, ...]
    metadata: dict[str, str] = field(default_factory=dict)


def build_node_inventory(graph: IRGraph) -> tuple[NodeInventoryEntry, ...]:
    """Build a stable per-node inventory projection from the canonical IR."""

    data_dependencies: dict[str, set[str]] = {node.node_id: set() for node in graph.nodes}
    data_dependents: dict[str, set[str]] = {node.node_id: set() for node in graph.nodes}
    bind_dependencies: dict[str, set[str]] = {node.node_id: set() for node in graph.nodes}
    bind_dependents: dict[str, set[str]] = {node.node_id: set() for node in graph.nodes}
    container_parents: dict[str, str] = {}
    contained_children: dict[str, set[str]] = {node.node_id: set() for node in graph.nodes}

    for edge in graph.edges:
        if edge.kind == "data":
            data_dependents.setdefault(edge.source_node_id, set()).add(edge.target_node_id)
            data_dependencies.setdefault(edge.target_node_id, set()).add(edge.source_node_id)
        if edge.kind == "bind":
            bind_dependents.setdefault(edge.source_node_id, set()).add(edge.target_node_id)
            bind_dependencies.setdefault(edge.target_node_id, set()).add(edge.source_node_id)
        if edge.kind == "contains":
            contained_children.setdefault(edge.source_node_id, set()).add(edge.target_node_id)
            container_parents[edge.target_node_id] = edge.source_node_id

    entries = [
        NodeInventoryEntry(
            node_id=node.node_id,
            kind=node.kind,
            label=node.label,
            runtime_context=node.metadata.get("runtime_context"),
            plugin_kind=node.metadata.get(PLUGIN_KIND_METADATA_KEY),
            plugin_origin=node.metadata.get(PLUGIN_ORIGIN_METADATA_KEY),
            plugin_version=node.metadata.get(PLUGIN_VERSION_METADATA_KEY),
            data_dependencies=tuple(sorted(data_dependencies[node.node_id])),
            data_dependents=tuple(sorted(data_dependents[node.node_id])),
            bind_dependencies=tuple(sorted(bind_dependencies[node.node_id])),
            bind_dependents=tuple(sorted(bind_dependents[node.node_id])),
            container_parent=container_parents.get(node.node_id),
            contained_children=tuple(sorted(contained_children[node.node_id])),
            block_name=node.metadata.get("block_name"),
            block_inputs=_split_csv(node.metadata.get("block_inputs")),
            block_outputs=_split_csv(node.metadata.get("block_outputs")),
            resource_keys=_split_csv(node.metadata.get("resource_keys")),
            metadata=dict(sorted(node.metadata.items())),
        )
        for node in sorted(graph.nodes, key=lambda item: (item.kind, item.node_id))
    ]
    return tuple(entries)


def invariant_nodes(graph: IRGraph) -> tuple[IRNode, ...]:
    """Return invariant nodes in stable order."""

    return tuple(sorted((node for node in graph.nodes if node.kind == "invariant"), key=_node_key))


def runtime_context_groups(graph: IRGraph) -> dict[str, tuple[IRNode, ...]]:
    """Group nodes by declared runtime context."""

    grouped: dict[str, list[IRNode]] = {}
    for node in sorted(graph.nodes, key=_node_key):
        runtime_context = node.metadata.get("runtime_context")
        if runtime_context is None:
            continue
        grouped.setdefault(runtime_context, []).append(node)
    return {key: tuple(value) for key, value in sorted(grouped.items())}


def _node_key(node: IRNode) -> tuple[str, str]:
    return (node.kind, node.node_id)


def _split_csv(raw: str | None) -> tuple[str, ...]:
    if raw is None or not raw:
        return tuple()
    return tuple(part for part in raw.split(",") if part)
