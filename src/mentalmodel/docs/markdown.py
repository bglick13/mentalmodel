from __future__ import annotations

from dataclasses import dataclass
from textwrap import indent

from mentalmodel.analysis.findings import Finding
from mentalmodel.docs.inventory import (
    NodeInventoryEntry,
    build_node_inventory,
    invariant_nodes,
    runtime_context_groups,
)
from mentalmodel.ir.graph import IRGraph


@dataclass(slots=True, frozen=True)
class MarkdownArtifacts:
    """Deterministic markdown projections for a lowered graph."""

    topology: str
    node_inventory: str
    invariants: str
    runtime_contexts: str

    def as_mapping(self) -> dict[str, str]:
        """Return the standard file-name mapping for generated docs."""

        return {
            "topology.md": self.topology,
            "node-inventory.md": self.node_inventory,
            "invariants.md": self.invariants,
            "runtime-contexts.md": self.runtime_contexts,
        }


def render_markdown_artifacts(
    graph: IRGraph,
    *,
    findings: tuple[Finding, ...] = (),
) -> MarkdownArtifacts:
    """Render the standard markdown documentation set for a lowered graph."""

    inventory = build_node_inventory(graph)
    return MarkdownArtifacts(
        topology=render_topology_markdown(graph, findings=findings),
        node_inventory=render_node_inventory_markdown(graph.graph_id, inventory),
        invariants=render_invariants_markdown(graph),
        runtime_contexts=render_runtime_contexts_markdown(graph),
    )


def render_topology_markdown(graph: IRGraph, *, findings: tuple[Finding, ...] = ()) -> str:
    """Render the topology overview markdown."""

    lines = [
        f"# {graph.graph_id} Topology",
        "",
        f"- Nodes: {len(graph.nodes)}",
        f"- Edges: {len(graph.edges)}",
        f"- Findings: {len(findings)}",
        "",
        "## Node Kinds",
        "",
    ]

    counts: dict[str, int] = {}
    for node in graph.nodes:
        counts[node.kind] = counts.get(node.kind, 0) + 1
    for kind, count in sorted(counts.items()):
        lines.append(f"- `{kind}`: {count}")

    lines.extend(["", "## Edges", ""])
    for edge in sorted(
        graph.edges,
        key=lambda item: (item.kind, item.source_node_id, item.target_node_id),
    ):
        lines.append(f"- `{edge.source_node_id}` -> `{edge.target_node_id}` (`{edge.kind}`)")

    lines.extend(["", "## Findings", ""])
    if findings:
        for finding in findings:
            location = f" node={finding.node_id}" if finding.node_id else ""
            lines.append(f"- `{finding.severity}` `{finding.code}`{location}: {finding.message}")
    else:
        lines.append("- No findings.")
    return "\n".join(lines)


def render_node_inventory_markdown(
    graph_id: str,
    inventory: tuple[NodeInventoryEntry, ...],
) -> str:
    """Render the node inventory markdown."""

    lines = [f"# {graph_id} Node Inventory", ""]
    for entry in inventory:
        lines.extend(
            [
                f"## `{entry.node_id}`",
                "",
                f"- Kind: `{entry.kind}`",
                f"- Label: `{entry.label}`",
                (
                    f"- Runtime Context: `{entry.runtime_context}`"
                    if entry.runtime_context is not None
                    else "- Runtime Context: none"
                ),
                (
                    f"- Plugin Kind: `{entry.plugin_kind}`"
                    if entry.plugin_kind is not None
                    else "- Plugin Kind: none"
                ),
                (
                    f"- Plugin Origin: `{entry.plugin_origin}`"
                    if entry.plugin_origin is not None
                    else "- Plugin Origin: none"
                ),
                (
                    f"- Plugin Version: `{entry.plugin_version}`"
                    if entry.plugin_version is not None
                    else "- Plugin Version: none"
                ),
                (
                    "- Data Dependencies: "
                    + (", ".join(f"`{node_id}`" for node_id in entry.data_dependencies) or "none")
                ),
                (
                    "- Data Dependents: "
                    + (", ".join(f"`{node_id}`" for node_id in entry.data_dependents) or "none")
                ),
                (
                    f"- Container Parent: `{entry.container_parent}`"
                    if entry.container_parent is not None
                    else "- Container Parent: none"
                ),
                (
                    "- Contained Children: "
                    + (", ".join(f"`{node_id}`" for node_id in entry.contained_children) or "none")
                ),
            ]
        )
        if entry.metadata:
            lines.extend(["- Metadata:", "", indent(_metadata_list(entry.metadata), prefix="  ")])
        lines.append("")
    return "\n".join(lines).rstrip()


def render_invariants_markdown(graph: IRGraph) -> str:
    """Render invariant-focused documentation."""

    lines = [f"# {graph.graph_id} Invariants", ""]
    nodes = invariant_nodes(graph)
    if not nodes:
        lines.append("No invariants declared.")
        return "\n".join(lines)

    dependency_map = _incoming_dependency_map(graph)
    for node in nodes:
        dependencies = dependency_map[node.node_id]
        input_summary = (
            ", ".join(f"`{dependency}`" for dependency in dependencies) if dependencies else "none"
        )
        lines.extend(
            [
                f"## `{node.node_id}`",
                "",
                f"- Severity: `{node.metadata.get('severity', 'error')}`",
                f"- Checker: `{node.metadata.get('checker', 'unknown')}`",
                f"- Inputs: {input_summary}",
                "",
            ]
        )
    return "\n".join(lines).rstrip()


def render_runtime_contexts_markdown(graph: IRGraph) -> str:
    """Render runtime-context group documentation."""

    lines = [f"# {graph.graph_id} Runtime Contexts", ""]
    groups = runtime_context_groups(graph)
    if not groups:
        lines.append("No runtime contexts declared.")
        return "\n".join(lines)

    for runtime_context, nodes in groups.items():
        lines.extend([f"## `{runtime_context}`", ""])
        for node in nodes:
            provenance = node.metadata.get("plugin_kind", "unknown")
            lines.append(f"- `{node.node_id}` (`{node.kind}`, provenance=`{provenance}`)")
        lines.append("")
    return "\n".join(lines).rstrip()


def _incoming_dependency_map(graph: IRGraph) -> dict[str, tuple[str, ...]]:
    dependencies: dict[str, set[str]] = {node.node_id: set() for node in graph.nodes}
    for edge in graph.edges:
        if edge.kind == "data":
            dependencies.setdefault(edge.target_node_id, set()).add(edge.source_node_id)
    return {key: tuple(sorted(value)) for key, value in dependencies.items()}


def _metadata_list(metadata: dict[str, str]) -> str:
    return "\n".join(f"- `{key}`: `{value}`" for key, value in metadata.items())
