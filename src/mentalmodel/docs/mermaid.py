from __future__ import annotations

from mentalmodel.ir.graph import IREdge, IRGraph, IRNode


def render_mermaid(graph: IRGraph) -> str:
    """Render a lowered graph as a deterministic Mermaid flowchart."""

    lines = ["flowchart LR"]
    for node in sorted(graph.nodes, key=_node_key):
        lines.append(f'    {_mermaid_id(node.node_id)}["{_quote(_node_label(node))}"]')
    for edge in sorted(graph.edges, key=_edge_key):
        label = edge.kind if edge.kind != "data" else "data"
        lines.append(
            "    "
            f'{_mermaid_id(edge.source_node_id)} -->|"{_quote(label)}"| '
            f"{_mermaid_id(edge.target_node_id)}"
        )
    return "\n".join(lines)


def _node_label(node: IRNode) -> str:
    return f"{node.label}<br/>{node.kind}"


def _mermaid_id(raw: str) -> str:
    chars = [char if char.isalnum() else "_" for char in raw]
    sanitized = "".join(chars)
    if not sanitized:
        return "node"
    if sanitized[0].isdigit():
        return f"n_{sanitized}"
    return sanitized


def _quote(value: str) -> str:
    return value.replace('"', '\\"')


def _node_key(node: IRNode) -> tuple[str, str]:
    return (node.kind, node.node_id)


def _edge_key(edge: IREdge) -> tuple[str, str, str, str]:
    return (edge.kind, edge.source_node_id, edge.target_node_id, edge.edge_id)
