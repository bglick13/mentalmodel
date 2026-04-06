from __future__ import annotations

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.ir.graph import IREdge, IRGraph, IRNode


def ir_graph_to_json(graph: IRGraph) -> dict[str, JsonValue]:
    """Serialize one lowered graph into a JSON-safe payload."""

    return {
        "graph_id": graph.graph_id,
        "metadata": dict(graph.metadata),
        "nodes": [
            {
                "node_id": node.node_id,
                "kind": node.kind,
                "label": node.label,
                "metadata": dict(node.metadata),
            }
            for node in graph.nodes
        ],
        "edges": [
            {
                "edge_id": edge.edge_id,
                "source_node_id": edge.source_node_id,
                "source_port": edge.source_port,
                "target_node_id": edge.target_node_id,
                "target_port": edge.target_port,
                "kind": edge.kind,
            }
            for edge in graph.edges
        ],
    }


def ir_graph_from_json(payload: dict[str, JsonValue]) -> IRGraph:
    """Load one lowered graph from a persisted JSON payload."""

    graph_id = _require_str(payload, "graph_id")
    metadata = _require_str_mapping(payload, "metadata")
    nodes_payload = _require_object_list(payload, "nodes")
    edges_payload = _require_object_list(payload, "edges")
    return IRGraph(
        graph_id=graph_id,
        nodes=tuple(
            IRNode(
                node_id=_require_str(node_payload, "node_id"),
                kind=_require_str(node_payload, "kind"),
                label=_require_str(node_payload, "label"),
                metadata=_require_str_mapping(node_payload, "metadata"),
            )
            for node_payload in nodes_payload
        ),
        edges=tuple(
            IREdge(
                edge_id=_require_str(edge_payload, "edge_id"),
                source_node_id=_require_str(edge_payload, "source_node_id"),
                source_port=_require_str(edge_payload, "source_port"),
                target_node_id=_require_str(edge_payload, "target_node_id"),
                target_port=_require_str(edge_payload, "target_port"),
                kind=_require_str(edge_payload, "kind"),
            )
            for edge_payload in edges_payload
        ),
        metadata=metadata,
    )


def _require_str(payload: dict[str, JsonValue], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, str):
        return value
    raise TypeError(f"Expected {key!r} to be a string.")


def _require_str_mapping(payload: dict[str, JsonValue], key: str) -> dict[str, str]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise TypeError(f"Expected {key!r} to be an object.")
    result: dict[str, str] = {}
    for inner_key, inner_value in value.items():
        if not isinstance(inner_key, str) or not isinstance(inner_value, str):
            raise TypeError(f"Expected {key!r} to contain only string values.")
        result[inner_key] = inner_value
    return result


def _require_object_list(
    payload: dict[str, JsonValue],
    key: str,
) -> tuple[dict[str, JsonValue], ...]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise TypeError(f"Expected {key!r} to be a list.")
    items: list[dict[str, JsonValue]] = []
    for item in value:
        if not isinstance(item, dict):
            raise TypeError(f"Expected every item in {key!r} to be an object.")
        items.append(item)
    return tuple(items)
