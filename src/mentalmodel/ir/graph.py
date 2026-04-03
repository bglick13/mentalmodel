from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class IRNode:
    """Canonical node in the lowered semantic graph."""

    node_id: str
    kind: str
    label: str
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class IREdge:
    """Canonical directed edge in the lowered graph."""

    edge_id: str
    source_node_id: str
    source_port: str
    target_node_id: str
    target_port: str
    kind: str = "data"


@dataclass(slots=True)
class IRFragment:
    """Fragment emitted by a primitive during lowering."""

    nodes: list[IRNode] = field(default_factory=list)
    edges: list[IREdge] = field(default_factory=list)

    def extend(self, other: IRFragment) -> None:
        self.nodes.extend(other.nodes)
        self.edges.extend(other.edges)


@dataclass(slots=True, frozen=True)
class IRGraph:
    """Complete lowered graph for a program."""

    graph_id: str
    nodes: tuple[IRNode, ...]
    edges: tuple[IREdge, ...]
    metadata: dict[str, str] = field(default_factory=dict)

    def node_ids(self) -> set[str]:
        return {node.node_id for node in self.nodes}
