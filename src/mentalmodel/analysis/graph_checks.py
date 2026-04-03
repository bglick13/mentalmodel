from __future__ import annotations

from mentalmodel.analysis.findings import Finding
from mentalmodel.ir.graph import IRGraph


def run_graph_checks(graph: IRGraph) -> list[Finding]:
    """Run Milestone 1 structural validations against a lowered graph."""

    findings: list[Finding] = []
    seen_ids: set[str] = set()
    for node in graph.nodes:
        if node.node_id in seen_ids:
            findings.append(
                Finding(
                    code="duplicate-node-id",
                    severity="error",
                    message=f"Duplicate node id {node.node_id!r} found in graph.",
                    node_id=node.node_id,
                )
            )
        seen_ids.add(node.node_id)

    node_ids = graph.node_ids()
    for edge in graph.edges:
        if edge.source_node_id not in node_ids:
            findings.append(
                Finding(
                    code="unknown-edge-source",
                    severity="error",
                    message=(f"Edge references unknown source node {edge.source_node_id!r}."),
                    node_id=edge.target_node_id,
                )
            )
        if edge.target_node_id not in node_ids:
            findings.append(
                Finding(
                    code="unknown-edge-target",
                    severity="error",
                    message=(f"Edge references unknown target node {edge.target_node_id!r}."),
                    node_id=edge.source_node_id,
                )
            )

    if not graph.nodes:
        findings.append(
            Finding(
                code="empty-graph",
                severity="error",
                message="Lowered graph contains no nodes.",
            )
        )
    return findings
