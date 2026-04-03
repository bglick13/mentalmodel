from __future__ import annotations

from mentalmodel.analysis.findings import Finding
from mentalmodel.ir.graph import IRGraph


def run_semantic_checks(graph: IRGraph) -> list[Finding]:
    """Run Milestone 1 semantic validations against a lowered graph."""

    findings: list[Finding] = []
    workflow_nodes = [node for node in graph.nodes if node.kind == "workflow"]
    if not workflow_nodes:
        findings.append(
            Finding(
                code="missing-workflow-root",
                severity="warning",
                message="Graph does not declare a workflow root node.",
            )
        )

    for node in graph.nodes:
        if node.kind in {"actor", "effect", "invariant"} and "runtime_context" not in node.metadata:
            findings.append(
                Finding(
                    code="missing-runtime-context",
                    severity="warning",
                    message="Node does not inherit or declare a runtime context.",
                    node_id=node.node_id,
                )
            )
    return findings
