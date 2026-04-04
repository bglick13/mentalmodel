from __future__ import annotations

import unittest

from mentalmodel.analysis import run_graph_checks, run_semantic_checks
from mentalmodel.ir.graph import IREdge, IRGraph, IRNode
from mentalmodel.ir.provenance import CORE_PLUGIN_KIND, CORE_PLUGIN_ORIGIN


class AnalysisTest(unittest.TestCase):
    def test_graph_checks_report_duplicate_node_ids(self) -> None:
        graph = IRGraph(
            graph_id="dupes",
            nodes=(
                IRNode(node_id="dup", kind="actor", label="dup"),
                IRNode(node_id="dup", kind="effect", label="dup"),
            ),
            edges=(),
        )
        findings = run_graph_checks(graph)
        self.assertEqual([finding.code for finding in findings], ["duplicate-node-id"])

    def test_graph_checks_report_unknown_edge_source_and_target(self) -> None:
        graph = IRGraph(
            graph_id="bad-edges",
            nodes=(IRNode(node_id="known", kind="actor", label="known"),),
            edges=(
                IREdge(
                    edge_id="missing-source",
                    source_node_id="missing",
                    source_port="default",
                    target_node_id="known",
                    target_port="input",
                ),
                IREdge(
                    edge_id="missing-target",
                    source_node_id="known",
                    source_port="default",
                    target_node_id="missing-target",
                    target_port="input",
                ),
            ),
        )
        findings = run_graph_checks(graph)
        codes = {finding.code for finding in findings}
        self.assertEqual(codes, {"unknown-edge-source", "unknown-edge-target"})

    def test_graph_checks_report_empty_graph(self) -> None:
        graph = IRGraph(graph_id="empty", nodes=(), edges=())
        findings = run_graph_checks(graph)
        self.assertEqual([finding.code for finding in findings], ["empty-graph"])

    def test_semantic_checks_report_missing_workflow_root_and_runtime_context(self) -> None:
        graph = IRGraph(
            graph_id="semantic",
            nodes=(
                IRNode(
                    node_id="actor1",
                    kind="actor",
                    label="actor1",
                    metadata={
                        "plugin_kind": CORE_PLUGIN_KIND,
                        "plugin_origin": CORE_PLUGIN_ORIGIN,
                    },
                ),
                IRNode(
                    node_id="effect1",
                    kind="effect",
                    label="effect1",
                    metadata={
                        "plugin_kind": CORE_PLUGIN_KIND,
                        "plugin_origin": CORE_PLUGIN_ORIGIN,
                    },
                ),
            ),
            edges=(),
        )
        findings = run_semantic_checks(graph)
        codes = [finding.code for finding in findings]
        self.assertIn("missing-workflow-root", codes)
        self.assertEqual(codes.count("missing-runtime-context"), 2)

    def test_semantic_checks_report_missing_plugin_provenance(self) -> None:
        graph = IRGraph(
            graph_id="provenance",
            nodes=(
                IRNode(node_id="workflow", kind="workflow", label="workflow"),
                IRNode(
                    node_id="actor1",
                    kind="actor",
                    label="actor1",
                    metadata={"runtime_context": "local"},
                ),
            ),
            edges=(),
        )
        findings = run_semantic_checks(graph)
        codes = [finding.code for finding in findings]
        self.assertIn("missing-plugin-provenance", codes)


if __name__ == "__main__":
    unittest.main()
