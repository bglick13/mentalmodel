from __future__ import annotations

import unittest

from mentalmodel.analysis import run_analysis
from mentalmodel.docs import build_node_inventory, render_markdown_artifacts, render_mermaid
from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.examples.runtime_environment.demo import build_program as build_runtime_program
from mentalmodel.ir.lowering import lower_program


class DocsTest(unittest.TestCase):
    def test_build_node_inventory_contains_runtime_context_metadata(self) -> None:
        inventory = build_node_inventory(lower_program(build_program()))
        sample_policy = next(entry for entry in inventory if entry.node_id == "sample_policy")
        runtime_context = next(entry for entry in inventory if entry.node_id == "remote_sampling")
        self.assertEqual(sample_policy.runtime_context, "sandbox")
        self.assertEqual(sample_policy.data_dependencies, ("batch_source", "policy_snapshot"))
        self.assertEqual(sample_policy.container_parent, "remote_sampling")
        self.assertEqual(sample_policy.plugin_kind, "core")
        self.assertEqual(runtime_context.plugin_kind, "runtime_context")
        self.assertEqual(
            runtime_context.plugin_origin,
            "mentalmodel.plugins.runtime_context",
        )

    def test_render_mermaid_includes_expected_nodes_and_edges(self) -> None:
        graph = lower_program(build_program())
        mermaid = render_mermaid(graph)
        self.assertIn("flowchart LR", mermaid)
        self.assertIn('sample_policy["sample_policy<br/>effect"]', mermaid)
        self.assertIn('batch_source -->|"data"| sample_policy', mermaid)
        self.assertIn('local_control_plane -->|"contains"| batch_source', mermaid)

    def test_render_markdown_artifacts_include_expected_sections(self) -> None:
        graph = lower_program(build_program())
        report = run_analysis(graph)
        artifacts = render_markdown_artifacts(graph, findings=report.findings)
        mapping = artifacts.as_mapping()
        self.assertEqual(
            set(mapping.keys()),
            {
                "topology.md",
                "node-inventory.md",
                "invariants.md",
                "runtime-contexts.md",
            },
        )
        self.assertIn("# async_rl_demo Topology", artifacts.topology)
        self.assertIn("## `sample_policy`", artifacts.node_inventory)
        self.assertIn("- Plugin Kind: `runtime_context`", artifacts.node_inventory)
        self.assertIn("## `staleness_invariant`", artifacts.invariants)
        self.assertIn("## `sandbox`", artifacts.runtime_contexts)
        self.assertIn("provenance=`runtime_context`", artifacts.runtime_contexts)

    def test_node_inventory_surfaces_declared_resource_keys(self) -> None:
        inventory = build_node_inventory(lower_program(build_runtime_program()))
        scale = next(entry for entry in inventory if entry.node_id == "fixture_scaling.scale")
        self.assertEqual(scale.resource_keys, ("multiplier",))
