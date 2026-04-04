from __future__ import annotations

import asyncio
import unittest
from typing import cast

from mentalmodel.core import InvariantResult
from mentalmodel.examples.async_rl.demo import RolloutJoinOutput, SamplePolicyOutput, build_program
from mentalmodel.ir.lowering import lower_program
from mentalmodel.runtime.executor import AsyncExecutor


class AsyncRlDemoTest(unittest.TestCase):
    def test_build_program_lowers_to_expected_graph_shape(self) -> None:
        program = build_program()
        graph = lower_program(program)
        self.assertEqual(graph.graph_id, "async_rl_demo")
        node_ids = {node.node_id for node in graph.nodes}
        self.assertIn("sample_policy", node_ids)
        self.assertIn("rollout_join", node_ids)
        self.assertIn("remote_sampling", node_ids)
        self.assertGreaterEqual(len(graph.edges), 1)

    def test_demo_execution_produces_expected_rollout_structure(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        sample_policy = cast(SamplePolicyOutput, result.outputs["sample_policy"])
        self.assertEqual(sample_policy["sampled_policy_version"], 0)
        self.assertEqual(sample_policy["current_policy_version"], 0)
        self.assertEqual(len(sample_policy["samples"]), 8)

        rollout = cast(RolloutJoinOutput, result.outputs["rollout_join"])
        self.assertEqual(
            set(rollout.keys()),
            {
                "sampled_policy_version",
                "current_policy_version",
                "samples",
                "pangram_scores",
                "quality_scores",
                "kl_prefetch",
            },
        )
        self.assertEqual(len(rollout["samples"]), 8)
        self.assertEqual(len(rollout["pangram_scores"]), 8)
        self.assertEqual(len(rollout["quality_scores"]), 8)
        self.assertEqual(len(rollout["kl_prefetch"]), 8)

    def test_demo_execution_records_expected_invariant_output(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        invariant_result = cast(InvariantResult[int], result.outputs["staleness_invariant"])
        self.assertTrue(invariant_result.passed)
        self.assertEqual(
            dict(invariant_result.details),
            {
                "sampled_policy_version": 0,
                "current_policy_version": 0,
            },
        )


if __name__ == "__main__":
    unittest.main()
