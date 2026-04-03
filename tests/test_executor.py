from __future__ import annotations

import asyncio
import unittest
from typing import cast

from mentalmodel.examples.async_rl.demo import LearnerState, RefreshOutput, build_program
from mentalmodel.runtime import compile_program
from mentalmodel.runtime.executor import AsyncExecutor
from mentalmodel.runtime.plan import CompiledEffectNode, CompiledJoinNode


class ExecutorTest(unittest.TestCase):
    def test_compile_program_builds_typed_execution_plan_nodes(self) -> None:
        compiled = compile_program(build_program())
        sample_policy = compiled.plan.nodes["sample_policy"]
        rollout_join = compiled.plan.nodes["rollout_join"]
        self.assertIsInstance(sample_policy, CompiledEffectNode)
        self.assertIsInstance(rollout_join, CompiledJoinNode)
        self.assertEqual(sample_policy.metadata.dependencies, ("batch_source",))
        self.assertEqual(
            rollout_join.metadata.dependencies,
            ("kl_prefetch", "pangram_reward", "quality_reward", "sample_policy"),
        )

    def test_async_executor_runs_demo_end_to_end(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        self.assertIn("refresh_sampler", result.outputs)
        refresh_output = cast(RefreshOutput, result.outputs["refresh_sampler"])
        self.assertEqual(refresh_output["refreshed_to_policy_version"], 1)
        self.assertIn("learner_update", result.state)
        learner_state = cast(LearnerState, result.state["learner_update"])
        self.assertEqual(learner_state["policy_version"], 1)

    def test_execution_records_include_semantic_events(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        event_types = {record.event_type for record in result.records}
        self.assertIn("node.started", event_types)
        self.assertIn("state.transition", event_types)
        self.assertIn("effect.invoked", event_types)
        self.assertIn("invariant.checked", event_types)


if __name__ == "__main__":
    unittest.main()
