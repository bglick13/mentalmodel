from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import cast

from mentalmodel.core import InvariantResult
from mentalmodel.examples.agent_tool_use import (
    DEFAULT_TASKS,
    expected_artifact_names,
    generate_demo_artifacts,
    read_expected_demo_artifacts,
    write_demo_artifacts,
)
from mentalmodel.examples.agent_tool_use.demo import AnswerOutput, build_program, make_task
from mentalmodel.ir.lowering import lower_program
from mentalmodel.runtime import InvariantViolationError
from mentalmodel.runtime.executor import AsyncExecutor


class AgentToolUseDemoTest(unittest.TestCase):
    def test_build_program_lowers_to_expected_graph_shape(self) -> None:
        graph = lower_program(build_program())
        self.assertEqual(graph.graph_id, "agent_tool_use_demo")
        node_ids = {node.node_id for node in graph.nodes}
        self.assertIn("task_source", node_ids)
        self.assertIn("plan_lookup", node_ids)
        self.assertIn("discount_lookup", node_ids)
        self.assertIn("support_lookup", node_ids)
        self.assertIn("answer_invariant", node_ids)
        self.assertIn("sandbox_tools", node_ids)

    def test_demo_execution_produces_expected_answer(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        answer = cast(AnswerOutput, result.outputs["answer_synthesizer"])
        expected_task = DEFAULT_TASKS[0]
        self.assertEqual(
            answer["total_monthly_cost"],
            expected_task["expected_total_monthly_cost"],
        )
        self.assertEqual(
            answer["priority_support"],
            expected_task["expected_priority_support"],
        )
        self.assertEqual(answer["tool_call_count"], 4)
        self.assertEqual(answer["success_score"], 1.0)

    def test_demo_execution_records_expected_invariant_output(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        invariant_result = cast(
            InvariantResult[float | bool],
            result.outputs["answer_invariant"],
        )
        self.assertTrue(invariant_result.passed)

    def test_demo_invariant_fails_for_incorrect_expected_total(self) -> None:
        bad_task = make_task(
            task_id="broken-task",
            plan_name="starter",
            seats=2,
            discount_name="none",
        )
        bad_task["expected_total_monthly_cost"] = 999.0
        with self.assertRaises(InvariantViolationError):
            asyncio.run(AsyncExecutor().run(build_program(bad_task)))

    def test_generated_demo_artifacts_match_checked_in_files(self) -> None:
        self.assertEqual(generate_demo_artifacts(), read_expected_demo_artifacts())

    def test_write_demo_artifacts_writes_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            written = write_demo_artifacts(Path(tmpdir))
            self.assertEqual(tuple(path.name for path in written), expected_artifact_names())
            for path in written:
                self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
