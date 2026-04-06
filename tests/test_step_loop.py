from __future__ import annotations

import asyncio
import importlib
import tempfile
import unittest
from pathlib import Path
from typing import cast

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.core.loop import StepLoopResult
from mentalmodel.examples.step_loop.demo import build_program
from mentalmodel.runtime import AsyncExecutor, build_replay_report, compile_program
from mentalmodel.runtime.runs import load_run_node_output
from mentalmodel.testing import run_verification


class StepLoopTest(unittest.TestCase):
    def test_compile_program_keeps_loop_body_out_of_top_level_plan(self) -> None:
        compiled = compile_program(build_program())

        self.assertIn("steps", compiled.plan.nodes)
        self.assertNotIn("steps.step.square_item", compiled.plan.nodes)
        self.assertNotIn("steps.step.next_total", compiled.plan.nodes)

    def test_step_loop_runs_with_carried_state_and_history(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))

        self.assertIn("steps", result.outputs)
        step_result = result.outputs["steps"]
        self.assertIsInstance(step_result, StepLoopResult)
        loop_result = cast(StepLoopResult, step_result)
        self.assertEqual(loop_result.iteration_count, 3)
        self.assertEqual(loop_result.final_outputs["next_total"], 14)
        history = cast(list[dict[str, int]], loop_result.history_outputs["step_report"])
        self.assertEqual(len(history), 3)
        self.assertEqual(history[1]["next_total"], 5)

        frame_ids = {
            entry.frame.frame_id
            for entry in result.framed_outputs
            if entry.node_id == "steps.step.step_report"
        }
        self.assertEqual(frame_ids, {"steps[0]", "steps[1]", "steps[2]"})

    def test_step_loop_run_artifacts_support_framed_body_output_lookup(self) -> None:
        module = importlib.import_module("mentalmodel.examples.step_loop.demo")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report = run_verification(build_program(), module=module, runs_dir=root)
            self.assertTrue(report.success)

            second_report = load_run_node_output(
                runs_dir=root,
                graph_id="step_loop_demo",
                node_id="steps.step.step_report",
                frame_id="steps[1]",
            )
            self.assertEqual(
                second_report,
                {
                    "item": 2,
                    "next_total": 5,
                    "prior_total": 1,
                    "squared": 4,
                },
            )

            loop_output = load_run_node_output(
                runs_dir=root,
                graph_id="step_loop_demo",
                node_id="steps",
            )
            loop_output_mapping = cast(dict[str, JsonValue], loop_output)
            final_outputs = cast(
                dict[str, JsonValue],
                loop_output_mapping["final_outputs"],
            )
            self.assertEqual(final_outputs["next_total"], 14)

    def test_replay_can_scope_to_one_loop_iteration(self) -> None:
        module = importlib.import_module("mentalmodel.examples.step_loop.demo")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report = run_verification(build_program(), module=module, runs_dir=root)
            run_id = report.runtime.run_id
            self.assertIsNotNone(run_id)
            replay = build_replay_report(
                runs_dir=root,
                graph_id="step_loop_demo",
                run_id=run_id,
                frame_id="steps[2]",
            )
            self.assertEqual(replay.frame_ids, ("steps[2]",))
            node_ids = {summary.node_id for summary in replay.node_summaries}
            self.assertIn("steps.step.square_item", node_ids)
            self.assertIn("steps.step.step_report", node_ids)
            self.assertIn("steps.step.next_total", node_ids)


if __name__ == "__main__":
    unittest.main()
