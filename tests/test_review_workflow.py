from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import cast

from mentalmodel.examples.review_workflow import build_environment, build_program
from mentalmodel.examples.review_workflow.types import QueueSummary
from mentalmodel.runtime.runs import load_run_node_output, resolve_run_summary
from mentalmodel.testing import run_verification


class ReviewWorkflowTest(unittest.TestCase):
    def test_fixture_profile_reference_workflow_verifies(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report = run_verification(
                build_program(review_runtime="fixture_review"),
                runs_dir=Path(tmpdir),
                environment=build_environment(),
                invocation_name="review_workflow_fixture",
            )
            self.assertTrue(report.success)
            summary = cast(
                QueueSummary,
                load_run_node_output(
                    runs_dir=Path(tmpdir),
                    graph_id="review_workflow",
                    node_id="queue_summary",
                ),
            )
            self.assertEqual(
                summary,
                {
                    "processed": 3,
                    "escalations": 1,
                    "auto_publish": 2,
                },
            )
            run_summary = resolve_run_summary(
                runs_dir=Path(tmpdir),
                graph_id="review_workflow",
            )
            self.assertEqual(run_summary.invocation_name, "review_workflow_fixture")
            self.assertEqual(run_summary.runtime_profile_names, ("fixture_review", "strict_review"))

    def test_strict_profile_escalates_additional_ticket(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report = run_verification(
                build_program(review_runtime="strict_review"),
                runs_dir=Path(tmpdir),
                environment=build_environment(),
                invocation_name="review_workflow_strict",
            )
            self.assertTrue(report.success)
            summary = cast(
                QueueSummary,
                load_run_node_output(
                    runs_dir=Path(tmpdir),
                    graph_id="review_workflow",
                    node_id="queue_summary",
                ),
            )
            self.assertEqual(
                summary,
                {
                    "processed": 3,
                    "escalations": 2,
                    "auto_publish": 1,
                },
            )
