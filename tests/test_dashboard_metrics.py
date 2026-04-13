from __future__ import annotations

import unittest
from typing import cast

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.observability.dashboard_metrics import (
    evaluate_metric_groups,
    metric_rows_from_outputs_payload,
)
from mentalmodel.ui.catalog import DashboardMetricGroup


class DashboardMetricsTest(unittest.TestCase):
    def test_iterative_frame_metrics_collapse_into_single_series(self) -> None:
        metric_rows = metric_rows_from_outputs_payload(
            {
                "outputs": {},
                "framed_outputs": [
                    {
                        "node_id": "training_loop.training_step.tracking.step_operator_summary",
                        "frame_id": "training_loop[0]",
                        "loop_node_id": "training_loop",
                        "iteration_index": 0,
                        "value": {
                            "reward": {
                                "prompt_count": 64,
                            }
                        },
                    },
                    {
                        "node_id": "training_loop.training_step.tracking.step_operator_summary",
                        "frame_id": "training_loop[1]",
                        "loop_node_id": "training_loop",
                        "iteration_index": 1,
                        "value": {
                            "reward": {
                                "prompt_count": 72,
                            }
                        },
                    },
                ],
            }
        )
        groups = evaluate_metric_groups(
            groups=(
                DashboardMetricGroup(
                    group_id="step-reward",
                    title="Step Reward",
                    description="Per-step reward metrics.",
                    metric_path_prefixes=(
                        "reward.",
                        "training_loop.training_step.tracking.step_operator_summary.reward.",
                    ),
                    max_items=8,
                ),
            ),
            metric_rows=metric_rows,
            step_start=None,
            step_end=None,
            max_points=120,
        )
        self.assertEqual(len(groups), 1)
        series = cast(list[dict[str, JsonValue]], groups[0]["series"])
        self.assertEqual(len(series), 1)
        grouped_series = series[0]
        self.assertEqual(grouped_series["label"], "reward.prompt_count")
        self.assertEqual(grouped_series["semantic_kind"], "trend")
        self.assertEqual(grouped_series["render_hint"], "line")
        points = cast(list[dict[str, JsonValue]], grouped_series["points"])
        self.assertEqual(
            [point["iteration_index"] for point in points],
            [0, 1],
        )


if __name__ == "__main__":
    unittest.main()
