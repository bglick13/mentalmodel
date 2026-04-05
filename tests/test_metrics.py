from __future__ import annotations

import unittest
from typing import cast

from mentalmodel.observability.metrics import (
    MetricContext,
    MetricDefinition,
    MetricKind,
    MetricObservation,
    OutputMetricSpec,
    cast_metric_specs,
    derive_output_metrics,
    extract_output_metrics,
    infer_output_metric_observations,
    infer_output_metrics,
)


class ConstantMetricExtractor:
    def extract(
        self,
        output: dict[str, float],
        context: MetricContext,
    ) -> tuple[MetricObservation, ...]:
        del output
        return (
            MetricObservation(
                definition=MetricDefinition(
                    name="mentalmodel.test.extractor",
                    kind=MetricKind.HISTOGRAM,
                ),
                value=1.5,
                attributes=context.default_attributes(),
            ),
        )


class MetricsTest(unittest.TestCase):
    def test_safe_inference_accepts_flat_numeric_summary(self) -> None:
        observations = infer_output_metric_observations(
            output={"sample_count": 8, "updated_policy_version": 4},
            context=MetricContext(
                graph_id="graph",
                run_id="run-1",
                node_id="learner_update",
                node_kind="actor",
                runtime_context="local",
                service_name="mentalmodel-test",
            ),
            prefix="mentalmodel.demo.learner_update",
        )
        self.assertEqual(
            [observation.definition.name for observation in observations],
            [
                "mentalmodel.demo.learner_update.sample_count",
                "mentalmodel.demo.learner_update.updated_policy_version",
            ],
        )
        self.assertEqual([observation.value for observation in observations], [8, 4])

    def test_safe_inference_rejects_dynamic_reward_map(self) -> None:
        observations = infer_output_metric_observations(
            output={"prompt-0:0": 0.8, "prompt-0:1": 0.7},
            context=MetricContext(
                graph_id="graph",
                run_id="run-1",
                node_id="pangram_reward",
                node_kind="effect",
                runtime_context="sandbox",
                service_name="mentalmodel-test",
            ),
        )
        self.assertEqual(observations, tuple())

    def test_derive_output_metrics_combines_extractors_and_inference(self) -> None:
        context = MetricContext(
            graph_id="graph",
            run_id="run-1",
            node_id="learner_update",
            node_kind="actor",
            runtime_context="local",
            service_name="mentalmodel-test",
        )
        specs: tuple[OutputMetricSpec[object], ...] = cast_metric_specs(
            (
                cast(
                    OutputMetricSpec[dict[str, float]],
                    extract_output_metrics(ConstantMetricExtractor()),
                ),
                infer_output_metrics(prefix="mentalmodel.demo.learner_update"),
            )
        )
        observations = derive_output_metrics(
            output={"sample_count": 8, "updated_policy_version": 4},
            context=context,
            specs=specs,
        )
        metric_names = [observation.definition.name for observation in observations]
        self.assertIn("mentalmodel.test.extractor", metric_names)
        self.assertIn("mentalmodel.demo.learner_update.sample_count", metric_names)
        self.assertIn(
            "mentalmodel.demo.learner_update.updated_policy_version",
            metric_names,
        )


if __name__ == "__main__":
    unittest.main()
