from __future__ import annotations

import unittest
from collections.abc import Mapping
from typing import cast

from mentalmodel.observability.metrics import (
    MetricContext,
    MetricDefinition,
    MetricFieldProjection,
    MetricKind,
    MetricMapProjection,
    MetricObservation,
    OutputMetricSpec,
    cast_metric_specs,
    derive_output_metrics,
    extract_output_metrics,
    infer_output_metric_observations,
    infer_output_metrics,
    project_flat_metric_map,
    project_metric_map,
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
    def _context(
        self,
        *,
        node_id: str,
        node_kind: str,
        runtime_context: str,
    ) -> MetricContext:
        return MetricContext(
            graph_id="graph",
            run_id="run-1",
            node_id=node_id,
            node_kind=node_kind,
            runtime_context=runtime_context,
            frame_id="root",
            loop_node_id=None,
            iteration_index=None,
            service_name="mentalmodel-test",
        )

    def test_safe_inference_accepts_flat_numeric_summary(self) -> None:
        observations = infer_output_metric_observations(
            output={"sample_count": 8, "updated_policy_version": 4},
            context=self._context(
                node_id="learner_update",
                node_kind="actor",
                runtime_context="local",
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
            context=self._context(
                node_id="pangram_reward",
                node_kind="effect",
                runtime_context="sandbox",
            ),
        )
        self.assertEqual(observations, tuple())

    def test_derive_output_metrics_combines_extractors_and_inference(self) -> None:
        context = self._context(
            node_id="learner_update",
            node_kind="actor",
            runtime_context="local",
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

    def test_project_metric_map_projects_stable_subset_from_nested_map(self) -> None:
        context = self._context(
            node_id="optimizer_update",
            node_kind="effect",
            runtime_context="trainer",
        )
        raw_output = {
            "optimizer_metrics": {
                "loss": 1.25,
                "approx_kl": 0.03,
                "tokens_per_second": 420.0,
            },
            "status": "ok",
        }
        spec: OutputMetricSpec[dict[str, object]] = project_metric_map(
            MetricMapProjection(
                metric_map_key="optimizer_metrics",
                metric_name_prefix="mentalmodel.demo.optimizer",
                fields=(
                    MetricFieldProjection(
                        source_key="loss",
                        metric_name="loss",
                        description="Provider loss metric.",
                    ),
                    MetricFieldProjection(
                        source_key="approx_kl",
                        metric_name="approx_kl",
                    ),
                ),
            )
        )
        observations = derive_output_metrics(
            output=raw_output,
            context=context,
            specs=cast_metric_specs((spec,)),
        )
        self.assertEqual(
            [observation.definition.name for observation in observations],
            [
                "mentalmodel.demo.optimizer.loss",
                "mentalmodel.demo.optimizer.approx_kl",
            ],
        )
        self.assertEqual([observation.value for observation in observations], [1.25, 0.03])
        raw_metrics = cast(dict[str, float], raw_output["optimizer_metrics"])
        self.assertEqual(raw_metrics["tokens_per_second"], 420.0)

    def test_project_flat_metric_map_uses_accessor_for_typed_output(self) -> None:
        context = self._context(
            node_id="answer_synthesizer",
            node_kind="effect",
            runtime_context="local",
        )

        def answer_metric_map(output: object) -> Mapping[str, object] | None:
            if not isinstance(output, Mapping):
                return None
            return {
                "total_monthly_cost": output["total_monthly_cost"],
                "success_score": output["success_score"],
                "tool_call_count": output["tool_call_count"],
            }

        spec = project_flat_metric_map(
            prefix="mentalmodel.demo.answer",
            fields=("total_monthly_cost", "success_score"),
            accessor=answer_metric_map,
        )
        observations = derive_output_metrics(
            output={
                "total_monthly_cost": 51.0,
                "success_score": 1.0,
                "tool_call_count": 3,
            },
            context=context,
            specs=cast_metric_specs((spec,)),
        )
        self.assertEqual(
            [observation.definition.name for observation in observations],
            [
                "mentalmodel.demo.answer.total_monthly_cost",
                "mentalmodel.demo.answer.success_score",
            ],
        )

    def test_project_metric_map_skips_missing_and_non_numeric_fields(self) -> None:
        context = self._context(
            node_id="optimizer_update",
            node_kind="effect",
            runtime_context="trainer",
        )
        spec: OutputMetricSpec[dict[str, object]] = project_metric_map(
            MetricMapProjection(
                metric_map_key="optimizer_metrics",
                fields=(
                    MetricFieldProjection(source_key="loss", metric_name="loss"),
                    MetricFieldProjection(source_key="status", metric_name="status"),
                    MetricFieldProjection(source_key="missing", metric_name="missing"),
                ),
            )
        )
        observations = derive_output_metrics(
            output={"optimizer_metrics": {"loss": 1.25, "status": "ok"}},
            context=context,
            specs=cast_metric_specs((spec,)),
        )
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].definition.name, "loss")
        self.assertEqual(observations[0].value, 1.25)

    def test_metric_map_projection_rejects_duplicate_emitted_names(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate metric name"):
            MetricMapProjection(
                metric_name_prefix="mentalmodel.demo.optimizer",
                fields=(
                    MetricFieldProjection(source_key="loss", metric_name="shared"),
                    MetricFieldProjection(source_key="approx_kl", metric_name="shared"),
                ),
            )


if __name__ == "__main__":
    unittest.main()
