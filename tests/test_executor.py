from __future__ import annotations

import asyncio
import unittest
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import cast

from mentalmodel.core import (
    Actor,
    ActorHandler,
    ActorResult,
    Effect,
    EffectHandler,
    Invariant,
    InvariantChecker,
    InvariantResult,
    Ref,
    Workflow,
)
from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import LoweringError
from mentalmodel.examples.async_rl.demo import LearnerState, RefreshOutput, build_program
from mentalmodel.examples.autoresearch_sorting.demo import (
    build_program as build_autoresearch_program,
)
from mentalmodel.integrations.autoresearch.plugin import AutoResearchOutput
from mentalmodel.observability.config import TracingConfig, TracingMode
from mentalmodel.observability.metrics import MetricObservation
from mentalmodel.observability.tracing import RecordedSpan, TracingAdapter
from mentalmodel.plugins.runtime_context import RuntimeContext
from mentalmodel.runtime import compile_program
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.errors import ExecutionError, InvariantViolationError
from mentalmodel.runtime.execution import CompiledPluginNode
from mentalmodel.runtime.executor import AsyncExecutor
from mentalmodel.runtime.plan import (
    CompiledActorNode,
    CompiledEffectNode,
    CompiledInvariantNode,
    CompiledJoinNode,
    MappingInputAdapter,
    compile_execution_node,
)
from mentalmodel.runtime.recorder import ExecutionRecorder


class NoOpHandler(ActorHandler[dict[str, object], object, str]):
    async def handle(
        self,
        inputs: dict[str, object],
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[str, object]:
        del inputs, state, ctx
        return ActorResult(output="ok")


class ExplodingEffect(EffectHandler[dict[str, object], str]):
    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> str:
        del inputs, ctx
        raise RuntimeError("boom")


class AlwaysFailInvariant(InvariantChecker[dict[str, object], JsonValue]):
    async def check(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> InvariantResult[JsonValue]:
        del inputs, ctx
        return InvariantResult(
            passed=False,
            details={"reason": "expected failure"},
        )


class WarningFailInvariant(InvariantChecker[dict[str, object], JsonValue]):
    async def check(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> InvariantResult[JsonValue]:
        del inputs, ctx
        return InvariantResult(
            passed=False,
            details={"reason": "warning failure"},
        )


class FakeTracingAdapter:
    def __init__(self) -> None:
        self.span_names: list[str] = []
        self.sink_configured = False
        self.config = TracingConfig(
            service_name="mentalmodel-test",
            mode=TracingMode.DISK,
        )

    @contextmanager
    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, str] | None = None,
    ) -> Iterator[object]:
        del attributes
        self.span_names.append(name)
        yield object()

    def snapshot_spans(self) -> tuple[RecordedSpan, ...]:
        return tuple()

    def flush(self) -> None:
        return None

    def trace_summary(self) -> dict[str, str | bool | None]:
        return {
            "trace_mode": "disk",
            "trace_otlp_endpoint": None,
            "trace_mirror_to_disk": True,
            "trace_capture_local_spans": False,
            "trace_sink_configured": False,
            "trace_service_name": "mentalmodel-test",
            "trace_service_namespace": None,
            "trace_service_version": None,
        }


@dataclass(slots=True)
class RecordingMetricEmitter:
    observations: list[MetricObservation] = field(default_factory=list)
    flush_calls: int = 0

    def emit(self, observations: Sequence[MetricObservation]) -> None:
        self.observations.extend(observations)

    def flush(self) -> None:
        self.flush_calls += 1


class RaisingMetricEmitter:
    def emit(self, observations: Sequence[MetricObservation]) -> None:
        del observations
        raise RuntimeError("metric export failed")

    def flush(self) -> None:
        return None


class ExecutorTest(unittest.TestCase):
    def test_compile_program_builds_typed_execution_plan_nodes(self) -> None:
        compiled = compile_program(build_program())
        batch_source = compiled.plan.nodes["batch_source"]
        sample_policy = compiled.plan.nodes["sample_policy"]
        rollout_join = compiled.plan.nodes["rollout_join"]
        staleness = compiled.plan.nodes["staleness_invariant"]
        self.assertIsInstance(batch_source, CompiledActorNode)
        self.assertIsInstance(sample_policy, CompiledEffectNode)
        self.assertIsInstance(rollout_join, CompiledJoinNode)
        self.assertIsInstance(staleness, CompiledInvariantNode)
        self.assertEqual(sample_policy.metadata.dependencies, ("batch_source", "policy_snapshot"))
        self.assertEqual(
            rollout_join.metadata.dependencies,
            (
                "kl_prefetch",
                "pangram_reward",
                "policy_snapshot",
                "quality_reward",
                "sample_policy",
            ),
        )

    def test_compile_program_builds_executable_plugin_node(self) -> None:
        compiled = compile_program(build_autoresearch_program())
        search = compiled.plan.nodes["autoresearch_sorting"]
        self.assertIsInstance(search, CompiledPluginNode)
        self.assertEqual(search.metadata.kind, "autoresearch")
        self.assertEqual(search.metadata.runtime_context, "local")

    def test_async_executor_runs_autoresearch_plugin_end_to_end(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_autoresearch_program()))
        search_output = cast(AutoResearchOutput, result.outputs["autoresearch_sorting"])
        self.assertEqual(search_output["best_candidate"], "merge")
        self.assertEqual(search_output["metric_name"], "mentalmodel.demo.sorting.comparison_count")
        invariant_output = cast(InvariantResult[float], result.outputs["search_result_invariant"])
        self.assertTrue(invariant_output.passed)

    def test_compile_program_excludes_container_nodes_from_execution_plan(self) -> None:
        compiled = compile_program(build_program())
        self.assertNotIn("async_rl_demo", compiled.plan.nodes)
        self.assertNotIn("local_control_plane", compiled.plan.nodes)
        self.assertNotIn("remote_sampling", compiled.plan.nodes)
        self.assertNotIn("reward_fanout", compiled.plan.nodes)

    def test_async_executor_runs_demo_end_to_end(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        self.assertIn("refresh_sampler", result.outputs)
        refresh_output = cast(RefreshOutput, result.outputs["refresh_sampler"])
        self.assertEqual(refresh_output["refreshed_to_policy_version"], 4)
        self.assertIn("learner_update", result.state)
        learner_state = cast(LearnerState, result.state["learner_update"])
        self.assertEqual(learner_state["policy_version"], 4)

    def test_execution_records_include_semantic_events(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        event_types = {record.event_type for record in result.records}
        self.assertIn("node.started", event_types)
        self.assertIn("node.inputs_resolved", event_types)
        self.assertIn("state.transition", event_types)
        self.assertIn("effect.invoked", event_types)
        self.assertIn("invariant.checked", event_types)
        self.assertTrue(all(record.frame.frame_id == "root" for record in result.records))
        self.assertTrue(all(record.frame.iteration_index is None for record in result.records))
        self.assertTrue(all(entry.frame.frame_id == "root" for entry in result.framed_outputs))
        self.assertTrue(all(entry.frame.frame_id == "root" for entry in result.framed_state))

    def test_execution_records_have_monotonic_sequences_and_expected_order(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        sequences = [record.sequence for record in result.records]
        self.assertEqual(sequences, sorted(sequences))
        batch_source_events = [
            record.event_type for record in result.records if record.node_id == "batch_source"
        ]
        self.assertEqual(
            batch_source_events[:3],
            ["node.started", "node.inputs_resolved", "state.read"],
        )
        sample_effect_records = [
            record for record in result.records if record.node_id == "sample_policy"
        ]
        sample_effect_events = [record.event_type for record in sample_effect_records]
        self.assertIn("node.inputs_resolved", sample_effect_events)
        self.assertIn("effect.invoked", sample_effect_events)
        self.assertIn("effect.completed", sample_effect_events)
        resolved = next(
            record
            for record in sample_effect_records
            if record.event_type == "node.inputs_resolved"
        )
        self.assertEqual(resolved.payload["input_keys"], ["batch_source", "policy_snapshot"])
        invoked = next(
            record for record in sample_effect_records if record.event_type == "effect.invoked"
        )
        self.assertEqual(invoked.payload["input_keys"], ["batch_source", "policy_snapshot"])
        completed = next(
            record for record in sample_effect_records if record.event_type == "effect.completed"
        )
        self.assertEqual(completed.payload["output_type"], "dict")

    def test_execution_records_capture_join_invariant_and_state_payloads(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        batch_transition = next(
            record
            for record in result.records
            if record.node_id == "batch_source" and record.event_type == "state.transition"
        )
        self.assertEqual(batch_transition.payload["from_state"], {"type": "None"})
        self.assertEqual(
            batch_transition.payload["to_state"],
            {"keys": ["cursor"], "type": "dict"},
        )

        learner_transition = next(
            record
            for record in result.records
            if record.node_id == "learner_update" and record.event_type == "state.transition"
        )
        self.assertEqual(
            learner_transition.payload["to_state"],
            {"keys": ["policy_version"], "type": "dict"},
        )

        join_record = next(
            record
            for record in result.records
            if record.node_id == "rollout_join" and record.event_type == "join.resolved"
        )
        self.assertEqual(
            join_record.payload["input_keys"],
            ["kl_prefetch", "pangram_reward", "policy_snapshot", "quality_reward", "sample_policy"],
        )

        invariant_record = next(
            record
            for record in result.records
            if record.node_id == "staleness_invariant" and record.event_type == "invariant.checked"
        )
        self.assertEqual(
            invariant_record.payload,
            {"passed": True, "severity": "error"},
        )
        invariant_inputs = next(
            record
            for record in result.records
            if record.node_id == "staleness_invariant"
            and record.event_type == "node.inputs_resolved"
        )
        self.assertEqual(
            invariant_inputs.payload["input_keys"],
            ["rollout_join"],
        )
        self.assertEqual(
            invariant_inputs.payload["inputs"],
            {
                "rollout_join": {
                    "current_policy_version": 3,
                    "kl_prefetch": {
                        "prompt-0:0": [0.1, 0.2, 0.3],
                        "prompt-0:1": [0.1, 0.2, 0.3],
                        "prompt-0:2": [0.1, 0.2, 0.3],
                        "prompt-0:3": [0.1, 0.2, 0.3],
                        "prompt-1:0": [0.1, 0.2, 0.3],
                        "prompt-1:1": [0.1, 0.2, 0.3],
                        "prompt-1:2": [0.1, 0.2, 0.3],
                        "prompt-1:3": [0.1, 0.2, 0.3],
                    },
                    "pangram_scores": {
                        "prompt-0:0": 0.8,
                        "prompt-0:1": 0.8,
                        "prompt-0:2": 0.8,
                        "prompt-0:3": 0.8,
                        "prompt-1:0": 0.8,
                        "prompt-1:1": 0.8,
                        "prompt-1:2": 0.8,
                        "prompt-1:3": 0.8,
                    },
                    "quality_scores": {
                        "prompt-0:0": 0.6,
                        "prompt-0:1": 0.6,
                        "prompt-0:2": 0.6,
                        "prompt-0:3": 0.6,
                        "prompt-1:0": 0.6,
                        "prompt-1:1": 0.6,
                        "prompt-1:2": 0.6,
                        "prompt-1:3": 0.6,
                    },
                    "sampled_policy_version": 3,
                    "samples": [
                        {
                            "completion_text": "sample 0 for Rewrite this sentence clearly.",
                            "prompt_id": "prompt-0",
                            "sample_index": 0,
                        },
                        {
                            "completion_text": "sample 1 for Rewrite this sentence clearly.",
                            "prompt_id": "prompt-0",
                            "sample_index": 1,
                        },
                        {
                            "completion_text": "sample 2 for Rewrite this sentence clearly.",
                            "prompt_id": "prompt-0",
                            "sample_index": 2,
                        },
                        {
                            "completion_text": "sample 3 for Rewrite this sentence clearly.",
                            "prompt_id": "prompt-0",
                            "sample_index": 3,
                        },
                        {
                            "completion_text": "sample 0 for Humanize this paragraph.",
                            "prompt_id": "prompt-1",
                            "sample_index": 0,
                        },
                        {
                            "completion_text": "sample 1 for Humanize this paragraph.",
                            "prompt_id": "prompt-1",
                            "sample_index": 1,
                        },
                        {
                            "completion_text": "sample 2 for Humanize this paragraph.",
                            "prompt_id": "prompt-1",
                            "sample_index": 2,
                        },
                        {
                            "completion_text": "sample 3 for Humanize this paragraph.",
                            "prompt_id": "prompt-1",
                            "sample_index": 3,
                        },
                    ],
                }
            },
        )

    def test_effect_failure_is_recorded_and_raised(self) -> None:
        program: Workflow[
            Actor[dict[str, object], str, object] | Effect[dict[str, object], str]
        ] = Workflow(
            name="effect_failure",
            children=[
                Actor(name="source", handler=NoOpHandler(), inputs=[]),
                Effect(name="explode", handler=ExplodingEffect(), inputs=[Ref("source")]),
            ],
        )
        recorder = ExecutionRecorder()
        with self.assertRaises(RuntimeError):
            asyncio.run(AsyncExecutor(recorder=recorder).run(program))
        event_types = [
            record.event_type for record in recorder.records if record.node_id == "explode"
        ]
        self.assertIn("node.failed", event_types)
        self.assertIn("effect.invoked", event_types)

    def test_invariant_failure_is_recorded_and_raised(self) -> None:
        program: Workflow[
            Actor[dict[str, object], str, object] | Invariant[dict[str, object], JsonValue]
        ] = Workflow(
            name="invariant_failure",
            children=[
                Actor(name="source", handler=NoOpHandler(), inputs=[]),
                Invariant(name="check", checker=AlwaysFailInvariant(), inputs=[Ref("source")]),
            ],
        )
        recorder = ExecutionRecorder()
        with self.assertRaises(InvariantViolationError):
            asyncio.run(AsyncExecutor(recorder=recorder).run(program))
        check_events = [
            record.event_type for record in recorder.records if record.node_id == "check"
        ]
        self.assertIn("invariant.checked", check_events)
        self.assertIn("node.failed", check_events)

    def test_warning_invariant_failure_is_recorded_without_failing_run(self) -> None:
        program: Workflow[
            Actor[dict[str, object], str, object] | Invariant[dict[str, object], JsonValue]
        ] = Workflow(
            name="warning_invariant_failure",
            children=[
                Actor(name="source", handler=NoOpHandler(), inputs=[]),
                Invariant(
                    name="warn_check",
                    checker=WarningFailInvariant(),
                    inputs=[Ref("source")],
                    severity="warning",
                ),
            ],
        )
        recorder = ExecutionRecorder()
        metrics = RecordingMetricEmitter()
        result = asyncio.run(AsyncExecutor(recorder=recorder, metrics=metrics).run(program))
        self.assertIn("warn_check", result.outputs)
        invariant_output = cast(InvariantResult[JsonValue], result.outputs["warn_check"])
        self.assertFalse(invariant_output.passed)
        warn_events = [
            record.event_type for record in recorder.records if record.node_id == "warn_check"
        ]
        self.assertIn("invariant.checked", warn_events)
        self.assertIn("node.succeeded", warn_events)
        self.assertNotIn("node.failed", warn_events)
        invariant_record = next(
            record
            for record in recorder.records
            if record.node_id == "warn_check" and record.event_type == "invariant.checked"
        )
        self.assertEqual(
            invariant_record.payload,
            {"passed": False, "severity": "warning"},
        )
        metric_names = [observation.definition.name for observation in metrics.observations]
        self.assertIn("mentalmodel.invariant.failures", metric_names)
        invariant_metric = next(
            observation
            for observation in metrics.observations
            if observation.definition.name == "mentalmodel.invariant.failures"
        )
        self.assertEqual(invariant_metric.attributes["severity"], "warning")

    def test_missing_dependency_raises_execution_error(self) -> None:
        program: Workflow[Actor[dict[str, object], str, object]] = Workflow(
            name="missing_dep",
            children=[
                Actor(name="sink", handler=NoOpHandler(), inputs=[Ref("missing")]),
            ],
        )
        with self.assertRaises(ExecutionError):
            asyncio.run(AsyncExecutor().run(program))

    def test_compile_execution_node_rejects_kind_mismatch(self) -> None:
        compiled = compile_program(build_program())
        runtime_context = cast(RuntimeContext, build_program().children[0])
        actor_primitive = runtime_context.children[0]
        metadata = compiled.plan.nodes["sample_policy"].metadata
        bad_metadata = type(metadata)(
            node_id=metadata.node_id,
            kind="effect",
            label=metadata.label,
            runtime_context=metadata.runtime_context,
            dependencies=metadata.dependencies,
            input_bindings=metadata.input_bindings,
        )
        with self.assertRaises(LoweringError):
            compile_execution_node(
                metadata=bad_metadata,
                primitive=actor_primitive,
                input_adapter=MappingInputAdapter[object](bad_metadata.input_bindings),
            )

    def test_executor_uses_tracing_adapter_for_node_spans(self) -> None:
        tracing = FakeTracingAdapter()
        asyncio.run(AsyncExecutor(tracing=cast(TracingAdapter, tracing)).run(build_program()))
        self.assertIn("actor:batch_source", tracing.span_names)
        self.assertIn("effect:sample_policy", tracing.span_names)

    def test_executor_emits_built_in_and_output_derived_metrics(self) -> None:
        metrics = RecordingMetricEmitter()
        asyncio.run(AsyncExecutor(metrics=metrics).run(build_program()))
        metric_names = [observation.definition.name for observation in metrics.observations]
        self.assertIn("mentalmodel.run.started", metric_names)
        self.assertIn("mentalmodel.run.completed", metric_names)
        self.assertIn("mentalmodel.node.executions", metric_names)
        self.assertIn("mentalmodel.node.duration_ms", metric_names)
        self.assertIn("mentalmodel.demo.reward.pangram.mean", metric_names)
        self.assertIn("mentalmodel.demo.reward.quality.count", metric_names)
        self.assertIn("mentalmodel.demo.learner_update.sample_count", metric_names)
        self.assertIn(
            "mentalmodel.demo.learner_update.updated_policy_version",
            metric_names,
        )
        pangram_mean = next(
            observation
            for observation in metrics.observations
            if observation.definition.name == "mentalmodel.demo.reward.pangram.mean"
        )
        self.assertEqual(pangram_mean.value, 0.8)
        pangram_count = next(
            observation
            for observation in metrics.observations
            if observation.definition.name == "mentalmodel.demo.reward.pangram.count"
        )
        self.assertEqual(pangram_count.value, 8)
        sample_count = next(
            observation
            for observation in metrics.observations
            if observation.definition.name == "mentalmodel.demo.learner_update.sample_count"
        )
        self.assertEqual(sample_count.value, 8)
        self.assertEqual(metrics.flush_calls, 1)

    def test_metric_emission_failures_do_not_break_execution(self) -> None:
        result = asyncio.run(AsyncExecutor(metrics=RaisingMetricEmitter()).run(build_program()))
        self.assertIn("refresh_sampler", result.outputs)


if __name__ == "__main__":
    unittest.main()
