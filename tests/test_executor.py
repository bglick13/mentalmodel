from __future__ import annotations

import asyncio
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
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
from mentalmodel.observability.tracing import TracingAdapter
from mentalmodel.plugins.runtime_context import RuntimeContext
from mentalmodel.runtime import compile_program
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.errors import ExecutionError, InvariantViolationError
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


class FakeTracingAdapter:
    def __init__(self) -> None:
        self.span_names: list[str] = []

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
        self.assertEqual(sample_policy.metadata.dependencies, ("batch_source",))
        self.assertEqual(
            rollout_join.metadata.dependencies,
            ("kl_prefetch", "pangram_reward", "quality_reward", "sample_policy"),
        )

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

    def test_execution_records_have_monotonic_sequences_and_expected_order(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        sequences = [record.sequence for record in result.records]
        self.assertEqual(sequences, sorted(sequences))
        batch_source_events = [
            record.event_type for record in result.records if record.node_id == "batch_source"
        ]
        self.assertEqual(
            batch_source_events[:3],
            ["node.started", "state.read", "state.transition"],
        )
        sample_effect_records = [
            record for record in result.records if record.node_id == "sample_policy"
        ]
        sample_effect_events = [record.event_type for record in sample_effect_records]
        self.assertIn("effect.invoked", sample_effect_events)
        self.assertIn("effect.completed", sample_effect_events)
        invoked = next(
            record for record in sample_effect_records if record.event_type == "effect.invoked"
        )
        self.assertEqual(invoked.payload["input_keys"], ["batch_source"])
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
            ["kl_prefetch", "pangram_reward", "quality_reward", "sample_policy"],
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
        )
        with self.assertRaises(LoweringError):
            compile_execution_node(
                metadata=bad_metadata,
                primitive=actor_primitive,
                input_adapter=MappingInputAdapter[object](bad_metadata.dependencies),
            )

    def test_executor_uses_tracing_adapter_for_node_spans(self) -> None:
        tracing = FakeTracingAdapter()
        asyncio.run(AsyncExecutor(tracing=cast(TracingAdapter, tracing)).run(build_program()))
        self.assertIn("actor:batch_source", tracing.span_names)
        self.assertIn("effect:sample_policy", tracing.span_names)


if __name__ == "__main__":
    unittest.main()
