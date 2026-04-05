from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TypeAlias, TypedDict

from hypothesis import strategies as st

from mentalmodel.core import (
    Actor,
    ActorHandler,
    Effect,
    EffectHandler,
    Invariant,
    InvariantChecker,
    InvariantResult,
    Join,
    JoinReducer,
    MetricContext,
    MetricExtractor,
    MetricObservation,
    Parallel,
    Ref,
    Workflow,
    extract_output_metrics,
    infer_output_metrics,
)
from mentalmodel.core.models import ActorResult
from mentalmodel.observability.metrics import MetricDefinition, MetricKind
from mentalmodel.plugins.runtime_context import RuntimeContext
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.testing import (
    assert_aligned_key_sets,
    assert_causal_order,
    assert_monotonic_non_decreasing,
    execute_program,
    hypothesis_property_check,
    invariant_fail,
    invariant_pass,
)


class BatchSourceInputs(TypedDict):
    pass


class BatchState(TypedDict):
    cursor: int


class PolicySnapshotInputs(TypedDict):
    pass


class PolicySnapshotState(TypedDict):
    policy_version: int


class PolicySnapshotOutput(TypedDict):
    current_policy_version: int


class PromptRecord(TypedDict):
    prompt_id: str
    prompt_text: str


class SamplePolicyInputs(TypedDict):
    batch_source: list[PromptRecord]
    policy_snapshot: PolicySnapshotOutput


class SampleRecord(TypedDict):
    prompt_id: str
    sample_index: int
    completion_text: str


class SamplePolicyOutput(TypedDict):
    sampled_policy_version: int
    samples: list[SampleRecord]


RewardScores: TypeAlias = dict[str, float]
KLPrefetchOutput: TypeAlias = dict[str, list[float]]


class RewardInputs(TypedDict):
    sample_policy: SamplePolicyOutput


class RolloutJoinInputs(TypedDict):
    sample_policy: SamplePolicyOutput
    policy_snapshot: PolicySnapshotOutput
    pangram_reward: RewardScores
    quality_reward: RewardScores
    kl_prefetch: KLPrefetchOutput


class RolloutJoinOutput(TypedDict):
    sampled_policy_version: int
    current_policy_version: int
    samples: list[SampleRecord]
    pangram_scores: RewardScores
    quality_scores: RewardScores
    kl_prefetch: KLPrefetchOutput


class LearnerState(TypedDict):
    policy_version: int


class LearnerInputs(TypedDict):
    rollout_join: RolloutJoinOutput


class LearnerOutput(TypedDict):
    updated_policy_version: int
    sample_count: int


class RefreshInputs(TypedDict):
    learner_update: LearnerOutput


class RefreshOutput(TypedDict):
    refreshed_to_policy_version: int


class InvariantInputs(TypedDict):
    rollout_join: RolloutJoinOutput


@dataclass(slots=True, frozen=True)
class RewardSummaryMetrics(MetricExtractor[RewardScores]):
    metric_prefix: str

    def extract(
        self,
        output: RewardScores,
        context: MetricContext,
    ) -> tuple[MetricObservation, ...]:
        if not output:
            return tuple()
        values = list(output.values())
        mean_value = sum(values) / len(values)
        base_attributes = context.default_attributes()
        return (
            MetricObservation(
                definition=MetricDefinition(
                    name=f"{self.metric_prefix}.mean",
                    kind=MetricKind.HISTOGRAM,
                    description="Mean reward across sampled rollouts.",
                ),
                value=mean_value,
                attributes=dict(base_attributes),
            ),
            MetricObservation(
                definition=MetricDefinition(
                    name=f"{self.metric_prefix}.max",
                    kind=MetricKind.HISTOGRAM,
                    description="Maximum reward across sampled rollouts.",
                ),
                value=max(values),
                attributes=dict(base_attributes),
            ),
            MetricObservation(
                definition=MetricDefinition(
                    name=f"{self.metric_prefix}.count",
                    kind=MetricKind.HISTOGRAM,
                    description="Number of scored rollouts in one reward pass.",
                ),
                value=len(values),
                attributes=dict(base_attributes),
            ),
        )


@dataclass(slots=True)
class PolicySnapshot(ActorHandler[PolicySnapshotInputs, PolicySnapshotState, PolicySnapshotOutput]):
    initial_policy_version: int

    async def handle(
        self,
        inputs: PolicySnapshotInputs,
        state: PolicySnapshotState | None,
        ctx: ExecutionContext,
    ) -> ActorResult[PolicySnapshotOutput, PolicySnapshotState]:
        del inputs, ctx
        current_version = (
            self.initial_policy_version if state is None else state["policy_version"]
        )
        return ActorResult(
            output={"current_policy_version": current_version},
            next_state={"policy_version": current_version},
        )


@dataclass(slots=True)
class BatchSource(ActorHandler[BatchSourceInputs, BatchState, list[PromptRecord]]):
    async def handle(
        self,
        inputs: BatchSourceInputs,
        state: BatchState | None,
        ctx: ExecutionContext,
    ) -> ActorResult[list[PromptRecord], BatchState]:
        del inputs, ctx
        cursor = 0 if state is None else state["cursor"]
        batch: list[PromptRecord] = [
            {"prompt_id": f"prompt-{cursor}", "prompt_text": "Rewrite this sentence clearly."},
            {"prompt_id": f"prompt-{cursor + 1}", "prompt_text": "Humanize this paragraph."},
        ]
        return ActorResult(output=batch, next_state={"cursor": cursor + len(batch)})


@dataclass(slots=True)
class PolicySampler(EffectHandler[SamplePolicyInputs, SamplePolicyOutput]):
    group_size: int
    sampler_lag: int

    async def invoke(
        self,
        inputs: SamplePolicyInputs,
        ctx: ExecutionContext,
    ) -> SamplePolicyOutput:
        del ctx
        batch_value = inputs["batch_source"]
        current_version = inputs["policy_snapshot"]["current_policy_version"]
        await asyncio.sleep(0.01)
        samples: list[SampleRecord] = []
        for prompt in batch_value:
            for sample_index in range(self.group_size):
                samples.append(
                    {
                        "prompt_id": prompt["prompt_id"],
                        "sample_index": sample_index,
                        "completion_text": (f"sample {sample_index} for {prompt['prompt_text']}"),
                    }
                )
        return {
            "sampled_policy_version": max(0, current_version - self.sampler_lag),
            "samples": samples,
        }


@dataclass(slots=True)
class PangramScorer(EffectHandler[RewardInputs, RewardScores]):
    async def invoke(
        self,
        inputs: RewardInputs,
        ctx: ExecutionContext,
    ) -> RewardScores:
        del ctx
        samples = inputs["sample_policy"]["samples"]
        await asyncio.sleep(0.02)
        return {f"{item['prompt_id']}:{item['sample_index']}": 0.8 for item in samples}


@dataclass(slots=True)
class QualityScorer(EffectHandler[RewardInputs, RewardScores]):
    async def invoke(
        self,
        inputs: RewardInputs,
        ctx: ExecutionContext,
    ) -> RewardScores:
        del ctx
        samples = inputs["sample_policy"]["samples"]
        await asyncio.sleep(0.015)
        return {f"{item['prompt_id']}:{item['sample_index']}": 0.6 for item in samples}


@dataclass(slots=True)
class KLPrefetch(EffectHandler[RewardInputs, KLPrefetchOutput]):
    async def invoke(
        self,
        inputs: RewardInputs,
        ctx: ExecutionContext,
    ) -> KLPrefetchOutput:
        del ctx
        samples = inputs["sample_policy"]["samples"]
        await asyncio.sleep(0.01)
        return {f"{item['prompt_id']}:{item['sample_index']}": [0.1, 0.2, 0.3] for item in samples}


@dataclass(slots=True)
class RolloutJoinReducer(JoinReducer[RolloutJoinInputs, RolloutJoinOutput]):
    async def reduce(
        self,
        inputs: RolloutJoinInputs,
        ctx: ExecutionContext,
    ) -> RolloutJoinOutput:
        del ctx
        sample_payload = inputs["sample_policy"]
        return {
            "sampled_policy_version": sample_payload["sampled_policy_version"],
            "current_policy_version": inputs["policy_snapshot"]["current_policy_version"],
            "samples": sample_payload["samples"],
            "pangram_scores": inputs["pangram_reward"],
            "quality_scores": inputs["quality_reward"],
            "kl_prefetch": inputs["kl_prefetch"],
        }


@dataclass(slots=True)
class LearnerUpdate(ActorHandler[LearnerInputs, LearnerState, LearnerOutput]):
    async def handle(
        self,
        inputs: LearnerInputs,
        state: LearnerState | None,
        ctx: ExecutionContext,
    ) -> ActorResult[LearnerOutput, LearnerState]:
        del ctx
        rollout = inputs["rollout_join"]
        previous_version = (
            rollout["current_policy_version"] if state is None else state["policy_version"]
        )
        next_version = previous_version + 1
        return ActorResult(
            output={
                "updated_policy_version": next_version,
                "sample_count": len(rollout["samples"]),
            },
            next_state={"policy_version": next_version},
            observations={"previous_policy_version": previous_version},
        )


@dataclass(slots=True)
class RefreshSampler(EffectHandler[RefreshInputs, RefreshOutput]):
    async def invoke(
        self,
        inputs: RefreshInputs,
        ctx: ExecutionContext,
    ) -> RefreshOutput:
        del ctx
        learner_output = inputs["learner_update"]
        await asyncio.sleep(0.005)
        return {
            "refreshed_to_policy_version": learner_output["updated_policy_version"],
        }


@dataclass(slots=True)
class PolicyStalenessChecker(InvariantChecker[InvariantInputs, int]):
    max_off_policy_steps: int

    async def check(
        self,
        inputs: InvariantInputs,
        ctx: ExecutionContext,
    ) -> InvariantResult[int]:
        del ctx
        rollout = inputs["rollout_join"]
        sampled = rollout["sampled_policy_version"]
        current = rollout["current_policy_version"]
        details = {
            "sampled_policy_version": sampled,
            "current_policy_version": current,
        }
        try:
            assert_causal_order(
                sampled,
                current,
                max_lag=self.max_off_policy_steps,
            )
        except AssertionError:
            return invariant_fail(details=details)
        return invariant_pass(details=details)


AsyncRlNode: TypeAlias = (
    RuntimeContext
    | Actor[PolicySnapshotInputs, PolicySnapshotOutput, PolicySnapshotState]
    | Actor[BatchSourceInputs, list[PromptRecord], BatchState]
    | Effect[SamplePolicyInputs, SamplePolicyOutput]
    | Parallel[Effect[RewardInputs, RewardScores] | Effect[RewardInputs, KLPrefetchOutput]]
    | Join[RolloutJoinInputs, RolloutJoinOutput]
    | Invariant[InvariantInputs, int]
    | Actor[LearnerInputs, LearnerOutput, LearnerState]
    | Effect[RefreshInputs, RefreshOutput]
)


def build_program(
    group_size: int = 4,
    *,
    sampler_lag: int = 0,
    max_off_policy_steps: int = 0,
    initial_policy_version: int = 3,
) -> Workflow[AsyncRlNode]:
    """Build the milestone-two async RL authoring model."""

    return Workflow(
        name="async_rl_demo",
        description="Milestone 2 async RL demo authored in semantic primitives.",
        children=[
            RuntimeContext(
                name="local_control_plane",
                runtime="local",
                children=[
                    Actor[PolicySnapshotInputs, PolicySnapshotOutput, PolicySnapshotState](
                        "policy_snapshot",
                        handler=PolicySnapshot(initial_policy_version=initial_policy_version),
                    ),
                    Actor[BatchSourceInputs, list[PromptRecord], BatchState](
                        "batch_source",
                        handler=BatchSource(),
                    ),
                    RuntimeContext(
                        name="remote_sampling",
                        runtime="sandbox",
                        children=[
                            Effect[SamplePolicyInputs, SamplePolicyOutput](
                                "sample_policy",
                                handler=PolicySampler(
                                    group_size=group_size,
                                    sampler_lag=sampler_lag,
                                ),
                                inputs=[Ref("batch_source"), Ref("policy_snapshot")],
                            )
                        ],
                    ),
                    Parallel[
                        Effect[RewardInputs, RewardScores] | Effect[RewardInputs, KLPrefetchOutput]
                    ](
                        name="reward_fanout",
                        children=[
                            Effect[RewardInputs, RewardScores](
                                "pangram_reward",
                                handler=PangramScorer(),
                                inputs=[Ref("sample_policy")],
                                metrics=[
                                    extract_output_metrics(
                                        RewardSummaryMetrics(
                                            metric_prefix="mentalmodel.demo.reward.pangram"
                                        )
                                    )
                                ],
                            ),
                            Effect[RewardInputs, RewardScores](
                                "quality_reward",
                                handler=QualityScorer(),
                                inputs=[Ref("sample_policy")],
                                metrics=[
                                    extract_output_metrics(
                                        RewardSummaryMetrics(
                                            metric_prefix="mentalmodel.demo.reward.quality"
                                        )
                                    )
                                ],
                            ),
                            Effect[RewardInputs, KLPrefetchOutput](
                                "kl_prefetch",
                                handler=KLPrefetch(),
                                inputs=[Ref("sample_policy")],
                            ),
                        ],
                    ),
                    Join[RolloutJoinInputs, RolloutJoinOutput](
                        "rollout_join",
                        inputs=[
                            Ref("policy_snapshot"),
                            Ref("sample_policy"),
                            Ref("pangram_reward"),
                            Ref("quality_reward"),
                            Ref("kl_prefetch"),
                        ],
                        reducer=RolloutJoinReducer(),
                    ),
                    Invariant[InvariantInputs, int](
                        "staleness_invariant",
                        checker=PolicyStalenessChecker(
                            max_off_policy_steps=max_off_policy_steps
                        ),
                        inputs=[Ref("rollout_join")],
                    ),
                    Actor[LearnerInputs, LearnerOutput, LearnerState](
                        "learner_update",
                        handler=LearnerUpdate(),
                        inputs=[Ref("rollout_join")],
                        metrics=[
                            infer_output_metrics(
                                prefix="mentalmodel.demo.learner_update"
                            )
                        ],
                    ),
                    Effect[RefreshInputs, RefreshOutput](
                        "refresh_sampler",
                        handler=RefreshSampler(),
                        inputs=[Ref("learner_update")],
                    ),
                ],
            )
        ],
    )


@hypothesis_property_check(
    "score maps align with sampled rollouts",
    group_size=st.integers(min_value=1, max_value=5),
)
def property_rollout_scores_align(
    program: Workflow[AsyncRlNode],
    group_size: int,
) -> None:
    del program
    result = execute_program(build_program(group_size=group_size))
    sample_policy = result.outputs["sample_policy"]
    assert isinstance(sample_policy, dict)
    samples = sample_policy["samples"]
    assert isinstance(samples, list)
    sample_keys = {
        f"{sample['prompt_id']}:{sample['sample_index']}"
        for sample in samples
    }
    expected_count = len(sample_keys)
    assert expected_count == len(samples)

    rollout_join = result.outputs["rollout_join"]
    assert isinstance(rollout_join, dict)
    pangram_scores = rollout_join["pangram_scores"]
    quality_scores = rollout_join["quality_scores"]
    kl_prefetch = rollout_join["kl_prefetch"]
    assert isinstance(pangram_scores, dict)
    assert isinstance(quality_scores, dict)
    assert isinstance(kl_prefetch, dict)
    assert sample_policy["sampled_policy_version"] == rollout_join["sampled_policy_version"]

    aligned = assert_aligned_key_sets(
        pangram_scores,
        quality_scores,
        kl_prefetch,
        expected_keys=sample_keys,
        labels=("pangram_scores", "quality_scores", "kl_prefetch"),
    )
    assert len(aligned) == expected_count


@hypothesis_property_check(
    "staleness invariant respects configured off-policy budget",
    group_size=st.integers(min_value=1, max_value=4),
    sampler_lag=st.integers(min_value=0, max_value=3),
    max_off_policy_steps=st.integers(min_value=0, max_value=2),
)
def property_staleness_budget_is_enforced(
    program: Workflow[AsyncRlNode],
    group_size: int,
    sampler_lag: int,
    max_off_policy_steps: int,
) -> None:
    del program
    budget_program = build_program(
        group_size=group_size,
        sampler_lag=sampler_lag,
        max_off_policy_steps=max_off_policy_steps,
    )
    if sampler_lag <= max_off_policy_steps:
        result = execute_program(budget_program)
        invariant = result.outputs["staleness_invariant"]
        assert isinstance(invariant, InvariantResult)
        assert invariant.passed
        rollout_join = result.outputs["rollout_join"]
        learner_update = result.outputs["learner_update"]
        refresh_sampler = result.outputs["refresh_sampler"]
        assert isinstance(rollout_join, dict)
        assert isinstance(learner_update, dict)
        assert isinstance(refresh_sampler, dict)
        assert_monotonic_non_decreasing(
            [
                rollout_join["sampled_policy_version"],
                rollout_join["current_policy_version"],
                learner_update["updated_policy_version"],
                refresh_sampler["refreshed_to_policy_version"],
            ],
            label="policy version flow",
        )
        return

    from mentalmodel.runtime import InvariantViolationError

    try:
        execute_program(budget_program)
    except InvariantViolationError:
        return
    raise AssertionError("expected staleness invariant to fail for stale sampler output")
