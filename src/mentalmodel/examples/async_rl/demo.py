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
    Parallel,
    Ref,
    Workflow,
)
from mentalmodel.core.models import ActorResult
from mentalmodel.plugins.runtime_context import RuntimeContext
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.testing import execute_program, hypothesis_property_check


class BatchSourceInputs(TypedDict):
    pass


class BatchState(TypedDict):
    cursor: int


class PromptRecord(TypedDict):
    prompt_id: str
    prompt_text: str


class SamplePolicyInputs(TypedDict):
    batch_source: list[PromptRecord]


class SampleRecord(TypedDict):
    prompt_id: str
    sample_index: int
    completion_text: str


class SamplePolicyOutput(TypedDict):
    sampled_policy_version: int
    current_policy_version: int
    samples: list[SampleRecord]


RewardScores: TypeAlias = dict[str, float]
KLPrefetchOutput: TypeAlias = dict[str, list[float]]


class RewardInputs(TypedDict):
    sample_policy: SamplePolicyOutput


class RolloutJoinInputs(TypedDict):
    sample_policy: SamplePolicyOutput
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

    async def invoke(
        self,
        inputs: SamplePolicyInputs,
        ctx: ExecutionContext,
    ) -> SamplePolicyOutput:
        del ctx
        batch_value = inputs["batch_source"]
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
            "sampled_policy_version": 0,
            "current_policy_version": 0,
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
            "current_policy_version": sample_payload["current_policy_version"],
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
        previous_version = 0 if state is None else state["policy_version"]
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
        delta = abs(current - sampled)
        return InvariantResult(
            passed=delta <= self.max_off_policy_steps,
            details={"sampled_policy_version": sampled, "current_policy_version": current},
        )


AsyncRlNode: TypeAlias = (
    RuntimeContext
    | Actor[BatchSourceInputs, list[PromptRecord], BatchState]
    | Effect[SamplePolicyInputs, SamplePolicyOutput]
    | Parallel[Effect[RewardInputs, RewardScores] | Effect[RewardInputs, KLPrefetchOutput]]
    | Join[RolloutJoinInputs, RolloutJoinOutput]
    | Invariant[InvariantInputs, int]
    | Actor[LearnerInputs, LearnerOutput, LearnerState]
    | Effect[RefreshInputs, RefreshOutput]
)


def build_program(group_size: int = 4) -> Workflow[AsyncRlNode]:
    """Build the milestone-two async RL authoring model."""

    return Workflow(
        name="async_rl_demo",
        description="Milestone 2 async RL demo authored in semantic primitives.",
        children=[
            RuntimeContext(
                name="local_control_plane",
                runtime="local",
                children=[
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
                                handler=PolicySampler(group_size=group_size),
                                inputs=[Ref("batch_source")],
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
                            ),
                            Effect[RewardInputs, RewardScores](
                                "quality_reward",
                                handler=QualityScorer(),
                                inputs=[Ref("sample_policy")],
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
                            Ref("sample_policy"),
                            Ref("pangram_reward"),
                            Ref("quality_reward"),
                            Ref("kl_prefetch"),
                        ],
                        reducer=RolloutJoinReducer(),
                    ),
                    Invariant[InvariantInputs, int](
                        "staleness_invariant",
                        checker=PolicyStalenessChecker(max_off_policy_steps=0),
                        inputs=[Ref("rollout_join")],
                    ),
                    Actor[LearnerInputs, LearnerOutput, LearnerState](
                        "learner_update",
                        handler=LearnerUpdate(),
                        inputs=[Ref("rollout_join")],
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
    expected_count = len(samples)

    rollout_join = result.outputs["rollout_join"]
    assert isinstance(rollout_join, dict)
    pangram_scores = rollout_join["pangram_scores"]
    quality_scores = rollout_join["quality_scores"]
    kl_prefetch = rollout_join["kl_prefetch"]
    assert isinstance(pangram_scores, dict)
    assert isinstance(quality_scores, dict)
    assert isinstance(kl_prefetch, dict)

    assert len(pangram_scores) == expected_count
    assert len(quality_scores) == expected_count
    assert len(kl_prefetch) == expected_count
