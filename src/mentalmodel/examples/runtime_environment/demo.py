from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict, cast

from mentalmodel import (
    Block,
    BlockDefaults,
    BlockInput,
    BlockOutput,
    BlockRef,
    Effect,
    Invariant,
    Join,
    Ref,
    ResourceKey,
    RuntimeEnvironment,
    RuntimeProfile,
    Use,
    Workflow,
)
from mentalmodel.core.interfaces import (
    EffectHandler,
    InvariantChecker,
    JoinReducer,
    NamedPrimitive,
)
from mentalmodel.core.models import InvariantResult
from mentalmodel.runtime.context import ExecutionContext


@dataclass(slots=True, frozen=True)
class Multiplier:
    value: int


MULTIPLIER_RESOURCE = ResourceKey("multiplier", Multiplier)


class ComparisonResult(TypedDict):
    fixture_scaled: int
    real_scaled: int


class SeedSource(EffectHandler[dict[str, object], int]):
    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> int:
        del inputs, ctx
        return 7


class ScaleValue(EffectHandler[dict[str, object], int]):
    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> int:
        value = cast(int, inputs["value"])
        multiplier = ctx.resources.require(MULTIPLIER_RESOURCE)
        return value * multiplier.value


class ComparisonReducer(JoinReducer[dict[str, object], ComparisonResult]):
    async def reduce(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> ComparisonResult:
        del ctx
        return {
            "fixture_scaled": cast(int, inputs["fixture_scaling.scale"]),
            "real_scaled": cast(int, inputs["real_scaling.scale"]),
        }


class ScalingInvariant(InvariantChecker[dict[str, object], int]):
    async def check(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> InvariantResult[int]:
        del ctx
        comparison = cast(ComparisonResult, inputs["comparison"])
        return InvariantResult(
            passed=comparison["fixture_scaled"] == 14 and comparison["real_scaled"] == 35,
            details={
                "expected_fixture": 14,
                "expected_real": 35,
                "observed_fixture": comparison["fixture_scaled"],
                "observed_real": comparison["real_scaled"],
            },
        )


scaling_block = Block(
    "scaling_block",
    inputs={"value": BlockInput[object]()},
    outputs={"scaled": BlockOutput[object]("scale")},
    defaults=BlockDefaults(
        resources=cast(
            tuple[ResourceKey[object], ...],
            (MULTIPLIER_RESOURCE,),
        )
    ),
    children=(
        Effect(
            "scale",
            handler=ScaleValue(),
            inputs=[BlockRef("value")],
        ),
    ),
)


def build_environment() -> RuntimeEnvironment:
    return RuntimeEnvironment(
        profiles={
            "fixture": RuntimeProfile(
                name="fixture",
                resources=cast(
                    dict[ResourceKey[object], object],
                    {MULTIPLIER_RESOURCE: Multiplier(2)},
                ),
                metadata={"mode": "fixture"},
            ),
            "real": RuntimeProfile(
                name="real",
                resources=cast(
                    dict[ResourceKey[object], object],
                    {MULTIPLIER_RESOURCE: Multiplier(5)},
                ),
                metadata={"mode": "real"},
            ),
        }
    )


def build_program() -> Workflow[NamedPrimitive]:
    fixture_scaling = Use(
        "fixture_scaling",
        block=scaling_block,
        bind={"value": Ref("seed")},
        defaults=BlockDefaults(runtime_context="fixture"),
    )
    real_scaling = Use(
        "real_scaling",
        block=scaling_block,
        bind={"value": Ref("seed")},
        defaults=BlockDefaults(runtime_context="real"),
    )
    return Workflow(
        "runtime_environment_demo",
        children=(
            Effect("seed", handler=SeedSource()),
            fixture_scaling,
            real_scaling,
            Join(
                "comparison",
                reducer=ComparisonReducer(),
                inputs=[
                    fixture_scaling.output_ref("scaled"),
                    real_scaling.output_ref("scaled"),
                ],
            ),
            Invariant(
                "scaling_matches_profiles",
                checker=ScalingInvariant(),
                inputs=[Ref("comparison")],
            ),
        ),
    )
