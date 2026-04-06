from __future__ import annotations

from collections.abc import Mapping
from typing import TypedDict, cast

from mentalmodel.core import (
    Block,
    BlockInput,
    BlockOutput,
    BlockRef,
    Effect,
    Invariant,
    Join,
    LoopCarry,
    LoopItemRef,
    LoopStateRef,
    LoopSummary,
    Ref,
    StepLoop,
    StepLoopResult,
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


class StepReport(TypedDict):
    item: int
    squared: int
    prior_total: int
    next_total: int


class SquareResult(TypedDict):
    item: int
    squared: int


class NumbersSource(EffectHandler[dict[str, object], list[int]]):
    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> list[int]:
        del inputs, ctx
        return [1, 2, 3]


class SquareItem(EffectHandler[dict[str, object], SquareResult]):
    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> SquareResult:
        del ctx
        item = cast(int, inputs["item"])
        return {"item": item, "squared": item * item}


class RunningTotalReducer(JoinReducer[dict[str, object], int]):
    async def reduce(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> int:
        del ctx
        square = cast(SquareResult, inputs["square_item"])
        running_total = cast(int, inputs["running_total"])
        return running_total + square["squared"]


class StepReportReducer(JoinReducer[dict[str, object], StepReport]):
    async def reduce(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> StepReport:
        del ctx
        square = cast(SquareResult, inputs["square_item"])
        running_total = cast(int, inputs["running_total"])
        return {
            "item": square["item"],
            "squared": square["squared"],
            "prior_total": running_total,
            "next_total": running_total + square["squared"],
        }


class FinalTotalChecker(InvariantChecker[dict[str, object], int]):
    async def check(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> InvariantResult[int]:
        del ctx
        loop_result = inputs["steps"]
        if isinstance(loop_result, StepLoopResult):
            observed = cast(int, loop_result.final_outputs.get("next_total", -1))
            return InvariantResult(
                passed=observed == 14,
                details={"expected_total": 14, "observed_total": observed},
            )
        if not isinstance(loop_result, Mapping):
            return InvariantResult(
                passed=False,
                details={"expected_total": 14, "observed_total": -1},
            )
        final_outputs = loop_result.get("final_outputs")
        if not isinstance(final_outputs, Mapping):
            return InvariantResult(
                passed=False,
                details={"expected_total": 14, "observed_total": -1},
            )
        observed = cast(int, final_outputs["next_total"])
        return InvariantResult(
            passed=observed == 14,
            details={"expected_total": 14, "observed_total": observed},
        )


step_block = Block(
    "training_step",
    inputs={
        "item": BlockInput[object](),
        "running_total": BlockInput[object](),
    },
    outputs={
        "next_total": BlockOutput[object]("next_total"),
        "step_report": BlockOutput[object]("step_report"),
    },
    children=(
        Effect(
            "square_item",
            handler=SquareItem(),
            inputs=[BlockRef("item")],
        ),
        Join(
            "next_total",
            reducer=RunningTotalReducer(),
            inputs=[Ref("square_item"), BlockRef("running_total")],
        ),
        Join(
            "step_report",
            reducer=StepReportReducer(),
            inputs=[Ref("square_item"), BlockRef("running_total")],
        ),
    ),
)


def build_program() -> Workflow[NamedPrimitive]:
    return Workflow(
        "step_loop_demo",
        children=(
            Effect("numbers", handler=NumbersSource()),
            StepLoop(
                "steps",
                body=Use(
                    "step",
                    block=step_block,
                    bind={
                        "item": LoopItemRef(),
                        "running_total": LoopStateRef("running_total"),
                    },
                ),
                for_each=Ref("numbers"),
                carry=LoopCarry[object](
                    state_name="running_total",
                    initial=0,
                    next_state_output="next_total",
                ),
                summary=LoopSummary(
                    final_outputs=("next_total",),
                    history_outputs=("step_report",),
                ),
            ),
            Invariant(
                "final_total_matches",
                checker=FinalTotalChecker(),
                inputs=[Ref("steps")],
            ),
        ),
    )
