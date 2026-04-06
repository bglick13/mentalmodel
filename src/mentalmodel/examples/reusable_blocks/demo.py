from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict

from mentalmodel import (
    Block,
    BlockInput,
    BlockOutput,
    BlockRef,
    Effect,
    Invariant,
    Join,
    Ref,
    Use,
    Workflow,
)
from mentalmodel.core import EffectHandler, InvariantChecker, InvariantResult, NamedPrimitive
from mentalmodel.runtime.context import ExecutionContext


class NumberPayload(TypedDict):
    value: int


class PairInputs(TypedDict):
    first: NumberPayload
    second: NumberPayload


@dataclass(slots=True)
class EmitNumber(EffectHandler[dict[str, object], NumberPayload]):
    value: int

    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> NumberPayload:
        del inputs, ctx
        return {"value": self.value}


@dataclass(slots=True)
class DoubleNumber(EffectHandler[dict[str, NumberPayload], NumberPayload]):
    async def invoke(
        self,
        inputs: dict[str, NumberPayload],
        ctx: ExecutionContext,
    ) -> NumberPayload:
        del ctx
        return {"value": inputs["upstream"]["value"] * 2}


@dataclass(slots=True)
class PositiveNumber(InvariantChecker[dict[str, NumberPayload], int]):
    async def check(
        self,
        inputs: dict[str, NumberPayload],
        ctx: ExecutionContext,
    ) -> InvariantResult[int]:
        del ctx
        value = inputs["double"]["value"]
        return InvariantResult(
            passed=value > 0,
            details={"value": value},
        )


number_block = Block(
    name="number_processing",
    inputs={"upstream": BlockInput[object]()},
    outputs={
        "result": BlockOutput[object]("double"),
        "check": BlockOutput[object]("positive"),
    },
    children=(
        Effect[dict[str, NumberPayload], NumberPayload](
            "double",
            handler=DoubleNumber(),
            inputs=[BlockRef("upstream")],
        ),
        Invariant[dict[str, NumberPayload], int](
            "positive",
            checker=PositiveNumber(),
            inputs=[Ref("double")],
            severity="warning",
        ),
    ),
    description="A tiny reusable block that doubles a number and validates positivity.",
)


def build_program() -> Workflow[NamedPrimitive]:
    first = Use("first", block=number_block, bind={"upstream": Ref("source")})
    second = Use("second", block=number_block, bind={"upstream": first.output_ref("result")})
    return Workflow(
        name="reusable_blocks_demo",
        description="Reference workflow showing reusable block instantiation.",
        children=(
            Effect[dict[str, object], NumberPayload](
                "source",
                handler=EmitNumber(value=3),
            ),
            first,
            second,
            Join[PairInputs, PairInputs](
                "pair_join",
                inputs=[
                    first.output_ref("result"),
                    second.output_ref("result"),
                ],
                reducer=None,
            ),
        ),
    )
