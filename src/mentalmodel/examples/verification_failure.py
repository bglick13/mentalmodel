from __future__ import annotations

from dataclasses import dataclass

from mentalmodel.core import (
    Actor,
    ActorHandler,
    Invariant,
    InvariantChecker,
    InvariantResult,
    Ref,
    Workflow,
)
from mentalmodel.core.models import ActorResult
from mentalmodel.runtime.context import ExecutionContext


@dataclass(slots=True)
class SourceHandler(ActorHandler[dict[str, object], object, dict[str, int]]):
    async def handle(
        self,
        inputs: dict[str, object],
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[dict[str, int], object]:
        del inputs, state, ctx
        return ActorResult(output={"value": 1})


@dataclass(slots=True)
class AlwaysFailChecker(InvariantChecker[dict[str, dict[str, int]], int]):
    async def check(
        self,
        inputs: dict[str, dict[str, int]],
        ctx: ExecutionContext,
    ) -> InvariantResult[int]:
        del inputs, ctx
        return InvariantResult(passed=False, details={"expected": 0})


FailureNode = (
    Actor[dict[str, object], dict[str, int], object] | Invariant[dict[str, dict[str, int]], int]
)


def build_program() -> Workflow[FailureNode]:
    """Build a deliberately failing verification example."""

    return Workflow(
        name="verification_failure",
        children=[
            Actor(
                name="source",
                handler=SourceHandler(),
                metadata={"runtime_context": "local"},
            ),
            Invariant(
                name="always_fail",
                checker=AlwaysFailChecker(),
                inputs=[Ref("source")],
                metadata={"runtime_context": "local"},
            ),
        ],
    )
