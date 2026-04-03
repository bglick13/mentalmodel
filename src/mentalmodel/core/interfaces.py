from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, TypeAlias, TypeVar

if TYPE_CHECKING:
    from mentalmodel.core.models import ActorResult, InvariantResult
    from mentalmodel.ir.graph import IRFragment
    from mentalmodel.ir.lowering import LoweringContext
    from mentalmodel.runtime.context import ExecutionContext

JsonPrimitive: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonPrimitive | dict[str, "JsonValue"] | list["JsonValue"]
RuntimeValue: TypeAlias = object
ResolvedInputs: TypeAlias = Mapping[str, RuntimeValue]

InputT_contra = TypeVar("InputT_contra", contravariant=True)
ActorOutputT = TypeVar("ActorOutputT")
ReturnedOutputT_co = TypeVar("ReturnedOutputT_co", covariant=True)
StateT = TypeVar("StateT")
DetailT = TypeVar("DetailT", bound=JsonValue)


class LowersToIR(Protocol):
    """Protocol implemented by authoring-layer primitives."""

    name: str

    def lower(self, ctx: LoweringContext) -> IRFragment:
        """Lower the primitive into an IR fragment."""


class NamedPrimitive(Protocol):
    """Minimal protocol shared by authored primitives."""

    name: str


class ActorHandler(Protocol[InputT_contra, StateT, ActorOutputT]):
    """Runtime contract for actor handlers."""

    async def handle(
        self,
        inputs: InputT_contra,
        state: StateT | None,
        ctx: ExecutionContext,
    ) -> ActorResult[ActorOutputT, StateT]:
        """Process inputs and return the next actor result."""


class EffectHandler(Protocol[InputT_contra, ReturnedOutputT_co]):
    """Runtime contract for effect handlers."""

    async def invoke(
        self,
        inputs: InputT_contra,
        ctx: ExecutionContext,
    ) -> ReturnedOutputT_co:
        """Invoke the effect and return its output."""


class InvariantChecker(Protocol[InputT_contra, DetailT]):
    """Runtime contract for invariant checkers."""

    async def check(
        self,
        inputs: InputT_contra,
        ctx: ExecutionContext,
    ) -> InvariantResult[DetailT]:
        """Evaluate the invariant against resolved inputs."""


class JoinReducer(Protocol[InputT_contra, ReturnedOutputT_co]):
    """Runtime contract for join reducers."""

    async def reduce(
        self,
        inputs: InputT_contra,
        ctx: ExecutionContext,
    ) -> ReturnedOutputT_co:
        """Reduce resolved inputs into a single value."""
