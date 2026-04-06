from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

ValueT = TypeVar("ValueT")


@dataclass(slots=True, frozen=True)
class ExecutionFrameSegment:
    """One nested loop segment in an execution frame path."""

    loop_node_id: str
    iteration_index: int

    @property
    def segment_id(self) -> str:
        return f"{self.loop_node_id}[{self.iteration_index}]"


@dataclass(slots=True, frozen=True)
class ExecutionFrame:
    """Stable identity for one execution frame."""

    path: tuple[ExecutionFrameSegment, ...] = ()

    @property
    def frame_id(self) -> str:
        if not self.path:
            return "root"
        return "/".join(segment.segment_id for segment in self.path)

    @property
    def loop_node_id(self) -> str | None:
        if not self.path:
            return None
        return self.path[-1].loop_node_id

    @property
    def iteration_index(self) -> int | None:
        if not self.path:
            return None
        return self.path[-1].iteration_index

    def child(self, *, loop_node_id: str, iteration_index: int) -> ExecutionFrame:
        """Return a nested child frame below this frame."""

        return ExecutionFrame(
            path=(
                *self.path,
                ExecutionFrameSegment(
                    loop_node_id=loop_node_id,
                    iteration_index=iteration_index,
                ),
            )
        )


ROOT_FRAME = ExecutionFrame()


@dataclass(slots=True, frozen=True)
class FramedNodeValue(Generic[ValueT]):
    """One node output associated with a specific execution frame."""

    node_id: str
    frame: ExecutionFrame = field(default_factory=lambda: ROOT_FRAME)
    value: ValueT | None = None


@dataclass(slots=True, frozen=True)
class FramedStateValue(Generic[ValueT]):
    """One state value associated with a specific execution frame."""

    state_key: str
    frame: ExecutionFrame = field(default_factory=lambda: ROOT_FRAME)
    value: ValueT | None = None
