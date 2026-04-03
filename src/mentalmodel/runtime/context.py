from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import uuid4

from mentalmodel.core.interfaces import RuntimeValue
from mentalmodel.ir.graph import IRGraph, IRNode

if TYPE_CHECKING:
    from mentalmodel.observability.tracing import TracingAdapter
    from mentalmodel.runtime.recorder import ExecutionRecorder


class Clock:
    """Clock abstraction for runtime records and tests."""

    def now_ms(self) -> int:
        return int(time.time() * 1000)


@dataclass(slots=True)
class ExecutionContext:
    """Per-run and per-node execution context."""

    run_id: str
    graph: IRGraph
    recorder: ExecutionRecorder
    tracing: TracingAdapter
    clock: Clock = field(default_factory=Clock)
    node_id: str | None = None
    node_kind: str | None = None
    runtime_context: str | None = None
    state_store: dict[str, RuntimeValue] = field(default_factory=dict)
    outputs: dict[str, RuntimeValue] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        graph: IRGraph,
        recorder: ExecutionRecorder,
        tracing: TracingAdapter,
    ) -> ExecutionContext:
        return cls(
            run_id=f"run-{uuid4().hex}",
            graph=graph,
            recorder=recorder,
            tracing=tracing,
        )

    def for_node(self, node: IRNode) -> ExecutionContext:
        return ExecutionContext(
            run_id=self.run_id,
            graph=self.graph,
            recorder=self.recorder,
            tracing=self.tracing,
            clock=self.clock,
            node_id=node.node_id,
            node_kind=node.kind,
            runtime_context=node.metadata.get("runtime_context"),
            state_store=self.state_store,
            outputs=self.outputs,
        )

    def span_attributes(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        attrs = {
            "mentalmodel.run_id": self.run_id,
        }
        if self.node_id is not None:
            attrs["mentalmodel.node.id"] = self.node_id
        if self.node_kind is not None:
            attrs["mentalmodel.node.kind"] = self.node_kind
        if self.runtime_context is not None:
            attrs["mentalmodel.runtime.context"] = self.runtime_context
        if extra:
            attrs.update(extra)
        return attrs
