from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass

from mentalmodel.core import Workflow
from mentalmodel.core.interfaces import JsonValue, NamedPrimitive, RuntimeValue
from mentalmodel.ir.graph import IRGraph, IRNode
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.config import TracingConfig
from mentalmodel.observability.metrics import (
    MetricEmitter,
    create_metric_emitter,
    emit_metric_batch,
    node_duration_observation,
    node_execution_observation,
    run_completed_observation,
    run_started_observation,
)
from mentalmodel.observability.tracing import RecordedSpan, TracingAdapter, create_tracing_adapter
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.errors import ExecutionError
from mentalmodel.runtime.events import NODE_FAILED, NODE_STARTED, NODE_SUCCEEDED
from mentalmodel.runtime.frame import FramedNodeValue, FramedStateValue
from mentalmodel.runtime.plan import (
    CompiledProgram,
    ExecutionPlan,
    PlanNode,
    compile_program,
)
from mentalmodel.runtime.recorder import ExecutionRecorder


@dataclass(slots=True, frozen=True)
class ExecutionResult:
    """Outputs and semantic records from one program run."""

    run_id: str
    graph: IRGraph
    outputs: dict[str, RuntimeValue]
    framed_outputs: tuple[FramedNodeValue[RuntimeValue], ...]
    records: tuple[ExecutionRecord, ...]
    state: dict[str, RuntimeValue]
    framed_state: tuple[FramedStateValue[RuntimeValue], ...]
    spans: tuple[RecordedSpan, ...]
    trace_summary: dict[str, str | bool | None]


class AsyncExecutor:
    """Deterministic async executor for the compiled execution plan."""

    def __init__(
        self,
        *,
        max_concurrency: int = 8,
        recorder: ExecutionRecorder | None = None,
        tracing: TracingAdapter | None = None,
        metrics: MetricEmitter | None = None,
        tracing_config: TracingConfig | None = None,
    ) -> None:
        self.max_concurrency = max(1, max_concurrency)
        self.recorder = recorder or ExecutionRecorder()
        self.tracing = tracing or create_tracing_adapter(config=tracing_config)
        metric_config = tracing_config if tracing_config is not None else self.tracing.config
        self.metrics = metrics or create_metric_emitter(config=metric_config)

    async def run(self, program: Workflow[NamedPrimitive]) -> ExecutionResult:
        compiled = compile_program(program)
        context = ExecutionContext.create(
            graph=compiled.graph,
            recorder=self.recorder,
            tracing=self.tracing,
            metrics=self.metrics,
        )
        emit_metric_batch(self.metrics, [run_started_observation(context.metric_context())])
        outputs: dict[str, RuntimeValue]
        execution_success = False
        try:
            outputs = await self._execute(compiled=compiled, context=context)
            execution_success = True
        finally:
            emit_metric_batch(
                self.metrics,
                [run_completed_observation(context.metric_context(), success=execution_success)],
            )
            self.metrics.flush()
            self.tracing.flush()
        return ExecutionResult(
            run_id=context.run_id,
            graph=compiled.graph,
            outputs=dict(outputs),
            framed_outputs=tuple(
                FramedNodeValue(node_id=node_id, frame=context.frame, value=value)
                for node_id, value in sorted(outputs.items())
            ),
            records=tuple(self.recorder.records),
            state=dict(context.state_store),
            framed_state=tuple(
                FramedStateValue(state_key=state_key, frame=context.frame, value=value)
                for state_key, value in sorted(context.state_store.items())
            ),
            spans=self.tracing.snapshot_spans(),
            trace_summary=self.tracing.trace_summary(),
        )

    async def _execute(
        self,
        *,
        compiled: CompiledProgram,
        context: ExecutionContext,
    ) -> dict[str, RuntimeValue]:
        plan = compiled.plan
        dependents = build_dependents(plan)
        ready = sorted(
            node_id for node_id, node in plan.nodes.items() if not node.metadata.dependencies
        )
        running: dict[asyncio.Task[tuple[str, RuntimeValue]], str] = {}
        semaphore = asyncio.Semaphore(self.max_concurrency)
        outputs = context.outputs

        while ready or running:
            while ready and len(running) < self.max_concurrency:
                node_id = ready.pop(0)
                node = plan.nodes[node_id]
                task = asyncio.create_task(
                    self._run_node(
                        node=node,
                        outputs=outputs,
                        context=context,
                        semaphore=semaphore,
                    )
                )
                running[task] = node_id

            if not running:
                break

            done, _ = await asyncio.wait(
                list(running.keys()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                node_id = running.pop(task)
                completed_node_id, output = task.result()
                outputs[completed_node_id] = output
                for dependent in sorted(dependents.get(node_id, set())):
                    dependency_ids = set(plan.dependencies_for(dependent))
                    if dependency_ids.issubset(outputs.keys()) and dependent not in ready:
                        ready.append(dependent)
                ready.sort()

        unresolved = [
            node_id
            for node_id in plan.nodes
            if node_id not in outputs and plan.nodes[node_id].metadata.kind != "invariant"
        ]
        if unresolved:
            raise ExecutionError(
                f"Execution finished with unresolved executable nodes: {sorted(unresolved)!r}"
            )
        return outputs

    async def _run_node(
        self,
        *,
        node: PlanNode,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, RuntimeValue]:
        async with semaphore:
            node_ctx = context.for_node(
                IRNode(
                    node_id=node.metadata.node_id,
                    kind=node.metadata.kind,
                    label=node.metadata.label,
                    metadata=(
                        {"runtime_context": node.metadata.runtime_context}
                        if node.metadata.runtime_context is not None
                        else {}
                    ),
                )
            )
            emit_metric_batch(
                self.metrics,
                [node_execution_observation(node_ctx.metric_context())],
            )
            self.recorder.record(
                run_id=node_ctx.run_id,
                node_id=node.metadata.node_id,
                event_type=NODE_STARTED,
                timestamp_ms=node_ctx.clock.now_ms(),
                frame=node_ctx.frame,
                payload={
                    "kind": node.metadata.kind,
                    "input_count": len(node.metadata.dependencies),
                },
            )
            start_time_ms = node_ctx.clock.now_ms()
            node_success = False
            try:
                with node_ctx.tracing.start_span(
                    f"{node.metadata.kind}:{node.metadata.node_id}",
                    attributes=node_ctx.span_attributes(),
                ):
                    output = await node.execute(outputs, node_ctx)
                node_success = True
            except Exception as exc:
                self.recorder.record(
                    run_id=node_ctx.run_id,
                    node_id=node.metadata.node_id,
                    event_type=NODE_FAILED,
                    timestamp_ms=node_ctx.clock.now_ms(),
                    frame=node_ctx.frame,
                    payload=error_payload(exc),
                )
                raise
            finally:
                duration_ms = max(0.0, node_ctx.clock.now_ms() - start_time_ms)
                emit_metric_batch(
                    self.metrics,
                    [
                        node_duration_observation(
                            node_ctx.metric_context(),
                            duration_ms=duration_ms,
                            success=node_success,
                        )
                    ],
                )
            self.recorder.record(
                run_id=node_ctx.run_id,
                node_id=node.metadata.node_id,
                event_type=NODE_SUCCEEDED,
                timestamp_ms=node_ctx.clock.now_ms(),
                frame=node_ctx.frame,
                payload={
                    "kind": node.metadata.kind,
                    "output_type": type(output).__name__,
                },
            )
            if node.metadata.kind == "invariant":
                return node.metadata.node_id, output
            return node.metadata.node_id, output


def build_dependents(plan: ExecutionPlan) -> dict[str, set[str]]:
    """Build reverse dependency edges from a compiled plan."""

    dependents: dict[str, set[str]] = defaultdict(set)
    for node in plan.nodes.values():
        for dependency in node.metadata.dependencies:
            dependents[dependency].add(node.metadata.node_id)
    return dependents


def error_payload(exc: Exception) -> dict[str, JsonValue]:
    """Convert an exception into recorder-safe error metadata."""

    return {"error": type(exc).__name__, "message": str(exc)}
