from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from mentalmodel.core import Workflow
from mentalmodel.core.interfaces import JsonValue, NamedPrimitive, RuntimeValue
from mentalmodel.environment import (
    EMPTY_RUNTIME_ENVIRONMENT,
    RuntimeEnvironment,
)
from mentalmodel.ir.graph import IRGraph, IRNode
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.config import TracingConfig
from mentalmodel.observability.export import serialize_runtime_value
from mentalmodel.observability.metrics import (
    MetricEmitter,
    create_metric_emitter,
    emit_metric_batch,
    node_duration_observation,
    node_execution_observation,
    run_completed_observation,
    run_started_observation,
)
from mentalmodel.observability.tracing import (
    RecordedSpan,
    SpanListener,
    TracingAdapter,
    create_tracing_adapter,
)
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.events import NODE_FAILED, NODE_STARTED, NODE_SUCCEEDED
from mentalmodel.runtime.frame import FramedNodeValue, FramedStateValue
from mentalmodel.runtime.plan import (
    CompiledProgram,
    PlanNode,
    compile_program,
)
from mentalmodel.runtime.recorder import ExecutionRecorder
from mentalmodel.runtime.scheduler import execute_plan_nodes


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
    invocation_name: str | None
    runtime_default_profile_name: str | None
    runtime_profile_names: tuple[str, ...]


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
        environment: RuntimeEnvironment = EMPTY_RUNTIME_ENVIRONMENT,
        invocation_name: str | None = None,
        span_listeners: Sequence[SpanListener] = (),
    ) -> None:
        self.max_concurrency = max(1, max_concurrency)
        self.recorder = recorder or ExecutionRecorder()
        self.tracing = tracing or create_tracing_adapter(
            config=tracing_config,
            listeners=span_listeners,
        )
        metric_config = tracing_config if tracing_config is not None else self.tracing.config
        self.metrics = metrics or create_metric_emitter(config=metric_config)
        self.environment = environment
        self.invocation_name = invocation_name

    async def run(self, program: Workflow[NamedPrimitive]) -> ExecutionResult:
        compiled = compile_program(program)
        context = ExecutionContext.create(
            graph=compiled.graph,
            recorder=self.recorder,
            tracing=self.tracing,
            metrics=self.metrics,
            environment=self.environment,
            invocation_name=self.invocation_name,
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
            framed_outputs=tuple(context.framed_outputs),
            records=tuple(self.recorder.records),
            state=dict(context.state_store),
            framed_state=tuple(context.framed_state),
            spans=self.tracing.snapshot_spans(),
            trace_summary=self.tracing.trace_summary(),
            invocation_name=self.invocation_name,
            runtime_default_profile_name=self.environment.default_profile_name,
            runtime_profile_names=self.environment.profile_names(),
        )

    async def _execute(
        self,
        *,
        compiled: CompiledProgram,
        context: ExecutionContext,
    ) -> dict[str, RuntimeValue]:
        return await execute_plan_nodes(
            plan=compiled.plan,
            context=context,
            run_node=self._run_node,
            max_concurrency=self.max_concurrency,
            initial_outputs=context.outputs,
        )

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
            payload: dict[str, JsonValue] = {
                "kind": node.metadata.kind,
                "input_count": len(node.metadata.dependencies),
            }
            if node_ctx.runtime_profile is not None:
                payload["runtime_profile"] = node_ctx.runtime_profile
            self.recorder.record(
                run_id=node_ctx.run_id,
                node_id=node.metadata.node_id,
                event_type=NODE_STARTED,
                timestamp_ms=node_ctx.clock.now_ms(),
                frame=node_ctx.frame,
                payload=payload,
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
                    "output": serialize_runtime_value(output),
                },
            )
            node_ctx.framed_outputs.append(
                FramedNodeValue(
                    node_id=node.metadata.node_id,
                    frame=node_ctx.frame,
                    value=output,
                )
            )
            if node.metadata.kind == "invariant":
                return node.metadata.node_id, output
            return node.metadata.node_id, output
def error_payload(exc: Exception) -> dict[str, JsonValue]:
    """Convert an exception into recorder-safe error metadata."""

    return {"error": type(exc).__name__, "message": str(exc)}
