from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from mentalmodel.analysis import run_analysis
from mentalmodel.core.interfaces import NamedPrimitive, RuntimeValue
from mentalmodel.core.workflow import Workflow
from mentalmodel.environment import EMPTY_RUNTIME_ENVIRONMENT, RuntimeEnvironment
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.config import TracingConfig
from mentalmodel.observability.live import AsyncLiveExporter, LiveIngestionConfig
from mentalmodel.observability.metrics import MetricEmitter, MetricObservation
from mentalmodel.observability.telemetry import TelemetryResourceContext
from mentalmodel.observability.tracing import RecordedSpan, SpanListener
from mentalmodel.remote.backend import RemoteCompletedRunSink, RemoteRunStore
from mentalmodel.remote.contracts import CatalogSource
from mentalmodel.remote.project_config import (
    MentalModelProjectConfig,
    discover_project_config_path,
    load_project_config,
)
from mentalmodel.remote.sinks import (
    CompletedRunPublishResult,
    CompletedRunSink,
    ExecutionRecordSink,
    LiveExecutionPublishResult,
    LiveExecutionSink,
    record_listener_for_sink,
)
from mentalmodel.remote.sync import (
    RemoteServiceCompletedRunSink,
    failed_completed_run_publish,
)
from mentalmodel.runtime.context import generate_run_id
from mentalmodel.runtime.events import INVARIANT_CHECKED
from mentalmodel.runtime.executor import AsyncExecutor, ExecutionResult
from mentalmodel.runtime.frame import FramedNodeValue, FramedStateValue
from mentalmodel.runtime.recorder import ExecutionRecorder, RecordListener
from mentalmodel.runtime.runs import RunArtifacts, write_run_artifacts


@dataclass(slots=True, frozen=True)
class ManagedRunTarget:
    runs_dir: Path | None = None
    project_id: str | None = None
    project_label: str | None = None
    environment_name: str | None = None
    catalog_entry_id: str | None = None
    catalog_source: CatalogSource | None = None

    def with_project_defaults(
        self,
        config: MentalModelProjectConfig,
    ) -> ManagedRunTarget:
        return ManagedRunTarget(
            runs_dir=self.runs_dir if self.runs_dir is not None else config.default_runs_dir,
            project_id=self.project_id if self.project_id is not None else config.project_id,
            project_label=self.project_label if self.project_label is not None else config.label,
            environment_name=(
                self.environment_name
                if self.environment_name is not None
                else config.default_environment
            ),
            catalog_entry_id=self.catalog_entry_id,
            catalog_source=self.catalog_source,
        )


@dataclass(slots=True, frozen=True)
class ManagedExecutionOptions:
    target: ManagedRunTarget = field(default_factory=ManagedRunTarget)
    persist_run_artifacts: bool = True
    discover_linked_project: bool = True
    config_search_start: Path | None = None
    linked_project_config: MentalModelProjectConfig | None = None
    remote_run_store: RemoteRunStore | None = None
    completed_run_sink: CompletedRunSink | None = None
    live_execution_sink: LiveExecutionSink | None = None
    live_ingestion: LiveIngestionConfig | None = None
    enable_completed_run_upload: bool = True
    enable_live_execution: bool = True


@dataclass(slots=True, frozen=True)
class ManagedExecutionResult:
    success: bool
    run_id: str | None
    invocation_name: str | None
    error: str | None
    execution: ExecutionResult | None
    records: tuple[ExecutionRecord, ...]
    outputs: dict[str, RuntimeValue]
    framed_outputs: tuple[FramedNodeValue[RuntimeValue], ...]
    state: dict[str, RuntimeValue]
    framed_state: tuple[FramedStateValue[RuntimeValue], ...]
    spans: tuple[RecordedSpan, ...]
    trace_sink_configured: bool
    trace_summary: dict[str, str | bool | None]
    runtime_default_profile_name: str | None
    runtime_profile_names: tuple[str, ...]
    run_artifacts: RunArtifacts | None
    completed_run_upload: CompletedRunPublishResult | None
    live_execution_delivery: LiveExecutionPublishResult | None
    linked_project_config: MentalModelProjectConfig | None
    target: ManagedRunTarget
    invariant_failures: tuple[ManagedInvariantFailure, ...]

    @property
    def run_artifacts_dir(self) -> str | None:
        if self.run_artifacts is None:
            return None
        return str(self.run_artifacts.run_dir)


@dataclass(slots=True, frozen=True)
class ManagedInvariantFailure:
    node_id: str
    severity: str


@dataclass(slots=True, frozen=True)
class _ManagedRunResolution:
    linked_project_config: MentalModelProjectConfig | None
    target: ManagedRunTarget
    completed_run_sink: CompletedRunSink | None
    live_execution_sink: LiveExecutionSink | None


@dataclass(slots=True, frozen=True)
class _ManagedRuntimeCapture:
    success: bool
    run_id: str | None
    error: str | None
    execution: ExecutionResult | None
    records: tuple[ExecutionRecord, ...]
    outputs: dict[str, RuntimeValue]
    framed_outputs: tuple[FramedNodeValue[RuntimeValue], ...]
    state: dict[str, RuntimeValue]
    framed_state: tuple[FramedStateValue[RuntimeValue], ...]
    spans: tuple[RecordedSpan, ...]
    trace_sink_configured: bool
    trace_summary: dict[str, str | bool | None]
    invocation_name: str | None
    runtime_default_profile_name: str | None
    runtime_profile_names: tuple[str, ...]
    invariant_failures: tuple[ManagedInvariantFailure, ...]


def run_managed(
    program: Workflow[NamedPrimitive],
    *,
    environment: RuntimeEnvironment | None = None,
    invocation_name: str | None = None,
    run_id: str | None = None,
    options: ManagedExecutionOptions | None = None,
    record_listeners: Sequence[RecordListener] = (),
    span_listeners: Sequence[SpanListener] = (),
    record_sinks: Sequence[ExecutionRecordSink] = (),
) -> ManagedExecutionResult:
    resolved_options = options or ManagedExecutionOptions()
    resolved_environment = environment or EMPTY_RUNTIME_ENVIRONMENT
    resolved_run_id = run_id or generate_run_id()
    resolution = resolve_managed_execution(
        options=resolved_options,
        run_id=resolved_run_id,
        invocation_name=invocation_name,
        environment=resolved_environment,
    )

    graph = None
    if resolution.live_execution_sink is not None:
        graph = lower_graph(program)
        resolution.live_execution_sink.start(
            graph=graph,
            analysis=run_analysis(graph),
        )

    runtime_capture = _capture_managed_runtime(
        program,
        environment=resolved_environment,
        invocation_name=invocation_name,
        run_id=resolved_run_id,
        record_listeners=record_listeners,
        span_listeners=span_listeners,
        record_sinks=record_sinks,
        live_execution_sink=resolution.live_execution_sink,
    )

    if not resolved_options.persist_run_artifacts:
        if resolution.live_execution_sink is not None:
            resolution.live_execution_sink.complete(
                success=runtime_capture.success,
                error=runtime_capture.error,
            )
        return ManagedExecutionResult(
            success=runtime_capture.success,
            run_id=runtime_capture.run_id,
            invocation_name=runtime_capture.invocation_name,
            error=runtime_capture.error,
            execution=runtime_capture.execution,
            records=runtime_capture.records,
            outputs=runtime_capture.outputs,
            framed_outputs=runtime_capture.framed_outputs,
            state=runtime_capture.state,
            framed_state=runtime_capture.framed_state,
            spans=runtime_capture.spans,
            trace_sink_configured=runtime_capture.trace_sink_configured,
            trace_summary=runtime_capture.trace_summary,
            runtime_default_profile_name=runtime_capture.runtime_default_profile_name,
            runtime_profile_names=runtime_capture.runtime_profile_names,
            run_artifacts=None,
            completed_run_upload=None,
            live_execution_delivery=(
                None
                if resolution.live_execution_sink is None
                else resolution.live_execution_sink.delivery_result()
            ),
            linked_project_config=resolution.linked_project_config,
            target=resolution.target,
            invariant_failures=runtime_capture.invariant_failures,
        )

    artifacts = write_run_artifacts(
        graph=graph or lower_graph(program),
        run_id=runtime_capture.run_id or "run-failed",
        success=runtime_capture.success,
        records=runtime_capture.records,
        outputs=runtime_capture.outputs,
        framed_outputs=runtime_capture.framed_outputs,
        state=runtime_capture.state,
        framed_state=runtime_capture.framed_state,
        spans=runtime_capture.spans,
        runs_dir=resolution.target.runs_dir,
        verification_payload=None,
        trace_sink_configured=runtime_capture.trace_sink_configured,
        trace_summary=runtime_capture.trace_summary,
        invocation_name=runtime_capture.invocation_name,
        runtime_default_profile_name=runtime_capture.runtime_default_profile_name,
        runtime_profile_names=runtime_capture.runtime_profile_names,
        project_id=resolution.target.project_id,
        project_label=resolution.target.project_label,
        environment_name=resolution.target.environment_name,
        catalog_entry_id=resolution.target.catalog_entry_id,
        catalog_source=resolution.target.catalog_source,
    )

    completed_run_upload: CompletedRunPublishResult | None = None
    if resolution.completed_run_sink is not None:
        try:
            completed_run_upload = resolution.completed_run_sink.publish(
                manifest=artifacts.manifest,
                run_dir=artifacts.run_dir,
            )
        except Exception as exc:
            completed_run_upload = failed_completed_run_publish(
                transport=type(resolution.completed_run_sink).__name__,
                manifest=artifacts.manifest,
                error=exc,
                project_id=resolution.target.project_id,
            )
    if resolution.live_execution_sink is not None:
        resolution.live_execution_sink.complete(
            success=runtime_capture.success,
            error=runtime_capture.error,
        )
    live_execution_delivery = (
        None
        if resolution.live_execution_sink is None
        else resolution.live_execution_sink.delivery_result()
    )
    return ManagedExecutionResult(
        success=runtime_capture.success,
        run_id=runtime_capture.run_id,
        invocation_name=runtime_capture.invocation_name,
        error=runtime_capture.error,
        execution=runtime_capture.execution,
        records=runtime_capture.records,
        outputs=runtime_capture.outputs,
        framed_outputs=runtime_capture.framed_outputs,
        state=runtime_capture.state,
        framed_state=runtime_capture.framed_state,
        spans=runtime_capture.spans,
        trace_sink_configured=runtime_capture.trace_sink_configured,
        trace_summary=runtime_capture.trace_summary,
        runtime_default_profile_name=runtime_capture.runtime_default_profile_name,
        runtime_profile_names=runtime_capture.runtime_profile_names,
        run_artifacts=artifacts,
        completed_run_upload=completed_run_upload,
        live_execution_delivery=live_execution_delivery,
        linked_project_config=resolution.linked_project_config,
        target=resolution.target,
        invariant_failures=runtime_capture.invariant_failures,
    )


def resolve_managed_execution(
    *,
    options: ManagedExecutionOptions,
    run_id: str,
    invocation_name: str | None,
    environment: RuntimeEnvironment | None,
) -> _ManagedRunResolution:
    linked_project_config = options.linked_project_config
    if linked_project_config is None and options.discover_linked_project:
        config_path = discover_project_config_path(options.config_search_start)
        if config_path is not None:
            linked_project_config = load_project_config(config_path)

    target = options.target
    if linked_project_config is not None:
        target = target.with_project_defaults(linked_project_config)

    completed_run_sink = options.completed_run_sink
    if completed_run_sink is None and options.enable_completed_run_upload:
        if linked_project_config is not None:
            completed_run_sink = RemoteServiceCompletedRunSink(
                linked_project_config,
                project_id=target.project_id,
                project_label=target.project_label,
                environment_name=target.environment_name,
                catalog_entry_id=target.catalog_entry_id,
                catalog_source=target.catalog_source,
            )
        elif options.remote_run_store is not None:
            completed_run_sink = RemoteCompletedRunSink(
                options.remote_run_store,
                project_id=target.project_id,
                project_label=target.project_label,
                environment_name=target.environment_name,
                catalog_entry_id=target.catalog_entry_id,
                catalog_source=target.catalog_source,
            )

    live_execution_sink = options.live_execution_sink
    if (
        live_execution_sink is None
        and options.enable_live_execution
        and options.live_ingestion is not None
    ):
        runtime_profile_names: tuple[str, ...] = ()
        runtime_default_profile_name: str | None = None
        if environment is not None:
            runtime_profile_names = environment.profile_names()
            runtime_default_profile_name = environment.default_profile_name
        live_execution_sink = AsyncLiveExporter(
            config=options.live_ingestion,
            run_id=run_id,
            invocation_name=invocation_name,
            resource_context=TelemetryResourceContext(
                project_id=target.project_id,
                project_label=target.project_label,
                environment_name=target.environment_name,
                catalog_entry_id=target.catalog_entry_id,
                catalog_source=(
                    None if target.catalog_source is None else target.catalog_source.value
                ),
            ),
            runtime_default_profile_name=runtime_default_profile_name,
            runtime_profile_names=runtime_profile_names,
        )

    return _ManagedRunResolution(
        linked_project_config=linked_project_config,
        target=target,
        completed_run_sink=completed_run_sink,
        live_execution_sink=live_execution_sink,
    )


def lower_graph(program: Workflow[NamedPrimitive]) -> IRGraph:
    from mentalmodel.ir.lowering import lower_program

    return lower_program(program)


def _capture_managed_runtime(
    program: Workflow[NamedPrimitive],
    *,
    environment: RuntimeEnvironment,
    invocation_name: str | None,
    run_id: str,
    record_listeners: Sequence[RecordListener],
    span_listeners: Sequence[SpanListener],
    record_sinks: Sequence[ExecutionRecordSink],
    live_execution_sink: LiveExecutionSink | None,
) -> _ManagedRuntimeCapture:
    record_sink_listeners = tuple(record_listener_for_sink(sink) for sink in record_sinks)
    live_listeners: tuple[RecordListener, ...] = (
        ()
        if live_execution_sink is None
        else (record_listener_for_sink(_LiveRecordSinkAdapter(live_execution_sink)),)
    )
    live_span_listeners: tuple[SpanListener, ...] = (
        ()
        if live_execution_sink is None
        else (_live_span_listener(live_execution_sink),)
    )
    live_metric_emitter: MetricEmitter | None = (
        None
        if live_execution_sink is None
        else _LiveMetricEmitterAdapter(live_execution_sink)
    )
    live_tracing_config = (
        None
        if live_execution_sink is None
        else _runtime_tracing_config_for_sink(live_execution_sink)
    )
    recorder = ExecutionRecorder(
        listeners=tuple(record_listeners) + record_sink_listeners + live_listeners
    )
    executor = AsyncExecutor(
        recorder=recorder,
        environment=environment,
        invocation_name=invocation_name,
        span_listeners=tuple(span_listeners) + live_span_listeners,
        metrics=live_metric_emitter,
        tracing_config=live_tracing_config,
        run_id=run_id,
    )
    try:
        result = asyncio.run(_run_executor_with_instance(executor, program))
    except Exception as exc:
        return _ManagedRuntimeCapture(
            success=False,
            run_id=recorder.last_run_id,
            error=f"{type(exc).__name__}: {exc}",
            execution=None,
            records=tuple(recorder.records),
            outputs={},
            framed_outputs=tuple(),
            state={},
            framed_state=tuple(),
            spans=executor.tracing.snapshot_spans(),
            trace_sink_configured=executor.tracing.sink_configured,
            trace_summary=executor.tracing.trace_summary(),
            invocation_name=invocation_name,
            runtime_default_profile_name=executor.environment.default_profile_name,
            runtime_profile_names=executor.environment.profile_names(),
            invariant_failures=_collect_invariant_failures(tuple(recorder.records)),
        )
    return _ManagedRuntimeCapture(
        success=True,
        run_id=result.run_id,
        error=None,
        execution=result,
        records=result.records,
        outputs=result.outputs,
        framed_outputs=result.framed_outputs,
        state=result.state,
        framed_state=result.framed_state,
        spans=result.spans,
        trace_sink_configured=executor.tracing.sink_configured,
        trace_summary=result.trace_summary,
        invocation_name=result.invocation_name,
        runtime_default_profile_name=result.runtime_default_profile_name,
        runtime_profile_names=result.runtime_profile_names,
        invariant_failures=_collect_invariant_failures(result.records),
    )


async def _run_executor_with_instance(
    executor: AsyncExecutor,
    program: Workflow[NamedPrimitive],
) -> ExecutionResult:
    try:
        return await executor.run(program)
    finally:
        await executor.environment.finalize()


def _collect_invariant_failures(
    records: tuple[ExecutionRecord, ...],
) -> tuple[ManagedInvariantFailure, ...]:
    failures: dict[str, ManagedInvariantFailure] = {}
    for record in records:
        if record.event_type != INVARIANT_CHECKED:
            continue
        passed = record.payload.get("passed")
        if not isinstance(passed, bool) or passed:
            continue
        severity = record.payload.get("severity")
        if not isinstance(severity, str):
            severity = "error"
        failures[record.node_id] = ManagedInvariantFailure(
            node_id=record.node_id,
            severity=severity,
        )
    return tuple(failures[node_id] for node_id in sorted(failures))


@dataclass(slots=True, frozen=True)
class _LiveRecordSinkAdapter:
    sink: LiveExecutionSink

    def emit(self, record: ExecutionRecord) -> None:
        self.sink.emit_record(record)


@dataclass(slots=True, frozen=True)
class _LiveMetricEmitterAdapter:
    sink: LiveExecutionSink

    def emit(self, observations: Sequence[MetricObservation]) -> None:
        emit_metrics = getattr(self.sink, "emit_metrics", None)
        if callable(emit_metrics):
            emit_metrics(observations)

    def flush(self) -> None:
        return None


def _runtime_tracing_config_for_sink(
    sink: LiveExecutionSink,
) -> TracingConfig | None:
    runtime_tracing_config = getattr(sink, "runtime_tracing_config", None)
    if callable(runtime_tracing_config):
        resolved = runtime_tracing_config()
        if resolved is None or isinstance(resolved, TracingConfig):
            return resolved
    return None


def _live_span_listener(sink: LiveExecutionSink) -> SpanListener:
    def _listener(span: RecordedSpan) -> None:
        sink.emit_span(span)

    return _listener
