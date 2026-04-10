from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from mentalmodel.analysis import AnalysisReport, run_analysis
from mentalmodel.core.interfaces import NamedPrimitive, RuntimeValue
from mentalmodel.core.workflow import Workflow
from mentalmodel.environment import EMPTY_RUNTIME_ENVIRONMENT, RuntimeEnvironment
from mentalmodel.ir.lowering import lower_program
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.export import write_json
from mentalmodel.observability.tracing import RecordedSpan, SpanListener
from mentalmodel.remote import (
    CompletedRunPublishResult,
    CompletedRunSink,
    ExecutionRecordSink,
    LiveExecutionPublishResult,
    LiveExecutionSink,
    record_listener_for_sink,
)
from mentalmodel.remote.sync import failed_completed_run_publish
from mentalmodel.runtime import AsyncExecutor, ExecutionRecorder, ExecutionResult
from mentalmodel.runtime.context import generate_run_id
from mentalmodel.runtime.events import INVARIANT_CHECKED
from mentalmodel.runtime.frame import FramedNodeValue, FramedStateValue
from mentalmodel.runtime.recorder import RecordListener
from mentalmodel.runtime.runs import RunArtifacts, write_run_artifacts
from mentalmodel.testing.invariants import PropertyCheckResult, run_property_checks


@dataclass(slots=True, frozen=True)
class RuntimeInvariantFailure:
    """Observed invariant failure from runtime semantic records."""

    node_id: str
    severity: str

    def as_dict(self) -> dict[str, str]:
        return {"node_id": self.node_id, "severity": self.severity}


@dataclass(slots=True, frozen=True)
class RuntimeVerificationResult:
    """Runtime verification outcome for a workflow execution."""

    success: bool
    record_count: int
    output_count: int
    state_count: int
    run_id: str | None = None
    run_artifacts_dir: str | None = None
    invocation_name: str | None = None
    error: str | None = None
    completed_run_upload: CompletedRunPublishResult | None = None
    live_execution_delivery: LiveExecutionPublishResult | None = None
    invariant_failures: tuple[RuntimeInvariantFailure, ...] = ()

    @property
    def warning_invariant_failures(self) -> tuple[RuntimeInvariantFailure, ...]:
        return tuple(
            failure
            for failure in self.invariant_failures
            if failure.severity == "warning"
        )

    @property
    def error_invariant_failures(self) -> tuple[RuntimeInvariantFailure, ...]:
        return tuple(
            failure
            for failure in self.invariant_failures
            if failure.severity != "warning"
        )


@dataclass(slots=True, frozen=True)
class VerificationReport:
    """Combined static, runtime, and property-check verification report."""

    analysis: AnalysisReport
    runtime: RuntimeVerificationResult
    property_checks: tuple[PropertyCheckResult, ...]
    run_artifacts: RunArtifacts | None = None

    @property
    def success(self) -> bool:
        """Return whether every verification layer succeeded."""

        return (
            not self.analysis.has_errors
            and self.runtime.success
            and all(result.success for result in self.property_checks)
        )

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-safe projection of the report."""

        return {
            "graph_id": self.analysis.graph.graph_id,
            "analysis": {
                "error_count": self.analysis.error_count,
                "warning_count": self.analysis.warning_count,
                "findings": [
                    {
                        "code": finding.code,
                        "severity": finding.severity,
                        "message": finding.message,
                        "node_id": finding.node_id,
                    }
                    for finding in self.analysis.findings
                ],
            },
            "runtime": {
                "success": self.runtime.success,
                "record_count": self.runtime.record_count,
                "output_count": self.runtime.output_count,
                "state_count": self.runtime.state_count,
                "run_id": self.runtime.run_id,
                "run_artifacts_dir": self.runtime.run_artifacts_dir,
                "invocation_name": self.runtime.invocation_name,
                "error": self.runtime.error,
                "completed_run_upload": (
                    None
                    if self.runtime.completed_run_upload is None
                    else self.runtime.completed_run_upload.as_dict()
                ),
                "live_execution_delivery": (
                    None
                    if self.runtime.live_execution_delivery is None
                    else self.runtime.live_execution_delivery.as_dict()
                ),
                "warning_invariant_failures": [
                    failure.as_dict()
                    for failure in self.runtime.warning_invariant_failures
                ],
                "error_invariant_failures": [
                    failure.as_dict()
                    for failure in self.runtime.error_invariant_failures
                ],
            },
            "property_checks": [
                {
                    "name": result.name,
                    "success": result.success,
                    "hypothesis_backed": result.hypothesis_backed,
                    "error": result.error,
                }
                for result in self.property_checks
            ],
            "success": self.success,
        }


@dataclass(slots=True, frozen=True)
class RuntimeExecutionCapture:
    """Captured runtime details used for report generation and run artifacts."""

    result: RuntimeVerificationResult
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


def execute_program(
    program: Workflow[NamedPrimitive],
    *,
    environment: RuntimeEnvironment | None = None,
    invocation_name: str | None = None,
    run_id: str | None = None,
) -> ExecutionResult:
    """Run one workflow through the deterministic async executor."""

    resolved_environment = environment or EMPTY_RUNTIME_ENVIRONMENT
    return asyncio.run(
        _run_executor(
            program,
            environment=resolved_environment,
            invocation_name=invocation_name,
            run_id=run_id,
        )
    )


def run_verification(
    program: Workflow[NamedPrimitive],
    *,
    module: ModuleType | None = None,
    runs_dir: Path | None = None,
    persist_run_artifacts: bool = True,
    environment: RuntimeEnvironment | None = None,
    invocation_name: str | None = None,
    record_listeners: Sequence[RecordListener] = (),
    span_listeners: Sequence[SpanListener] = (),
    record_sinks: Sequence[ExecutionRecordSink] = (),
    completed_run_sink: CompletedRunSink | None = None,
    live_execution_sink: LiveExecutionSink | None = None,
    run_id: str | None = None,
) -> VerificationReport:
    """Run static analysis, runtime execution, and property checks."""

    graph = lower_program(program)
    analysis = run_analysis(graph)
    resolved_run_id = run_id or generate_run_id()
    if live_execution_sink is not None:
        live_execution_sink.start(graph=graph, analysis=analysis)
    runtime_capture = _capture_runtime(
        program,
        environment=environment,
        invocation_name=invocation_name,
        run_id=resolved_run_id,
        record_listeners=record_listeners,
        span_listeners=(
            tuple(span_listeners)
            + (() if live_execution_sink is None else (live_execution_sink.emit_span,))
        ),
        record_sinks=record_sinks,
        live_execution_sink=live_execution_sink,
    )
    property_checks = (
        run_property_checks(module, program)
        if module is not None
        else tuple[PropertyCheckResult, ...]()
    )
    report = VerificationReport(
        analysis=analysis,
        runtime=runtime_capture.result,
        property_checks=property_checks,
    )
    if not persist_run_artifacts:
        if live_execution_sink is not None:
            live_execution_sink.complete(
                success=runtime_capture.result.success,
                error=runtime_capture.result.error,
            )
        return report

    completed_run_upload: CompletedRunPublishResult | None = None
    artifacts = write_run_artifacts(
        graph=graph,
        run_id=runtime_capture.result.run_id or "run-failed",
        success=report.success,
        records=runtime_capture.records,
        outputs=runtime_capture.outputs,
        framed_outputs=runtime_capture.framed_outputs,
        state=runtime_capture.state,
        framed_state=runtime_capture.framed_state,
        spans=runtime_capture.spans,
        runs_dir=runs_dir,
        verification_payload=report.as_dict(),
        trace_sink_configured=runtime_capture.trace_sink_configured,
        trace_summary=runtime_capture.trace_summary,
        invocation_name=runtime_capture.invocation_name,
        runtime_default_profile_name=runtime_capture.runtime_default_profile_name,
        runtime_profile_names=runtime_capture.runtime_profile_names,
    )
    if completed_run_sink is not None:
        try:
            completed_run_upload = completed_run_sink.publish(
                manifest=artifacts.manifest,
                run_dir=artifacts.run_dir,
            )
        except Exception as exc:
            completed_run_upload = failed_completed_run_publish(
                transport=type(completed_run_sink).__name__,
                manifest=artifacts.manifest,
                error=exc,
            )
    if live_execution_sink is not None:
        live_execution_sink.complete(
            success=runtime_capture.result.success,
            error=runtime_capture.result.error,
        )
    live_execution_delivery = (
        None if live_execution_sink is None else live_execution_sink.delivery_result()
    )
    runtime = RuntimeVerificationResult(
        success=runtime_capture.result.success,
        record_count=runtime_capture.result.record_count,
        output_count=runtime_capture.result.output_count,
        state_count=runtime_capture.result.state_count,
        run_id=runtime_capture.result.run_id,
        run_artifacts_dir=str(artifacts.run_dir),
        invocation_name=runtime_capture.result.invocation_name,
        error=runtime_capture.result.error,
        completed_run_upload=completed_run_upload,
        live_execution_delivery=live_execution_delivery,
        invariant_failures=runtime_capture.result.invariant_failures,
    )
    final_report = VerificationReport(
        analysis=analysis,
        runtime=runtime,
        property_checks=property_checks,
        run_artifacts=artifacts,
    )
    if artifacts.verification_path is not None:
        write_json(artifacts.verification_path, final_report.as_dict())
    return final_report


def _capture_runtime(
    program: Workflow[NamedPrimitive],
    *,
    environment: RuntimeEnvironment | None = None,
    invocation_name: str | None = None,
    run_id: str | None = None,
    record_listeners: Sequence[RecordListener] = (),
    span_listeners: Sequence[SpanListener] = (),
    record_sinks: Sequence[ExecutionRecordSink] = (),
    live_execution_sink: LiveExecutionSink | None = None,
) -> RuntimeExecutionCapture:
    record_sink_listeners = tuple(record_listener_for_sink(sink) for sink in record_sinks)
    live_listeners: tuple[RecordListener, ...] = (
        ()
        if live_execution_sink is None
        else (record_listener_for_sink(_LiveRecordSinkAdapter(live_execution_sink)),)
    )
    recorder = ExecutionRecorder(
        listeners=tuple(record_listeners) + record_sink_listeners + live_listeners
    )
    resolved_environment = environment or EMPTY_RUNTIME_ENVIRONMENT
    executor = AsyncExecutor(
        recorder=recorder,
        environment=resolved_environment,
        invocation_name=invocation_name,
        span_listeners=span_listeners,
        run_id=run_id,
    )
    try:
        result = asyncio.run(
            _run_executor_with_instance(
                executor,
                program,
            )
        )
    except Exception as exc:
        invariant_failures = _collect_invariant_failures(tuple(recorder.records))
        return RuntimeExecutionCapture(
            result=RuntimeVerificationResult(
                success=False,
                record_count=len(recorder.records),
                output_count=0,
                state_count=0,
                run_id=recorder.last_run_id,
                invocation_name=invocation_name,
                error=f"{type(exc).__name__}: {exc}",
                invariant_failures=invariant_failures,
            ),
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
        )
    return RuntimeExecutionCapture(
        result=RuntimeVerificationResult(
            success=True,
            record_count=len(result.records),
            output_count=len(result.outputs),
            state_count=len(result.state),
            run_id=result.run_id,
            invocation_name=result.invocation_name,
            invariant_failures=_collect_invariant_failures(result.records),
        ),
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
    )


async def _run_executor(
    program: Workflow[NamedPrimitive],
    *,
    environment: RuntimeEnvironment,
    invocation_name: str | None,
    recorder: ExecutionRecorder | None = None,
    run_id: str | None = None,
) -> ExecutionResult:
    executor = AsyncExecutor(
        recorder=recorder,
        environment=environment,
        invocation_name=invocation_name,
        run_id=run_id,
    )
    return await _run_executor_with_instance(executor, program)


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
) -> tuple[RuntimeInvariantFailure, ...]:
    failures: dict[str, RuntimeInvariantFailure] = {}
    for record in records:
        if record.event_type != INVARIANT_CHECKED:
            continue
        passed = record.payload.get("passed")
        if not isinstance(passed, bool) or passed:
            continue
        severity = record.payload.get("severity")
        if not isinstance(severity, str):
            severity = "error"
        failures[record.node_id] = RuntimeInvariantFailure(
            node_id=record.node_id,
            severity=severity,
        )
    return tuple(failures[node_id] for node_id in sorted(failures))


@dataclass(slots=True, frozen=True)
class _LiveRecordSinkAdapter:
    sink: LiveExecutionSink

    def emit(self, record: ExecutionRecord) -> None:
        self.sink.emit_record(record)
