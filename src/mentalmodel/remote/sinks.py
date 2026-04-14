from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from mentalmodel.analysis import AnalysisReport
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.config import TracingConfig
from mentalmodel.observability.metrics import MetricObservation
from mentalmodel.observability.tracing import RecordedSpan
from mentalmodel.remote.contracts import RunManifest


@dataclass(slots=True, frozen=True)
class CompletedRunPublishResult:
    """Outcome of publishing one completed run bundle to a downstream sink."""

    transport: str
    success: bool
    graph_id: str
    run_id: str
    project_id: str | None = None
    server_url: str | None = None
    remote_run_dir: str | None = None
    uploaded_at_ms: int | None = None
    attempt_count: int = 1
    retryable: bool | None = None
    error_category: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if not self.transport:
            raise ValueError("CompletedRunPublishResult.transport cannot be empty.")
        if not self.graph_id:
            raise ValueError("CompletedRunPublishResult.graph_id cannot be empty.")
        if not self.run_id:
            raise ValueError("CompletedRunPublishResult.run_id cannot be empty.")
        if self.server_url == "":
            raise ValueError("CompletedRunPublishResult.server_url cannot be empty.")
        if self.remote_run_dir == "":
            raise ValueError("CompletedRunPublishResult.remote_run_dir cannot be empty.")
        if self.uploaded_at_ms is not None and self.uploaded_at_ms < 0:
            raise ValueError("CompletedRunPublishResult.uploaded_at_ms cannot be negative.")
        if self.attempt_count < 1:
            raise ValueError("CompletedRunPublishResult.attempt_count must be at least 1.")
        if self.success and self.error is not None:
            raise ValueError(
                "CompletedRunPublishResult.error must be None when success is true."
            )
        if not self.success and not self.error:
            raise ValueError(
                "CompletedRunPublishResult.error is required when success is false."
            )

    def as_dict(self) -> dict[str, str | int | bool | None]:
        return {
            "transport": self.transport,
            "success": self.success,
            "graph_id": self.graph_id,
            "run_id": self.run_id,
            "project_id": self.project_id,
            "server_url": self.server_url,
            "remote_run_dir": self.remote_run_dir,
            "uploaded_at_ms": self.uploaded_at_ms,
            "attempt_count": self.attempt_count,
            "retryable": self.retryable,
            "error_category": self.error_category,
            "error": self.error,
        }


@dataclass(slots=True, frozen=True)
class LiveExecutionPublishResult:
    """Outcome of remote live execution delivery for one run."""

    transport: str
    delivery_mode: str
    success: bool
    graph_id: str
    run_id: str
    project_id: str | None = None
    otlp_endpoint: str | None = None
    required: bool = False
    accepted_log_count: int = 0
    accepted_span_count: int = 0
    accepted_metric_count: int = 0
    exported_log_count: int = 0
    exported_span_count: int = 0
    exported_metric_count: int = 0
    outbox_depth: int = 0
    outbox_bytes: int = 0
    ack_lag_ms: int | None = None
    retry_count: int = 0
    last_batch_size: int = 0
    last_batch_latency_ms: int | None = None
    degraded: bool = False
    failed_open: bool = False
    accepting_events: bool = True
    completed: bool = False
    last_error: str | None = None

    def __post_init__(self) -> None:
        if not self.transport:
            raise ValueError("LiveExecutionPublishResult.transport cannot be empty.")
        if not self.delivery_mode:
            raise ValueError("LiveExecutionPublishResult.delivery_mode cannot be empty.")
        if not self.graph_id:
            raise ValueError("LiveExecutionPublishResult.graph_id cannot be empty.")
        if not self.run_id:
            raise ValueError("LiveExecutionPublishResult.run_id cannot be empty.")
        if self.otlp_endpoint == "":
            raise ValueError("LiveExecutionPublishResult.otlp_endpoint cannot be empty.")
        if self.accepted_log_count < 0 or self.accepted_span_count < 0:
            raise ValueError("LiveExecutionPublishResult accepted counts cannot be negative.")
        if self.accepted_metric_count < 0:
            raise ValueError("LiveExecutionPublishResult accepted counts cannot be negative.")
        if self.exported_log_count < 0 or self.exported_span_count < 0:
            raise ValueError("LiveExecutionPublishResult exported counts cannot be negative.")
        if self.exported_metric_count < 0:
            raise ValueError("LiveExecutionPublishResult exported counts cannot be negative.")
        if self.outbox_depth < 0 or self.outbox_bytes < 0:
            raise ValueError("LiveExecutionPublishResult outbox state cannot be negative.")
        if self.ack_lag_ms is not None and self.ack_lag_ms < 0:
            raise ValueError("LiveExecutionPublishResult.ack_lag_ms cannot be negative.")
        if self.retry_count < 0:
            raise ValueError("LiveExecutionPublishResult.retry_count cannot be negative.")
        if self.last_batch_size < 0:
            raise ValueError("LiveExecutionPublishResult.last_batch_size cannot be negative.")
        if self.last_batch_latency_ms is not None and self.last_batch_latency_ms < 0:
            raise ValueError(
                "LiveExecutionPublishResult.last_batch_latency_ms cannot be negative."
            )
        if self.success and self.last_error is not None:
            raise ValueError(
                "LiveExecutionPublishResult.last_error must be None when success is true."
            )
        if not self.success and not self.last_error:
            raise ValueError(
                "LiveExecutionPublishResult.last_error is required when success is false."
            )

    def as_dict(self) -> dict[str, str | int | bool | None]:
        return {
            "transport": self.transport,
            "delivery_mode": self.delivery_mode,
            "success": self.success,
            "graph_id": self.graph_id,
            "run_id": self.run_id,
            "project_id": self.project_id,
            "otlp_endpoint": self.otlp_endpoint,
            "required": self.required,
            "accepted_log_count": self.accepted_log_count,
            "accepted_span_count": self.accepted_span_count,
            "accepted_metric_count": self.accepted_metric_count,
            "exported_log_count": self.exported_log_count,
            "exported_span_count": self.exported_span_count,
            "exported_metric_count": self.exported_metric_count,
            "outbox_depth": self.outbox_depth,
            "outbox_bytes": self.outbox_bytes,
            "ack_lag_ms": self.ack_lag_ms,
            "retry_count": self.retry_count,
            "last_batch_size": self.last_batch_size,
            "last_batch_latency_ms": self.last_batch_latency_ms,
            "degraded": self.degraded,
            "failed_open": self.failed_open,
            "accepting_events": self.accepting_events,
            "completed": self.completed,
            "last_error": self.last_error,
        }


class CompletedRunSink(Protocol):
    """Transport-neutral sink for one finalized run manifest and artifact directory."""

    def publish(
        self,
        *,
        manifest: RunManifest,
        run_dir: Path,
    ) -> CompletedRunPublishResult | None:
        """Publish one completed run."""


class ExecutionRecordSink(Protocol):
    """Transport-neutral sink for semantic execution records."""

    def emit(self, record: ExecutionRecord) -> None:
        """Emit one semantic execution record."""


class LiveExecutionSink(Protocol):
    """Transport-neutral sink for one in-progress run's live execution stream."""

    def start(self, *, graph: IRGraph, analysis: AnalysisReport) -> None:
        """Open or refresh the remote live session before events stream."""

    def emit_record(self, record: ExecutionRecord) -> None:
        """Emit one semantic execution record."""

    def emit_span(self, span: RecordedSpan) -> None:
        """Emit one recorded span."""

    def emit_metrics(self, observations: Sequence[MetricObservation]) -> None:
        """Emit one batch of metric observations."""

    def complete(self, *, success: bool, error: str | None = None) -> None:
        """Flush and mark the live session terminal."""

    def runtime_tracing_config(self) -> TracingConfig | None:
        """Return one runtime tracing override when live export owns delivery."""

    def delivery_result(self) -> LiveExecutionPublishResult | None:
        """Return the current delivery outcome for this live stream."""


class NoOpCompletedRunSink:
    """Completed-run sink that intentionally does nothing."""

    def publish(
        self,
        *,
        manifest: RunManifest,
        run_dir: Path,
    ) -> CompletedRunPublishResult | None:
        del manifest, run_dir
        return None


class CompositeCompletedRunSink:
    """Fan out one completed run publish operation to multiple sinks."""

    def __init__(self, sinks: Sequence[CompletedRunSink]) -> None:
        self._sinks = tuple(sinks)

    def publish(
        self,
        *,
        manifest: RunManifest,
        run_dir: Path,
    ) -> CompletedRunPublishResult | None:
        latest: CompletedRunPublishResult | None = None
        for sink in self._sinks:
            result = sink.publish(manifest=manifest, run_dir=run_dir)
            if result is not None:
                latest = result
        return latest


class NoOpExecutionRecordSink:
    """Execution record sink that intentionally does nothing."""

    def emit(self, record: ExecutionRecord) -> None:
        del record


class NoOpLiveExecutionSink:
    """Live execution sink that intentionally does nothing."""

    def start(self, *, graph: IRGraph, analysis: AnalysisReport) -> None:
        del graph, analysis
        return None

    def emit_record(self, record: ExecutionRecord) -> None:
        del record

    def emit_span(self, span: RecordedSpan) -> None:
        del span

    def emit_metrics(self, observations: Sequence[MetricObservation]) -> None:
        del observations

    def complete(self, *, success: bool, error: str | None = None) -> None:
        del success, error

    def runtime_tracing_config(self) -> TracingConfig | None:
        return None

    def delivery_result(self) -> LiveExecutionPublishResult | None:
        return None


class CompositeExecutionRecordSink:
    """Fan out one execution record to multiple sinks."""

    def __init__(self, sinks: Sequence[ExecutionRecordSink]) -> None:
        self._sinks = tuple(sinks)

    def emit(self, record: ExecutionRecord) -> None:
        for sink in self._sinks:
            sink.emit(record)


def record_listener_for_sink(
    sink: ExecutionRecordSink,
) -> Callable[[ExecutionRecord], None]:
    """Adapt one execution-record sink to the recorder listener callback shape."""

    def _listener(record: ExecutionRecord) -> None:
        sink.emit(record)

    return _listener
