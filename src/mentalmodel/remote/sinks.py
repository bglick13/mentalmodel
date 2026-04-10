from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from mentalmodel.analysis import AnalysisReport
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.records import ExecutionRecord
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
    success: bool
    graph_id: str
    run_id: str
    project_id: str | None = None
    server_url: str | None = None
    start_attempt_count: int = 0
    update_attempt_count: int = 0
    delivered_record_count: int = 0
    delivered_span_count: int = 0
    buffered_record_count: int = 0
    buffered_span_count: int = 0
    retryable: bool | None = None
    error_category: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if not self.transport:
            raise ValueError("LiveExecutionPublishResult.transport cannot be empty.")
        if not self.graph_id:
            raise ValueError("LiveExecutionPublishResult.graph_id cannot be empty.")
        if not self.run_id:
            raise ValueError("LiveExecutionPublishResult.run_id cannot be empty.")
        if self.start_attempt_count < 0 or self.update_attempt_count < 0:
            raise ValueError("LiveExecutionPublishResult attempt counts cannot be negative.")
        if self.delivered_record_count < 0 or self.delivered_span_count < 0:
            raise ValueError("LiveExecutionPublishResult delivered counts cannot be negative.")
        if self.buffered_record_count < 0 or self.buffered_span_count < 0:
            raise ValueError("LiveExecutionPublishResult buffered counts cannot be negative.")
        if self.success and self.error is not None:
            raise ValueError("LiveExecutionPublishResult.error must be None when success is true.")
        if not self.success and not self.error:
            raise ValueError(
                "LiveExecutionPublishResult.error is required when success is false."
            )

    def as_dict(self) -> dict[str, str | int | bool | None]:
        return {
            "transport": self.transport,
            "success": self.success,
            "graph_id": self.graph_id,
            "run_id": self.run_id,
            "project_id": self.project_id,
            "server_url": self.server_url,
            "start_attempt_count": self.start_attempt_count,
            "update_attempt_count": self.update_attempt_count,
            "delivered_record_count": self.delivered_record_count,
            "delivered_span_count": self.delivered_span_count,
            "buffered_record_count": self.buffered_record_count,
            "buffered_span_count": self.buffered_span_count,
            "retryable": self.retryable,
            "error_category": self.error_category,
            "error": self.error,
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

    def complete(self, *, success: bool, error: str | None = None) -> None:
        """Flush and mark the live session terminal."""

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

    def complete(self, *, success: bool, error: str | None = None) -> None:
        del success, error

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
