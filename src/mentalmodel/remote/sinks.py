from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.remote.contracts import RunManifest


class CompletedRunSink(Protocol):
    """Transport-neutral sink for one finalized run manifest and artifact directory."""

    def publish(self, *, manifest: RunManifest, run_dir: Path) -> None:
        """Publish one completed run."""


class ExecutionRecordSink(Protocol):
    """Transport-neutral sink for semantic execution records."""

    def emit(self, record: ExecutionRecord) -> None:
        """Emit one semantic execution record."""


class NoOpCompletedRunSink:
    """Completed-run sink that intentionally does nothing."""

    def publish(self, *, manifest: RunManifest, run_dir: Path) -> None:
        del manifest, run_dir


class CompositeCompletedRunSink:
    """Fan out one completed run publish operation to multiple sinks."""

    def __init__(self, sinks: Sequence[CompletedRunSink]) -> None:
        self._sinks = tuple(sinks)

    def publish(self, *, manifest: RunManifest, run_dir: Path) -> None:
        for sink in self._sinks:
            sink.publish(manifest=manifest, run_dir=run_dir)


class NoOpExecutionRecordSink:
    """Execution record sink that intentionally does nothing."""

    def emit(self, record: ExecutionRecord) -> None:
        del record


class CompositeExecutionRecordSink:
    """Fan out one execution record to multiple sinks."""

    def __init__(self, sinks: Sequence[ExecutionRecordSink]) -> None:
        self._sinks = tuple(sinks)

    def emit(self, record: ExecutionRecord) -> None:
        for sink in self._sinks:
            sink.emit(record)


def record_listener_for_sink(sink: ExecutionRecordSink):
    """Adapt one execution-record sink to the recorder listener callback shape."""

    def _listener(record: ExecutionRecord) -> None:
        sink.emit(record)

    return _listener
