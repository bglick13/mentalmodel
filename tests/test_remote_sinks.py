from __future__ import annotations

import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from mentalmodel.analysis import AnalysisReport
from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.metrics import MetricObservation
from mentalmodel.observability.tracing import RecordedSpan
from mentalmodel.remote import RunManifest
from mentalmodel.remote.sinks import LiveExecutionPublishResult
from mentalmodel.testing import run_verification


class RecordingCompletedRunSink:
    def __init__(self) -> None:
        self.published: list[tuple[RunManifest, Path]] = []

    def publish(self, *, manifest: RunManifest, run_dir: Path) -> None:
        self.published.append((manifest, run_dir))


class RecordingExecutionRecordSink:
    def __init__(self) -> None:
        self.records: list[ExecutionRecord] = []

    def emit(self, record: ExecutionRecord) -> None:
        self.records.append(record)


class RecordingLiveExecutionSink:
    def __init__(self) -> None:
        self.started: list[tuple[IRGraph, AnalysisReport]] = []
        self.records: list[ExecutionRecord] = []
        self.spans: list[RecordedSpan] = []
        self.metric_count = 0
        self.completions: list[tuple[bool, str | None]] = []

    def start(self, *, graph: IRGraph, analysis: AnalysisReport) -> None:
        self.started.append((graph, analysis))

    def emit_record(self, record: ExecutionRecord) -> None:
        self.records.append(record)

    def emit_span(self, span: RecordedSpan) -> None:
        self.spans.append(span)

    def emit_metrics(self, observations: Sequence[MetricObservation]) -> None:
        self.metric_count += len(observations)

    def complete(self, *, success: bool, error: str | None = None) -> None:
        self.completions.append((success, error))

    def runtime_tracing_config(self) -> None:
        return None

    def delivery_result(self) -> LiveExecutionPublishResult | None:
        if not self.started:
            return None
        return LiveExecutionPublishResult(
            transport="recording-live",
            delivery_mode="recording",
            success=True,
            graph_id=self.started[0][0].graph_id,
            run_id=self.records[0].run_id if self.records else "run-missing",
            required=False,
            accepted_log_count=len(self.records),
            accepted_span_count=len(self.spans),
            accepted_metric_count=self.metric_count,
            exported_log_count=len(self.records),
            exported_span_count=len(self.spans),
            exported_metric_count=self.metric_count,
        )


class RemoteSinksTest(unittest.TestCase):
    def test_run_verification_emits_record_and_completed_run_sinks(self) -> None:
        record_sink = RecordingExecutionRecordSink()
        completed_sink = RecordingCompletedRunSink()
        with tempfile.TemporaryDirectory() as tmpdir:
            report = run_verification(
                build_program(),
                runs_dir=Path(tmpdir),
                record_sinks=(record_sink,),
                completed_run_sink=completed_sink,
            )
        self.assertTrue(report.success)
        self.assertGreater(len(record_sink.records), 0)
        self.assertEqual(len(completed_sink.published), 1)
        manifest, run_dir = completed_sink.published[0]
        self.assertEqual(manifest.run_id, report.runtime.run_id)
        self.assertEqual(run_dir.name, report.runtime.run_id)

    def test_run_verification_emits_live_execution_sink(self) -> None:
        live_sink = RecordingLiveExecutionSink()
        with tempfile.TemporaryDirectory() as tmpdir:
            report = run_verification(
                build_program(),
                runs_dir=Path(tmpdir),
                live_execution_sink=live_sink,
            )
        self.assertTrue(report.success)
        self.assertEqual(len(live_sink.started), 1)
        self.assertEqual(live_sink.started[0][0].graph_id, report.as_dict()["graph_id"])
        self.assertGreater(len(live_sink.records), 0)
        self.assertGreater(len(live_sink.spans), 0)
        self.assertGreater(live_sink.metric_count, 0)
        self.assertEqual(live_sink.completions, [(True, None)])
        self.assertTrue(all(record.run_id == report.runtime.run_id for record in live_sink.records))
        self.assertTrue(all(span.sequence > 0 for span in live_sink.spans))
        self.assertIsNotNone(report.runtime.live_execution_delivery)


if __name__ == "__main__":
    unittest.main()
