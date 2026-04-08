from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.remote import RunManifest
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


if __name__ == "__main__":
    unittest.main()
