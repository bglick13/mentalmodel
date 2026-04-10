from __future__ import annotations

import unittest

from mentalmodel.remote.backend import InMemoryEventIndex, RemoteEventStore
from mentalmodel.remote.events import (
    RemoteOperationEvent,
    RemoteOperationKind,
    RemoteOperationStatus,
)


class RemoteEventsTest(unittest.TestCase):
    def test_event_store_summarizes_project_and_run_health(self) -> None:
        store = RemoteEventStore(event_index=InMemoryEventIndex())
        store.record_event(
            RemoteOperationEvent(
                event_id="evt-1",
                occurred_at_ms=1000,
                kind=RemoteOperationKind.PROJECT_LINK,
                status=RemoteOperationStatus.SUCCEEDED,
                project_id="pangramanizer",
            )
        )
        store.record_event(
            RemoteOperationEvent(
                event_id="evt-2",
                occurred_at_ms=2000,
                kind=RemoteOperationKind.RUN_UPLOAD,
                status=RemoteOperationStatus.FAILED,
                project_id="pangramanizer",
                graph_id="trainer",
                run_id="run-1",
                error_message="upload failed",
                error_category="RemoteRequestError",
            )
        )

        project_summary = store.summarize_project(project_id="pangramanizer")
        self.assertEqual(project_summary.last_kind, RemoteOperationKind.RUN_UPLOAD)
        self.assertEqual(project_summary.last_status, RemoteOperationStatus.FAILED)
        self.assertEqual(project_summary.recent_success_count, 1)
        self.assertEqual(project_summary.recent_failure_count, 1)

        run_summary = store.summarize_run(graph_id="trainer", run_id="run-1")
        self.assertEqual(run_summary.last_error_message, "upload failed")
        self.assertEqual(run_summary.recent_failure_count, 1)


if __name__ == "__main__":
    unittest.main()
