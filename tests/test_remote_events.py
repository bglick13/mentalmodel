from __future__ import annotations

import unittest

from mentalmodel.remote.backend import (
    InMemoryEventIndex,
    RemoteEventStore,
    _json_list_from_db_payload,
    _json_object_from_db_payload,
)
from mentalmodel.remote.events import (
    RemoteOperationEvent,
    RemoteOperationKind,
    RemoteOperationStatus,
)


class RemoteEventsTest(unittest.TestCase):
    def test_json_payload_helpers_accept_decoded_jsonb_values(self) -> None:
        object_payload = _json_object_from_db_payload(
            {"kind": "run.upload", "attempts": 2},
            "remote_operation_events.metadata_json",
        )
        list_payload = _json_list_from_db_payload(
            ["fixture", "real"],
            "remote_live_sessions.runtime_profile_names",
        )

        self.assertEqual(object_payload["kind"], "run.upload")
        self.assertEqual(object_payload["attempts"], 2)
        self.assertEqual(list_payload, ["fixture", "real"])

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
