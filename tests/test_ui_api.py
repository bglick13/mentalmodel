from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from mentalmodel.ui.api import create_dashboard_app


class DashboardApiTest(unittest.TestCase):
    def test_review_workflow_dashboard_api_launches_and_inspects_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            client = TestClient(
                create_dashboard_app(runs_dir=Path(tmpdir), frontend_dist=None)
            )

            catalog_response = client.get("/api/catalog")
            self.assertEqual(catalog_response.status_code, 200)
            entries = catalog_response.json()["entries"]
            self.assertGreaterEqual(len(entries), 2)
            fixture_entry = next(
                entry for entry in entries if entry["spec_id"] == "review-workflow-fixture"
            )

            graph_response = client.get(
                f"/api/catalog/{fixture_entry['spec_id']}/graph"
            )
            self.assertEqual(graph_response.status_code, 200)
            graph_payload = graph_response.json()
            self.assertEqual(graph_payload["graph"]["graph_id"], "review_workflow")
            self.assertEqual(graph_payload["analysis"]["warning_count"], 0)

            launch_response = client.post(
                "/api/executions",
                json={"spec_id": fixture_entry["spec_id"]},
            )
            self.assertEqual(launch_response.status_code, 200)
            execution = launch_response.json()
            execution_id = execution["execution_id"]

            for _ in range(60):
                execution = client.get(f"/api/executions/{execution_id}").json()
                if execution["status"] in {"succeeded", "failed"}:
                    break
                time.sleep(0.05)
            self.assertEqual(execution["status"], "succeeded")
            self.assertTrue(execution["records"])
            run_summary = execution["run_summary"]["summary"]
            self.assertEqual(run_summary["graph_id"], "review_workflow")
            run_id = run_summary["run_id"]

            runs_response = client.get(
                "/api/runs",
                params={
                    "graph_id": "review_workflow",
                    "invocation_name": "review_workflow_fixture",
                },
            )
            self.assertEqual(runs_response.status_code, 200)
            runs = runs_response.json()["runs"]
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["run_id"], run_id)

            overview_response = client.get(f"/api/runs/review_workflow/{run_id}/overview")
            self.assertEqual(overview_response.status_code, 200)
            overview = overview_response.json()
            metric_labels = {metric["label"] for metric in overview["metrics"]}
            self.assertIn("queue_summary.auto_publish", metric_labels)
            self.assertIn("queue_summary.escalations", metric_labels)

            node_detail_response = client.get(
                f"/api/runs/review_workflow/{run_id}/nodes/queue_summary"
            )
            self.assertEqual(node_detail_response.status_code, 200)
            node_detail = node_detail_response.json()
            self.assertEqual(node_detail["output"]["processed"], 3)
            self.assertEqual(node_detail["output"]["auto_publish"], 2)

            replay_response = client.get(
                f"/api/runs/review_workflow/{run_id}/replay",
                params={"loop_node_id": "ticket_review_loop"},
            )
            self.assertEqual(replay_response.status_code, 200)
            replay = replay_response.json()
            self.assertEqual(len(replay["frame_ids"]), 3)

            records_response = client.get(f"/api/runs/review_workflow/{run_id}/records")
            self.assertEqual(records_response.status_code, 200)
            records = records_response.json()["records"]
            self.assertGreater(len(records), 0)
