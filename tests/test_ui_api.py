from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from mentalmodel.ui.api import create_dashboard_app
from mentalmodel.ui.catalog import DashboardCatalogEntry, default_dashboard_catalog


class DashboardApiTest(unittest.TestCase):
    def test_dashboard_api_accepts_custom_catalog_entries(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        custom_entry = DashboardCatalogEntry(
            spec_id="custom-review",
            label="Custom Review",
            description="Custom mounted dashboard entry.",
            spec_path=fixture_entry.spec_path,
            graph_id=fixture_entry.graph_id,
            invocation_name="custom_review",
            category="integration",
            tags=("custom", "demo"),
            default_loop_node_id=fixture_entry.default_loop_node_id,
            metric_groups=fixture_entry.metric_groups,
            pinned_nodes=fixture_entry.pinned_nodes,
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                catalog_entries=(custom_entry,),
            )
        )

        catalog_response = client.get("/api/catalog")
        self.assertEqual(catalog_response.status_code, 200)
        entries = catalog_response.json()["entries"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["spec_id"], "custom-review")
        self.assertEqual(entries[0]["category"], "integration")
        self.assertEqual(entries[0]["default_loop_node_id"], "ticket_review_loop")
        self.assertTrue(entries[0]["metric_groups"])
        self.assertTrue(entries[0]["pinned_nodes"])

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
            queue_auto_publish_count = sum(
                1
                for metric in overview["metrics"]
                if metric["label"] == "queue_summary.auto_publish"
            )
            self.assertEqual(queue_auto_publish_count, 1)

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

    def test_dashboard_registers_spec_from_absolute_path(self) -> None:
        fixture_path = Path(__file__).resolve().parents[1] / Path(
            "src/mentalmodel/examples/review_workflow/review_workflow_fixture.toml"
        )
        client = TestClient(create_dashboard_app(runs_dir=None, frontend_dist=None))
        response = client.post("/api/catalog/from-path", json={"spec_path": str(fixture_path)})
        self.assertEqual(response.status_code, 200)
        entry = response.json()["entry"]
        self.assertEqual(entry["graph_id"], "review_workflow")
        self.assertTrue(entry["spec_id"].startswith("path-"))

        graph = client.get(f"/api/catalog/{entry['spec_id']}/graph")
        self.assertEqual(graph.status_code, 200)

    def test_dashboard_execution_accepts_spec_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(__file__).resolve().parents[1] / Path(
                "src/mentalmodel/examples/review_workflow/review_workflow_fixture.toml"
            )
            client = TestClient(
                create_dashboard_app(runs_dir=Path(tmpdir), frontend_dist=None)
            )
            launch = client.post(
                "/api/executions",
                json={"spec_path": str(fixture_path)},
            )
            self.assertEqual(launch.status_code, 200)
            execution_id = launch.json()["execution_id"]
            for _ in range(60):
                execution = client.get(f"/api/executions/{execution_id}").json()
                if execution["status"] in {"succeeded", "failed"}:
                    break
                time.sleep(0.05)
            self.assertEqual(execution["status"], "succeeded")

    def test_analytics_timeseries_returns_buckets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            client = TestClient(
                create_dashboard_app(runs_dir=Path(tmpdir), frontend_dist=None)
            )
            catalog = client.get("/api/catalog").json()["entries"]
            fixture = next(e for e in catalog if e["spec_id"] == "review-workflow-fixture")
            until = int(time.time() * 1000)
            since = until - 3_600_000
            ts = client.get(
                "/api/analytics/timeseries",
                params={
                    "graph_id": fixture["graph_id"],
                    "invocation_name": fixture["invocation_name"],
                    "since_ms": since,
                    "until_ms": until,
                    "rollup_ms": 60_000,
                },
            )
            self.assertEqual(ts.status_code, 200)
            body = ts.json()
            self.assertIn("buckets", body)
            self.assertGreaterEqual(len(body["buckets"]), 1)
            self.assertLessEqual(len(body["buckets"]), 500)
            self.assertIn("records_per_sec", body["buckets"][0])
