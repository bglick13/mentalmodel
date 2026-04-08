from __future__ import annotations

import io
import json
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.remote import (
    InMemoryArtifactStore,
    InMemoryManifestIndex,
    ProjectCatalog,
    ProjectRegistration,
    RemoteRunStore,
    build_run_bundle_upload,
)
from mentalmodel.testing import run_verification
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

    def test_remote_ingest_endpoint_persists_run_for_existing_read_api(self) -> None:
        with (
            tempfile.TemporaryDirectory() as local_tmp,
            tempfile.TemporaryDirectory() as remote_tmp,
        ):
            local_root = Path(local_tmp)
            remote_root = Path(remote_tmp)
            report = run_verification(build_program(), runs_dir=local_root)
            self.assertTrue(report.success)
            upload = build_run_bundle_upload(
                runs_dir=local_root,
                graph_id="async_rl_demo",
                run_id=report.runtime.run_id,
                project_id="mentalmodel-examples",
                project_label="Mentalmodel Examples",
            )
            remote_store = RemoteRunStore(
                manifest_index=InMemoryManifestIndex(),
                artifact_store=InMemoryArtifactStore(),
                cache_dir=remote_root,
            )
            client = TestClient(
                create_dashboard_app(
                    runs_dir=None,
                    frontend_dist=None,
                    remote_run_store=remote_store,
                )
            )
            ingest = client.post("/api/remote/runs", json=upload.as_dict())
            self.assertEqual(ingest.status_code, 200)
            runs_response = client.get(
                "/api/runs",
                params={"graph_id": "async_rl_demo"},
            )
            self.assertEqual(runs_response.status_code, 200)
            runs = runs_response.json()["runs"]
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["run_id"], report.runtime.run_id)
            overview = client.get(
                f"/api/runs/async_rl_demo/{report.runtime.run_id}/overview"
            )
            self.assertEqual(overview.status_code, 200)

    def test_external_project_catalog_graph_uses_subprocess_loader(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        external_entry = DashboardCatalogEntry(
            spec_id="pangram-real-verify3",
            label="Pangram Real Verify3",
            description="real verify3",
            spec_path=fixture_entry.spec_path,
            graph_id="pangramanizer_training_real_verify3",
            invocation_name="pangram_real_verify3",
        )
        catalog = ProjectCatalog(
            project=ProjectRegistration(
                project_id="pangramanizer-training",
                label="Pangramanizer Training",
                root_dir=Path("/Users/ben/repos/pangramanizer"),
                runs_dir=Path("/tmp/pangram-runs"),
            ),
            entries=(external_entry,),
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                project_catalogs=(catalog,),
            )
        )
        payload = {
            "graph": {"graph_id": "pangramanizer_training_real_verify3", "nodes": [], "edges": []},
            "analysis": {"error_count": 0, "warning_count": 0, "findings": []},
        }
        with patch("mentalmodel.ui.service.subprocess.run") as run_subprocess:
            run_subprocess.return_value = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=json.dumps(payload),
                stderr="",
            )
            response = client.get("/api/catalog/pangram-real-verify3/graph")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["graph"]["graph_id"], "pangramanizer_training_real_verify3")
        run_subprocess.assert_called_once()

    def test_external_project_execution_uses_subprocess_runner(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        external_entry = DashboardCatalogEntry(
            spec_id="pangram-real-verify3",
            label="Pangram Real Verify3",
            description="real verify3",
            spec_path=fixture_entry.spec_path,
            graph_id="pangramanizer_training_real_verify3",
            invocation_name="pangram_real_verify3",
        )
        catalog = ProjectCatalog(
            project=ProjectRegistration(
                project_id="pangramanizer-training",
                label="Pangramanizer Training",
                root_dir=Path("/Users/ben/repos/pangramanizer"),
                runs_dir=Path("/tmp/pangram-runs"),
            ),
            entries=(external_entry,),
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                project_catalogs=(catalog,),
            )
        )
        payload = {
            "success": True,
            "runtime": {
                "success": True,
                "record_count": 0,
                "output_count": 0,
                "state_count": 0,
                "run_id": None,
                "run_artifacts_dir": None,
                "invocation_name": "pangram_real_verify3",
                "error": None,
                "warning_invariant_failures": [],
                "error_invariant_failures": [],
            },
            "analysis": {"error_count": 0, "warning_count": 0, "findings": []},
            "property_checks": [],
            "graph_id": "pangramanizer_training_real_verify3",
        }
        class FakePopen:
            def __init__(self, args: list[str], **_: object) -> None:
                self.args = args
                self.stdout = io.StringIO("Your Tinker SDK version is outdated.\n")
                self.stderr = io.StringIO("")

            def wait(self) -> int:
                Path(self.args[-1]).write_text(json.dumps(payload), encoding="utf-8")
                return 0

        with patch("mentalmodel.ui.service.subprocess.Popen", side_effect=FakePopen) as popen:
            launch = client.post("/api/executions", json={"spec_id": "pangram-real-verify3"})
            self.assertEqual(launch.status_code, 200)
            execution_id = launch.json()["execution_id"]
            for _ in range(20):
                execution = client.get(f"/api/executions/{execution_id}").json()
                if execution["status"] in {"succeeded", "failed"}:
                    break
                time.sleep(0.01)
        self.assertEqual(execution["status"], "succeeded")
        self.assertTrue(
            any(
                message["message"] == "Your Tinker SDK version is outdated."
                for message in execution["messages"]
            )
        )
        popen.assert_called_once()

    def test_external_spec_path_registration_and_execution_use_subprocess(self) -> None:
        with (
            tempfile.TemporaryDirectory() as project_tmp,
            tempfile.TemporaryDirectory() as runs_tmp,
            tempfile.TemporaryDirectory() as cache_tmp,
        ):
            project_root = Path(project_tmp)
            external_spec_path = (
                project_root
                / "pangramanizer"
                / "mentalmodel_training"
                / "verification"
                / "real_smoke.toml"
            )
            external_spec_path.parent.mkdir(parents=True, exist_ok=True)
            external_spec_path.write_text(
                "\n".join(
                    (
                        "[program]",
                        "entrypoint = "
                        '"pangramanizer.mentalmodel_training.verification.real_smoke:build_program"',
                        "",
                        "[runtime]",
                        'invocation_name = "pangram_real_smoke"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            persisted = run_verification(
                build_program(),
                runs_dir=Path(runs_tmp),
                invocation_name="pangram_real_smoke",
            )
            self.assertTrue(persisted.success)
            manifest_index = InMemoryManifestIndex()
            remote_store = RemoteRunStore(
                manifest_index=manifest_index,
                artifact_store=InMemoryArtifactStore(),
                cache_dir=Path(cache_tmp),
            )
            catalog = ProjectCatalog(
                project=ProjectRegistration(
                    project_id="pangramanizer-training",
                    label="Pangramanizer Training",
                    root_dir=project_root,
                    runs_dir=Path(runs_tmp),
                ),
                entries=(),
            )
            client = TestClient(
                create_dashboard_app(
                    runs_dir=None,
                    frontend_dist=None,
                    project_catalogs=(catalog,),
                    remote_run_store=remote_store,
                )
            )
            metadata = {
                "graph_id": "async_rl_demo",
                "invocation_name": "pangram_real_smoke",
            }
            verification_payload = {
                "success": True,
                "runtime": {
                    "success": True,
                    "record_count": persisted.runtime.record_count,
                    "output_count": persisted.runtime.output_count,
                    "state_count": persisted.runtime.state_count,
                    "run_id": persisted.runtime.run_id,
                    "run_artifacts_dir": persisted.runtime.run_artifacts_dir,
                    "invocation_name": "pangram_real_smoke",
                    "error": None,
                    "warning_invariant_failures": [],
                    "error_invariant_failures": [],
                },
                "analysis": {"error_count": 0, "warning_count": 0, "findings": []},
                "property_checks": [],
                "graph_id": "async_rl_demo",
            }
            class FakePopen:
                def __init__(self, args: list[str], **_: object) -> None:
                    self.args = args
                    self.stdout = io.StringIO("external verification is starting\n")
                    self.stderr = io.StringIO("")

                def wait(self) -> int:
                    Path(self.args[-1]).write_text(
                        json.dumps(verification_payload),
                        encoding="utf-8",
                    )
                    return 0

            with (
                patch("mentalmodel.ui.service.subprocess.run") as run_subprocess,
                patch("mentalmodel.ui.service.subprocess.Popen", side_effect=FakePopen),
            ):
                run_subprocess.side_effect = [
                    subprocess.CompletedProcess(
                        args=[],
                        returncode=0,
                        stdout=json.dumps(metadata),
                        stderr="",
                    ),
                    subprocess.CompletedProcess(
                        args=[],
                        returncode=0,
                        stdout=json.dumps(verification_payload),
                        stderr="",
                    ),
                ]
                launch = client.post(
                    "/api/executions",
                    json={"spec_path": str(external_spec_path)},
                )
                self.assertEqual(launch.status_code, 200)
                execution_id = launch.json()["execution_id"]
                for _ in range(20):
                    execution = client.get(f"/api/executions/{execution_id}").json()
                    if execution["status"] in {"succeeded", "failed"}:
                        break
                    time.sleep(0.01)
            self.assertEqual(execution["status"], "succeeded")
            self.assertEqual(run_subprocess.call_count, 1)
            self.assertTrue(
                any(
                    message["message"] == "external verification is starting"
                    for message in execution["messages"]
                )
            )
            runs_response = client.get(
                "/api/runs",
                params={
                    "graph_id": "async_rl_demo",
                    "invocation_name": "pangram_real_smoke",
                },
            )
            self.assertEqual(runs_response.status_code, 200)
            runs = runs_response.json()["runs"]
            self.assertEqual(len(runs), 1)
            indexed = manifest_index.get_run(
                graph_id="async_rl_demo",
                run_id=persisted.runtime.run_id,
            )
            self.assertEqual(indexed.manifest.project_id, "pangramanizer-training")
