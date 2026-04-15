from __future__ import annotations

import io
import json
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.examples.verification_failure import (
    build_program as build_failure_program,
)
from mentalmodel.observability.dashboard_metrics import metric_rows_from_live_records
from mentalmodel.observability.export import write_json, write_jsonl
from mentalmodel.remote.backend import (
    InMemoryArtifactStore,
    InMemoryEventIndex,
    InMemoryManifestIndex,
    InMemoryPersistedRunIndex,
    InMemoryProjectIndex,
    RemoteEventStore,
    RemoteProjectStore,
    RemoteRunStore,
)
from mentalmodel.remote.contracts import (
    ProjectCatalog,
    ProjectCatalogSnapshot,
    ProjectRegistration,
    RemoteProjectCatalogPublishRequest,
    RemoteProjectLinkRequest,
)
from mentalmodel.remote.events import (
    RemoteDeliveryHealthSummary,
    RemoteOperationKind,
    RemoteOperationStatus,
)
from mentalmodel.remote.sync import build_run_bundle_upload
from mentalmodel.remote.telemetry_store import (
    InMemoryTelemetryStore,
    TelemetryRecordRow,
    TelemetryRunRecord,
    TelemetrySpanRow,
)
from mentalmodel.runtime.runs import RUN_SCHEMA_VERSION
from mentalmodel.testing import run_verification
from mentalmodel.ui.api import create_dashboard_app
from mentalmodel.ui.catalog import DashboardCatalogEntry, default_dashboard_catalog
from mentalmodel.ui.custom_views import (
    DashboardCustomView,
    DashboardTableColumn,
    DashboardTableRowSource,
    DashboardValueSelector,
)
from mentalmodel.ui.execution_worker import WORKER_EVENT_PREFIX
from mentalmodel.ui.service import DashboardService


class DashboardApiTest(unittest.TestCase):
    def _materialize_custom_view_run(
        self,
        root: Path,
    ) -> tuple[str, str]:
        graph_id = "custom_view_graph"
        run_id = "run-custom-view"
        run_dir = root / ".runs" / graph_id / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        write_json(
            run_dir / "summary.json",
            {
                "schema_version": RUN_SCHEMA_VERSION,
                "graph_id": graph_id,
                "run_id": run_id,
                "created_at_ms": 1000,
                "success": True,
                "node_count": 2,
                "edge_count": 0,
                "record_count": 1,
                "output_count": 2,
                "state_count": 0,
                "trace_sink_configured": False,
                "trace_mode": "disk",
                "trace_mirror_to_disk": True,
                "trace_capture_local_spans": True,
                "trace_service_name": "mentalmodel",
                "runtime_default_profile_name": None,
                "runtime_profile_names": [],
            },
        )
        write_jsonl(run_dir / "records.jsonl", [])
        write_json(
            run_dir / "outputs.json",
            {
                "outputs": {
                    "sample_rows": {
                        "rows": [
                            {
                                "prompt_text": "Prompt A",
                                "completion_text": "Sample A",
                                "reward": {"pangram": 0.9, "total": 1.1},
                            },
                            {
                                "prompt_text": "Prompt B",
                                "completion_text": "Sample B",
                                "reward": {"pangram": 0.7, "total": 0.8},
                            },
                        ]
                    },
                    "summary_node": {
                        "run_label": "synthetic",
                    },
                },
                "framed_outputs": [],
            },
        )
        write_json(run_dir / "state.json", {"state": {}, "framed_state": []})
        return graph_id, run_id

    def _telemetry_record_row(
        self,
        *,
        graph_id: str,
        run_id: str,
        record_id: str,
        sequence: int,
        timestamp_ms: int,
        event_type: str,
        node_id: str,
        frame_id: str,
        frame_path: tuple[str, ...] = ("root",),
        payload: JsonValue,
        loop_node_id: str | None = None,
        iteration_index: int | None = None,
        invocation_name: str | None = None,
        runtime_profile_name: str | None = None,
    ) -> TelemetryRecordRow:
        return TelemetryRecordRow(
            graph_id=graph_id,
            run_id=run_id,
            record_id=record_id,
            sequence=sequence,
            timestamp_ms=timestamp_ms,
            event_type=event_type,
            node_id=node_id,
            frame_id=frame_id,
            frame_path=list(frame_path),
            payload=payload,
            loop_node_id=loop_node_id,
            iteration_index=iteration_index,
            invocation_name=invocation_name,
            runtime_profile_name=runtime_profile_name,
        )

    def _telemetry_span_row(
        self,
        *,
        graph_id: str,
        run_id: str,
        span_key: str,
        sequence: int,
        name: str,
        start_time_ns: int,
        end_time_ns: int,
        frame_id: str,
        attributes: dict[str, JsonValue],
        trace_id: str | None = None,
        otel_span_id: str | None = None,
        parent_span_id: str | None = None,
        loop_node_id: str | None = None,
        iteration_index: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> TelemetrySpanRow:
        return TelemetrySpanRow(
            graph_id=graph_id,
            run_id=run_id,
            span_key=span_key,
            sequence=sequence,
            name=name,
            start_time_ns=start_time_ns,
            end_time_ns=end_time_ns,
            frame_id=frame_id,
            attributes=attributes,
            trace_id=trace_id,
            otel_span_id=otel_span_id,
            parent_span_id=parent_span_id,
            loop_node_id=loop_node_id,
            iteration_index=iteration_index,
            error_type=error_type,
            error_message=error_message,
        )

    def _seed_remote_telemetry_run(
        self,
        store: InMemoryTelemetryStore,
        *,
        graph_id: str,
        run_id: str,
        created_at_ms: int,
        updated_at_ms: int | None = None,
        status: str = "running",
        success: bool | None = None,
        invocation_name: str | None = None,
        project_id: str | None = None,
        project_label: str | None = None,
        environment_name: str | None = None,
        catalog_entry_id: str | None = None,
        runtime_default_profile_name: str | None = None,
        runtime_profile_names: tuple[str, ...] = (),
        graph: dict[str, JsonValue] | None = None,
        analysis: dict[str, JsonValue] | None = None,
        error_message: str | None = None,
        records: tuple[TelemetryRecordRow, ...] = (),
        spans: tuple[TelemetrySpanRow, ...] = (),
    ) -> None:
        effective_updated_at_ms = (
            updated_at_ms
            if updated_at_ms is not None
            else (
                max((row.timestamp_ms for row in records), default=created_at_ms)
            )
        )
        effective_graph: dict[str, JsonValue] = (
            graph
            if graph is not None
            else {
                "graph_id": graph_id,
                "metadata": {},
                "nodes": [],
                "edges": [],
            }
        )
        effective_analysis: dict[str, JsonValue] = (
            analysis
            if analysis is not None
            else {"error_count": 0, "warning_count": 0, "findings": []}
        )
        store.replace_run(
            TelemetryRunRecord(
                graph_id=graph_id,
                run_id=run_id,
                created_at_ms=created_at_ms,
                updated_at_ms=effective_updated_at_ms,
                status=status,
                success=success,
                invocation_name=invocation_name,
                project_id=project_id,
                project_label=project_label,
                environment_name=environment_name,
                catalog_entry_id=catalog_entry_id,
                runtime_default_profile_name=runtime_default_profile_name,
                runtime_profile_names=runtime_profile_names,
                graph=effective_graph,
                analysis=effective_analysis,
                error_message=error_message,
            )
        )
        store.replace_records(graph_id=graph_id, run_id=run_id, rows=records)
        store.replace_spans(graph_id=graph_id, run_id=run_id, rows=spans)
        store.replace_metric_rows(
            graph_id=graph_id,
            run_id=run_id,
            rows=metric_rows_from_live_records(
                cast(
                    tuple[dict[str, object], ...],
                    tuple(row.as_dict(include_payload=True) for row in records),
                )
            ),
        )

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

    def test_remote_project_link_endpoint_persists_project_and_lists_it(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                remote_project_store=project_store,
                remote_api_key="test-key",
            )
        )
        entry = default_dashboard_catalog()[0]
        response = client.post(
            "/api/remote/projects/link",
            headers={"Authorization": "Bearer test-key"},
            json=RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                description="Training workflows",
                default_environment="prod",
                catalog_provider="pangramanizer.dashboard:catalog",
                default_runs_dir=".runs",
                default_verify_spec="verification/real_smoke.toml",
                catalog_snapshot=ProjectCatalogSnapshot(
                    project_id="pangramanizer",
                    provider="pangramanizer.dashboard:catalog",
                    published_at_ms=1000,
                    entries=(entry.as_dict(),),
                    default_entry_id=entry.spec_id,
                ),
            ).as_dict(),
        )
        self.assertEqual(response.status_code, 200)
        project = response.json()["project"]
        self.assertEqual(project["project_id"], "pangramanizer")
        self.assertTrue(project["catalog_published"])
        projects_response = client.get("/api/projects")
        self.assertEqual(projects_response.status_code, 200)
        projects = projects_response.json()["projects"]
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]["project_id"], "pangramanizer")
        self.assertEqual(projects[0]["source"], "remote")

    def test_remote_project_endpoints_require_auth_when_configured(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                remote_project_store=project_store,
                remote_api_key="test-key",
            )
        )
        response = client.get("/api/remote/projects")
        self.assertEqual(response.status_code, 401)

    def test_remote_catalog_entries_render_without_local_repo_imports(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        linked_entry = default_dashboard_catalog()[0]
        remote_entry_payload = dict(linked_entry.as_dict())
        remote_entry_payload["spec_id"] = "pangramanizer-smoke"
        remote_entry_payload["label"] = "Pangramanizer Smoke"
        remote_entry_payload["graph_id"] = "pangramanizer_training"
        remote_entry_payload["invocation_name"] = "pangram_real_smoke"
        remote_entry_payload["spec_path"] = "/srv/repos/pangramanizer/verification/real_smoke.toml"
        project_store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                description="Training workflows",
                default_environment="prod",
                catalog_provider="pangramanizer.dashboard:catalog",
                catalog_snapshot=ProjectCatalogSnapshot(
                    project_id="pangramanizer",
                    provider="pangramanizer.dashboard:catalog",
                    published_at_ms=1000,
                    entries=(remote_entry_payload,),
                    default_entry_id="pangramanizer-smoke",
                ),
            )
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                remote_project_store=project_store,
            )
        )

        catalog_response = client.get("/api/catalog")
        self.assertEqual(catalog_response.status_code, 200)
        entries = catalog_response.json()["entries"]
        remote_entry = next(entry for entry in entries if entry["spec_id"] == "pangramanizer-smoke")
        self.assertEqual(remote_entry["project_id"], "pangramanizer")
        self.assertFalse(remote_entry["launch_enabled"])
        self.assertTrue(remote_entry["metric_groups"])
        self.assertTrue(remote_entry["pinned_nodes"])

        graph_response = client.get("/api/catalog/pangramanizer-smoke/graph")
        self.assertEqual(graph_response.status_code, 200)
        graph_payload = graph_response.json()
        self.assertEqual(graph_payload["graph"]["graph_id"], "pangramanizer_training")
        self.assertEqual(graph_payload["graph"]["nodes"], [])

        launch_response = client.post(
            "/api/executions",
            json={"spec_id": "pangramanizer-smoke"},
        )
        self.assertEqual(launch_response.status_code, 400)

    def test_remote_backed_catalog_does_not_include_local_demo_entries(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        linked_entry = default_dashboard_catalog()[0]
        remote_entry_payload = dict(linked_entry.as_dict())
        remote_entry_payload["spec_id"] = "pangramanizer-smoke"
        remote_entry_payload["label"] = "Pangramanizer Smoke"
        remote_entry_payload["graph_id"] = "pangramanizer_training"
        remote_entry_payload["invocation_name"] = "pangram_real_smoke"
        remote_entry_payload["spec_path"] = "/srv/repos/pangramanizer/verification/real_smoke.toml"
        project_store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                description="Training workflows",
                default_environment="prod",
                catalog_provider="pangramanizer.dashboard:catalog",
                catalog_snapshot=ProjectCatalogSnapshot(
                    project_id="pangramanizer",
                    provider="pangramanizer.dashboard:catalog",
                    published_at_ms=1000,
                    entries=(remote_entry_payload,),
                    default_entry_id="pangramanizer-smoke",
                ),
            )
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                remote_project_store=project_store,
            )
        )

        catalog_response = client.get("/api/catalog")
        self.assertEqual(catalog_response.status_code, 200)
        spec_ids = {entry["spec_id"] for entry in catalog_response.json()["entries"]}
        self.assertEqual(spec_ids, {"pangramanizer-smoke"})

    def test_remote_catalog_entries_override_local_duplicates(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        local_entry = default_dashboard_catalog()[0]
        remote_entry_payload = dict(local_entry.as_dict())
        remote_entry_payload["label"] = "Remote Override"
        remote_entry_payload["launch_enabled"] = False
        project_store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                catalog_provider="pangramanizer.dashboard:catalog",
                catalog_snapshot=ProjectCatalogSnapshot(
                    project_id="pangramanizer",
                    provider="pangramanizer.dashboard:catalog",
                    published_at_ms=1000,
                    entries=(remote_entry_payload,),
                    default_entry_id=local_entry.spec_id,
                ),
            )
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                catalog_entries=(local_entry,),
                remote_project_store=project_store,
            )
        )

        response = client.get("/api/catalog")
        self.assertEqual(response.status_code, 200)
        entries = response.json()["entries"]
        matching = [entry for entry in entries if entry["spec_id"] == local_entry.spec_id]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0]["label"], "Remote Override")
        self.assertFalse(matching[0]["launch_enabled"])

    def test_remote_catalog_publish_endpoint_updates_existing_snapshot(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                remote_project_store=project_store,
                remote_api_key="test-key",
            )
        )
        project_store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                catalog_provider="pangramanizer.dashboard:catalog",
            )
        )
        entry = default_dashboard_catalog()[1]
        response = client.post(
            "/api/remote/projects/pangramanizer/catalog",
            headers={"Authorization": "Bearer test-key"},
            json=RemoteProjectCatalogPublishRequest(
                project_id="pangramanizer",
                catalog_provider="pangramanizer.dashboard:catalog",
                catalog_snapshot=ProjectCatalogSnapshot(
                    project_id="pangramanizer",
                    provider="pangramanizer.dashboard:catalog",
                    published_at_ms=2000,
                    entries=(entry.as_dict(),),
                    default_entry_id=entry.spec_id,
                    version=2,
                ),
            ).as_dict(),
        )
        self.assertEqual(response.status_code, 200)
        project = response.json()["project"]
        self.assertEqual(project["catalog_version"], 2)
        self.assertEqual(project["catalog_entry_count"], 1)

    def test_remote_event_endpoint_and_run_overview_include_delivery_health(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        telemetry_store = InMemoryTelemetryStore()
        event_store = RemoteEventStore(event_index=InMemoryEventIndex())
        run_store = RemoteRunStore(
            artifact_store=InMemoryArtifactStore(),
            manifest_index=InMemoryManifestIndex(),
            telemetry_store=telemetry_store,
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                remote_project_store=project_store,
                remote_telemetry_store=telemetry_store,
                remote_run_store=run_store,
                remote_event_store=event_store,
                remote_api_key="test-key",
            )
        )
        project_store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                catalog_provider="pangramanizer.dashboard:catalog",
            )
        )
        report = run_verification(build_program(), persist_run_artifacts=True)
        self.assertIsNotNone(report.run_artifacts)
        assert report.run_artifacts is not None
        upload = build_run_bundle_upload(
            runs_dir=report.run_artifacts.run_dir.parent.parent,
            graph_id=report.analysis.graph.graph_id,
            run_id=report.runtime.run_id,
            project_id="pangramanizer",
            project_label="Pangramanizer",
        )
        ingest_response = client.post(
            "/api/remote/runs",
            headers={"Authorization": "Bearer test-key"},
            json=upload.as_dict(),
        )
        self.assertEqual(ingest_response.status_code, 200)
        overview = client.get(
            f"/api/runs/{report.analysis.graph.graph_id}/{report.runtime.run_id}/overview"
        )
        self.assertEqual(overview.status_code, 200)
        delivery = overview.json()["remote_delivery"]
        self.assertIn(delivery["last_kind"], {"run.upload", "live.commit"})
        self.assertEqual(delivery["last_status"], "succeeded")
        events = client.get(
            "/api/remote/events",
            headers={"Authorization": "Bearer test-key"},
            params={
                "graph_id": report.analysis.graph.graph_id,
                "run_id": report.runtime.run_id,
            },
        )
        self.assertEqual(events.status_code, 200)
        payload = events.json()["events"]
        self.assertGreaterEqual(len(payload), 1)
        self.assertEqual(payload[0]["kind"], "run.upload")

    def test_remote_live_overview_uses_shallow_session_and_invariant_index(self) -> None:
        telemetry_store = InMemoryTelemetryStore()
        self._seed_remote_telemetry_run(
            telemetry_store,
            graph_id="graph",
            run_id="run-1",
            created_at_ms=1000,
            invocation_name="live",
            records=(
                self._telemetry_record_row(
                    graph_id="graph",
                    run_id="run-1",
                    record_id="run-1:1",
                    sequence=1,
                    timestamp_ms=1100,
                    event_type="node.succeeded",
                    node_id="metrics",
                    frame_id="training_loop[0]",
                    frame_path=("root", "training_loop[0]"),
                    loop_node_id="training_loop",
                    iteration_index=0,
                    payload={"output": {"train.prompt_count": 4}},
                ),
                self._telemetry_record_row(
                    graph_id="graph",
                    run_id="run-1",
                    record_id="run-1:2",
                    sequence=2,
                    timestamp_ms=1101,
                    event_type="invariant.checked",
                    node_id="check",
                    frame_id="training_loop[0]",
                    frame_path=("root", "training_loop[0]"),
                    loop_node_id="training_loop",
                    iteration_index=0,
                    payload={"status": "failed", "severity": "warning"},
                ),
            ),
        )
        service = DashboardService(
            runs_dir=None,
            catalog_entries=(
                DashboardCatalogEntry.from_dict(
                    {
                        "spec_id": "live-spec",
                        "label": "Live",
                        "description": "Live spec",
                        "spec_path": "/tmp/live.toml",
                        "graph_id": "graph",
                        "invocation_name": "live",
                        "metric_groups": [
                            {
                                "group_id": "train",
                                "title": "Training",
                                "metric_path_prefixes": ["train."],
                            }
                        ],
                    },
                    launch_enabled=False,
                ),
            ),
            remote_telemetry_store=telemetry_store,
        )
        with (
            patch.object(
                telemetry_store,
                "list_invariants",
                wraps=telemetry_store.list_invariants,
            ) as invariants_spy,
            patch.object(
                telemetry_store,
                "get_run",
                wraps=telemetry_store.get_run,
            ) as get_run_spy,
        ):
            overview = service.get_run_overview(graph_id="graph", run_id="run-1")
        overview_summary = cast(dict[str, object], overview["summary"])
        overview_invariants = cast(list[object], overview["invariants"])
        self.assertEqual(overview_summary["status"], "running")
        self.assertEqual(len(overview_invariants), 1)
        self.assertEqual(invariants_spy.call_count, 1)
        self.assertEqual(get_run_spy.call_args.kwargs["graph_id"], "graph")
        self.assertEqual(get_run_spy.call_args.kwargs["run_id"], "run-1")

    def test_run_overview_uses_short_lived_cache(self) -> None:
        telemetry_store = InMemoryTelemetryStore()
        self._seed_remote_telemetry_run(
            telemetry_store,
            graph_id="graph",
            run_id="run-1",
            created_at_ms=1000,
            invocation_name="live",
        )
        service = DashboardService(
            runs_dir=None,
            catalog_entries=(
                DashboardCatalogEntry.from_dict(
                    {
                        "spec_id": "live-spec",
                        "label": "Live",
                        "description": "Live spec",
                        "spec_path": "/tmp/live.toml",
                        "graph_id": "graph",
                        "invocation_name": "live",
                    },
                    launch_enabled=False,
                ),
            ),
            remote_telemetry_store=telemetry_store,
        )
        service.remote_event_store = Mock()
        service.remote_event_store.summarize_run.return_value = RemoteDeliveryHealthSummary(
            last_event_at_ms=1200,
            last_status=RemoteOperationStatus.SUCCEEDED,
            last_kind=RemoteOperationKind.RUN_UPLOAD,
            last_error_message=None,
            recent_success_count=1,
            recent_failure_count=0,
        )
        with patch.object(
            telemetry_store,
            "list_invariants",
            wraps=telemetry_store.list_invariants,
        ) as invariants_spy:
            first = service.get_run_overview(graph_id="graph", run_id="run-1")
            second = service.get_run_overview(graph_id="graph", run_id="run-1")
        self.assertEqual(first, second)
        self.assertEqual(invariants_spy.call_count, 1)
        service.remote_event_store.summarize_run.assert_called_once()

    def test_load_catalog_graph_uses_short_lived_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            spec_path = Path(tmpdir) / "cached.toml"
            spec_path.write_text(
                "[program]\nentrypoint = \"demo:build_program\"\n",
                encoding="utf-8",
            )
            service = DashboardService(
                catalog_entries=(
                    DashboardCatalogEntry.from_dict(
                        {
                            "spec_id": "cached-spec",
                            "label": "Cached",
                            "description": "Cached graph",
                            "spec_path": str(spec_path),
                            "graph_id": "cached_graph",
                            "invocation_name": "cached_graph",
                        },
                    ),
                ),
            )
            with patch(
                "mentalmodel.ui.service.read_verify_invocation_spec",
                return_value=Mock(program="demo:build_program"),
            ), patch(
                "mentalmodel.ui.service.load_workflow_subject",
                return_value=(None, object()),
            ), patch(
                "mentalmodel.ui.service.lower_program",
                return_value=Mock(graph_id="cached_graph", nodes=(), edges=(), metadata={}),
            ) as lower_spy, patch(
                "mentalmodel.ui.service.run_analysis",
                return_value=Mock(findings=(), error_count=0, warning_count=0),
            ):
                first = service.load_catalog_graph("cached-spec")
                second = service.load_catalog_graph("cached-spec")
        self.assertEqual(first, second)
        self.assertEqual(lower_spy.call_count, 1)

    def test_remote_live_metric_groups_use_live_metric_index(self) -> None:
        telemetry_store = InMemoryTelemetryStore()
        self._seed_remote_telemetry_run(
            telemetry_store,
            graph_id="graph",
            run_id="run-1",
            created_at_ms=1000,
            invocation_name="live",
            records=(
                self._telemetry_record_row(
                    graph_id="graph",
                    run_id="run-1",
                    record_id="run-1:1",
                    sequence=1,
                    timestamp_ms=1100,
                    event_type="node.succeeded",
                    node_id="metrics",
                    frame_id="training_loop[0]",
                    frame_path=("root", "training_loop[0]"),
                    loop_node_id="training_loop",
                    iteration_index=0,
                    payload={"output": {"train.prompt_count": 4}},
                ),
            ),
        )
        service = DashboardService(
            runs_dir=None,
            catalog_entries=(
                DashboardCatalogEntry.from_dict(
                    {
                        "spec_id": "live-spec",
                        "label": "Live",
                        "description": "Live spec",
                        "spec_path": "/tmp/live.toml",
                        "graph_id": "graph",
                        "invocation_name": "live",
                        "metric_groups": [
                            {
                                "group_id": "train",
                                "title": "Training",
                                "metric_path_prefixes": ["train."],
                            }
                        ],
                    },
                    launch_enabled=False,
                ),
            ),
            remote_telemetry_store=telemetry_store,
        )
        with (
            patch.object(
                telemetry_store,
                "list_metrics",
                wraps=telemetry_store.list_metrics,
            ) as metrics_spy,
        ):
            payload = service.get_run_metric_groups(
                spec_id="live-spec",
                run_id="run-1",
                step_start=None,
                step_end=None,
                max_points=120,
            )
        metric_groups = cast(list[object], payload["groups"])
        self.assertEqual(len(metric_groups), 1)
        self.assertEqual(metrics_spy.call_count, 1)

    def test_run_records_default_to_compact_rows(self) -> None:
        telemetry_store = InMemoryTelemetryStore()
        self._seed_remote_telemetry_run(
            telemetry_store,
            graph_id="graph",
            run_id="run-1",
            created_at_ms=1000,
            records=(
                self._telemetry_record_row(
                    graph_id="graph",
                    run_id="run-1",
                    record_id="run-1:1",
                    sequence=1,
                    timestamp_ms=1100,
                    event_type="node.succeeded",
                    node_id="metrics",
                    frame_id="root",
                    payload={"output": {"value": 1}},
                ),
            ),
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                remote_telemetry_store=telemetry_store,
            )
        )
        compact = client.get("/api/runs/graph/run-1/records", params={"limit": 10})
        self.assertEqual(compact.status_code, 200)
        compact_item = compact.json()["items"][0]
        self.assertNotIn("payload", compact_item)

        full = client.get(
            "/api/runs/graph/run-1/records",
            params={"limit": 10, "include_payload": "true"},
        )
        self.assertEqual(full.status_code, 200)
        full_item = full.json()["items"][0]
        self.assertIn("payload", full_item)

    def test_run_overview_exposes_runtime_failure_context_for_failed_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runs_dir = Path(tmpdir)
            report = run_verification(
                build_failure_program(),
                runs_dir=runs_dir,
                persist_run_artifacts=True,
            )
            self.assertFalse(report.success)
            self.assertIsNotNone(report.runtime.run_id)
            assert report.runtime.run_id is not None

            client = TestClient(
                create_dashboard_app(runs_dir=runs_dir, frontend_dist=None)
            )
            overview = client.get(
                f"/api/runs/verification_failure/{report.runtime.run_id}/overview"
            )
            self.assertEqual(overview.status_code, 200)
            payload = overview.json()
            self.assertEqual(payload["summary"]["status"], "failed")
            self.assertFalse(payload["verification_success"])
            self.assertIsInstance(payload["runtime_error"], str)
            self.assertTrue(payload["runtime_error"])

    def test_run_overview_does_not_build_replay_for_persisted_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runs_dir = Path(tmpdir)
            report = run_verification(
                build_failure_program(),
                runs_dir=runs_dir,
                persist_run_artifacts=True,
            )
            assert report.runtime.run_id is not None

            client = TestClient(
                create_dashboard_app(runs_dir=runs_dir, frontend_dist=None)
            )
            with patch(
                "mentalmodel.ui.service.build_replay_report",
                side_effect=AssertionError("overview should not build replay"),
            ):
                overview = client.get(
                    f"/api/runs/verification_failure/{report.runtime.run_id}/overview"
                )
            self.assertEqual(overview.status_code, 200)
            payload = overview.json()
            self.assertEqual(payload["summary"]["status"], "failed")
            self.assertIsInstance(payload["invariants"], list)

    def test_dashboard_service_caches_remote_project_catalog_reads(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        linked_entry = default_dashboard_catalog()[0]
        project_store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                catalog_provider="pangramanizer.dashboard:catalog",
                catalog_snapshot=ProjectCatalogSnapshot(
                    project_id="pangramanizer",
                    provider="pangramanizer.dashboard:catalog",
                    published_at_ms=1000,
                    entries=(linked_entry.as_dict(),),
                    default_entry_id=linked_entry.spec_id,
                ),
            )
        )
        service = DashboardService(
            runs_dir=None,
            remote_project_store=project_store,
        )
        with patch.object(project_store, "list_projects", wraps=project_store.list_projects) as spy:
            first = service.list_catalog()
            second = service.list_catalog()
        self.assertEqual(first[0].spec_id, linked_entry.spec_id)
        self.assertEqual(second[0].spec_id, linked_entry.spec_id)
        self.assertEqual(spy.call_count, 1)

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
            self.assertEqual(overview["metrics"], [])

            metrics_response = client.get(
                f"/api/catalog/{fixture_entry['spec_id']}/runs/{run_id}/metrics"
            )
            self.assertEqual(metrics_response.status_code, 200)
            metric_groups = metrics_response.json()["groups"]
            self.assertEqual(len(metric_groups), 1)
            series_labels = {series["label"] for series in metric_groups[0]["series"]}
            self.assertIn("auto_publish", series_labels)
            self.assertIn("escalations", series_labels)

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
            records = records_response.json()["items"]
            self.assertGreater(len(records), 0)

    def test_dashboard_custom_view_endpoint_evaluates_table_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runs_dir = Path(tmpdir)
            graph_id, run_id = self._materialize_custom_view_run(runs_dir)
            fixture_entry = default_dashboard_catalog()[0]
            custom_entry = DashboardCatalogEntry(
                spec_id="custom-view-entry",
                label="Custom View Entry",
                description="Synthetic custom table view entry.",
                spec_path=fixture_entry.spec_path,
                graph_id=graph_id,
                invocation_name="custom_view_invocation",
                category="integration",
                custom_views=(
                    DashboardCustomView(
                        view_id="sample-quality",
                        title="Sample Quality",
                        description="Prompt and reward table.",
                        kind="table",
                        row_source=DashboardTableRowSource(
                            kind="node_output_items",
                            node_id="sample_rows",
                            items_path="rows",
                        ),
                        columns=(
                            DashboardTableColumn(
                                column_id="prompt_text",
                                title="Prompt",
                                selector=DashboardValueSelector(
                                    kind="row_item",
                                    path="prompt_text",
                                ),
                            ),
                            DashboardTableColumn(
                                column_id="pangram_score",
                                title="Pangram",
                                selector=DashboardValueSelector(
                                    kind="row_item",
                                    path="reward.pangram",
                                ),
                            ),
                            DashboardTableColumn(
                                column_id="run_label",
                                title="Run Label",
                                selector=DashboardValueSelector(
                                    kind="node_output",
                                    node_id="summary_node",
                                    path="run_label",
                                ),
                            ),
                        ),
                    ),
                ),
            )
            client = TestClient(
                create_dashboard_app(
                    runs_dir=runs_dir,
                    frontend_dist=None,
                    catalog_entries=(custom_entry,),
                )
            )

            response = client.get(
                f"/api/catalog/{custom_entry.spec_id}/runs/{run_id}/views/sample-quality"
            )
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["view"]["view_id"], "sample-quality")
            self.assertEqual(payload["row_count"], 2)
            self.assertEqual(payload["rows"][0]["values"]["prompt_text"], "Prompt A")
            self.assertEqual(payload["rows"][0]["values"]["pangram_score"], 0.9)
            self.assertEqual(payload["rows"][1]["values"]["run_label"], "synthetic")
            self.assertEqual(payload["warnings"], [])

    def test_dashboard_custom_view_endpoint_evaluates_live_rows(self) -> None:
        custom_entry = DashboardCatalogEntry(
            spec_id="custom-live-view-entry",
            label="Custom Live View Entry",
            description="Synthetic live custom table view entry.",
            spec_path=Path("/tmp/custom-live.toml"),
            graph_id="custom_live_graph",
            invocation_name="custom_live_invocation",
            category="integration",
            launch_enabled=False,
            custom_views=(
                DashboardCustomView(
                    view_id="sample-quality",
                    title="Sample Quality",
                    description="Prompt and reward table.",
                    kind="table",
                    row_source=DashboardTableRowSource(
                        kind="node_output_items",
                        node_id="sample_rows",
                        items_path="rows",
                    ),
                    columns=(
                        DashboardTableColumn(
                            column_id="prompt_text",
                            title="Prompt",
                            selector=DashboardValueSelector(
                                kind="row_item",
                                path="prompt_text",
                            ),
                        ),
                        DashboardTableColumn(
                            column_id="pangram_score",
                            title="Pangram",
                            selector=DashboardValueSelector(
                                kind="row_item",
                                path="reward.pangram",
                            ),
                        ),
                        DashboardTableColumn(
                            column_id="run_label",
                            title="Run Label",
                            selector=DashboardValueSelector(
                                kind="node_output",
                                node_id="summary_node",
                                path="run_label",
                            ),
                        ),
                    ),
                ),
            ),
        )
        telemetry_store = InMemoryTelemetryStore()
        self._seed_remote_telemetry_run(
            telemetry_store,
            graph_id="custom_live_graph",
            run_id="run-live-custom-view",
            created_at_ms=1000,
            invocation_name="custom_live_invocation",
            project_id="custom-project",
            records=(
                self._telemetry_record_row(
                    graph_id="custom_live_graph",
                    run_id="run-live-custom-view",
                    record_id="live:sample_rows:succeeded",
                    sequence=1,
                    timestamp_ms=1100,
                    event_type="node.succeeded",
                    node_id="sample_rows",
                    frame_id="root",
                    payload={
                        "output": {
                            "rows": [
                                {
                                    "prompt_text": "Prompt A",
                                    "reward": {"pangram": 0.9},
                                },
                                {
                                    "prompt_text": "Prompt B",
                                    "reward": {"pangram": 0.7},
                                },
                            ]
                        }
                    },
                ),
                self._telemetry_record_row(
                    graph_id="custom_live_graph",
                    run_id="run-live-custom-view",
                    record_id="live:summary_node:succeeded",
                    sequence=2,
                    timestamp_ms=1101,
                    event_type="node.succeeded",
                    node_id="summary_node",
                    frame_id="root",
                    payload={"output": {"run_label": "live-synthetic"}},
                ),
            ),
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                catalog_entries=(custom_entry,),
                remote_telemetry_store=telemetry_store,
            )
        )

        response = client.get(
            "/api/catalog/custom-live-view-entry/runs/run-live-custom-view/views/sample-quality"
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["view"]["view_id"], "sample-quality")
        self.assertEqual(payload["row_count"], 2)
        self.assertEqual(payload["rows"][0]["values"]["prompt_text"], "Prompt A")
        self.assertEqual(payload["rows"][0]["values"]["pangram_score"], 0.9)
        self.assertEqual(payload["rows"][1]["values"]["run_label"], "live-synthetic")
        self.assertEqual(payload["warnings"], [])

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
            self.assertIsNotNone(report.runtime.run_id)
            run_id = report.runtime.run_id
            assert run_id is not None
            upload = build_run_bundle_upload(
                runs_dir=local_root,
                graph_id="async_rl_demo",
                run_id=run_id,
                project_id="mentalmodel-examples",
                project_label="Mentalmodel Examples",
            )
            remote_store = RemoteRunStore(
                manifest_index=InMemoryManifestIndex(),
                artifact_store=InMemoryArtifactStore(),
                persisted_run_index=InMemoryPersistedRunIndex(),
                cache_dir=remote_root,
            )
            project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
            project_store.link_project(
                RemoteProjectLinkRequest(
                    project_id="mentalmodel-examples",
                    label="Mentalmodel Examples",
                    catalog_provider="mentalmodel.ui.catalog:default_dashboard_catalog",
                )
            )
            client = TestClient(
                create_dashboard_app(
                    runs_dir=None,
                    frontend_dist=None,
                    remote_run_store=remote_store,
                    remote_project_store=project_store,
                )
            )
            ingest = client.post("/api/remote/runs", json=upload.as_dict())
            self.assertEqual(ingest.status_code, 200)
            receipt = ingest.json()
            self.assertEqual(receipt["project_id"], "mentalmodel-examples")
            self.assertIsInstance(receipt["uploaded_at_ms"], int)
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
            project = project_store.get_project(project_id="mentalmodel-examples")
            self.assertEqual(project.last_completed_run_id, report.runtime.run_id)

    def test_remote_ingest_rejects_unknown_project_id(self) -> None:
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
                project_id="missing-project",
                project_label="Missing Project",
            )
            remote_store = RemoteRunStore(
                manifest_index=InMemoryManifestIndex(),
                artifact_store=InMemoryArtifactStore(),
                cache_dir=remote_root,
            )
            project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
            client = TestClient(
                create_dashboard_app(
                    runs_dir=None,
                    frontend_dist=None,
                    remote_run_store=remote_store,
                    remote_project_store=project_store,
                )
            )
            ingest = client.post("/api/remote/runs", json=upload.as_dict())
            self.assertEqual(ingest.status_code, 400)

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
                "record_count": 2,
                "output_count": 1,
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
        live_inputs_record = {
            "run_id": "run-external-live",
            "timestamp_ms": 1234,
            "node_id": "rollout_join",
            "frame_id": "root",
            "event_type": "node.inputs_resolved",
            "payload": {
                "inputs": {
                    "prompt": "Write a pangram about space travel.",
                }
            },
        }
        live_output_record = {
            "run_id": "run-external-live",
            "timestamp_ms": 1235,
            "node_id": "rollout_join",
            "frame_id": "root",
            "event_type": "node.succeeded",
            "payload": {
                "output": {
                    "sample_text": "Sphinx of black quartz, judge my vow.",
                    "pangram_score": 1.0,
                }
            },
        }
        live_span = {
            "name": "join:rollout_join",
            "start_time_ns": 10,
            "end_time_ns": 20,
            "duration_ns": 10,
            "attributes": {
                "mentalmodel.node.id": "rollout_join",
            },
            "frame_id": "root",
            "loop_node_id": None,
            "iteration_index": None,
            "error_type": None,
            "error_message": None,
        }
        completion_gate = threading.Event()

        class ControlledPipe:
            def __init__(
                self,
                *,
                initial_lines: tuple[str, ...],
                completion_line: str | None = None,
                completion_gate: threading.Event | None = None,
            ) -> None:
                self._initial_lines = list(initial_lines)
                self._completion_line = completion_line
                self._completion_gate = completion_gate
                self._completion_emitted = completion_line is None

            def __iter__(self) -> ControlledPipe:
                return self

            def __next__(self) -> str:
                if self._initial_lines:
                    return self._initial_lines.pop(0)
                if self._completion_emitted:
                    raise StopIteration
                assert self._completion_gate is not None
                released = self._completion_gate.wait(timeout=1.0)
                if not released:
                    raise StopIteration
                self._completion_emitted = True
                assert self._completion_line is not None
                return self._completion_line

            def close(self) -> None:
                return

        class FakePopen:
            def __init__(self, args: list[str], **_: object) -> None:
                self.args = args
                self.stdout = ControlledPipe(
                    initial_lines=(
                        "Your Tinker SDK version is outdated.\n",
                        WORKER_EVENT_PREFIX
                        + json.dumps(
                            {
                                "kind": "record",
                                "payload": live_inputs_record,
                            }
                        )
                        + "\n",
                        WORKER_EVENT_PREFIX
                        + json.dumps(
                            {
                                "kind": "record",
                                "payload": live_output_record,
                            }
                        )
                        + "\n",
                        WORKER_EVENT_PREFIX
                        + json.dumps(
                            {
                                "kind": "span",
                                "payload": live_span,
                            }
                        )
                        + "\n",
                    ),
                    completion_line=WORKER_EVENT_PREFIX
                    + json.dumps(
                        {
                            "kind": "completion",
                            "payload": payload,
                        }
                    )
                    + "\n",
                    completion_gate=completion_gate,
                )
                self.stderr = io.StringIO("")

            def wait(self) -> int:
                return 0

        with patch(
            "mentalmodel.ui.execution_worker.subprocess.Popen",
            side_effect=FakePopen,
        ) as popen, patch("mentalmodel.ui.service.subprocess.run") as run_subprocess:
            run_subprocess.return_value = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=json.dumps(
                    {
                        "graph": {
                            "graph_id": "pangramanizer_training_real_verify3",
                            "metadata": {},
                            "nodes": [],
                            "edges": [],
                        },
                        "analysis": {
                            "error_count": 0,
                            "warning_count": 0,
                            "findings": [],
                        },
                    }
                ),
                stderr="",
            )
            launch = client.post("/api/executions", json={"spec_id": "pangram-real-verify3"})
            self.assertEqual(launch.status_code, 200)
            execution_id = launch.json()["execution_id"]
            for _ in range(40):
                execution = client.get(f"/api/executions/{execution_id}").json()
                if (
                    execution["status"] == "running"
                    and execution["run_id"] == "run-external-live"
                    and len(execution["records"]) == 2
                    and len(execution["spans"]) == 1
                ):
                    break
                time.sleep(0.01)
            self.assertEqual(execution["status"], "running")
            self.assertEqual(execution["run_id"], "run-external-live")
            self.assertEqual(len(execution["records"]), 2)
            self.assertEqual(len(execution["spans"]), 1)
            node_detail_response = client.get(
                "/api/runs/pangramanizer_training_real_verify3/run-external-live/nodes/rollout_join"
            )
            self.assertEqual(node_detail_response.status_code, 200)
            node_detail = node_detail_response.json()
            self.assertEqual(
                node_detail["inputs"],
                {"prompt": "Write a pangram about space travel."},
            )
            self.assertEqual(
                node_detail["output"],
                {
                    "sample_text": "Sphinx of black quartz, judge my vow.",
                    "pangram_score": 1.0,
                },
            )
            self.assertEqual(len(node_detail["trace"]["records"]), 2)
            self.assertEqual(len(node_detail["trace"]["spans"]), 1)
            spans_response = client.get(
                "/api/runs/pangramanizer_training_real_verify3/run-external-live/spans"
            )
            self.assertEqual(spans_response.status_code, 200)
            self.assertEqual(len(spans_response.json()["items"]), 1)
            completion_gate.set()
            for _ in range(40):
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
        self.assertEqual(execution["run_id"], "run-external-live")
        self.assertEqual(len(execution["records"]), 2)
        self.assertEqual(len(execution["spans"]), 1)
        self.assertEqual(execution["records"][0]["node_id"], "rollout_join")
        runs_response = client.get(
            "/api/runs",
            params={
                "graph_id": "pangramanizer_training_real_verify3",
                "invocation_name": "pangram_real_verify3",
            },
        )
        self.assertEqual(runs_response.status_code, 200)
        runs = runs_response.json()["runs"]
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["run_id"], "run-external-live")
        self.assertEqual(runs[0]["status"], "succeeded")
        self.assertEqual(runs[0]["source"], "active")
        self.assertTrue(runs[0]["availability"]["records"])
        self.assertTrue(runs[0]["availability"]["spans"])
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
                    self.stdout = io.StringIO(
                        "\n".join(
                            (
                                "external verification is starting",
                                WORKER_EVENT_PREFIX
                                + json.dumps(
                                    {
                                        "kind": "record",
                                        "payload": {
                                            "timestamp_ms": 999,
                                            "node_id": "rollout_join",
                                            "frame_id": "root",
                                            "event": "node_completed",
                                        },
                                    }
                                ),
                                WORKER_EVENT_PREFIX
                                + json.dumps(
                                    {
                                        "kind": "completion",
                                        "payload": verification_payload,
                                    }
                                ),
                            )
                        )
                        + "\n"
                    )
                    self.stderr = io.StringIO("")

                def wait(self) -> int:
                    return 0

            with (
                patch("mentalmodel.ui.service.subprocess.run") as run_subprocess,
                patch(
                    "mentalmodel.ui.execution_worker.subprocess.Popen",
                    side_effect=FakePopen,
                ),
            ):
                run_subprocess.side_effect = [
                    subprocess.CompletedProcess(
                        args=[],
                        returncode=0,
                        stdout=json.dumps(metadata),
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
            self.assertEqual(len(execution["records"]), 1)
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
            self.assertIsNotNone(persisted.runtime.run_id)
            assert persisted.runtime.run_id is not None
            indexed = manifest_index.get_run(
                graph_id="async_rl_demo",
                run_id=persisted.runtime.run_id,
            )
            self.assertEqual(indexed.manifest.project_id, "pangramanizer-training")

    def test_remote_telemetry_run_appears_in_run_queries(self) -> None:
        project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        telemetry_store = InMemoryTelemetryStore()
        entry = default_dashboard_catalog()[0]
        remote_entry_payload = dict(entry.as_dict())
        remote_entry_payload["spec_id"] = "pangramanizer-smoke"
        remote_entry_payload["label"] = "Pangramanizer Smoke"
        remote_entry_payload["graph_id"] = "pangramanizer_training"
        remote_entry_payload["invocation_name"] = "pangram_real_smoke"
        remote_entry_payload["spec_path"] = "/srv/repos/pangramanizer/verification/real_smoke.toml"
        project_store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                description="Training workflows",
                default_environment="prod",
                catalog_provider="pangramanizer.dashboard:catalog",
                catalog_snapshot=ProjectCatalogSnapshot(
                    project_id="pangramanizer",
                    provider="pangramanizer.dashboard:catalog",
                    published_at_ms=1000,
                    entries=(remote_entry_payload,),
                    default_entry_id="pangramanizer-smoke",
                ),
            )
        )
        client = TestClient(
            create_dashboard_app(
                runs_dir=None,
                frontend_dist=None,
                remote_project_store=project_store,
                remote_telemetry_store=telemetry_store,
            )
        )
        self._seed_remote_telemetry_run(
            telemetry_store,
            graph_id="pangramanizer_training",
            run_id="run-live-123",
            created_at_ms=1000,
            invocation_name="pangram_real_smoke",
            project_id="pangramanizer",
            environment_name="prod",
            catalog_entry_id="pangramanizer-smoke",
            runtime_default_profile_name="real",
            runtime_profile_names=("real",),
            graph={
                "graph_id": "pangramanizer_training",
                "metadata": {},
                "nodes": [{"node_id": "source", "kind": "actor", "label": "Source"}],
                "edges": [],
            },
            records=(
                self._telemetry_record_row(
                    graph_id="pangramanizer_training",
                    run_id="run-live-123",
                    record_id="run-live-123:1",
                    sequence=1,
                    timestamp_ms=1100,
                    event_type="node.succeeded",
                    node_id="source",
                    frame_id="root",
                    payload={
                        "output": {
                            "queue_summary": {
                                "auto_publish": 2,
                                "escalations": 1,
                            }
                        }
                    },
                ),
            ),
            spans=(
                self._telemetry_span_row(
                    graph_id="pangramanizer_training",
                    run_id="run-live-123",
                    span_key="span-1:root:100:actor:source",
                    sequence=1,
                    name="actor:source",
                    start_time_ns=100,
                    end_time_ns=200,
                    frame_id="root",
                    attributes={"mentalmodel.node_id": "source"},
                ),
            ),
        )

        runs_response = client.get(
            "/api/runs",
            params={
                "graph_id": "pangramanizer_training",
                "invocation_name": "pangram_real_smoke",
            },
        )
        self.assertEqual(runs_response.status_code, 200)
        runs = runs_response.json()["runs"]
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["run_id"], "run-live-123")
        self.assertEqual(runs[0]["source"], "remote")

        overview = client.get("/api/runs/pangramanizer_training/run-live-123/overview")
        self.assertEqual(overview.status_code, 200)
        self.assertEqual(overview.json()["summary"]["status"], "running")
        self.assertEqual(overview.json()["metrics"], [])

        metrics = client.get("/api/catalog/pangramanizer-smoke/runs/run-live-123/metrics")
        self.assertEqual(metrics.status_code, 200)
        self.assertTrue(metrics.json()["groups"])

        records = client.get("/api/runs/pangramanizer_training/run-live-123/records")
        self.assertEqual(records.status_code, 200)
        self.assertEqual(len(records.json()["items"]), 1)

    def test_completed_bundle_replaces_remote_live_row_in_query_model(self) -> None:
        with (
            tempfile.TemporaryDirectory() as local_tmp,
            tempfile.TemporaryDirectory() as remote_tmp,
        ):
            local_root = Path(local_tmp)
            remote_root = Path(remote_tmp)
            report = run_verification(build_program(), runs_dir=local_root)
            self.assertTrue(report.success)
            self.assertIsNotNone(report.runtime.run_id)
            run_id = report.runtime.run_id
            assert run_id is not None
            upload = build_run_bundle_upload(
                runs_dir=local_root,
                graph_id="async_rl_demo",
                run_id=run_id,
                project_id="mentalmodel-examples",
                project_label="Mentalmodel Examples",
            )
            remote_store = RemoteRunStore(
                manifest_index=InMemoryManifestIndex(),
                artifact_store=InMemoryArtifactStore(),
                cache_dir=remote_root,
                telemetry_store=InMemoryTelemetryStore(),
            )
            telemetry_store = cast(InMemoryTelemetryStore, remote_store._telemetry_store)
            project_store = RemoteProjectStore(project_index=InMemoryProjectIndex())
            project_store.link_project(
                RemoteProjectLinkRequest(
                    project_id="mentalmodel-examples",
                    label="Mentalmodel Examples",
                    catalog_provider="mentalmodel.ui.catalog:default_dashboard_catalog",
                )
            )
            client = TestClient(
                create_dashboard_app(
                    runs_dir=None,
                    frontend_dist=None,
                    remote_run_store=remote_store,
                    remote_telemetry_store=telemetry_store,
                    remote_project_store=project_store,
                    remote_api_key="test-key",
                )
            )
            self._seed_remote_telemetry_run(
                telemetry_store,
                graph_id="async_rl_demo",
                run_id=run_id,
                created_at_ms=1000,
                invocation_name="async_rl_demo",
                project_id="mentalmodel-examples",
                environment_name="prod",
                graph={
                    "graph_id": "async_rl_demo",
                    "metadata": {},
                    "nodes": [
                        {
                            "node_id": "prompt_sampler",
                            "kind": "actor",
                            "label": "Prompt Sampler",
                        }
                    ],
                    "edges": [],
                },
                records=(
                    self._telemetry_record_row(
                        graph_id="async_rl_demo",
                        run_id=run_id,
                        record_id=f"{run_id}:1",
                        sequence=1,
                        timestamp_ms=1100,
                        event_type="node.succeeded",
                        node_id="prompt_sampler",
                        frame_id="root",
                        payload={"output": {"prompt_count": 8}},
                    ),
                ),
            )

            before = client.get(
                "/api/runs",
                params={"graph_id": "async_rl_demo"},
            )
            self.assertEqual(before.status_code, 200)
            self.assertEqual(before.json()["runs"][0]["source"], "remote")
            self.assertEqual(before.json()["runs"][0]["status"], "running")

            ingest = client.post(
                "/api/remote/runs",
                headers={"Authorization": "Bearer test-key"},
                json=upload.as_dict(),
            )
            self.assertEqual(ingest.status_code, 200)

            after = client.get(
                "/api/runs",
                params={"graph_id": "async_rl_demo"},
            )
            self.assertEqual(after.status_code, 200)
            runs = after.json()["runs"]
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["run_id"], run_id)
            self.assertEqual(runs[0]["source"], "remote")
            self.assertEqual(runs[0]["status"], "succeeded")

            overview = client.get(f"/api/runs/async_rl_demo/{run_id}/overview")
            self.assertEqual(overview.status_code, 200)
            self.assertEqual(overview.json()["summary"]["source"], "remote")
            records = client.get(
                f"/api/runs/async_rl_demo/{run_id}/records",
                params={"limit": 2, "include_payload": "true"},
            )
            self.assertEqual(records.status_code, 200)
            self.assertGreater(records.json()["total_count"], 0)
            self.assertIn("payload", records.json()["items"][0])
            spans = client.get(
                f"/api/runs/async_rl_demo/{run_id}/spans",
                params={"limit": 2},
            )
            self.assertEqual(spans.status_code, 200)
            self.assertIn("total_count", spans.json())
            indexed_run = telemetry_store.get_run(graph_id="async_rl_demo", run_id=run_id)
            self.assertGreater(indexed_run.record_count, 1)
