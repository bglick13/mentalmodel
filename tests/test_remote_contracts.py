from __future__ import annotations

import unittest
from pathlib import Path

from mentalmodel.remote import (
    ArtifactDescriptor,
    ArtifactName,
    CatalogSource,
    ProjectCatalog,
    ProjectCatalogSnapshot,
    ProjectRegistration,
    RemoteContractError,
    RemoteProjectCatalogPublishRequest,
    RemoteProjectLinkRequest,
    RemoteProjectRecord,
    RemoteRunUploadReceipt,
    RunManifest,
    RunManifestStatus,
    RunTraceSummary,
    WorkspaceConfig,
)
from mentalmodel.ui.catalog import default_dashboard_catalog


class RemoteContractsTest(unittest.TestCase):
    def test_artifact_descriptor_rejects_absolute_relative_path(self) -> None:
        with self.assertRaises(RemoteContractError):
            ArtifactDescriptor(
                logical_name=ArtifactName.SUMMARY,
                relative_path="/tmp/summary.json",
                content_type="application/json",
            )

    def test_run_manifest_reports_missing_required_artifacts(self) -> None:
        manifest = RunManifest(
            run_id="run-123",
            graph_id="demo_graph",
            created_at_ms=100,
            completed_at_ms=200,
            status=RunManifestStatus.SEALED,
            success=True,
            run_schema_version=7,
            record_schema_version=1,
            trace_summary=RunTraceSummary(mode="disk", service_name="mentalmodel"),
            artifacts=(
                ArtifactDescriptor(
                    logical_name=ArtifactName.SUMMARY,
                    relative_path="summary.json",
                    content_type="application/json",
                ),
                ArtifactDescriptor(
                    logical_name=ArtifactName.GRAPH,
                    relative_path="graph.json",
                    content_type="application/json",
                ),
            ),
            invocation_name="demo",
            project_id="mentalmodel-examples",
            project_label="Mentalmodel Examples",
            catalog_source=CatalogSource.BUILTIN,
        )
        self.assertEqual(
            manifest.missing_required_artifacts(),
            (
                ArtifactName.RECORDS,
                ArtifactName.OUTPUTS,
                ArtifactName.STATE,
            ),
        )

    def test_run_manifest_rejects_duplicate_artifact_names(self) -> None:
        descriptor = ArtifactDescriptor(
            logical_name=ArtifactName.SUMMARY,
            relative_path="summary.json",
            content_type="application/json",
        )
        with self.assertRaises(RemoteContractError):
            RunManifest(
                run_id="run-123",
                graph_id="demo_graph",
                created_at_ms=100,
                completed_at_ms=200,
                status=RunManifestStatus.SEALED,
                success=True,
                run_schema_version=7,
                trace_summary=RunTraceSummary(mode="disk", service_name="mentalmodel"),
                artifacts=(descriptor, descriptor),
            )

    def test_project_registration_requires_absolute_root_dir(self) -> None:
        with self.assertRaises(RemoteContractError):
            ProjectRegistration(
                project_id="pangramanizer-training",
                label="Pangramanizer",
                root_dir=Path("relative/path"),
            )

    def test_workspace_config_rejects_duplicate_project_ids(self) -> None:
        project = ProjectRegistration(
            project_id="mentalmodel-examples",
            label="Mentalmodel Examples",
            root_dir=Path("/Users/ben/repos/mentalmodel"),
        )
        with self.assertRaises(RemoteContractError):
            WorkspaceConfig(
                workspace_id="local-dev",
                label="Local Dev",
                projects=(project, project),
            )

    def test_project_catalog_validates_default_entry_id(self) -> None:
        entry = default_dashboard_catalog()[0]
        project = ProjectRegistration(
            project_id="mentalmodel-examples",
            label="Mentalmodel Examples",
            root_dir=Path("/Users/ben/repos/mentalmodel"),
        )
        catalog = ProjectCatalog(
            project=project,
            entries=(entry,),
            default_entry_id=entry.spec_id,
        )
        self.assertEqual(catalog.default_entry_id, entry.spec_id)

    def test_project_catalog_rejects_unknown_default_entry_id(self) -> None:
        entry = default_dashboard_catalog()[0]
        project = ProjectRegistration(
            project_id="mentalmodel-examples",
            label="Mentalmodel Examples",
            root_dir=Path("/Users/ben/repos/mentalmodel"),
        )
        with self.assertRaises(RemoteContractError):
            ProjectCatalog(
                project=project,
                entries=(entry,),
                default_entry_id="not-real",
            )

    def test_project_catalog_snapshot_validates_entries(self) -> None:
        entry = default_dashboard_catalog()[0]
        snapshot = ProjectCatalogSnapshot(
            project_id="mentalmodel-examples",
            provider="mentalmodel.ui.catalog:default_dashboard_catalog",
            published_at_ms=1000,
            entries=(entry.as_dict(),),
            default_entry_id=entry.spec_id,
        )
        self.assertEqual(snapshot.entry_count, 1)

    def test_remote_project_link_request_rejects_mismatched_snapshot(self) -> None:
        entry = default_dashboard_catalog()[0]
        snapshot = ProjectCatalogSnapshot(
            project_id="other-project",
            provider="mentalmodel.ui.catalog:default_dashboard_catalog",
            published_at_ms=1000,
            entries=(entry.as_dict(),),
        )
        with self.assertRaises(RemoteContractError):
            RemoteProjectLinkRequest(
                project_id="mentalmodel-examples",
                label="Mentalmodel Examples",
                catalog_snapshot=snapshot,
            )

    def test_remote_project_catalog_publish_request_rejects_mismatched_snapshot(self) -> None:
        entry = default_dashboard_catalog()[0]
        snapshot = ProjectCatalogSnapshot(
            project_id="other-project",
            provider="mentalmodel.ui.catalog:default_dashboard_catalog",
            published_at_ms=1000,
            entries=(entry.as_dict(),),
        )
        with self.assertRaises(RemoteContractError):
            RemoteProjectCatalogPublishRequest(
                project_id="mentalmodel-examples",
                catalog_provider="mentalmodel.ui.catalog:default_dashboard_catalog",
                catalog_snapshot=snapshot,
            )

    def test_remote_project_record_exposes_catalog_status(self) -> None:
        entry = default_dashboard_catalog()[0]
        snapshot = ProjectCatalogSnapshot(
            project_id="mentalmodel-examples",
            provider="mentalmodel.ui.catalog:default_dashboard_catalog",
            published_at_ms=1000,
            entries=(entry.as_dict(),),
        )
        record = RemoteProjectRecord(
            project_id="mentalmodel-examples",
            label="Mentalmodel Examples",
            linked_at_ms=1000,
            updated_at_ms=1001,
            catalog_snapshot=snapshot,
            last_completed_run_upload_at_ms=2000,
            last_completed_run_graph_id="async_rl_demo",
            last_completed_run_id="run-123",
            last_completed_run_invocation_name="fixture",
        )
        self.assertTrue(record.catalog_published)
        self.assertEqual(record.catalog_entry_count, 1)
        self.assertEqual(record.catalog_published_at_ms, 1000)
        self.assertEqual(record.as_dict()["last_completed_run_id"], "run-123")

    def test_remote_run_upload_receipt_round_trips(self) -> None:
        receipt = RemoteRunUploadReceipt(
            graph_id="async_rl_demo",
            run_id="run-123",
            uploaded_at_ms=1000,
            run_dir="/tmp/remote/.runs/async_rl_demo/run-123",
            project_id="mentalmodel-examples",
        )
        decoded = RemoteRunUploadReceipt.from_dict(receipt.as_dict())
        self.assertEqual(decoded.graph_id, "async_rl_demo")
        self.assertEqual(decoded.project_id, "mentalmodel-examples")


if __name__ == "__main__":
    unittest.main()
