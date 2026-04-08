from __future__ import annotations

import unittest
from pathlib import Path

from mentalmodel.remote import (
    ArtifactDescriptor,
    ArtifactName,
    CatalogSource,
    ProjectCatalog,
    ProjectRegistration,
    RemoteContractError,
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


if __name__ == "__main__":
    unittest.main()
