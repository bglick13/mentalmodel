from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from mentalmodel.remote import ProjectCatalog, ProjectRegistration
from mentalmodel.ui.api import create_dashboard_app
from mentalmodel.ui.catalog import DashboardCatalogEntry, default_dashboard_catalog
from mentalmodel.ui.workspace import flatten_project_catalogs, load_project_catalog_subject


class UiWorkspaceTest(unittest.TestCase):
    def test_load_project_catalog_subject_accepts_project_provider(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        custom_entry = DashboardCatalogEntry(
            spec_id="pangram-real-smoke",
            label="Pangram Real Smoke",
            description="tracker smoke",
            spec_path=fixture_entry.spec_path,
            graph_id="pangram_graph",
            invocation_name="pangram_real_smoke",
        )
        project = ProjectRegistration(
            project_id="pangramanizer-training",
            label="Pangramanizer Training",
            root_dir=Path("/Users/ben/repos/pangramanizer"),
        )
        catalog = ProjectCatalog(project=project, entries=(custom_entry,))

        module_name = "mentalmodel.tests.synthetic_project_catalog"
        module = types.ModuleType(module_name)
        module.__dict__["project_catalog"] = lambda: catalog
        sys.modules[module_name] = module
        try:
            loaded_module, loaded_catalog = load_project_catalog_subject(
                f"{module_name}:project_catalog"
            )
        finally:
            sys.modules.pop(module_name, None)
        self.assertIs(loaded_module, module)
        self.assertEqual(loaded_catalog.project.project_id, "pangramanizer-training")

    def test_flatten_project_catalogs_applies_project_metadata(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        custom_entry = DashboardCatalogEntry(
            spec_id="pangram-real-smoke",
            label="Pangram Real Smoke",
            description="tracker smoke",
            spec_path=fixture_entry.spec_path,
            graph_id="pangram_graph",
            invocation_name="pangram_real_smoke",
        )
        project = ProjectRegistration(
            project_id="pangramanizer-training",
            label="Pangramanizer Training",
            root_dir=Path("/Users/ben/repos/pangramanizer"),
        )
        catalog = ProjectCatalog(project=project, entries=(custom_entry,))
        flattened = flatten_project_catalogs((catalog,))
        self.assertEqual(flattened[0].project_id, "pangramanizer-training")
        self.assertEqual(flattened[0].project_label, "Pangramanizer Training")
        self.assertEqual(flattened[0].catalog_source, "module-provider")

    def test_dashboard_api_lists_registered_projects(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        custom_entry = DashboardCatalogEntry(
            spec_id="pangram-real-smoke",
            label="Pangram Real Smoke",
            description="tracker smoke",
            spec_path=fixture_entry.spec_path,
            graph_id="pangram_graph",
            invocation_name="pangram_real_smoke",
        )
        project = ProjectRegistration(
            project_id="pangramanizer-training",
            label="Pangramanizer Training",
            root_dir=Path("/Users/ben/repos/pangramanizer"),
        )
        catalog = ProjectCatalog(
            project=project,
            entries=(custom_entry,),
            description="Training fork",
        )
        client = TestClient(
            create_dashboard_app(
                frontend_dist=None,
                project_catalogs=(catalog,),
            )
        )
        response = client.get("/api/projects")
        self.assertEqual(response.status_code, 200)
        projects = response.json()["projects"]
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]["project_id"], "pangramanizer-training")
        catalog_response = client.get("/api/catalog")
        entries = catalog_response.json()["entries"]
        self.assertTrue(
            any(entry["project_id"] == "pangramanizer-training" for entry in entries)
        )


if __name__ == "__main__":
    unittest.main()
