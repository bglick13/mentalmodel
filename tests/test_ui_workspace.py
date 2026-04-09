from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from mentalmodel.remote import ProjectCatalog, ProjectRegistration, WorkspaceConfig
from mentalmodel.remote.workspace import load_workspace_config, write_workspace_config
from mentalmodel.ui.api import create_dashboard_app
from mentalmodel.ui.catalog import DashboardCatalogEntry, default_dashboard_catalog
from mentalmodel.ui.workspace import (
    flatten_project_catalogs,
    load_project_catalog_subject,
    workspace_catalog_entries,
    workspace_project_catalogs,
)


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

    def test_workspace_catalog_entries_resolves_external_spec_catalog_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            package_dir = root / "externalproj"
            package_dir.mkdir()
            (package_dir / "__init__.py").write_text("", encoding="utf-8")
            spec_path = root / "external_async_rl.toml"
            spec_path.write_text(
                "\n".join(
                    (
                        "[program]",
                        'entrypoint = "mentalmodel.examples.async_rl.demo:build_program"',
                        "",
                        "[runtime]",
                        'invocation_name = "external_async_rl"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            (package_dir / "spec_catalog.py").write_text(
                "\n".join(
                    (
                        "from dataclasses import dataclass",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationRowSource:",
                        "    kind: str",
                        "    node_id: str",
                        "    items_path: str",
                        "    loop_node_id: str | None = None",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationValueSelector:",
                        "    kind: str",
                        "    path: str | None = None",
                        "    node_id: str | None = None",
                        "    event_type: str | None = None",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationTableColumn:",
                        "    column_id: str",
                        "    title: str",
                        "    selector: VerificationValueSelector",
                        "    description: str = ''",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationCustomView:",
                        "    view_id: str",
                        "    title: str",
                        "    description: str",
                        "    kind: str",
                        "    row_source: VerificationRowSource",
                        "    columns: tuple[VerificationTableColumn, ...]",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationMetricGroup:",
                        "    group_id: str",
                        "    title: str",
                        "    metric_path_prefixes: tuple[str, ...]",
                        "    description: str = ''",
                        "    max_items: int = 8",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationPinnedNode:",
                        "    node_id: str",
                        "    title: str",
                        "    description: str = ''",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationSpecEntry:",
                        "    label: str",
                        "    spec_path: str",
                        "    graph_id: str",
                        "    invocation_name: str",
                        "    category: str",
                        "    description: str",
                        "    default_loop_node_id: str | None = None",
                        "    metric_groups: tuple[VerificationMetricGroup, ...] = ()",
                        "    pinned_nodes: tuple[VerificationPinnedNode, ...] = ()",
                        "    tags: tuple[str, ...] = ()",
                        "    custom_views: tuple[VerificationCustomView, ...] = ()",
                        "",
                        "def verification_spec_catalog():",
                        "    return (",
                        "        VerificationSpecEntry(",
                        "            label='real_smoke',",
                        f"            spec_path={str(spec_path)!r},",
                        "            graph_id='external_async_rl_graph',",
                        "            invocation_name='external_async_rl',",
                        "            category='real',",
                        "            description='External project smoke',",
                        "            default_loop_node_id='training_loop',",
                        "            metric_groups=(",
                        "                VerificationMetricGroup(",
                        "                    group_id='reward',",
                        "                    title='Reward Metrics',",
                        "                    metric_path_prefixes=('metrics.reward/',),",
                        "                ),",
                        "            ),",
                        "            pinned_nodes=(",
                        "                VerificationPinnedNode(",
                        (
                            "                    "
                            "node_id='training_loop.training_step.rollout.reward_join',"
                        ),
                        "                    title='Reward Join',",
                        "                ),",
                        "            ),",
                        "            tags=('real', 'operator'),",
                        "            custom_views=(",
                        "                VerificationCustomView(",
                        "                    view_id='sample-quality',",
                        "                    title='Sample Quality',",
                        "                    description='Prompt and score table',",
                        "                    kind='table',",
                        "                    row_source=VerificationRowSource(",
                        "                        kind='node_output_items',",
                        (
                            "                        "
                            "node_id='training_loop.training_step.rollout.rollout_join',"
                        ),
                        "                        items_path='artifacts.rollout_batch.samples',",
                        "                        loop_node_id='training_loop',",
                        "                    ),",
                        "                    columns=(",
                        "                        VerificationTableColumn(",
                        "                            column_id='prompt_text',",
                        "                            title='Prompt',",
                        "                            selector=VerificationValueSelector(",
                        "                                kind='row_item',",
                        "                                path='prompt_text',",
                        "                            ),",
                        "                        ),",
                        "                    ),",
                        "                ),",
                        "            ),",
                        "        ),",
                        "    )",
                        "",
                    )
                ),
                encoding="utf-8",
            )
            workspace = WorkspaceConfig(
                workspace_id="workspace",
                label="Workspace",
                projects=(
                    ProjectRegistration(
                        project_id="external-training",
                        label="External Training",
                        root_dir=root,
                        catalog_provider="externalproj.spec_catalog:verification_spec_catalog",
                    ),
                ),
            )
            entries = workspace_catalog_entries(workspace)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].project_id, "external-training")
            self.assertEqual(entries[0].label, "real_smoke")
            self.assertEqual(entries[0].graph_id, "external_async_rl_graph")
            self.assertEqual(entries[0].default_loop_node_id, "training_loop")
            self.assertEqual(entries[0].metric_groups[0].group_id, "reward")
            self.assertEqual(
                entries[0].pinned_nodes[0].node_id,
                "training_loop.training_step.rollout.reward_join",
            )
            self.assertEqual(entries[0].tags, ("real", "operator"))
            self.assertEqual(entries[0].custom_views[0].view_id, "sample-quality")
            self.assertEqual(
                entries[0].custom_views[0].row_source.items_path,
                "artifacts.rollout_batch.samples",
            )
            catalogs = workspace_project_catalogs(workspace)
            self.assertEqual(catalogs[0].project.project_id, "external-training")

    def test_workspace_catalog_entries_can_bypass_failing_package_init(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            package_dir = root / "externalproj"
            verification_dir = package_dir / "verification"
            verification_dir.mkdir(parents=True)
            (package_dir / "__init__.py").write_text(
                "import missing_dependency\n",
                encoding="utf-8",
            )
            (verification_dir / "__init__.py").write_text("", encoding="utf-8")
            spec_path = root / "external_async_rl.toml"
            spec_path.write_text(
                "\n".join(
                    (
                        "[program]",
                        'entrypoint = "mentalmodel.examples.async_rl.demo:build_program"',
                        "",
                        "[runtime]",
                        'invocation_name = "external_async_rl"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            (verification_dir / "spec_catalog.py").write_text(
                "\n".join(
                    (
                        "from dataclasses import dataclass",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationSpecEntry:",
                        "    label: str",
                        "    spec_path: str",
                        "    graph_id: str",
                        "    invocation_name: str",
                        "    category: str",
                        "    description: str",
                        "",
                        "def verification_spec_catalog():",
                        "    return (",
                        "        VerificationSpecEntry(",
                        "            label='real_verify3',",
                        f"            spec_path={str(spec_path)!r},",
                        "            graph_id='external_async_rl_graph',",
                        "            invocation_name='external_async_rl',",
                        "            category='real',",
                        "            description='External project smoke',",
                        "        ),",
                        "    )",
                        "",
                    )
                ),
                encoding="utf-8",
            )
            workspace = WorkspaceConfig(
                workspace_id="workspace",
                label="Workspace",
                projects=(
                    ProjectRegistration(
                        project_id="external-training",
                        label="External Training",
                        root_dir=root,
                        catalog_provider="externalproj.verification.spec_catalog:verification_spec_catalog",
                    ),
                ),
            )
            entries = workspace_catalog_entries(workspace)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].spec_id, "external-training:real-verify3")

    def test_workspace_catalog_entries_can_use_external_subprocess_for_spec_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            package_dir = root / "externalproj"
            verification_dir = package_dir / "verification"
            verification_dir.mkdir(parents=True)
            (package_dir / "__init__.py").write_text("", encoding="utf-8")
            (verification_dir / "__init__.py").write_text("", encoding="utf-8")
            spec_path = root / "external_async_rl.toml"
            spec_path.write_text(
                "\n".join(
                    (
                        "[program]",
                        'entrypoint = "externalproj.verification.rollout_fixture:build_program"',
                        "",
                        "[runtime]",
                        'invocation_name = "external_async_rl"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            (verification_dir / "spec_catalog.py").write_text(
                "\n".join(
                    (
                        "from dataclasses import dataclass",
                        "",
                        "@dataclass(slots=True, frozen=True)",
                        "class VerificationSpecEntry:",
                        "    label: str",
                        "    spec_path: str",
                        "    graph_id: str",
                        "    invocation_name: str",
                        "    category: str",
                        "    description: str",
                        "",
                        "def verification_spec_catalog():",
                        "    return (",
                        "        VerificationSpecEntry(",
                        "            label='real_verify1',",
                        f"            spec_path={str(spec_path)!r},",
                        "            graph_id='external_graph_from_provider',",
                        "            invocation_name='external_async_rl',",
                        "            category='real',",
                        "            description='External verify1',",
                        "        ),",
                        "    )",
                        "",
                    )
                ),
                encoding="utf-8",
            )
            workspace = WorkspaceConfig(
                workspace_id="workspace",
                label="Workspace",
                projects=(
                    ProjectRegistration(
                        project_id="external-training",
                        label="External Training",
                        root_dir=root,
                        catalog_provider="externalproj.verification.spec_catalog:verification_spec_catalog",
                    ),
                ),
            )
            metadata = {"graph_id": "external_graph", "invocation_name": "external_async_rl"}
            with patch("mentalmodel.ui.workspace.subprocess.run") as run_subprocess:
                run_subprocess.return_value = subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout=json.dumps(metadata),
                    stderr="",
                )
                entries = workspace_catalog_entries(workspace)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].graph_id, "external_graph_from_provider")
            self.assertEqual(entries[0].spec_id, "external-training:real-verify1")
            run_subprocess.assert_not_called()

    def test_workspace_config_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = WorkspaceConfig(
                workspace_id="workspace",
                label="Workspace",
                description="Shared stack",
                projects=(
                    ProjectRegistration(
                        project_id="mentalmodel-examples",
                        label="Mentalmodel Examples",
                        root_dir=Path(tmpdir).resolve(),
                        catalog_provider="mentalmodel.ui.catalog:default_dashboard_catalog",
                    ),
                ),
            )
            path = write_workspace_config(Path(tmpdir) / "workspace.toml", workspace)
            loaded = load_workspace_config(path)
            self.assertEqual(loaded.workspace_id, "workspace")
            self.assertEqual(loaded.projects[0].project_id, "mentalmodel-examples")


if __name__ == "__main__":
    unittest.main()
