from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from mentalmodel.remote.bootstrap import build_remote_doctor_report, write_remote_demo
from mentalmodel.remote.contracts import ProjectRegistration, WorkspaceConfig
from mentalmodel.remote.workspace import load_workspace_config, write_workspace_config


class RemoteBootstrapTest(unittest.TestCase):
    def test_write_remote_demo_writes_workspace_and_helper_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "remote-demo"
            written = write_remote_demo(
                output_dir=output_dir,
                profile="minimal",
                mentalmodel_root=Path(__file__).resolve().parents[1],
                pangramanizer_root=Path(tmpdir) / "missing-pangramanizer",
            )
            written_paths = {path.name for path in written}
            self.assertIn("workspace.toml", written_paths)
            self.assertIn("docker-compose.remote-minimal.yml", written_paths)
            self.assertIn("run-dashboard.sh", written_paths)
            self.assertIn("start-stack.sh", written_paths)
            self.assertIn("stop-stack.sh", written_paths)
            self.assertIn("sync-local-runs.sh", written_paths)
            self.assertIn("REMOTE-DEMO.md", written_paths)
            self.assertTrue((output_dir / "otel" / "docker-compose.otel-lgtm.yml").exists())
            workspace = load_workspace_config(output_dir / "workspace.toml")
            self.assertEqual(len(workspace.projects), 1)
            self.assertEqual(workspace.projects[0].project_id, "mentalmodel-examples")
            env_text = (output_dir / "mentalmodel.remote.env").read_text(encoding="utf-8")
            self.assertIn("MENTALMODEL_REMOTE_DATABASE_URL=", env_text)
            self.assertIn("MENTALMODEL_REMOTE_OBJECT_STORE_BUCKET=", env_text)
            dashboard_script = (output_dir / "run-dashboard.sh").read_text(encoding="utf-8")
            start_script = (output_dir / "start-stack.sh").read_text(encoding="utf-8")
            sync_script = (output_dir / "sync-local-runs.sh").read_text(encoding="utf-8")
            self.assertIn("source \"$SCRIPT_DIR/mentalmodel.remote.env\"", dashboard_script)
            self.assertIn("docker compose", start_script)
            self.assertIn("uv run --directory", dashboard_script)
            self.assertIn("uv run --directory", sync_script)

    def test_write_remote_demo_does_not_resolve_external_providers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "remote-demo"
            pangram_root = Path(tmpdir) / "pangramanizer"
            pangram_package = (
                pangram_root
                / "pangramanizer"
                / "mentalmodel_training"
                / "verification"
            )
            pangram_package.mkdir(parents=True)
            for package_dir in (
                pangram_root / "pangramanizer",
                pangram_root / "pangramanizer" / "mentalmodel_training",
                pangram_package,
            ):
                (package_dir / "__init__.py").write_text("", encoding="utf-8")
            (pangram_package / "spec_catalog.py").write_text(
                "import missing_dependency\n",
                encoding="utf-8",
            )
            write_remote_demo(
                output_dir=output_dir,
                profile="minimal",
                mentalmodel_root=Path(__file__).resolve().parents[1],
                pangramanizer_root=pangram_root,
            )
            workspace = load_workspace_config(output_dir / "workspace.toml")
            pangram = next(
                project
                for project in workspace.projects
                if project.project_id == "pangramanizer-training"
            )
            self.assertTrue(pangram.enabled)
            self.assertEqual(pangram.runs_dir, (output_dir / "data").resolve())

    def test_remote_doctor_passes_for_generated_demo(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "remote-demo"
            write_remote_demo(
                output_dir=output_dir,
                profile="minimal",
                mentalmodel_root=Path(__file__).resolve().parents[1],
                pangramanizer_root=Path(tmpdir) / "missing-pangramanizer",
            )
            report = build_remote_doctor_report(
                workspace_config=output_dir / "workspace.toml",
                runs_dir=output_dir / "data",
            )
            self.assertTrue(report.success)

    def test_remote_doctor_fails_when_enabled_project_has_no_runs_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "remote-demo"
            workspace = WorkspaceConfig(
                workspace_id="workspace",
                label="Workspace",
                projects=(
                    ProjectRegistration(
                        project_id="project-a",
                        label="Project A",
                        root_dir=Path(__file__).resolve().parents[1],
                        catalog_provider="mentalmodel.ui.catalog:default_dashboard_catalog",
                    ),
                ),
            )
            write_workspace_config(output_dir / "workspace.toml", workspace)
            report = build_remote_doctor_report(
                workspace_config=output_dir / "workspace.toml",
                runs_dir=output_dir / "data",
            )
            self.assertFalse(report.success)
            route_check = next(check for check in report.checks if check.name == "project_routes")
            self.assertEqual(route_check.status.value, "fail")
            self.assertIn("project-a", route_check.message + json.dumps(route_check.details))


if __name__ == "__main__":
    unittest.main()
