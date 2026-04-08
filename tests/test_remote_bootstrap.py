from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel.remote.bootstrap import build_remote_doctor_report, write_remote_demo
from mentalmodel.remote.workspace import load_workspace_config


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
            self.assertIn("run-dashboard.sh", written_paths)
            self.assertIn("sync-local-runs.sh", written_paths)
            self.assertIn("REMOTE-DEMO.md", written_paths)
            self.assertTrue((output_dir / "otel" / "docker-compose.otel-lgtm.yml").exists())
            workspace = load_workspace_config(output_dir / "workspace.toml")
            self.assertEqual(len(workspace.projects), 1)
            self.assertEqual(workspace.projects[0].project_id, "mentalmodel-examples")
            dashboard_script = (output_dir / "run-dashboard.sh").read_text(encoding="utf-8")
            sync_script = (output_dir / "sync-local-runs.sh").read_text(encoding="utf-8")
            self.assertIn("uv run --directory", dashboard_script)
            self.assertIn("uv run --directory", sync_script)

    def test_write_remote_demo_disables_unresolvable_pangram_project(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "remote-demo"
            pangram_root = Path(tmpdir) / "pangramanizer"
            pangram_package = pangram_root / "pangramanizer" / "mentalmodel_training" / "verification"
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
            self.assertFalse(pangram.enabled)
            self.assertIn("provider did not resolve", pangram.description)

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


if __name__ == "__main__":
    unittest.main()
