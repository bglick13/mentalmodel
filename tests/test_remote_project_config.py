from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mentalmodel.remote.project_config import (
    MentalModelProjectConfig,
    ProjectConfigError,
    discover_project_config_path,
    load_project_config,
)


class RemoteProjectConfigTest(unittest.TestCase):
    def test_load_project_config_resolves_relative_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            spec_path = root / "verify.toml"
            spec_path.write_text(
                (
                    "[program]\n"
                    'entrypoint = "mentalmodel.examples.async_rl.demo:build_program"\n'
                ),
                encoding="utf-8",
            )
            config_path = root / "mentalmodel.toml"
            config_path.write_text(
                "\n".join(
                    (
                        "[project]",
                        'project_id = "demo-project"',
                        'label = "Demo Project"',
                        "",
                        "[remote]",
                        'server_url = "http://127.0.0.1:8765"',
                        'api_key_env = "MENTALMODEL_API_KEY"',
                        'default_environment = "prod"',
                        "",
                        "[catalog]",
                        'provider = "mentalmodel.ui.catalog:default_dashboard_catalog"',
                        "publish_on_link = false",
                        "",
                        "[runs]",
                        'default_runs_dir = ".runs"',
                        "",
                        "[verify]",
                        'default_spec = "verify.toml"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            loaded = load_project_config(config_path)
            self.assertIsInstance(loaded, MentalModelProjectConfig)
            self.assertEqual(loaded.project_id, "demo-project")
            self.assertEqual(loaded.default_runs_dir, (root / ".runs").resolve())
            self.assertEqual(loaded.default_verify_spec, spec_path.resolve())

    def test_discover_project_config_walks_parent_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            nested = root / "a" / "b"
            nested.mkdir(parents=True)
            config_path = root / "mentalmodel.toml"
            config_path.write_text(
                "\n".join(
                    (
                        "[project]",
                        'project_id = "demo-project"',
                        'label = "Demo Project"',
                        "",
                        "[remote]",
                        'server_url = "http://127.0.0.1:8765"',
                        'api_key_env = "MENTALMODEL_API_KEY"',
                        "",
                        "[catalog]",
                        'provider = "mentalmodel.ui.catalog:default_dashboard_catalog"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            discovered = discover_project_config_path(nested)
            self.assertEqual(discovered, config_path.resolve())

    def test_resolve_api_key_requires_env_var(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "mentalmodel.toml"
            config_path.write_text(
                "\n".join(
                    (
                        "[project]",
                        'project_id = "demo-project"',
                        'label = "Demo Project"',
                        "",
                        "[remote]",
                        'server_url = "http://127.0.0.1:8765"',
                        'api_key_env = "MENTALMODEL_API_KEY"',
                        "",
                        "[catalog]",
                        'provider = "mentalmodel.ui.catalog:default_dashboard_catalog"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            loaded = load_project_config(config_path)
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(ProjectConfigError):
                    loaded.resolve_api_key()

    def test_resolve_optional_api_key_allows_missing_env_var(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "mentalmodel.toml"
            config_path.write_text(
                "\n".join(
                    (
                        "[project]",
                        'project_id = "demo-project"',
                        'label = "Demo Project"',
                        "",
                        "[remote]",
                        'server_url = "http://127.0.0.1:8765"',
                        'api_key_env = "MENTALMODEL_API_KEY"',
                        "",
                        "[catalog]",
                        'provider = "mentalmodel.ui.catalog:default_dashboard_catalog"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            loaded = load_project_config(config_path)
            with patch.dict(os.environ, {}, clear=True):
                self.assertIsNone(loaded.resolve_optional_api_key())


if __name__ == "__main__":
    unittest.main()
