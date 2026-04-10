from __future__ import annotations

import json
import unittest
from pathlib import Path


class DocsSiteTest(unittest.TestCase):
    def test_docs_json_references_existing_pages(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        config = json.loads((repo_root / "docs.json").read_text(encoding="utf-8"))
        pages: list[str] = []
        for tab in config["navigation"]["tabs"]:
            for group in tab["groups"]:
                pages.extend(group["pages"])

        missing: list[str] = []
        for page in pages:
            if not any((repo_root / f"{page}{ext}").exists() for ext in (".mdx", ".md")):
                missing.append(page)

        self.assertEqual(missing, [])

    def test_docs_include_current_package_model_page(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        page = repo_root / "concepts" / "current-package-model.mdx"
        content = page.read_text(encoding="utf-8")
        self.assertIn("What mentalmodel already supports well", content)
        self.assertIn("What is intentionally not native yet", content)
        self.assertIn("Block` and `Use", content)

    def test_docs_include_reusable_blocks_guide(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        page = repo_root / "guides" / "reusable-blocks.mdx"
        content = page.read_text(encoding="utf-8")
        self.assertIn("`Block` and `Use`", content)
        self.assertIn("Use.output_ref", content)

    def test_phase_16_5_docs_require_output_interpretation(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        quickstart = (repo_root / "quickstart.mdx").read_text(encoding="utf-8")
        runs = (repo_root / "cli" / "runs.mdx").read_text(encoding="utf-8")
        verify = (repo_root / "cli" / "verify.mdx").read_text(encoding="utf-8")

        self.assertIn("How to read that", quickstart)
        self.assertIn("A practical debugging sequence", runs)
        self.assertIn("How to use that", verify)
        self.assertIn("--params-json", verify)
        self.assertIn("--params-file", verify)
        self.assertIn("--environment-entrypoint", verify)
        self.assertIn("--spec", verify)
        self.assertIn("--invocation-name", verify)

    def test_runtime_environment_docs_cover_cli_binding(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        page = repo_root / "guides" / "runtime-environments.mdx"
        content = page.read_text(encoding="utf-8")
        self.assertIn("--environment-entrypoint", content)
        self.assertIn("--spec", content)
        self.assertIn("invocation_name", content)

    def test_phase_23_docs_cover_new_recipes_and_reference_example(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        recipes = (repo_root / "docs.json").read_text(encoding="utf-8")
        reusable_blocks = (repo_root / "guides" / "reusable-blocks.mdx").read_text(
            encoding="utf-8"
        )
        step_loops = (repo_root / "guides" / "step-loops.mdx").read_text(
            encoding="utf-8"
        )
        self.assertIn("docs/recipes/block-reuse", recipes)
        self.assertIn("docs/recipes/loop-debugging", recipes)
        self.assertIn("docs/recipes/runtime-profile-selection", recipes)
        self.assertIn("docs/recipes/resource-injection", recipes)
        self.assertIn("docs/recipes/parameterized-verification", recipes)
        self.assertIn("review_workflow", reusable_blocks)
        self.assertIn("review_workflow", step_loops)

    def test_phase_26_docs_cover_dashboard_ui(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        docs_json = (repo_root / "docs.json").read_text(encoding="utf-8")
        cli_ui = (repo_root / "cli" / "ui.mdx").read_text(encoding="utf-8")
        guide = (repo_root / "guides" / "dashboard-ui.mdx").read_text(encoding="utf-8")

        self.assertIn("cli/ui", docs_json)
        self.assertIn("guides/dashboard-ui", docs_json)
        self.assertIn("mentalmodel ui", cli_ui)
        self.assertIn("--frontend-dev-url", cli_ui)
        self.assertIn("--catalog-entrypoint", cli_ui)
        self.assertIn("review_workflow", guide)
        self.assertIn("graph.json", guide)
        self.assertIn("dev:stack", guide)

    def test_remote_docs_cover_repo_linked_phase_one(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        remote_page = (repo_root / "cli" / "remote.mdx").read_text(encoding="utf-8")
        readme = (repo_root / "README.md").read_text(encoding="utf-8")

        self.assertIn("mentalmodel.toml", remote_page)
        self.assertIn("mentalmodel remote link", remote_page)
        self.assertIn("mentalmodel remote status", remote_page)
        self.assertIn("repo-owned", remote_page)
        self.assertIn("mentalmodel remote link", readme)
        self.assertIn("mentalmodel remote status", readme)
