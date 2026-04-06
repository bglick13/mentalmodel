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
