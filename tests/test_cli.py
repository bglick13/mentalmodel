from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from mentalmodel.cli import build_parser, main


class CliTest(unittest.TestCase):
    def test_demo_command_parses(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["demo", "async-rl"])
        self.assertEqual(args.command, "demo")
        self.assertEqual(args.name, "async-rl")

    def test_check_command_succeeds_for_demo_entrypoint(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "check",
                    "--entrypoint",
                    "mentalmodel.examples.async_rl.demo:build_program",
                ]
            )
        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("mentalmodel check summary", rendered)
        self.assertIn("async_rl_demo", rendered)

    def test_check_command_json_output_is_valid(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "check",
                    "--entrypoint",
                    "mentalmodel.examples.async_rl.demo:build_program",
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["graph_id"], "async_rl_demo")
        self.assertIn("findings", payload)

    def test_check_command_fails_for_missing_module(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["check", "--entrypoint", "missing.module:build_program"])
        self.assertEqual(exit_code, 1)
        self.assertIn("mentalmodel error:", stdout.getvalue())

    def test_check_command_fails_for_missing_attribute(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["check", "--entrypoint", "math:not_here"])
        self.assertEqual(exit_code, 1)
        self.assertIn("does not define", stdout.getvalue())

    def test_check_command_fails_for_non_workflow_entrypoint(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["check", "--entrypoint", "math:pi"])
        self.assertEqual(exit_code, 1)
        self.assertIn("must resolve to a Workflow", stdout.getvalue())

    def test_graph_command_outputs_mermaid(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "graph",
                    "--entrypoint",
                    "mentalmodel.examples.async_rl.demo:build_program",
                ]
            )
        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("flowchart LR", rendered)
        self.assertIn("sample_policy", rendered)

    def test_docs_command_outputs_markdown_sections(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "docs",
                    "--entrypoint",
                    "mentalmodel.examples.async_rl.demo:build_program",
                ]
            )
        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("async_rl_demo Topology", rendered)
        self.assertIn("async_rl_demo Runtime Contexts", rendered)

    def test_docs_command_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "docs",
                        "--entrypoint",
                        "mentalmodel.examples.async_rl.demo:build_program",
                        "--output-dir",
                        str(output_dir),
                    ]
                )
            self.assertEqual(exit_code, 0)
            self.assertTrue((output_dir / "topology.md").exists())
            self.assertTrue((output_dir / "node-inventory.md").exists())
            self.assertTrue((output_dir / "invariants.md").exists())
            self.assertTrue((output_dir / "runtime-contexts.md").exists())


if __name__ == "__main__":
    unittest.main()
