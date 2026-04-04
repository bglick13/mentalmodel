from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from mentalmodel.cli import build_parser, main


class CliTest(unittest.TestCase):
    def _materialize_demo_run(self, runs_dir: str) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "verify",
                    "--entrypoint",
                    "mentalmodel.examples.async_rl.demo:build_program",
                    "--runs-dir",
                    runs_dir,
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)

    def test_demo_command_parses(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["demo", "async-rl"])
        self.assertEqual(args.command, "demo")
        self.assertEqual(args.name, "async-rl")

    def test_demo_command_outputs_summary(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["demo", "async-rl"])
        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("mentalmodel demo summary", rendered)
        self.assertIn("expected_mermaid.txt", rendered)

    def test_demo_command_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "demo",
                        "async-rl",
                        "--write-artifacts",
                        "--output-dir",
                        tmpdir,
                    ]
                )
            self.assertEqual(exit_code, 0)
            self.assertTrue((Path(tmpdir) / "expected_mermaid.txt").exists())
            self.assertTrue((Path(tmpdir) / "topology.md").exists())

    def test_demo_command_json_output_is_valid(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["demo", "async-rl", "--json"])
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["demo"], "async-rl")
        self.assertEqual(payload["graph_id"], "async_rl_demo")
        self.assertIn("expected_mermaid.txt", payload["artifacts"])

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

    def test_verify_command_succeeds_for_demo_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "verify",
                        "--entrypoint",
                        "mentalmodel.examples.async_rl.demo:build_program",
                        "--runs-dir",
                        tmpdir,
                    ]
                )
            self.assertEqual(exit_code, 0)
            rendered = stdout.getvalue()
            self.assertIn("mentalmodel verify summary", rendered)
            runs_root = Path(tmpdir) / ".runs"
            self.assertTrue(runs_root.exists())

    def test_verify_command_fails_for_runtime_invariant_violation(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "verify",
                    "--entrypoint",
                    "mentalmodel.examples.verification_failure:build_program",
                ]
            )
        self.assertEqual(exit_code, 1)
        self.assertIn("Runtime Verification", stdout.getvalue())

    def test_verify_command_json_output_is_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "verify",
                        "--entrypoint",
                        "mentalmodel.examples.async_rl.demo:build_program",
                        "--runs-dir",
                        tmpdir,
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["success"])
            self.assertEqual(payload["graph_id"], "async_rl_demo")
            self.assertEqual(len(payload["property_checks"]), 2)
            run_dir = Path(payload["runtime"]["run_artifacts_dir"])
            self.assertTrue((run_dir / "verification.json").exists())

    def test_install_skills_dry_run_outputs_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "install-skills",
                        "--agent",
                        "codex",
                        "--target-dir",
                        tmpdir,
                        "--dry-run",
                    ]
                )
            self.assertEqual(exit_code, 0)
            self.assertIn("mentalmodel install-skills dry run", stdout.getvalue())
            self.assertFalse((Path(tmpdir) / "mentalmodel-base" / "SKILL.md").exists())

    def test_demo_command_json_output_includes_run_artifacts_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(["demo", "async-rl", "--runs-dir", tmpdir, "--json"])
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            run_dir = Path(payload["run_artifacts_dir"])
            self.assertTrue(run_dir.exists())
            self.assertTrue((run_dir / "records.jsonl").exists())

    def test_runs_list_command_outputs_materialized_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self._materialize_demo_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(["runs", "list", "--runs-dir", tmpdir, "--json"])
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(len(payload), 1)
            self.assertEqual(payload[0]["graph_id"], "async_rl_demo")

    def test_runs_show_command_json_outputs_latest_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self._materialize_demo_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    ["runs", "show", "--runs-dir", tmpdir, "--graph-id", "async_rl_demo", "--json"]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["graph_id"], "async_rl_demo")
            self.assertIn("records.jsonl", payload["files"]["records"])

    def test_runs_records_command_filters_node(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self._materialize_demo_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "records",
                        "--runs-dir",
                        tmpdir,
                        "--graph-id",
                        "async_rl_demo",
                        "--node-id",
                        "staleness_invariant",
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertGreaterEqual(len(payload), 1)
            self.assertTrue(all(record["node_id"] == "staleness_invariant" for record in payload))

    def test_install_skills_writes_template(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "install-skills",
                        "--agent",
                        "claude",
                        "--target-dir",
                        tmpdir,
                    ]
                )
            self.assertEqual(exit_code, 0)
            skill_file = Path(tmpdir) / "mentalmodel-base" / "SKILL.md"
            plugin_skill = Path(tmpdir) / "mentalmodel-plugin-authoring" / "SKILL.md"
            debug_skill = Path(tmpdir) / "mentalmodel-debugging" / "SKILL.md"
            self.assertTrue(skill_file.exists())
            self.assertTrue(plugin_skill.exists())
            self.assertTrue(debug_skill.exists())
            self.assertIn("mentalmodel Base", skill_file.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
