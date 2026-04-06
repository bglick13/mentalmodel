from __future__ import annotations

import contextlib
import importlib
import io
import json
import tempfile
import types
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from mentalmodel.cli import build_parser, main
from mentalmodel.core import (
    Actor,
    ActorHandler,
    ActorResult,
    Invariant,
    InvariantChecker,
    InvariantResult,
    Ref,
    Workflow,
)
from mentalmodel.core.interfaces import JsonValue, NamedPrimitive
from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.examples.runtime_environment.demo import (
    build_environment as build_runtime_environment,
)
from mentalmodel.examples.runtime_environment.demo import (
    build_program as build_runtime_environment_program,
)
from mentalmodel.observability.export import write_json, write_jsonl
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.skills import install_skills
from mentalmodel.testing import run_verification


class CliNoOpHandler(ActorHandler[dict[str, object], object, str]):
    async def handle(
        self,
        inputs: dict[str, object],
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[str, object]:
        del inputs, state, ctx
        return ActorResult(output="ok")


class CliWarningInvariant(InvariantChecker[dict[str, object], JsonValue]):
    async def check(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> InvariantResult[JsonValue]:
        del inputs, ctx
        return InvariantResult(
            passed=False,
            details={"reason": "warning failure"},
        )


def build_parameterized_program(
    *,
    group_size: int,
    sampler_lag: int = 0,
) -> Workflow[NamedPrimitive]:
    del sampler_lag
    return Workflow(
        name=f"param_graph_{group_size}",
        children=[
            Actor(name="source", handler=CliNoOpHandler(), inputs=[]),
        ],
    )


class CliTest(unittest.TestCase):
    def _materialize_demo_run(
        self,
        runs_dir: str,
    ) -> str:
        stdout = io.StringIO()
        command = [
            "verify",
            "--entrypoint",
            "mentalmodel.examples.async_rl.demo:build_program",
            "--runs-dir",
            runs_dir,
            "--json",
        ]
        with contextlib.redirect_stdout(stdout):
            exit_code = main(command)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertIsInstance(payload, dict)
        runtime = payload["runtime"]
        self.assertIsInstance(runtime, dict)
        run_id = cast(object, runtime["run_id"])
        self.assertIsInstance(run_id, str)
        return cast(str, run_id)

    def _materialize_parameterized_demo_run(
        self,
        runs_dir: str,
        *,
        group_size: int = 4,
        sampler_lag: int = 0,
        max_off_policy_steps: int = 0,
    ) -> str:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        report = run_verification(
            build_program(
                group_size=group_size,
                sampler_lag=sampler_lag,
                max_off_policy_steps=max_off_policy_steps,
            ),
            module=module,
            runs_dir=Path(runs_dir),
        )
        run_id = report.runtime.run_id
        self.assertIsNotNone(run_id)
        assert run_id is not None
        return run_id

    def _materialize_framed_run(self, runs_dir: str) -> str:
        root = Path(runs_dir)
        run_id = "run-framed"
        run_dir = root / ".runs" / "framed_graph" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        frame_zero = [{"iteration_index": 0, "loop_node_id": "steps"}]
        frame_one = [{"iteration_index": 1, "loop_node_id": "steps"}]
        write_json(
            run_dir / "summary.json",
            {
                "schema_version": 4,
                "graph_id": "framed_graph",
                "run_id": run_id,
                "created_at_ms": 1000,
                "success": True,
                "node_count": 1,
                "edge_count": 0,
                "record_count": 4,
                "output_count": 2,
                "state_count": 0,
                "trace_sink_configured": False,
                "trace_mode": "disk",
                "trace_mirror_to_disk": True,
                "trace_capture_local_spans": True,
                "trace_service_name": "mentalmodel",
            },
        )
        write_jsonl(
            run_dir / "records.jsonl",
            [
                {
                    "record_id": f"{run_id}:1",
                    "run_id": run_id,
                    "node_id": "step_result",
                    "frame_id": "steps[0]",
                    "frame_path": frame_zero,
                    "loop_node_id": "steps",
                    "iteration_index": 0,
                    "event_type": "node.inputs_resolved",
                    "sequence": 1,
                    "timestamp_ms": 1000,
                    "payload": {"inputs": {"item": "a"}},
                },
                {
                    "record_id": f"{run_id}:2",
                    "run_id": run_id,
                    "node_id": "step_result",
                    "frame_id": "steps[1]",
                    "frame_path": frame_one,
                    "loop_node_id": "steps",
                    "iteration_index": 1,
                    "event_type": "node.inputs_resolved",
                    "sequence": 2,
                    "timestamp_ms": 1001,
                    "payload": {"inputs": {"item": "b"}},
                },
            ],
        )
        write_json(
            run_dir / "outputs.json",
            {
                "outputs": {},
                "framed_outputs": [
                    {
                        "node_id": "step_result",
                        "frame_id": "steps[0]",
                        "frame_path": frame_zero,
                        "loop_node_id": "steps",
                        "iteration_index": 0,
                        "value": {"score": 1},
                    },
                    {
                        "node_id": "step_result",
                        "frame_id": "steps[1]",
                        "frame_path": frame_one,
                        "loop_node_id": "steps",
                        "iteration_index": 1,
                        "value": {"score": 2},
                    },
                ],
            },
        )
        write_json(run_dir / "state.json", {"state": {}, "framed_state": []})
        return run_id

    def test_demo_command_parses(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["demo", "async-rl"])
        self.assertEqual(args.command, "demo")
        self.assertEqual(args.name, "async-rl")

    def test_doctor_command_outputs_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir) / "skills"
            install_skills("codex", target_dir=target_dir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "doctor",
                        "--agent",
                        "codex",
                        "--target-dir",
                        str(target_dir),
                        "--entrypoint",
                        "mentalmodel.examples.async_rl.demo:build_program",
                        "--json",
                    ]
                )
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["success"])
        self.assertIn("checks", payload)

    def test_doctor_command_fails_when_skills_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "doctor",
                        "--agent",
                        "codex",
                        "--target-dir",
                        str(Path(tmpdir) / "skills"),
                        "--json",
                    ]
                )
        self.assertEqual(exit_code, 1)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["success"])

    def test_agent_tool_use_demo_command_outputs_summary(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["demo", "agent-tool-use"])
        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("mentalmodel demo summary", rendered)
        self.assertIn("mentalmodel-demo-agent", rendered)

    def test_autoresearch_sorting_demo_command_outputs_json(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["demo", "autoresearch-sorting", "--json"])
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["demo"], "autoresearch-sorting")
        self.assertEqual(payload["objective_name"], "sorting_efficiency")
        self.assertIn("best_candidate", payload)
        self.assertIn("results", payload)

    def test_autoresearch_sorting_demo_writes_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "demo",
                        "autoresearch-sorting",
                        "--write-artifacts",
                        "--output-dir",
                        tmpdir,
                    ]
                )
            self.assertEqual(exit_code, 0)
            self.assertTrue((Path(tmpdir) / "program.md").exists())
            self.assertTrue((Path(tmpdir) / "objective.json").exists())
            self.assertTrue((Path(tmpdir) / "candidates.json").exists())

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

    def test_verify_command_json_reports_warning_invariant_failures(self) -> None:
        program: Workflow[
            Actor[dict[str, object], str, object] | Invariant[dict[str, object], JsonValue]
        ] = Workflow(
            name="warning_cli",
            children=[
                Actor(name="source", handler=CliNoOpHandler(), inputs=[]),
                Invariant(
                    name="warn_check",
                    checker=CliWarningInvariant(),
                    inputs=[Ref("source")],
                    severity="warning",
                ),
            ],
        )
        stdout = io.StringIO()
        module = types.ModuleType("warning_cli_module")
        with patch("mentalmodel.cli.load_workflow_subject", return_value=(module, program)):
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "verify",
                        "--entrypoint",
                        "warning.module:build_program",
                        "--json",
                    ]
                )
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        runtime = payload["runtime"]
        self.assertEqual(
            runtime["warning_invariant_failures"],
            [{"node_id": "warn_check", "severity": "warning"}],
        )
        self.assertEqual(runtime["error_invariant_failures"], [])

    def test_verify_command_accepts_params_json_for_callable_entrypoint(self) -> None:
        stdout = io.StringIO()
        module = types.SimpleNamespace(build_program=build_parameterized_program)
        with patch("mentalmodel.invocation.importlib.import_module", return_value=module):
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "verify",
                        "--entrypoint",
                        "param.module:build_program",
                        "--params-json",
                        '{"group_size": 5, "sampler_lag": 1}',
                        "--json",
                    ]
                )
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["success"])
        self.assertEqual(payload["graph_id"], "param_graph_5")

    def test_verify_command_accepts_params_file_for_callable_entrypoint(self) -> None:
        stdout = io.StringIO()
        module = types.SimpleNamespace(build_program=build_parameterized_program)
        with tempfile.TemporaryDirectory() as tmpdir:
            params_file = Path(tmpdir) / "verify-params.json"
            params_file.write_text('{"group_size": 7}', encoding="utf-8")
            with patch("mentalmodel.invocation.importlib.import_module", return_value=module):
                with contextlib.redirect_stdout(stdout):
                    exit_code = main(
                        [
                            "verify",
                            "--entrypoint",
                            "param.module:build_program",
                            "--params-file",
                            str(params_file),
                            "--json",
                        ]
                    )
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["graph_id"], "param_graph_7")

    def test_verify_command_rejects_non_object_params_json(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "verify",
                    "--entrypoint",
                    "mentalmodel.examples.async_rl.demo:build_program",
                    "--params-json",
                    '["not", "an", "object"]',
                ]
            )
        self.assertEqual(exit_code, 1)
        self.assertIn("JSON object", stdout.getvalue())

    def test_verify_command_rejects_invalid_params_for_callable(self) -> None:
        stdout = io.StringIO()
        module = types.SimpleNamespace(build_program=build_parameterized_program)
        with patch("mentalmodel.invocation.importlib.import_module", return_value=module):
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "verify",
                        "--entrypoint",
                        "param.module:build_program",
                        "--params-json",
                        '{"unknown": 1}',
                    ]
        )
        self.assertEqual(exit_code, 1)
        self.assertIn("Invalid parameters for entrypoint", stdout.getvalue())

    def test_verify_command_rejects_params_for_non_callable_workflow_entrypoint(self) -> None:
        stdout = io.StringIO()
        module = types.SimpleNamespace(
            build_program=Workflow(
                name="prebuilt_graph",
                children=[Actor(name="source", handler=CliNoOpHandler(), inputs=[])],
            )
        )
        with patch("mentalmodel.invocation.importlib.import_module", return_value=module):
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "verify",
                        "--entrypoint",
                        "param.module:build_program",
                        "--params-json",
                        '{"group_size": 5}',
                    ]
                )
        self.assertEqual(exit_code, 1)
        self.assertIn("non-callable Workflow", stdout.getvalue())

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

    def test_verify_command_supports_environment_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "verify",
                        "--entrypoint",
                        "mentalmodel.examples.runtime_environment.demo:build_program",
                        "--environment-entrypoint",
                        "mentalmodel.examples.runtime_environment.demo:build_environment",
                        "--invocation-name",
                        "runtime_environment_demo",
                        "--runs-dir",
                        tmpdir,
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["success"])
            self.assertEqual(
                payload["runtime"]["invocation_name"],
                "runtime_environment_demo",
            )
            run_dir = Path(cast(str, payload["runtime"]["run_artifacts_dir"]))
            self.assertTrue((run_dir / "summary.json").exists())

    def test_verify_command_supports_toml_spec(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runs_dir = Path(tmpdir) / "artifacts"
            spec_path = Path(tmpdir) / "runtime-environment.toml"
            spec_path.write_text(
                "\n".join(
                    (
                        "[program]",
                        "entrypoint = "
                        '"mentalmodel.examples.runtime_environment.demo:build_program"',
                        "",
                        "[environment]",
                        "entrypoint = "
                        '"mentalmodel.examples.runtime_environment.demo:build_environment"',
                        "",
                        "[runtime]",
                        'invocation_name = "spec_runtime_environment"',
                        f'runs_dir = "{runs_dir.name}"',
                        "",
                    )
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(["verify", "--spec", str(spec_path), "--json"])
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(
                payload["runtime"]["invocation_name"],
                "spec_runtime_environment",
            )
            self.assertTrue((runs_dir / ".runs").exists())

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
            self.assertIn("schema_version", payload[0])

    def test_runs_list_command_filters_by_invocation_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_verification(
                build_runtime_environment_program(),
                runs_dir=root,
                environment=build_runtime_environment(),
                invocation_name="runtime_environment_demo",
            )
            run_verification(
                build_program(),
                module=importlib.import_module("mentalmodel.examples.async_rl.demo"),
                runs_dir=root,
                invocation_name="async_rl_demo",
            )
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "list",
                        "--runs-dir",
                        tmpdir,
                        "--invocation-name",
                        "runtime_environment_demo",
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(len(payload), 1)
            self.assertEqual(payload[0]["invocation_name"], "runtime_environment_demo")

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
            self.assertIn("schema_version", payload)
            self.assertIn("records.jsonl", payload["files"]["records"])

    def test_runs_latest_command_json_outputs_latest_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self._materialize_demo_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "latest",
                        "--runs-dir",
                        tmpdir,
                        "--graph-id",
                        "async_rl_demo",
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["graph_id"], "async_rl_demo")
            self.assertIn("schema_version", payload)
            self.assertIn("run_id", payload)

    def test_replay_command_json_outputs_semantic_timeline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_id = self._materialize_demo_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "replay",
                        "--runs-dir",
                        tmpdir,
                        "--graph-id",
                        "async_rl_demo",
                        "--run-id",
                        run_id,
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["run_id"], run_id)
            self.assertGreaterEqual(len(payload["events"]), 1)
            self.assertGreaterEqual(len(payload["node_summaries"]), 1)
            staleness = next(
                summary
                for summary in payload["node_summaries"]
                if summary["node_id"] == "staleness_invariant"
            )
            self.assertEqual(staleness["invariant_status"], "pass")
            self.assertEqual(staleness["invariant_severity"], "error")

    def test_runs_outputs_command_json_returns_node_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self._materialize_demo_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "outputs",
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
            self.assertEqual(payload["node_id"], "staleness_invariant")
            self.assertIn("output", payload)

    def test_runs_trace_command_json_returns_trace_for_node(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self._materialize_demo_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "trace",
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
            self.assertEqual(payload["node_id"], "staleness_invariant")
            self.assertGreaterEqual(len(payload["records"]), 1)

    def test_runs_outputs_command_json_supports_frame_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self._materialize_framed_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "outputs",
                        "--runs-dir",
                        tmpdir,
                        "--graph-id",
                        "framed_graph",
                        "--node-id",
                        "step_result",
                        "--frame-id",
                        "steps[1]",
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["frame_id"], "steps[1]")
            self.assertEqual(payload["output"], {"score": 2})

    def test_replay_command_json_supports_frame_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_id = self._materialize_framed_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "replay",
                        "--runs-dir",
                        tmpdir,
                        "--graph-id",
                        "framed_graph",
                        "--run-id",
                        run_id,
                        "--frame-id",
                        "steps[0]",
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["frame_ids"], ["steps[0]"])
            self.assertEqual(len(payload["events"]), 1)

    def test_runs_inputs_command_returns_node_input_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            self._materialize_demo_run(tmpdir)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "inputs",
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
            self.assertEqual(payload["node_id"], "staleness_invariant")
            self.assertEqual(payload["inputs"]["rollout_join"]["current_policy_version"], 3)

    def test_runs_diff_command_json_reports_changed_nodes_and_invariants(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_a = self._materialize_parameterized_demo_run(tmpdir, group_size=2)
            run_b = self._materialize_parameterized_demo_run(tmpdir, sampler_lag=2)

            diff_stdout = io.StringIO()
            with contextlib.redirect_stdout(diff_stdout):
                exit_code = main(
                    [
                        "runs",
                        "diff",
                        "--runs-dir",
                        tmpdir,
                        "--graph-id",
                        "async_rl_demo",
                        "--run-a",
                        run_a,
                        "--run-b",
                        run_b,
                        "--invariant",
                        "staleness_invariant",
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(diff_stdout.getvalue())
            self.assertEqual(payload["graph_id"], "async_rl_demo")
            self.assertEqual(len(payload["invariant_diffs"]), 1)
            self.assertEqual(payload["invariant_diffs"][0]["node_id"], "staleness_invariant")
            self.assertTrue(payload["invariant_diffs"][0]["outcome_run_a"])
            self.assertFalse(payload["invariant_diffs"][0]["outcome_run_b"])
            self.assertEqual(payload["invariant_diffs"][0]["severity_run_a"], "error")
            self.assertEqual(payload["invariant_diffs"][0]["severity_run_b"], "error")

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

    def test_runs_repair_command_json_reports_legacy_bundle_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / ".runs" / "legacy_graph" / "run-legacy"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "graph_id": "legacy_graph",
                        "run_id": "run-legacy",
                        "success": True,
                        "node_count": 1,
                        "edge_count": 0,
                        "record_count": 0,
                        "output_count": 0,
                        "state_count": 0,
                        "trace_sink_configured": False,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "repair",
                        "--runs-dir",
                        tmpdir,
                        "--graph-id",
                        "legacy_graph",
                        "--dry-run",
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["action_count"] >= 1)
            self.assertEqual(payload["actions"][0]["graph_id"], "legacy_graph")

    def test_runs_repair_command_applies_legacy_bundle_fix(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / ".runs" / "legacy_graph" / "run-legacy"
            run_dir.mkdir(parents=True, exist_ok=True)
            summary_path = run_dir / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "graph_id": "legacy_graph",
                        "run_id": "run-legacy",
                        "success": True,
                        "node_count": 1,
                        "edge_count": 0,
                        "record_count": 0,
                        "output_count": 0,
                        "state_count": 0,
                        "trace_sink_configured": False,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "runs",
                        "repair",
                        "--runs-dir",
                        tmpdir,
                        "--graph-id",
                        "legacy_graph",
                    ]
                )
            self.assertEqual(exit_code, 0)
            repaired = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertIn("schema_version", repaired)
            self.assertIn("created_at_ms", repaired)

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
