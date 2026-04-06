from __future__ import annotations

import importlib
import tempfile
import unittest
from pathlib import Path

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
from mentalmodel.core.interfaces import JsonValue
from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.testing import discover_property_checks, execute_program, run_verification


class VerificationNoOpHandler(ActorHandler[dict[str, object], object, str]):
    async def handle(
        self,
        inputs: dict[str, object],
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[str, object]:
        del inputs, state, ctx
        return ActorResult(output="ok")


class VerificationWarningInvariant(InvariantChecker[dict[str, object], JsonValue]):
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


class VerificationTest(unittest.TestCase):
    def test_execute_program_runs_demo(self) -> None:
        result = execute_program(build_program())
        self.assertIn("refresh_sampler", result.outputs)
        self.assertGreater(len(result.records), 0)

    def test_discover_property_checks_finds_demo_check(self) -> None:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        checks = discover_property_checks(module)
        self.assertEqual(len(checks), 2)
        self.assertEqual(
            [check.name for check in checks],
            [
                "score maps align with sampled rollouts",
                "staleness invariant respects configured off-policy budget",
            ],
        )
        self.assertTrue(all(check.hypothesis_backed for check in checks))

    def test_run_verification_succeeds_for_demo(self) -> None:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        with tempfile.TemporaryDirectory() as tmpdir:
            report = run_verification(build_program(), module=module, runs_dir=Path(tmpdir))
            self.assertTrue(report.success)
            self.assertTrue(report.runtime.success)
            self.assertEqual(len(report.property_checks), 2)
            self.assertTrue(all(check.success for check in report.property_checks))
            self.assertIsNotNone(report.runtime.run_artifacts_dir)
            run_dir = Path(report.runtime.run_artifacts_dir or "")
            self.assertTrue((run_dir / "verification.json").exists())
            self.assertTrue((run_dir / "records.jsonl").exists())
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "otel-spans.jsonl").exists())

    def test_run_verification_reports_runtime_failure(self) -> None:
        module = importlib.import_module("mentalmodel.examples.verification_failure")
        program = module.build_program()
        with tempfile.TemporaryDirectory() as tmpdir:
            report = run_verification(program, module=module, runs_dir=Path(tmpdir))
            self.assertFalse(report.success)
            self.assertFalse(report.runtime.success)
            self.assertIn("InvariantViolationError", report.runtime.error or "")
            self.assertIsNotNone(report.runtime.run_artifacts_dir)
            run_dir = Path(report.runtime.run_artifacts_dir or "")
            self.assertTrue((run_dir / "verification.json").exists())
            self.assertTrue((run_dir / "records.jsonl").exists())

    def test_run_verification_surfaces_warning_invariant_failures_without_failing(self) -> None:
        program: Workflow[
            Actor[dict[str, object], str, object] | Invariant[dict[str, object], JsonValue]
        ] = Workflow(
            name="warning_verification",
            children=[
                Actor(name="source", handler=VerificationNoOpHandler(), inputs=[]),
                Invariant(
                    name="warn_check",
                    checker=VerificationWarningInvariant(),
                    inputs=[Ref("source")],
                    severity="warning",
                ),
            ],
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            report = run_verification(program, runs_dir=Path(tmpdir))
            self.assertTrue(report.success)
            self.assertTrue(report.runtime.success)
            self.assertEqual(len(report.runtime.warning_invariant_failures), 1)
            self.assertEqual(report.runtime.warning_invariant_failures[0].node_id, "warn_check")
            self.assertEqual(report.runtime.warning_invariant_failures[0].severity, "warning")
            self.assertEqual(report.runtime.error_invariant_failures, ())
