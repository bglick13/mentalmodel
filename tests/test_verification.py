from __future__ import annotations

import importlib
import tempfile
import unittest
from pathlib import Path

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.testing import discover_property_checks, execute_program, run_verification


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
