from __future__ import annotations

import importlib
import unittest

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
        self.assertEqual(len(checks), 1)
        self.assertEqual(checks[0].name, "score maps align with sampled rollouts")
        self.assertTrue(checks[0].hypothesis_backed)

    def test_run_verification_succeeds_for_demo(self) -> None:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        report = run_verification(build_program(), module=module)
        self.assertTrue(report.success)
        self.assertTrue(report.runtime.success)
        self.assertEqual(len(report.property_checks), 1)
        self.assertTrue(report.property_checks[0].success)

    def test_run_verification_reports_runtime_failure(self) -> None:
        module = importlib.import_module("mentalmodel.examples.verification_failure")
        program = module.build_program()
        report = run_verification(program, module=module)
        self.assertFalse(report.success)
        self.assertFalse(report.runtime.success)
        self.assertIn("InvariantViolationError", report.runtime.error or "")
