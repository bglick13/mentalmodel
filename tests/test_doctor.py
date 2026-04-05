from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel.doctor import DoctorStatus, build_doctor_report
from mentalmodel.skills import install_skills


class DoctorTest(unittest.TestCase):
    def test_doctor_report_succeeds_with_installed_skills_and_valid_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir) / "skills"
            runs_dir = Path(tmpdir) / "runs"
            install_skills("codex", target_dir=target_dir)
            report = build_doctor_report(
                agent="codex",
                target_dir=target_dir,
                runs_dir=runs_dir,
                entrypoint="mentalmodel.examples.async_rl.demo:build_program",
            )
            self.assertTrue(report.success)
            self.assertEqual(report.fail_count, 0)
            statuses = {check.name: check.status for check in report.checks}
            self.assertEqual(statuses["skills"], DoctorStatus.PASS)
            self.assertEqual(statuses["entrypoint"], DoctorStatus.PASS)
            self.assertEqual(statuses["runs"], DoctorStatus.WARN)
            self.assertEqual(statuses["tracing"], DoctorStatus.PASS)
            self.assertEqual(statuses["package_data"], DoctorStatus.PASS)

    def test_doctor_report_fails_when_skills_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report = build_doctor_report(
                agent="codex",
                target_dir=Path(tmpdir) / "skills",
            )
            self.assertFalse(report.success)
            skills_check = next(check for check in report.checks if check.name == "skills")
            self.assertEqual(skills_check.status, DoctorStatus.FAIL)
            self.assertIn("missing_skills", skills_check.details)

    def test_doctor_report_fails_for_bad_entrypoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir) / "skills"
            install_skills("codex", target_dir=target_dir)
            report = build_doctor_report(
                agent="codex",
                target_dir=target_dir,
                entrypoint="math:pi",
            )
            self.assertFalse(report.success)
            entrypoint_check = next(check for check in report.checks if check.name == "entrypoint")
            self.assertEqual(entrypoint_check.status, DoctorStatus.FAIL)


if __name__ == "__main__":
    unittest.main()
