from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel import Effect, Workflow
from mentalmodel.doctor import DoctorStatus, build_doctor_report
from mentalmodel.skills import install_skills


class _NoopEffect:
    async def invoke(self, inputs, ctx):
        del inputs, ctx
        return {"ok": True}


def build_coarse_program() -> Workflow:
    return Workflow(
        "coarse_program",
        children=[
            Effect("step_one", handler=_NoopEffect()),
            Effect("step_two", handler=_NoopEffect()),
        ],
    )


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
            self.assertEqual(statuses["topology"], DoctorStatus.PASS)
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

    def test_doctor_report_warns_for_coarse_workflow_topology(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir) / "skills"
            install_skills("codex", target_dir=target_dir)
            report = build_doctor_report(
                agent="codex",
                target_dir=target_dir,
                entrypoint=f"{__name__}:build_coarse_program",
            )
            topology_check = next(check for check in report.checks if check.name == "topology")
            self.assertEqual(topology_check.status, DoctorStatus.WARN)
            self.assertEqual(topology_check.details["effect_count"], 2)


if __name__ == "__main__":
    unittest.main()
