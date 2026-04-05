from __future__ import annotations

import importlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import ModuleType
from typing import cast

from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.workflow import Workflow
from mentalmodel.errors import EntrypointLoadError, MentalModelError
from mentalmodel.observability import (
    load_tracing_config,
)
from mentalmodel.observability import (
    template_dir as otel_template_dir,
)
from mentalmodel.runtime.runs import list_run_summaries
from mentalmodel.skills.installer import (
    SUPPORTED_SKILLS,
    default_target_dir,
    template_path_for_agent,
)


class DoctorStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


@dataclass(slots=True, frozen=True)
class DoctorCheck:
    name: str
    status: DoctorStatus
    message: str
    details: dict[str, object]


@dataclass(slots=True, frozen=True)
class DoctorReport:
    checks: tuple[DoctorCheck, ...]

    @property
    def fail_count(self) -> int:
        return sum(1 for check in self.checks if check.status is DoctorStatus.FAIL)

    @property
    def warn_count(self) -> int:
        return sum(1 for check in self.checks if check.status is DoctorStatus.WARN)

    @property
    def success(self) -> bool:
        return self.fail_count == 0

    def as_dict(self) -> dict[str, object]:
        return {
            "success": self.success,
            "fail_count": self.fail_count,
            "warn_count": self.warn_count,
            "checks": [
                {
                    "name": check.name,
                    "status": check.status.value,
                    "message": check.message,
                    "details": check.details,
                }
                for check in self.checks
            ],
        }


def build_doctor_report(
    *,
    agent: str = "codex",
    target_dir: Path | None = None,
    runs_dir: Path | None = None,
    entrypoint: str | None = None,
) -> DoctorReport:
    checks = (
        _check_skill_installation(agent=agent, target_dir=target_dir),
        _check_entrypoint(entrypoint),
        _check_runs_dir(runs_dir),
        _check_tracing_config(),
        _check_package_data(),
    )
    return DoctorReport(checks=checks)


def _check_skill_installation(*, agent: str, target_dir: Path | None) -> DoctorCheck:
    resolved_dir = target_dir or default_target_dir(agent)
    missing: list[str] = []
    installed: list[str] = []
    for skill_name in SUPPORTED_SKILLS:
        skill_path = resolved_dir / skill_name / "SKILL.md"
        if skill_path.exists():
            installed.append(skill_name)
        else:
            missing.append(skill_name)
    if missing:
        return DoctorCheck(
            name="skills",
            status=DoctorStatus.FAIL,
            message="Missing packaged skill installs.",
            details={
                "agent": agent,
                "target_dir": str(resolved_dir),
                "installed_skills": installed,
                "missing_skills": missing,
            },
        )
    return DoctorCheck(
        name="skills",
        status=DoctorStatus.PASS,
        message="All packaged skills are installed.",
        details={
            "agent": agent,
            "target_dir": str(resolved_dir),
            "installed_skills": installed,
        },
    )


def _check_entrypoint(entrypoint: str | None) -> DoctorCheck:
    if entrypoint is None:
        return DoctorCheck(
            name="entrypoint",
            status=DoctorStatus.SKIP,
            message="No entrypoint requested.",
            details={},
        )
    try:
        module, workflow = _load_entrypoint(entrypoint)
    except EntrypointLoadError as exc:
        return DoctorCheck(
            name="entrypoint",
            status=DoctorStatus.FAIL,
            message=str(exc),
            details={"entrypoint": entrypoint},
        )
    return DoctorCheck(
        name="entrypoint",
        status=DoctorStatus.PASS,
        message="Entrypoint resolves to a Workflow.",
        details={
            "entrypoint": entrypoint,
            "module": module.__name__,
            "workflow_name": workflow.name,
        },
    )


def _check_runs_dir(runs_dir: Path | None) -> DoctorCheck:
    resolved_dir = runs_dir or (Path.cwd() / ".runs")
    if not resolved_dir.exists():
        return DoctorCheck(
            name="runs",
            status=DoctorStatus.WARN,
            message="Runs directory does not exist yet.",
            details={"runs_dir": str(resolved_dir), "run_count": 0},
        )
    summaries = list_run_summaries(runs_dir=resolved_dir)
    if not summaries:
        return DoctorCheck(
            name="runs",
            status=DoctorStatus.WARN,
            message="Runs directory exists but contains no persisted runs.",
            details={"runs_dir": str(resolved_dir), "run_count": 0},
        )
    graphs = sorted({summary.graph_id for summary in summaries})
    return DoctorCheck(
        name="runs",
        status=DoctorStatus.PASS,
        message="Runs directory is readable.",
        details={
            "runs_dir": str(resolved_dir),
            "run_count": len(summaries),
            "graphs": graphs,
        },
    )


def _check_tracing_config() -> DoctorCheck:
    try:
        config = load_tracing_config(service_name="mentalmodel-doctor")
    except MentalModelError as exc:
        return DoctorCheck(
            name="tracing",
            status=DoctorStatus.FAIL,
            message=str(exc),
            details={},
        )
    return DoctorCheck(
        name="tracing",
        status=DoctorStatus.PASS,
        message="Tracing configuration resolved successfully.",
        details=cast(dict[str, object], dict(config.summary())),
    )


def _check_package_data() -> DoctorCheck:
    missing: list[str] = []
    for agent in ("codex", "claude"):
        for skill_name in SUPPORTED_SKILLS:
            template_path = template_path_for_agent(agent, skill_name)
            if not template_path.exists():
                missing.append(str(template_path))
    for filename in (
        "docker-compose.otel-lgtm.yml",
        "docker-compose.otel-jaeger.yml",
        "mentalmodel.otel.env",
        "mentalmodel.otel.jaeger.env",
        "OTEL-DEMO.md",
    ):
        template_path = otel_template_dir() / filename
        if not template_path.exists():
            missing.append(str(template_path))
    if missing:
        return DoctorCheck(
            name="package_data",
            status=DoctorStatus.FAIL,
            message="Packaged templates are missing.",
            details={"missing_paths": missing},
        )
    return DoctorCheck(
        name="package_data",
        status=DoctorStatus.PASS,
        message="Packaged templates are present.",
        details={},
    )


def _load_entrypoint(raw: str) -> tuple[ModuleType, Workflow[NamedPrimitive]]:
    if ":" not in raw:
        raise EntrypointLoadError(
            "Entrypoint must be in the format 'module.submodule:function_name'."
        )
    module_name, attribute_name = raw.split(":", 1)
    if not module_name or not attribute_name:
        raise EntrypointLoadError("Entrypoint must include both a module and an attribute name.")
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # pragma: no cover - parity with CLI path.
        raise EntrypointLoadError(f"Failed to import module {module_name!r}: {exc}") from exc
    try:
        attribute = getattr(module, attribute_name)
    except AttributeError as exc:
        raise EntrypointLoadError(
            f"Module {module_name!r} does not define {attribute_name!r}."
        ) from exc
    loaded = attribute() if callable(attribute) else attribute
    if not isinstance(loaded, Workflow):
        raise EntrypointLoadError(
            f"Entrypoint {raw!r} must resolve to a Workflow, got {type(loaded).__name__}."
        )
    return module, loaded
