from __future__ import annotations

import importlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import ModuleType
from typing import cast

from mentalmodel import Actor, Block, Effect, Invariant, Join, Parallel, StepLoop, Use
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
    skill_check = _check_skill_installation(agent=agent, target_dir=target_dir)
    entrypoint_check, loaded_workflow = _check_entrypoint(entrypoint)
    topology_check = _check_workflow_topology(entrypoint, loaded_workflow)
    checks = (
        skill_check,
        entrypoint_check,
        topology_check,
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


def _check_entrypoint(
    entrypoint: str | None,
) -> tuple[DoctorCheck, Workflow[NamedPrimitive] | None]:
    if entrypoint is None:
        return (
            DoctorCheck(
                name="entrypoint",
                status=DoctorStatus.SKIP,
                message="No entrypoint requested.",
                details={},
            ),
            None,
        )
    try:
        module, workflow = _load_entrypoint(entrypoint)
    except EntrypointLoadError as exc:
        return (
            DoctorCheck(
                name="entrypoint",
                status=DoctorStatus.FAIL,
                message=str(exc),
                details={"entrypoint": entrypoint},
            ),
            None,
        )
    return (
        DoctorCheck(
            name="entrypoint",
            status=DoctorStatus.PASS,
            message="Entrypoint resolves to a Workflow.",
            details={
                "entrypoint": entrypoint,
                "module": module.__name__,
                "workflow_name": workflow.name,
            },
        ),
        workflow,
    )


def _check_workflow_topology(
    entrypoint: str | None,
    workflow: Workflow[NamedPrimitive] | None,
) -> DoctorCheck:
    if entrypoint is None:
        return DoctorCheck(
            name="topology",
            status=DoctorStatus.SKIP,
            message="No entrypoint requested.",
            details={},
        )
    if workflow is None:
        return DoctorCheck(
            name="topology",
            status=DoctorStatus.SKIP,
            message="Topology check skipped because the entrypoint did not resolve.",
            details={"entrypoint": entrypoint},
        )
    summary = _summarize_workflow_topology(workflow)
    if _is_coarse_effect_heavy_topology(summary):
        return DoctorCheck(
            name="topology",
            status=DoctorStatus.WARN,
            message=(
                "Workflow topology is very coarse. If effects encapsulate batching, "
                "retries, or multi-phase orchestration, spans and records will be too coarse."
            ),
            details={"entrypoint": entrypoint, **summary},
        )
    return DoctorCheck(
        name="topology",
        status=DoctorStatus.PASS,
        message="Workflow topology has explicit executable structure.",
        details={"entrypoint": entrypoint, **summary},
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


def _summarize_workflow_topology(
    workflow: Workflow[NamedPrimitive],
) -> dict[str, object]:
    counts: dict[str, int] = {
        "workflow_count": 1,
        "effect_count": 0,
        "join_count": 0,
        "invariant_count": 0,
        "actor_count": 0,
        "step_loop_count": 0,
        "parallel_count": 0,
        "use_count": 0,
        "block_count": 0,
        "runtime_context_count": 0,
        "max_depth": 0,
    }
    _walk_primitive(workflow, counts=counts, depth=0)
    counts["executable_count"] = (
        counts["effect_count"]
        + counts["join_count"]
        + counts["invariant_count"]
        + counts["actor_count"]
        + counts["step_loop_count"]
    )
    return cast(dict[str, object], counts)


def _walk_primitive(
    primitive: object,
    *,
    counts: dict[str, int],
    depth: int,
) -> None:
    counts["max_depth"] = max(counts["max_depth"], depth)
    if isinstance(primitive, Effect):
        counts["effect_count"] += 1
        return
    if isinstance(primitive, Join):
        counts["join_count"] += 1
        return
    if isinstance(primitive, Invariant):
        counts["invariant_count"] += 1
        return
    if isinstance(primitive, Actor):
        counts["actor_count"] += 1
        return
    if isinstance(primitive, StepLoop):
        counts["step_loop_count"] += 1
        _walk_primitive(primitive.body, counts=counts, depth=depth + 1)
        return
    if isinstance(primitive, Use):
        counts["use_count"] += 1
        _walk_primitive(primitive.block, counts=counts, depth=depth + 1)
        return
    if isinstance(primitive, Block):
        counts["block_count"] += 1
        for child in primitive.children:
            _walk_primitive(child, counts=counts, depth=depth + 1)
        return
    if isinstance(primitive, Parallel):
        counts["parallel_count"] += 1
        for child in primitive.children:
            _walk_primitive(child, counts=counts, depth=depth + 1)
        return
    children = getattr(primitive, "children", None)
    if isinstance(children, (tuple, list)):
        if type(primitive).__name__ == "RuntimeContext":
            counts["runtime_context_count"] += 1
        for child in children:
            _walk_primitive(child, counts=counts, depth=depth + 1)


def _is_coarse_effect_heavy_topology(summary: dict[str, object]) -> bool:
    executable_count = _require_summary_count(summary, "executable_count")
    effect_count = _require_summary_count(summary, "effect_count")
    step_loop_count = _require_summary_count(summary, "step_loop_count")
    use_count = _require_summary_count(summary, "use_count")
    block_count = _require_summary_count(summary, "block_count")
    actor_count = _require_summary_count(summary, "actor_count")
    parallel_count = _require_summary_count(summary, "parallel_count")
    return (
        executable_count <= 4
        and effect_count >= max(1, executable_count - 1)
        and step_loop_count == 0
        and use_count == 0
        and block_count == 0
        and actor_count == 0
        and parallel_count == 0
    )


def _require_summary_count(summary: dict[str, object], key: str) -> int:
    value = summary.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"Doctor topology summary field {key!r} must be an integer.")
    return value


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
