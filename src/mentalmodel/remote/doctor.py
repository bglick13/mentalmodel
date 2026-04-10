from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from mentalmodel.doctor import DoctorCheck, DoctorReport, DoctorStatus
from mentalmodel.remote.bootstrap import build_remote_doctor_report as build_local_stack_report
from mentalmodel.remote.project_config import (
    MentalModelProjectConfig,
    ProjectConfigError,
    discover_project_config_path,
    load_project_config,
)
from mentalmodel.remote.projects import fetch_remote_project_status
from mentalmodel.runtime.runs import list_run_summaries


class RemoteDoctorMode(StrEnum):
    HOSTED = "hosted"
    LOCAL = "local"


@dataclass(slots=True, frozen=True)
class RemoteDoctorResult:
    mode: RemoteDoctorMode
    report: DoctorReport

    @property
    def success(self) -> bool:
        return self.report.success

    def as_dict(self) -> dict[str, object]:
        payload = self.report.as_dict()
        payload["mode"] = self.mode.value
        return payload


def build_remote_mode_doctor_report(
    *,
    config: Path | None = None,
    workspace_config: Path | None = None,
    runs_dir: Path | None = None,
) -> RemoteDoctorResult:
    if config is not None and workspace_config is not None:
        raise ProjectConfigError(
            "Choose either hosted repo mode (--config) or local stack mode "
            "(--workspace-config), not both."
        )
    if workspace_config is not None:
        return RemoteDoctorResult(
            mode=RemoteDoctorMode.LOCAL,
            report=build_local_stack_report(
                workspace_config=workspace_config,
                runs_dir=runs_dir,
            ),
        )

    resolved_config = config
    if resolved_config is None:
        resolved_config = discover_project_config_path()
    if resolved_config is None:
        raise ProjectConfigError(
            "Could not find mentalmodel.toml. Run "
            "`mentalmodel remote doctor --workspace-config ...` "
            "for the generated local stack, or run this command from a repo-linked project."
        )
    return RemoteDoctorResult(
        mode=RemoteDoctorMode.HOSTED,
        report=_build_hosted_report(load_project_config(resolved_config)),
    )


def _build_hosted_report(config: MentalModelProjectConfig) -> DoctorReport:
    checks = (
        _config_check(config),
        _api_key_check(config),
        _catalog_check(config),
        _default_spec_check(config),
        _runs_dir_check(config),
        _remote_link_check(config),
    )
    return DoctorReport(checks=checks)


def _config_check(config: MentalModelProjectConfig) -> DoctorCheck:
    return DoctorCheck(
        name="project_config",
        status=DoctorStatus.PASS,
        message="Repo-linked project config loaded successfully.",
        details={
            "config_path": str(config.config_path),
            "project_id": config.project_id,
            "server_url": config.server_url,
            "catalog_provider": config.catalog_provider,
        },
    )


def _api_key_check(config: MentalModelProjectConfig) -> DoctorCheck:
    try:
        config.resolve_api_key()
    except ProjectConfigError as exc:
        return DoctorCheck(
            name="api_key",
            status=DoctorStatus.FAIL,
            message=str(exc),
            details={"api_key_env": config.api_key_env},
        )
    return DoctorCheck(
        name="api_key",
        status=DoctorStatus.PASS,
        message="Remote API credentials resolved successfully.",
        details={"api_key_env": config.api_key_env},
    )


def _catalog_check(config: MentalModelProjectConfig) -> DoctorCheck:
    try:
        snapshot = config.build_catalog_snapshot(force=True)
    except Exception as exc:
        return DoctorCheck(
            name="catalog_snapshot",
            status=DoctorStatus.FAIL,
            message=str(exc),
            details={"catalog_provider": config.catalog_provider},
        )
    assert snapshot is not None
    return DoctorCheck(
        name="catalog_snapshot",
        status=DoctorStatus.PASS,
        message="Catalog snapshot resolves for hosted publication.",
        details={
            "catalog_provider": config.catalog_provider,
            "catalog_entry_count": len(snapshot.entries),
            "default_entry_id": snapshot.default_entry_id,
        },
    )


def _default_spec_check(config: MentalModelProjectConfig) -> DoctorCheck:
    if config.default_verify_spec is None:
        return DoctorCheck(
            name="default_spec",
            status=DoctorStatus.WARN,
            message="No default verify spec is configured.",
            details={},
        )
    return DoctorCheck(
        name="default_spec",
        status=DoctorStatus.PASS,
        message="Default verify spec is configured.",
        details={"default_verify_spec": str(config.default_verify_spec)},
    )


def _runs_dir_check(config: MentalModelProjectConfig) -> DoctorCheck:
    if config.default_runs_dir is None:
        return DoctorCheck(
            name="runs_dir",
            status=DoctorStatus.WARN,
            message="No default runs directory is configured.",
            details={},
        )
    runs_dir = config.default_runs_dir
    if not runs_dir.exists():
        return DoctorCheck(
            name="runs_dir",
            status=DoctorStatus.WARN,
            message="Configured runs directory does not exist yet.",
            details={"runs_dir": str(runs_dir), "run_count": 0},
        )
    summaries = list_run_summaries(runs_dir=runs_dir)
    return DoctorCheck(
        name="runs_dir",
        status=DoctorStatus.PASS,
        message="Configured runs directory is readable.",
        details={
            "runs_dir": str(runs_dir),
            "run_count": len(summaries),
            "graphs": sorted({summary.graph_id for summary in summaries}),
        },
    )


def _remote_link_check(config: MentalModelProjectConfig) -> DoctorCheck:
    try:
        project = fetch_remote_project_status(config)
    except Exception as exc:
        return DoctorCheck(
            name="remote_link",
            status=DoctorStatus.FAIL,
            message=(
                "Remote project status is unavailable. "
                "Run `mentalmodel remote link` to create or refresh the hosted project record."
            ),
            details={
                "project_id": config.project_id,
                "server_url": config.server_url,
                "error": str(exc),
            },
        )
    return DoctorCheck(
        name="remote_link",
        status=DoctorStatus.PASS,
        message="Hosted project record is reachable.",
        details={
            "project_id": project.project_id,
            "catalog_published": project.catalog_published,
            "catalog_entry_count": project.catalog_entry_count,
            "last_completed_run_id": project.last_completed_run_id,
        },
    )
