from __future__ import annotations

import json
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from mentalmodel.remote.contracts import (
    CatalogSource,
    ProjectRegistration,
    RemoteContractError,
    WorkspaceConfig,
    validate_workspace_config,
)


def load_workspace_config(path: Path) -> WorkspaceConfig:
    """Load one workspace registry TOML file."""

    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise RemoteContractError(f"Workspace config not found: {resolved}")
    payload = tomllib.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RemoteContractError("Workspace config must decode to a TOML table.")
    workspace_id = _required_str(payload, "workspace_id")
    label = _required_str(payload, "label")
    description = _optional_str(payload, "description") or ""
    projects_payload = payload.get("projects", [])
    if not isinstance(projects_payload, list):
        raise RemoteContractError("Workspace config 'projects' must be an array of tables.")
    workspace = WorkspaceConfig(
        workspace_id=workspace_id,
        label=label,
        description=description,
        projects=tuple(_project_from_payload(item) for item in projects_payload),
    )
    return validate_workspace_config(workspace)


def write_workspace_config(path: Path, workspace: WorkspaceConfig) -> Path:
    """Write one workspace registry TOML file with stable formatting."""

    validate_workspace_config(workspace)
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(_encode_workspace_toml(workspace), encoding="utf-8")
    return resolved


def upsert_project_registration(
    workspace: WorkspaceConfig,
    project: ProjectRegistration,
) -> WorkspaceConfig:
    """Insert or replace one project registration by id."""

    projects: list[ProjectRegistration] = []
    replaced = False
    for existing in workspace.projects:
        if existing.project_id == project.project_id:
            projects.append(project)
            replaced = True
            continue
        projects.append(existing)
    if not replaced:
        projects.append(project)
    return WorkspaceConfig(
        workspace_id=workspace.workspace_id,
        label=workspace.label,
        description=workspace.description,
        projects=tuple(projects),
    )


def find_project_registration(
    workspace: WorkspaceConfig,
    project_id: str,
) -> ProjectRegistration:
    """Resolve one project registration by id."""

    for project in workspace.projects:
        if project.project_id == project_id:
            return project
    raise RemoteContractError(f"Unknown workspace project {project_id!r}.")


@dataclass(slots=True, frozen=True)
class ProjectRunTarget:
    """Resolved output routing for one launched run."""

    runs_dir: Path | None
    project_id: str | None = None
    project_label: str | None = None
    environment_name: str | None = None
    catalog_entry_id: str | None = None
    catalog_source: CatalogSource | None = None


def find_project_registration_for_path(
    projects: Sequence[ProjectRegistration],
    spec_path: Path,
) -> ProjectRegistration | None:
    """Resolve the owning project for one verify spec path."""

    resolved = spec_path.expanduser().resolve()
    for project in projects:
        root_dir = project.root_dir.expanduser().resolve()
        try:
            resolved.relative_to(root_dir)
        except ValueError:
            continue
        return project
    return None


def build_project_run_target(
    *,
    project: ProjectRegistration | None,
    fallback_runs_dir: Path | None,
    catalog_entry_id: str | None = None,
    catalog_source: CatalogSource | str | None = None,
) -> ProjectRunTarget:
    """Build output-routing metadata for one project-scoped or ad hoc launch."""

    resolved_source = (
        None
        if catalog_source in (None, "")
        else (
            catalog_source
            if isinstance(catalog_source, CatalogSource)
            else CatalogSource(cast(str, catalog_source))
        )
    )
    if project is None:
        return ProjectRunTarget(
            runs_dir=(
                None
                if fallback_runs_dir is None
                else fallback_runs_dir.expanduser().resolve()
            ),
            catalog_entry_id=catalog_entry_id,
            catalog_source=resolved_source,
        )
    if project.runs_dir is None:
        raise RemoteContractError(
            f"Project {project.project_id!r} must declare runs_dir for shared-stack launches."
        )
    return ProjectRunTarget(
        runs_dir=(project.runs_dir.expanduser().resolve()),
        project_id=project.project_id,
        project_label=project.label,
        environment_name=project.default_environment,
        catalog_entry_id=catalog_entry_id,
        catalog_source=resolved_source,
    )


def _project_from_payload(payload: object) -> ProjectRegistration:
    if not isinstance(payload, dict):
        raise RemoteContractError("Each workspace project entry must be a TOML table.")
    root_dir = _required_str(payload, "root_dir")
    runs_dir = _optional_str(payload, "runs_dir")
    tags = payload.get("tags", [])
    if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
        raise RemoteContractError("Workspace project tags must be an array of strings.")
    enabled = payload.get("enabled", True)
    if not isinstance(enabled, bool):
        raise RemoteContractError("Workspace project enabled must be a boolean.")
    return ProjectRegistration(
        project_id=_required_str(payload, "project_id"),
        label=_required_str(payload, "label"),
        root_dir=Path(root_dir).expanduser().resolve(),
        catalog_provider=_optional_str(payload, "catalog_provider"),
        runs_dir=(
            None
            if runs_dir is None
            else Path(runs_dir).expanduser().resolve()
        ),
        description=_optional_str(payload, "description") or "",
        tags=tuple(tags),
        default_environment=_optional_str(payload, "default_environment"),
        enabled=enabled,
    )


def _encode_workspace_toml(workspace: WorkspaceConfig) -> str:
    lines = [
        f"workspace_id = {json.dumps(workspace.workspace_id)}",
        f"label = {json.dumps(workspace.label)}",
        f"description = {json.dumps(workspace.description)}",
    ]
    for project in workspace.projects:
        lines.extend(
            (
                "",
                "[[projects]]",
                f"project_id = {json.dumps(project.project_id)}",
                f"label = {json.dumps(project.label)}",
                f"root_dir = {json.dumps(str(project.root_dir))}",
                (
                    f"catalog_provider = {json.dumps(project.catalog_provider)}"
                    if project.catalog_provider is not None
                    else "catalog_provider = \"\""
                ),
                (
                    f"runs_dir = {json.dumps(str(project.runs_dir))}"
                    if project.runs_dir is not None
                    else "runs_dir = \"\""
                ),
                f"description = {json.dumps(project.description)}",
                "tags = [" + ", ".join(json.dumps(tag) for tag in project.tags) + "]",
                (
                    f"default_environment = {json.dumps(project.default_environment)}"
                    if project.default_environment is not None
                    else "default_environment = \"\""
                ),
                f"enabled = {'true' if project.enabled else 'false'}",
            )
        )
    return "\n".join(lines) + "\n"


def _required_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    raise RemoteContractError(f"Workspace config {key!r} must be a non-empty string.")


def _optional_str(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value
    raise RemoteContractError(f"Workspace config {key!r} must be a string when present.")
