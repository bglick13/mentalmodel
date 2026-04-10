from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from mentalmodel.errors import MentalModelError
from mentalmodel.remote.contracts import (
    ProjectCatalogSnapshot,
    ProjectRegistration,
    RemoteProjectCatalogPublishRequest,
    RemoteProjectLinkRequest,
)
from mentalmodel.ui.workspace import resolve_project_catalog

DEFAULT_PROJECT_CONFIG_NAME = "mentalmodel.toml"


class ProjectConfigError(MentalModelError):
    """Raised when repo-owned mentalmodel project config is invalid."""


@dataclass(slots=True, frozen=True)
class MentalModelProjectConfig:
    """Repo-owned config for linking one project to a remote mentalmodel service."""

    config_path: Path
    repo_root: Path
    project_id: str
    label: str
    server_url: str
    api_key_env: str
    catalog_provider: str
    description: str = ""
    default_environment: str | None = None
    publish_on_link: bool = True
    default_runs_dir: Path | None = None
    default_runs_dir_text: str | None = None
    default_verify_spec: Path | None = None
    default_verify_spec_text: str | None = None

    def __post_init__(self) -> None:
        if self.config_path.name != DEFAULT_PROJECT_CONFIG_NAME:
            raise ProjectConfigError(
                f"Mentalmodel project config must be named {DEFAULT_PROJECT_CONFIG_NAME!r}."
            )
        if not self.config_path.is_absolute():
            raise ProjectConfigError("MentalModelProjectConfig.config_path must be absolute.")
        if not self.repo_root.is_absolute():
            raise ProjectConfigError("MentalModelProjectConfig.repo_root must be absolute.")
        if not self.project_id:
            raise ProjectConfigError("MentalModelProjectConfig.project_id cannot be empty.")
        if not self.label:
            raise ProjectConfigError("MentalModelProjectConfig.label cannot be empty.")
        if not self.server_url:
            raise ProjectConfigError("MentalModelProjectConfig.server_url cannot be empty.")
        if not self.api_key_env:
            raise ProjectConfigError("MentalModelProjectConfig.api_key_env cannot be empty.")
        if not self.catalog_provider:
            raise ProjectConfigError(
                "MentalModelProjectConfig.catalog_provider cannot be empty."
            )
        if self.default_environment == "":
            raise ProjectConfigError(
                "MentalModelProjectConfig.default_environment cannot be empty."
            )
        if self.default_runs_dir is not None and not self.default_runs_dir.is_absolute():
            raise ProjectConfigError(
                "MentalModelProjectConfig.default_runs_dir must be absolute when present."
            )
        if self.default_verify_spec is not None:
            if not self.default_verify_spec.is_absolute():
                raise ProjectConfigError(
                    "MentalModelProjectConfig.default_verify_spec must be absolute."
                )
            if not self.default_verify_spec.is_file():
                raise ProjectConfigError(
                    f"Default verify spec not found: {self.default_verify_spec}"
                )

    def resolve_api_key(self) -> str:
        value = os.environ.get(self.api_key_env)
        if value is None or value.strip() == "":
            raise ProjectConfigError(
                f"Expected API key in environment variable {self.api_key_env!r}."
            )
        return value

    def to_local_project_registration(self) -> ProjectRegistration:
        return ProjectRegistration(
            project_id=self.project_id,
            label=self.label,
            root_dir=self.repo_root,
            catalog_provider=self.catalog_provider,
            runs_dir=self.default_runs_dir,
            description=self.description,
            default_environment=self.default_environment,
        )

    def build_catalog_snapshot(
        self,
        *,
        force: bool = False,
    ) -> ProjectCatalogSnapshot | None:
        if not force and not self.publish_on_link:
            return None
        project_catalog = resolve_project_catalog(self.to_local_project_registration())
        return ProjectCatalogSnapshot(
            project_id=self.project_id,
            provider=self.catalog_provider,
            published_at_ms=_now_ms(),
            entries=tuple(dict(entry.as_dict()) for entry in project_catalog.entries),
            description=project_catalog.description,
            default_entry_id=project_catalog.default_entry_id,
        )

    def to_link_request(self) -> RemoteProjectLinkRequest:
        return RemoteProjectLinkRequest(
            project_id=self.project_id,
            label=self.label,
            description=self.description,
            default_environment=self.default_environment,
            catalog_provider=self.catalog_provider,
            default_runs_dir=self.default_runs_dir_text,
            default_verify_spec=self.default_verify_spec_text,
            catalog_snapshot=self.build_catalog_snapshot(),
        )

    def to_catalog_publish_request(self) -> RemoteProjectCatalogPublishRequest:
        snapshot = self.build_catalog_snapshot(force=True)
        assert snapshot is not None
        return RemoteProjectCatalogPublishRequest(
            project_id=self.project_id,
            catalog_provider=self.catalog_provider,
            catalog_snapshot=snapshot,
        )


def discover_project_config_path(start: Path | None = None) -> Path | None:
    resolved_start = (start or Path.cwd()).expanduser().resolve()
    search_root = resolved_start if resolved_start.is_dir() else resolved_start.parent
    for candidate_root in (search_root, *search_root.parents):
        candidate = candidate_root / DEFAULT_PROJECT_CONFIG_NAME
        if candidate.is_file():
            return candidate
    return None


def require_project_config_path(start: Path | None = None) -> Path:
    config_path = discover_project_config_path(start)
    if config_path is None:
        raise ProjectConfigError(
            f"Could not find {DEFAULT_PROJECT_CONFIG_NAME!r} in the current directory "
            "or any parent directory."
        )
    return config_path


def load_project_config(path: Path) -> MentalModelProjectConfig:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise ProjectConfigError(f"Project config not found: {resolved}")
    try:
        payload = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ProjectConfigError(f"Failed to read project config {resolved!r}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ProjectConfigError(
            f"Failed to parse project config {resolved!r}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise ProjectConfigError("Project config must decode to a TOML table.")
    repo_root = resolved.parent
    project_section = _require_table(payload, "project")
    remote_section = _require_table(payload, "remote")
    catalog_section = _require_table(payload, "catalog")
    runs_section = _optional_table(payload, "runs")
    verify_section = _optional_table(payload, "verify")

    default_runs_dir_text = _optional_str(runs_section, "default_runs_dir")
    default_verify_spec_text = _optional_str(verify_section, "default_spec")
    default_runs_dir = (
        None
        if default_runs_dir_text is None
        else (repo_root / default_runs_dir_text).resolve()
    )
    default_verify_spec = (
        None
        if default_verify_spec_text is None
        else (repo_root / default_verify_spec_text).resolve()
    )
    return MentalModelProjectConfig(
        config_path=resolved,
        repo_root=repo_root,
        project_id=_require_str(project_section, "project_id"),
        label=_require_str(project_section, "label"),
        description=_optional_str(project_section, "description") or "",
        server_url=_require_str(remote_section, "server_url"),
        api_key_env=_require_str(remote_section, "api_key_env"),
        default_environment=_optional_str(remote_section, "default_environment"),
        catalog_provider=_require_str(catalog_section, "provider"),
        publish_on_link=_optional_bool(catalog_section, "publish_on_link", default=True),
        default_runs_dir=default_runs_dir,
        default_runs_dir_text=default_runs_dir_text,
        default_verify_spec=default_verify_spec,
        default_verify_spec_text=default_verify_spec_text,
    )


def load_discovered_project_config(start: Path | None = None) -> MentalModelProjectConfig:
    return load_project_config(require_project_config_path(start))


def _require_table(payload: dict[str, object], key: str) -> dict[str, object]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ProjectConfigError(f"Project config requires a [{key}] table.")
    return value


def _optional_table(payload: dict[str, object], key: str) -> dict[str, object] | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ProjectConfigError(f"Project config [{key}] must be a TOML table.")
    return value


def _require_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    raise ProjectConfigError(f"Project config {key!r} must be a non-empty string.")


def _optional_str(payload: dict[str, object] | None, key: str) -> str | None:
    if payload is None:
        return None
    value = payload.get(key)
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value
    raise ProjectConfigError(f"Project config {key!r} must be a string when present.")


def _optional_bool(
    payload: dict[str, object] | None,
    key: str,
    *,
    default: bool,
) -> bool:
    if payload is None:
        return default
    value = payload.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise ProjectConfigError(f"Project config {key!r} must be a boolean when present.")


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)
