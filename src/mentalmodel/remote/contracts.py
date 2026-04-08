from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from mentalmodel.errors import MentalModelError

if TYPE_CHECKING:
    from mentalmodel.ui.catalog import DashboardCatalogEntry


class RemoteContractError(MentalModelError):
    """Raised when a remote data-plane contract value is invalid."""


class ArtifactName(StrEnum):
    """Logical artifact names supported by the canonical run manifest."""

    SUMMARY = "summary"
    GRAPH = "graph"
    RECORDS = "records"
    OUTPUTS = "outputs"
    STATE = "state"
    VERIFICATION = "verification"
    SPANS = "spans"


class RunManifestStatus(StrEnum):
    """Availability lifecycle for one persisted or remotely indexed run."""

    UPLOADING = "uploading"
    SEALED = "sealed"
    INDEXED = "indexed"
    FAILED = "failed"


class CatalogSource(StrEnum):
    """How one project or run was associated with a catalog entry."""

    BUILTIN = "builtin"
    MODULE_PROVIDER = "module-provider"
    PATH_SCAN = "path-scan"
    SPEC_PATH = "spec-path"


@dataclass(slots=True, frozen=True)
class RunTraceSummary:
    """Stable trace-export summary attached to a canonical run manifest."""

    mode: str
    service_name: str
    otlp_endpoint: str | None = None
    mirror_to_disk: bool = True
    capture_local_spans: bool = True
    sink_configured: bool = False
    service_namespace: str | None = None
    service_version: str | None = None

    def __post_init__(self) -> None:
        if not self.mode:
            raise RemoteContractError("RunTraceSummary.mode cannot be empty.")
        if not self.service_name:
            raise RemoteContractError("RunTraceSummary.service_name cannot be empty.")
        if self.otlp_endpoint == "":
            raise RemoteContractError("RunTraceSummary.otlp_endpoint cannot be empty.")

    def as_dict(self) -> dict[str, str | bool | None]:
        return {
            "trace_mode": self.mode,
            "trace_service_name": self.service_name,
            "trace_otlp_endpoint": self.otlp_endpoint,
            "trace_mirror_to_disk": self.mirror_to_disk,
            "trace_capture_local_spans": self.capture_local_spans,
            "trace_sink_configured": self.sink_configured,
            "trace_service_namespace": self.service_namespace,
            "trace_service_version": self.service_version,
        }


@dataclass(slots=True, frozen=True)
class ArtifactDescriptor:
    """Metadata for one named artifact in a local or remote run bundle."""

    logical_name: ArtifactName
    relative_path: str
    content_type: str
    byte_size: int | None = None
    checksum_sha256: str | None = None
    storage_uri: str | None = None
    compression: str | None = None
    required: bool = True

    def __post_init__(self) -> None:
        if not self.relative_path:
            raise RemoteContractError("ArtifactDescriptor.relative_path cannot be empty.")
        if Path(self.relative_path).is_absolute():
            raise RemoteContractError(
                "ArtifactDescriptor.relative_path must be relative to the run bundle root."
            )
        if not self.content_type:
            raise RemoteContractError("ArtifactDescriptor.content_type cannot be empty.")
        if self.byte_size is not None and self.byte_size < 0:
            raise RemoteContractError("ArtifactDescriptor.byte_size cannot be negative.")
        if self.checksum_sha256 is not None:
            checksum = self.checksum_sha256.lower()
            if len(checksum) != 64 or any(ch not in "0123456789abcdef" for ch in checksum):
                raise RemoteContractError(
                    "ArtifactDescriptor.checksum_sha256 must be a 64-character hex string."
                )
        if self.storage_uri == "":
            raise RemoteContractError("ArtifactDescriptor.storage_uri cannot be empty.")

    def as_dict(self) -> dict[str, str | int | bool | None]:
        return {
            "logical_name": self.logical_name.value,
            "relative_path": self.relative_path,
            "content_type": self.content_type,
            "byte_size": self.byte_size,
            "checksum_sha256": self.checksum_sha256,
            "storage_uri": self.storage_uri,
            "compression": self.compression,
            "required": self.required,
        }


@dataclass(slots=True, frozen=True)
class RunManifest:
    """Canonical source-of-truth manifest for one persisted run."""

    run_id: str
    graph_id: str
    created_at_ms: int
    completed_at_ms: int | None
    status: RunManifestStatus
    success: bool | None
    run_schema_version: int
    trace_summary: RunTraceSummary
    artifacts: tuple[ArtifactDescriptor, ...]
    invocation_name: str | None = None
    project_id: str | None = None
    project_label: str | None = None
    environment_name: str | None = None
    catalog_entry_id: str | None = None
    catalog_source: CatalogSource | None = None
    runtime_default_profile_name: str | None = None
    runtime_profile_names: tuple[str, ...] = ()
    record_schema_version: int | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.run_id, "RunManifest.run_id")
        _require_identifier(self.graph_id, "RunManifest.graph_id")
        _require_non_negative(self.created_at_ms, "RunManifest.created_at_ms")
        if self.completed_at_ms is not None:
            _require_non_negative(self.completed_at_ms, "RunManifest.completed_at_ms")
            if self.completed_at_ms < self.created_at_ms:
                raise RemoteContractError(
                    "RunManifest.completed_at_ms cannot be earlier than created_at_ms."
                )
        if self.status in {RunManifestStatus.SEALED, RunManifestStatus.INDEXED, RunManifestStatus.FAILED}:
            if self.completed_at_ms is None:
                raise RemoteContractError(
                    "Terminal RunManifest statuses require completed_at_ms."
                )
        if self.run_schema_version < 1:
            raise RemoteContractError("RunManifest.run_schema_version must be >= 1.")
        if self.record_schema_version is not None and self.record_schema_version < 1:
            raise RemoteContractError("RunManifest.record_schema_version must be >= 1.")
        artifact_names = [artifact.logical_name for artifact in self.artifacts]
        if len(set(artifact_names)) != len(artifact_names):
            raise RemoteContractError("RunManifest.artifacts cannot contain duplicate names.")
        if self.project_id is not None:
            _require_identifier(self.project_id, "RunManifest.project_id")
        if self.project_label == "":
            raise RemoteContractError("RunManifest.project_label cannot be empty.")
        if self.environment_name == "":
            raise RemoteContractError("RunManifest.environment_name cannot be empty.")
        if self.catalog_entry_id == "":
            raise RemoteContractError("RunManifest.catalog_entry_id cannot be empty.")

    def missing_required_artifacts(
        self,
        *,
        expected: Sequence[ArtifactName] | None = None,
    ) -> tuple[ArtifactName, ...]:
        expected_names = tuple(expected) if expected is not None else (
            ArtifactName.SUMMARY,
            ArtifactName.GRAPH,
            ArtifactName.RECORDS,
            ArtifactName.OUTPUTS,
            ArtifactName.STATE,
        )
        present = {artifact.logical_name for artifact in self.artifacts}
        return tuple(name for name in expected_names if name not in present)

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "graph_id": self.graph_id,
            "created_at_ms": self.created_at_ms,
            "completed_at_ms": self.completed_at_ms,
            "status": self.status.value,
            "success": self.success,
            "run_schema_version": self.run_schema_version,
            "record_schema_version": self.record_schema_version,
            "trace_summary": self.trace_summary.as_dict(),
            "artifacts": [artifact.as_dict() for artifact in self.artifacts],
            "invocation_name": self.invocation_name,
            "project_id": self.project_id,
            "project_label": self.project_label,
            "environment_name": self.environment_name,
            "catalog_entry_id": self.catalog_entry_id,
            "catalog_source": None if self.catalog_source is None else self.catalog_source.value,
            "runtime_default_profile_name": self.runtime_default_profile_name,
            "runtime_profile_names": list(self.runtime_profile_names),
        }


@dataclass(slots=True, frozen=True)
class ProjectRegistration:
    """Configuration needed to register one project with a shared stack."""

    project_id: str
    label: str
    root_dir: Path
    catalog_provider: str | None = None
    runs_dir: Path | None = None
    description: str = ""
    tags: tuple[str, ...] = ()
    default_environment: str | None = None
    enabled: bool = True

    def __post_init__(self) -> None:
        _require_identifier(self.project_id, "ProjectRegistration.project_id")
        if not self.label:
            raise RemoteContractError("ProjectRegistration.label cannot be empty.")
        if not self.root_dir.is_absolute():
            raise RemoteContractError("ProjectRegistration.root_dir must be absolute.")
        if self.runs_dir is not None and not self.runs_dir.is_absolute():
            raise RemoteContractError("ProjectRegistration.runs_dir must be absolute when set.")
        if self.catalog_provider == "":
            raise RemoteContractError("ProjectRegistration.catalog_provider cannot be empty.")
        if self.default_environment == "":
            raise RemoteContractError("ProjectRegistration.default_environment cannot be empty.")

    def as_dict(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "label": self.label,
            "root_dir": str(self.root_dir),
            "catalog_provider": self.catalog_provider,
            "runs_dir": None if self.runs_dir is None else str(self.runs_dir),
            "description": self.description,
            "tags": list(self.tags),
            "default_environment": self.default_environment,
            "enabled": self.enabled,
        }


@dataclass(slots=True, frozen=True)
class ProjectCatalog:
    """Resolved catalog metadata for one registered project."""

    project: ProjectRegistration
    entries: tuple["DashboardCatalogEntry", ...] = ()
    description: str = ""
    default_entry_id: str | None = None

    def __post_init__(self) -> None:
        from mentalmodel.ui.catalog import validate_dashboard_catalog

        normalized = validate_dashboard_catalog(self.entries)
        object.__setattr__(
            self,
            "entries",
            cast(tuple["DashboardCatalogEntry", ...], normalized),
        )
        if self.default_entry_id is not None:
            entry_ids = {entry.spec_id for entry in normalized}
            if self.default_entry_id not in entry_ids:
                raise RemoteContractError(
                    "ProjectCatalog.default_entry_id must reference one of the catalog entries."
                )


class ProjectCatalogProvider(Protocol):
    """Callable contract for project-scoped dashboard catalog providers."""

    def __call__(self) -> ProjectCatalog:
        """Return one validated project catalog."""


@dataclass(slots=True, frozen=True)
class WorkspaceConfig:
    """Project registry configuration for one local or remote stack."""

    workspace_id: str
    label: str
    projects: tuple[ProjectRegistration, ...] = field(default_factory=tuple)
    description: str = ""

    def __post_init__(self) -> None:
        _require_identifier(self.workspace_id, "WorkspaceConfig.workspace_id")
        if not self.label:
            raise RemoteContractError("WorkspaceConfig.label cannot be empty.")
        validate_workspace_config(self)

    def as_dict(self) -> dict[str, object]:
        return {
            "workspace_id": self.workspace_id,
            "label": self.label,
            "description": self.description,
            "projects": [project.as_dict() for project in self.projects],
        }


def validate_workspace_config(workspace: WorkspaceConfig) -> WorkspaceConfig:
    """Validate one workspace config and return it unchanged."""

    seen_project_ids: set[str] = set()
    for project in workspace.projects:
        if project.project_id in seen_project_ids:
            raise RemoteContractError(
                f"Duplicate project registration {project.project_id!r} in workspace."
            )
        seen_project_ids.add(project.project_id)
    return workspace


def _require_identifier(value: str, field_name: str) -> None:
    if not value:
        raise RemoteContractError(f"{field_name} cannot be empty.")


def _require_non_negative(value: int, field_name: str) -> None:
    if value < 0:
        raise RemoteContractError(f"{field_name} cannot be negative.")
