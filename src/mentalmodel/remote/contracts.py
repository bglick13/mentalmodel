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

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RunTraceSummary:
        mode = payload.get("trace_mode")
        service_name = payload.get("trace_service_name")
        if not isinstance(mode, str):
            raise RemoteContractError("RunTraceSummary.trace_mode must be a string.")
        if not isinstance(service_name, str):
            raise RemoteContractError("RunTraceSummary.trace_service_name must be a string.")
        return cls(
            mode=mode,
            service_name=service_name,
            otlp_endpoint=_optional_payload_str(payload, "trace_otlp_endpoint"),
            mirror_to_disk=_required_payload_bool(payload, "trace_mirror_to_disk"),
            capture_local_spans=_required_payload_bool(
                payload, "trace_capture_local_spans"
            ),
            sink_configured=_required_payload_bool(payload, "trace_sink_configured"),
            service_namespace=_optional_payload_str(payload, "trace_service_namespace"),
            service_version=_optional_payload_str(payload, "trace_service_version"),
        )


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

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ArtifactDescriptor:
        logical_name = payload.get("logical_name")
        relative_path = payload.get("relative_path")
        content_type = payload.get("content_type")
        if not isinstance(logical_name, str):
            raise RemoteContractError("ArtifactDescriptor.logical_name must be a string.")
        if not isinstance(relative_path, str):
            raise RemoteContractError("ArtifactDescriptor.relative_path must be a string.")
        if not isinstance(content_type, str):
            raise RemoteContractError("ArtifactDescriptor.content_type must be a string.")
        byte_size = payload.get("byte_size")
        checksum_sha256 = payload.get("checksum_sha256")
        storage_uri = payload.get("storage_uri")
        compression = payload.get("compression")
        required = payload.get("required", True)
        if byte_size is not None and not isinstance(byte_size, int):
            raise RemoteContractError("ArtifactDescriptor.byte_size must be an integer.")
        if checksum_sha256 is not None and not isinstance(checksum_sha256, str):
            raise RemoteContractError(
                "ArtifactDescriptor.checksum_sha256 must be a string when present."
            )
        if storage_uri is not None and not isinstance(storage_uri, str):
            raise RemoteContractError("ArtifactDescriptor.storage_uri must be a string.")
        if compression is not None and not isinstance(compression, str):
            raise RemoteContractError("ArtifactDescriptor.compression must be a string.")
        if not isinstance(required, bool):
            raise RemoteContractError("ArtifactDescriptor.required must be a boolean.")
        return cls(
            logical_name=ArtifactName(logical_name),
            relative_path=relative_path,
            content_type=content_type,
            byte_size=byte_size,
            checksum_sha256=checksum_sha256,
            storage_uri=storage_uri,
            compression=compression,
            required=required,
        )


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
        if self.status in {
            RunManifestStatus.SEALED,
            RunManifestStatus.INDEXED,
            RunManifestStatus.FAILED,
        }:
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

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RunManifest:
        artifacts_payload = payload.get("artifacts")
        trace_payload = payload.get("trace_summary")
        runtime_profile_names = payload.get("runtime_profile_names", [])
        if not isinstance(artifacts_payload, list):
            raise RemoteContractError("RunManifest.artifacts must be a list.")
        if not isinstance(trace_payload, dict):
            raise RemoteContractError("RunManifest.trace_summary must be an object.")
        if not isinstance(runtime_profile_names, list) or any(
            not isinstance(item, str) for item in runtime_profile_names
        ):
            raise RemoteContractError(
                "RunManifest.runtime_profile_names must be a list of strings."
            )
        run_id = payload.get("run_id")
        graph_id = payload.get("graph_id")
        created_at_ms = payload.get("created_at_ms")
        completed_at_ms = payload.get("completed_at_ms")
        status = payload.get("status")
        success = payload.get("success")
        run_schema_version = payload.get("run_schema_version")
        record_schema_version = payload.get("record_schema_version")
        if not isinstance(run_id, str):
            raise RemoteContractError("RunManifest.run_id must be a string.")
        if not isinstance(graph_id, str):
            raise RemoteContractError("RunManifest.graph_id must be a string.")
        if not isinstance(created_at_ms, int):
            raise RemoteContractError("RunManifest.created_at_ms must be an integer.")
        if completed_at_ms is not None and not isinstance(completed_at_ms, int):
            raise RemoteContractError("RunManifest.completed_at_ms must be an integer.")
        if not isinstance(status, str):
            raise RemoteContractError("RunManifest.status must be a string.")
        if success is not None and not isinstance(success, bool):
            raise RemoteContractError("RunManifest.success must be a boolean when present.")
        if not isinstance(run_schema_version, int):
            raise RemoteContractError("RunManifest.run_schema_version must be an integer.")
        if record_schema_version is not None and not isinstance(record_schema_version, int):
            raise RemoteContractError(
                "RunManifest.record_schema_version must be an integer when present."
            )
        return cls(
            run_id=run_id,
            graph_id=graph_id,
            created_at_ms=created_at_ms,
            completed_at_ms=completed_at_ms,
            status=RunManifestStatus(status),
            success=success,
            run_schema_version=run_schema_version,
            trace_summary=RunTraceSummary.from_dict(cast(dict[str, object], trace_payload)),
            artifacts=tuple(
                ArtifactDescriptor.from_dict(cast(dict[str, object], item))
                for item in artifacts_payload
            ),
            invocation_name=_optional_payload_str(payload, "invocation_name"),
            project_id=_optional_payload_str(payload, "project_id"),
            project_label=_optional_payload_str(payload, "project_label"),
            environment_name=_optional_payload_str(payload, "environment_name"),
            catalog_entry_id=_optional_payload_str(payload, "catalog_entry_id"),
            catalog_source=_optional_catalog_source(payload.get("catalog_source")),
            runtime_default_profile_name=_optional_payload_str(
                payload, "runtime_default_profile_name"
            ),
            runtime_profile_names=tuple(runtime_profile_names),
            record_schema_version=record_schema_version,
        )


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
    entries: tuple[DashboardCatalogEntry, ...] = ()
    description: str = ""
    default_entry_id: str | None = None

    def __post_init__(self) -> None:
        from mentalmodel.ui.catalog import validate_dashboard_catalog

        normalized = validate_dashboard_catalog(self.entries)
        object.__setattr__(
            self,
            "entries",
            normalized,
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
class ProjectCatalogSnapshot:
    """Serialized hosted-dashboard contract for one linked project."""

    project_id: str
    provider: str
    published_at_ms: int
    entries: tuple[dict[str, object], ...] = ()
    description: str = ""
    default_entry_id: str | None = None
    version: int = 1

    def __post_init__(self) -> None:
        _require_identifier(self.project_id, "ProjectCatalogSnapshot.project_id")
        if not self.provider:
            raise RemoteContractError("ProjectCatalogSnapshot.provider cannot be empty.")
        _require_non_negative(
            self.published_at_ms,
            "ProjectCatalogSnapshot.published_at_ms",
        )
        if self.version < 1:
            raise RemoteContractError("ProjectCatalogSnapshot.version must be >= 1.")
        entry_ids: set[str] = set()
        for entry in self.entries:
            if not isinstance(entry, dict):
                raise RemoteContractError(
                    "ProjectCatalogSnapshot.entries must contain JSON-object entries."
                )
            spec_id = entry.get("spec_id")
            label = entry.get("label")
            graph_id = entry.get("graph_id")
            invocation_name = entry.get("invocation_name")
            if not isinstance(spec_id, str) or not spec_id:
                raise RemoteContractError(
                    "ProjectCatalogSnapshot entries require a non-empty spec_id."
                )
            if spec_id in entry_ids:
                raise RemoteContractError(
                    f"Duplicate ProjectCatalogSnapshot spec_id {spec_id!r}."
                )
            entry_ids.add(spec_id)
            if not isinstance(label, str) or not label:
                raise RemoteContractError(
                    "ProjectCatalogSnapshot entries require a non-empty label."
                )
            if not isinstance(graph_id, str) or not graph_id:
                raise RemoteContractError(
                    "ProjectCatalogSnapshot entries require a non-empty graph_id."
                )
            if not isinstance(invocation_name, str) or not invocation_name:
                raise RemoteContractError(
                    "ProjectCatalogSnapshot entries require a non-empty invocation_name."
                )
        if self.default_entry_id is not None and self.default_entry_id not in entry_ids:
            raise RemoteContractError(
                "ProjectCatalogSnapshot.default_entry_id must reference one of the entries."
            )

    @property
    def entry_count(self) -> int:
        return len(self.entries)

    def as_dict(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "provider": self.provider,
            "published_at_ms": self.published_at_ms,
            "entries": [dict(entry) for entry in self.entries],
            "description": self.description,
            "default_entry_id": self.default_entry_id,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ProjectCatalogSnapshot:
        project_id = payload.get("project_id")
        provider = payload.get("provider")
        published_at_ms = payload.get("published_at_ms")
        entries = payload.get("entries", [])
        description = payload.get("description", "")
        default_entry_id = payload.get("default_entry_id")
        version = payload.get("version", 1)
        if not isinstance(project_id, str):
            raise RemoteContractError("ProjectCatalogSnapshot.project_id must be a string.")
        if not isinstance(provider, str):
            raise RemoteContractError("ProjectCatalogSnapshot.provider must be a string.")
        if not isinstance(published_at_ms, int):
            raise RemoteContractError(
                "ProjectCatalogSnapshot.published_at_ms must be an integer."
            )
        if not isinstance(entries, list):
            raise RemoteContractError("ProjectCatalogSnapshot.entries must be a list.")
        if not isinstance(description, str):
            raise RemoteContractError("ProjectCatalogSnapshot.description must be a string.")
        if default_entry_id is not None and not isinstance(default_entry_id, str):
            raise RemoteContractError(
                "ProjectCatalogSnapshot.default_entry_id must be a string when present."
            )
        if not isinstance(version, int):
            raise RemoteContractError("ProjectCatalogSnapshot.version must be an integer.")
        normalized_entries: list[dict[str, object]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise RemoteContractError(
                    "ProjectCatalogSnapshot.entries must contain JSON-object entries."
                )
            normalized_entries.append(dict(entry))
        return cls(
            project_id=project_id,
            provider=provider,
            published_at_ms=published_at_ms,
            entries=tuple(normalized_entries),
            description=description,
            default_entry_id=default_entry_id,
            version=version,
        )


@dataclass(slots=True, frozen=True)
class RemoteProjectLinkRequest:
    """Repo-owned project declaration sent to the remote service."""

    project_id: str
    label: str
    description: str = ""
    default_environment: str | None = None
    catalog_provider: str | None = None
    default_runs_dir: str | None = None
    default_verify_spec: str | None = None
    catalog_snapshot: ProjectCatalogSnapshot | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.project_id, "RemoteProjectLinkRequest.project_id")
        if not self.label:
            raise RemoteContractError("RemoteProjectLinkRequest.label cannot be empty.")
        if self.default_environment == "":
            raise RemoteContractError(
                "RemoteProjectLinkRequest.default_environment cannot be empty."
            )
        if self.catalog_provider == "":
            raise RemoteContractError(
                "RemoteProjectLinkRequest.catalog_provider cannot be empty."
            )
        if self.default_runs_dir == "":
            raise RemoteContractError(
                "RemoteProjectLinkRequest.default_runs_dir cannot be empty."
            )
        if self.default_verify_spec == "":
            raise RemoteContractError(
                "RemoteProjectLinkRequest.default_verify_spec cannot be empty."
            )
        if (
            self.catalog_snapshot is not None
            and self.catalog_snapshot.project_id != self.project_id
        ):
            raise RemoteContractError(
                "RemoteProjectLinkRequest.catalog_snapshot project_id mismatch."
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "label": self.label,
            "description": self.description,
            "default_environment": self.default_environment,
            "catalog_provider": self.catalog_provider,
            "default_runs_dir": self.default_runs_dir,
            "default_verify_spec": self.default_verify_spec,
            "catalog_snapshot": (
                None if self.catalog_snapshot is None else self.catalog_snapshot.as_dict()
            ),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RemoteProjectLinkRequest:
        project_id = payload.get("project_id")
        label = payload.get("label")
        description = payload.get("description", "")
        default_environment = payload.get("default_environment")
        catalog_provider = payload.get("catalog_provider")
        default_runs_dir = payload.get("default_runs_dir")
        default_verify_spec = payload.get("default_verify_spec")
        snapshot_payload = payload.get("catalog_snapshot")
        if not isinstance(project_id, str):
            raise RemoteContractError("RemoteProjectLinkRequest.project_id must be a string.")
        if not isinstance(label, str):
            raise RemoteContractError("RemoteProjectLinkRequest.label must be a string.")
        if not isinstance(description, str):
            raise RemoteContractError(
                "RemoteProjectLinkRequest.description must be a string."
            )
        if default_environment is not None and not isinstance(default_environment, str):
            raise RemoteContractError(
                "RemoteProjectLinkRequest.default_environment must be a string when present."
            )
        if catalog_provider is not None and not isinstance(catalog_provider, str):
            raise RemoteContractError(
                "RemoteProjectLinkRequest.catalog_provider must be a string when present."
            )
        if default_runs_dir is not None and not isinstance(default_runs_dir, str):
            raise RemoteContractError(
                "RemoteProjectLinkRequest.default_runs_dir must be a string when present."
            )
        if default_verify_spec is not None and not isinstance(default_verify_spec, str):
            raise RemoteContractError(
                "RemoteProjectLinkRequest.default_verify_spec must be a string when present."
            )
        if snapshot_payload is not None and not isinstance(snapshot_payload, dict):
            raise RemoteContractError(
                "RemoteProjectLinkRequest.catalog_snapshot must be a JSON object when present."
            )
        return cls(
            project_id=project_id,
            label=label,
            description=description,
            default_environment=default_environment,
            catalog_provider=catalog_provider,
            default_runs_dir=default_runs_dir,
            default_verify_spec=default_verify_spec,
            catalog_snapshot=(
                None
                if snapshot_payload is None
                else ProjectCatalogSnapshot.from_dict(snapshot_payload)
            ),
        )


@dataclass(slots=True, frozen=True)
class RemoteProjectRecord:
    """Service-owned remote project record for one linked repo."""

    project_id: str
    label: str
    linked_at_ms: int
    updated_at_ms: int
    description: str = ""
    default_environment: str | None = None
    catalog_provider: str | None = None
    default_runs_dir: str | None = None
    default_verify_spec: str | None = None
    catalog_snapshot: ProjectCatalogSnapshot | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.project_id, "RemoteProjectRecord.project_id")
        if not self.label:
            raise RemoteContractError("RemoteProjectRecord.label cannot be empty.")
        _require_non_negative(self.linked_at_ms, "RemoteProjectRecord.linked_at_ms")
        _require_non_negative(self.updated_at_ms, "RemoteProjectRecord.updated_at_ms")
        if self.updated_at_ms < self.linked_at_ms:
            raise RemoteContractError(
                "RemoteProjectRecord.updated_at_ms cannot be earlier than linked_at_ms."
            )
        if self.default_environment == "":
            raise RemoteContractError("RemoteProjectRecord.default_environment cannot be empty.")
        if self.catalog_provider == "":
            raise RemoteContractError("RemoteProjectRecord.catalog_provider cannot be empty.")
        if self.default_runs_dir == "":
            raise RemoteContractError("RemoteProjectRecord.default_runs_dir cannot be empty.")
        if self.default_verify_spec == "":
            raise RemoteContractError("RemoteProjectRecord.default_verify_spec cannot be empty.")
        if (
            self.catalog_snapshot is not None
            and self.catalog_snapshot.project_id != self.project_id
        ):
            raise RemoteContractError("RemoteProjectRecord.catalog_snapshot project_id mismatch.")

    @property
    def catalog_published(self) -> bool:
        return self.catalog_snapshot is not None

    @property
    def catalog_entry_count(self) -> int:
        return 0 if self.catalog_snapshot is None else self.catalog_snapshot.entry_count

    @property
    def catalog_published_at_ms(self) -> int | None:
        return None if self.catalog_snapshot is None else self.catalog_snapshot.published_at_ms

    @property
    def catalog_version(self) -> int | None:
        return None if self.catalog_snapshot is None else self.catalog_snapshot.version

    def as_dict(self, *, include_catalog_snapshot: bool = False) -> dict[str, object]:
        payload: dict[str, object] = {
            "project_id": self.project_id,
            "label": self.label,
            "description": self.description,
            "default_environment": self.default_environment,
            "catalog_provider": self.catalog_provider,
            "default_runs_dir": self.default_runs_dir,
            "default_verify_spec": self.default_verify_spec,
            "linked_at_ms": self.linked_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "catalog_published": self.catalog_published,
            "catalog_entry_count": self.catalog_entry_count,
            "catalog_published_at_ms": self.catalog_published_at_ms,
            "catalog_version": self.catalog_version,
        }
        if include_catalog_snapshot:
            payload["catalog_snapshot"] = (
                None if self.catalog_snapshot is None else self.catalog_snapshot.as_dict()
            )
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RemoteProjectRecord:
        project_id = payload.get("project_id")
        label = payload.get("label")
        linked_at_ms = payload.get("linked_at_ms")
        updated_at_ms = payload.get("updated_at_ms")
        description = payload.get("description", "")
        default_environment = payload.get("default_environment")
        catalog_provider = payload.get("catalog_provider")
        default_runs_dir = payload.get("default_runs_dir")
        default_verify_spec = payload.get("default_verify_spec")
        snapshot_payload = payload.get("catalog_snapshot")
        if not isinstance(project_id, str):
            raise RemoteContractError("RemoteProjectRecord.project_id must be a string.")
        if not isinstance(label, str):
            raise RemoteContractError("RemoteProjectRecord.label must be a string.")
        if not isinstance(linked_at_ms, int):
            raise RemoteContractError("RemoteProjectRecord.linked_at_ms must be an integer.")
        if not isinstance(updated_at_ms, int):
            raise RemoteContractError("RemoteProjectRecord.updated_at_ms must be an integer.")
        if not isinstance(description, str):
            raise RemoteContractError("RemoteProjectRecord.description must be a string.")
        if default_environment is not None and not isinstance(default_environment, str):
            raise RemoteContractError(
                "RemoteProjectRecord.default_environment must be a string when present."
            )
        if catalog_provider is not None and not isinstance(catalog_provider, str):
            raise RemoteContractError(
                "RemoteProjectRecord.catalog_provider must be a string when present."
            )
        if default_runs_dir is not None and not isinstance(default_runs_dir, str):
            raise RemoteContractError(
                "RemoteProjectRecord.default_runs_dir must be a string when present."
            )
        if default_verify_spec is not None and not isinstance(default_verify_spec, str):
            raise RemoteContractError(
                "RemoteProjectRecord.default_verify_spec must be a string when present."
            )
        if snapshot_payload is not None and not isinstance(snapshot_payload, dict):
            raise RemoteContractError(
                "RemoteProjectRecord.catalog_snapshot must be a JSON object when present."
            )
        return cls(
            project_id=project_id,
            label=label,
            linked_at_ms=linked_at_ms,
            updated_at_ms=updated_at_ms,
            description=description,
            default_environment=default_environment,
            catalog_provider=catalog_provider,
            default_runs_dir=default_runs_dir,
            default_verify_spec=default_verify_spec,
            catalog_snapshot=(
                None
                if snapshot_payload is None
                else ProjectCatalogSnapshot.from_dict(snapshot_payload)
            ),
        )


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


def _optional_payload_str(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise RemoteContractError(f"{key!r} must be a string when present.")


def _required_payload_bool(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    raise RemoteContractError(f"{key!r} must be a boolean.")


def _optional_catalog_source(value: object) -> CatalogSource | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RemoteContractError("catalog_source must be a string when present.")
    return CatalogSource(value)
