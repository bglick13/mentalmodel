"""Remote data-plane contract exports."""

from __future__ import annotations

from importlib import import_module

from mentalmodel.remote.contracts import (
    ArtifactDescriptor,
    ArtifactName,
    CatalogSource,
    ProjectCatalog,
    ProjectCatalogProvider,
    ProjectRegistration,
    RemoteContractError,
    RunManifest,
    RunManifestStatus,
    RunTraceSummary,
    WorkspaceConfig,
    validate_workspace_config,
)
from mentalmodel.remote.sinks import (
    CompositeCompletedRunSink,
    CompositeExecutionRecordSink,
    CompletedRunSink,
    ExecutionRecordSink,
    NoOpCompletedRunSink,
    NoOpExecutionRecordSink,
    record_listener_for_sink,
)
from mentalmodel.remote.workspace import (
    find_project_registration,
    load_workspace_config,
    upsert_project_registration,
    write_workspace_config,
)

__all__ = [
    "ArtifactDescriptor",
    "ArtifactName",
    "CatalogSource",
    "ProjectCatalog",
    "ProjectCatalogProvider",
    "ProjectRegistration",
    "RemoteContractError",
    "RunManifest",
    "RunManifestStatus",
    "RunTraceSummary",
    "CompletedRunSink",
    "ExecutionRecordSink",
    "NoOpCompletedRunSink",
    "CompositeCompletedRunSink",
    "NoOpExecutionRecordSink",
    "CompositeExecutionRecordSink",
    "record_listener_for_sink",
    "RemoteBackendConfig",
    "RemoteRunStore",
    "RemoteCompletedRunSink",
    "InMemoryArtifactStore",
    "InMemoryManifestIndex",
    "UploadedArtifact",
    "RunBundleUpload",
    "FileRemoteRunStore",
    "build_run_bundle_upload",
    "build_run_bundle_upload_from_run_dir",
    "sync_runs_to_server",
    "WorkspaceConfig",
    "validate_workspace_config",
    "load_workspace_config",
    "write_workspace_config",
    "upsert_project_registration",
    "find_project_registration",
]

_LAZY_EXPORTS = {
    "UploadedArtifact": ("mentalmodel.remote.store", "UploadedArtifact"),
    "RunBundleUpload": ("mentalmodel.remote.store", "RunBundleUpload"),
    "FileRemoteRunStore": ("mentalmodel.remote.store", "FileRemoteRunStore"),
    "RemoteBackendConfig": ("mentalmodel.remote.backend", "RemoteBackendConfig"),
    "RemoteRunStore": ("mentalmodel.remote.backend", "RemoteRunStore"),
    "RemoteCompletedRunSink": ("mentalmodel.remote.backend", "RemoteCompletedRunSink"),
    "InMemoryArtifactStore": ("mentalmodel.remote.backend", "InMemoryArtifactStore"),
    "InMemoryManifestIndex": ("mentalmodel.remote.backend", "InMemoryManifestIndex"),
    "build_run_bundle_upload": (
        "mentalmodel.remote.sync",
        "build_run_bundle_upload",
    ),
    "build_run_bundle_upload_from_run_dir": (
        "mentalmodel.remote.sync",
        "build_run_bundle_upload_from_run_dir",
    ),
    "sync_runs_to_server": ("mentalmodel.remote.sync", "sync_runs_to_server"),
}


def __getattr__(name: str) -> object:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_EXPORTS[name]
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
