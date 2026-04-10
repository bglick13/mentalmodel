"""Remote data-plane contract exports."""

from __future__ import annotations

from importlib import import_module

from mentalmodel.remote.contracts import (
    ArtifactDescriptor,
    ArtifactName,
    CatalogSource,
    ProjectCatalog,
    ProjectCatalogProvider,
    ProjectCatalogSnapshot,
    ProjectRegistration,
    RemoteContractError,
    RemoteProjectCatalogPublishRequest,
    RemoteProjectLinkRequest,
    RemoteProjectRecord,
    RemoteRunUploadReceipt,
    RunManifest,
    RunManifestStatus,
    RunTraceSummary,
    WorkspaceConfig,
    validate_workspace_config,
)
from mentalmodel.remote.sinks import (
    CompletedRunPublishResult,
    CompletedRunSink,
    CompositeCompletedRunSink,
    CompositeExecutionRecordSink,
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
    "ProjectCatalogSnapshot",
    "ProjectCatalogProvider",
    "ProjectRegistration",
    "RemoteContractError",
    "RemoteProjectCatalogPublishRequest",
    "RemoteProjectLinkRequest",
    "RemoteProjectRecord",
    "RemoteRunUploadReceipt",
    "RunManifest",
    "RunManifestStatus",
    "RunTraceSummary",
    "DEFAULT_PROJECT_CONFIG_NAME",
    "MentalModelProjectConfig",
    "ProjectConfigError",
    "discover_project_config_path",
    "load_project_config",
    "load_discovered_project_config",
    "require_project_config_path",
    "build_link_request",
    "build_catalog_publish_request",
    "link_project_to_server",
    "fetch_remote_project_status",
    "publish_catalog_to_server",
    "CompletedRunSink",
    "CompletedRunPublishResult",
    "ExecutionRecordSink",
    "NoOpCompletedRunSink",
    "CompositeCompletedRunSink",
    "NoOpExecutionRecordSink",
    "CompositeExecutionRecordSink",
    "record_listener_for_sink",
    "RemoteBackendConfig",
    "RemoteRunStore",
    "RemoteCompletedRunSink",
    "RemoteProjectStore",
    "InMemoryArtifactStore",
    "InMemoryManifestIndex",
    "InMemoryProjectIndex",
    "UploadedArtifact",
    "RunBundleUpload",
    "FileRemoteRunStore",
    "build_run_bundle_upload",
    "build_run_bundle_upload_from_run_dir",
    "RemoteServiceCompletedRunSink",
    "failed_completed_run_publish",
    "sync_runs_for_project",
    "sync_runs_to_server",
    "WorkspaceConfig",
    "validate_workspace_config",
    "load_workspace_config",
    "write_workspace_config",
    "upsert_project_registration",
    "find_project_registration",
]

_LAZY_EXPORTS = {
    "DEFAULT_PROJECT_CONFIG_NAME": (
        "mentalmodel.remote.project_config",
        "DEFAULT_PROJECT_CONFIG_NAME",
    ),
    "MentalModelProjectConfig": (
        "mentalmodel.remote.project_config",
        "MentalModelProjectConfig",
    ),
    "ProjectConfigError": ("mentalmodel.remote.project_config", "ProjectConfigError"),
    "discover_project_config_path": (
        "mentalmodel.remote.project_config",
        "discover_project_config_path",
    ),
    "load_project_config": ("mentalmodel.remote.project_config", "load_project_config"),
    "load_discovered_project_config": (
        "mentalmodel.remote.project_config",
        "load_discovered_project_config",
    ),
    "require_project_config_path": (
        "mentalmodel.remote.project_config",
        "require_project_config_path",
    ),
    "build_link_request": ("mentalmodel.remote.projects", "build_link_request"),
    "build_catalog_publish_request": (
        "mentalmodel.remote.projects",
        "build_catalog_publish_request",
    ),
    "link_project_to_server": ("mentalmodel.remote.projects", "link_project_to_server"),
    "fetch_remote_project_status": (
        "mentalmodel.remote.projects",
        "fetch_remote_project_status",
    ),
    "publish_catalog_to_server": (
        "mentalmodel.remote.projects",
        "publish_catalog_to_server",
    ),
    "UploadedArtifact": ("mentalmodel.remote.store", "UploadedArtifact"),
    "RunBundleUpload": ("mentalmodel.remote.store", "RunBundleUpload"),
    "FileRemoteRunStore": ("mentalmodel.remote.store", "FileRemoteRunStore"),
    "RemoteBackendConfig": ("mentalmodel.remote.backend", "RemoteBackendConfig"),
    "RemoteRunStore": ("mentalmodel.remote.backend", "RemoteRunStore"),
    "RemoteCompletedRunSink": ("mentalmodel.remote.backend", "RemoteCompletedRunSink"),
    "RemoteProjectStore": ("mentalmodel.remote.backend", "RemoteProjectStore"),
    "InMemoryArtifactStore": ("mentalmodel.remote.backend", "InMemoryArtifactStore"),
    "InMemoryManifestIndex": ("mentalmodel.remote.backend", "InMemoryManifestIndex"),
    "InMemoryProjectIndex": ("mentalmodel.remote.backend", "InMemoryProjectIndex"),
    "build_run_bundle_upload": (
        "mentalmodel.remote.sync",
        "build_run_bundle_upload",
    ),
    "build_run_bundle_upload_from_run_dir": (
        "mentalmodel.remote.sync",
        "build_run_bundle_upload_from_run_dir",
    ),
    "sync_runs_to_server": ("mentalmodel.remote.sync", "sync_runs_to_server"),
    "sync_runs_for_project": ("mentalmodel.remote.sync", "sync_runs_for_project"),
    "RemoteServiceCompletedRunSink": (
        "mentalmodel.remote.sync",
        "RemoteServiceCompletedRunSink",
    ),
    "failed_completed_run_publish": (
        "mentalmodel.remote.sync",
        "failed_completed_run_publish",
    ),
}


def __getattr__(name: str) -> object:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_EXPORTS[name]
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
