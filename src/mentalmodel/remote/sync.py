from __future__ import annotations

import base64
import time
from pathlib import Path
from typing import cast

from mentalmodel.remote.contracts import (
    CatalogSource,
    RemoteRunUploadReceipt,
    RunManifest,
)
from mentalmodel.remote.project_config import MentalModelProjectConfig
from mentalmodel.remote.sinks import (
    CompletedRunPublishResult,
    CompletedRunSink,
)
from mentalmodel.remote.store import RunBundleUpload, UploadedArtifact
from mentalmodel.remote.transport import RemoteRequestError, request_json_with_retry
from mentalmodel.runtime.runs import (
    build_run_manifest_from_summary,
    list_run_summaries,
    resolve_run_summary,
)


def build_run_bundle_upload(
    *,
    runs_dir: Path | None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    project_id: str | None = None,
    project_label: str | None = None,
    environment_name: str | None = None,
    catalog_entry_id: str | None = None,
    catalog_source: CatalogSource | None = None,
) -> RunBundleUpload:
    """Build an upload payload for one persisted local run bundle."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    manifest = build_run_manifest_from_summary(
        summary,
        project_id=project_id,
        project_label=project_label,
        environment_name=environment_name,
        catalog_entry_id=catalog_entry_id,
        catalog_source=catalog_source,
    )
    artifacts = []
    for descriptor in manifest.artifacts:
        content = (summary.run_dir / descriptor.relative_path).read_bytes()
        artifacts.append(
            UploadedArtifact(
                descriptor=descriptor,
                content_base64=base64.b64encode(content).decode("ascii"),
            )
        )
    return RunBundleUpload(manifest=manifest, artifacts=tuple(artifacts))


def build_run_bundle_upload_from_run_dir(
    *,
    run_dir: Path,
    project_id: str | None = None,
    project_label: str | None = None,
    environment_name: str | None = None,
    catalog_entry_id: str | None = None,
    catalog_source: CatalogSource | None = None,
) -> RunBundleUpload:
    """Build an upload payload from one concrete persisted run directory."""

    resolved = run_dir.expanduser().resolve()
    runs_root = resolved.parent.parent
    return build_run_bundle_upload(
        runs_dir=runs_root,
        graph_id=resolved.parent.name,
        run_id=resolved.name,
        project_id=project_id,
        project_label=project_label,
        environment_name=environment_name,
        catalog_entry_id=catalog_entry_id,
        catalog_source=catalog_source,
    )


def upload_run_bundle_to_server(
    *,
    server_url: str,
    upload: RunBundleUpload,
    api_key: str | None = None,
) -> tuple[RemoteRunUploadReceipt, int]:
    """Upload one completed run bundle to the remote ingest API."""

    response = _request_json(
        f"{server_url.rstrip('/')}/api/remote/runs",
        method="POST",
        payload=upload.as_dict(),
        api_key=api_key,
    )
    payload = cast(dict[str, object], response["payload"])
    attempt_count = cast(int, response["attempt_count"])
    return RemoteRunUploadReceipt.from_dict(payload), attempt_count


def sync_runs_to_server(
    *,
    server_url: str,
    runs_dir: Path | None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    project_id: str | None = None,
    project_label: str | None = None,
    environment_name: str | None = None,
    catalog_entry_id: str | None = None,
    catalog_source: CatalogSource | None = None,
    api_key: str | None = None,
) -> tuple[tuple[RemoteRunUploadReceipt, int], ...]:
    """Sync one or more local run bundles to the remote ingest API."""

    uploads: tuple[RunBundleUpload, ...]
    if run_id is not None:
        uploads = (
            build_run_bundle_upload(
                runs_dir=runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                invocation_name=invocation_name,
                project_id=project_id,
                project_label=project_label,
                environment_name=environment_name,
                catalog_entry_id=catalog_entry_id,
                catalog_source=catalog_source,
            ),
        )
    else:
        summaries = list_run_summaries(
            runs_dir=runs_dir,
            graph_id=graph_id,
            invocation_name=invocation_name,
        )
        uploads = tuple(
            build_run_bundle_upload(
                runs_dir=runs_dir,
                graph_id=summary.graph_id,
                run_id=summary.run_id,
                project_id=project_id,
                project_label=project_label,
                environment_name=environment_name,
                catalog_entry_id=catalog_entry_id,
                catalog_source=catalog_source,
            )
            for summary in summaries
        )
    return tuple(
        upload_run_bundle_to_server(
            server_url=server_url,
            upload=upload,
            api_key=api_key,
        )
        for upload in uploads
    )


def sync_runs_for_project(
    *,
    config: MentalModelProjectConfig,
    runs_dir: Path | None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    project_id: str | None = None,
    project_label: str | None = None,
    environment_name: str | None = None,
    catalog_entry_id: str | None = None,
    catalog_source: CatalogSource | None = None,
) -> tuple[tuple[RemoteRunUploadReceipt, int], ...]:
    """Sync one or more local run bundles using repo-linked remote config."""

    return sync_runs_to_server(
        server_url=config.server_url,
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
        project_id=project_id or config.project_id,
        project_label=project_label or config.label,
        environment_name=environment_name or config.default_environment,
        catalog_entry_id=catalog_entry_id,
        catalog_source=catalog_source,
        api_key=config.resolve_optional_api_key(),
    )


class RemoteServiceCompletedRunSink(CompletedRunSink):
    """Completed-run sink that publishes bundles through the hosted service API."""

    def __init__(
        self,
        config: MentalModelProjectConfig,
        *,
        project_id: str | None = None,
        project_label: str | None = None,
        environment_name: str | None = None,
        catalog_entry_id: str | None = None,
        catalog_source: CatalogSource | None = None,
    ) -> None:
        self._config = config
        self.project_id = project_id or config.project_id
        self.project_label = project_label or config.label
        self.environment_name = environment_name or config.default_environment
        self.catalog_entry_id = catalog_entry_id
        self.catalog_source = catalog_source

    @property
    def server_url(self) -> str:
        return self._config.server_url

    def publish(
        self,
        *,
        manifest: RunManifest,
        run_dir: Path,
    ) -> CompletedRunPublishResult:
        del manifest
        return self.publish_run_dir(run_dir)

    def publish_run_dir(self, run_dir: Path) -> CompletedRunPublishResult:
        upload = build_run_bundle_upload_from_run_dir(
            run_dir=run_dir,
            project_id=self.project_id,
            project_label=self.project_label,
            environment_name=self.environment_name,
            catalog_entry_id=self.catalog_entry_id,
            catalog_source=self.catalog_source,
        )
        receipt, attempt_count = upload_run_bundle_to_server(
            server_url=self._config.server_url,
            upload=upload,
            api_key=self._config.resolve_optional_api_key(),
        )
        return CompletedRunPublishResult(
            transport="service-api",
            success=True,
            graph_id=receipt.graph_id,
            run_id=receipt.run_id,
            project_id=receipt.project_id,
            server_url=self._config.server_url,
            remote_run_dir=receipt.run_dir,
            uploaded_at_ms=receipt.uploaded_at_ms,
            attempt_count=attempt_count,
        )


def _request_json(
    url: str,
    *,
    method: str,
    payload: dict[str, object] | None,
    api_key: str | None,
) -> dict[str, object]:
    response = request_json_with_retry(
        url=url,
        method=method,
        payload=payload,
        api_key=api_key,
    )
    return {"payload": response.payload, "attempt_count": response.attempt_count}


def failed_completed_run_publish(
    *,
    transport: str,
    manifest: RunManifest,
    error: Exception,
    server_url: str | None = None,
    project_id: str | None = None,
) -> CompletedRunPublishResult:
    """Build one stable failed-upload record for verification surfaces."""

    uploaded_at_ms = int(time.time() * 1000)
    return CompletedRunPublishResult(
        transport=transport,
        success=False,
        graph_id=manifest.graph_id,
        run_id=manifest.run_id,
        project_id=project_id or manifest.project_id,
        server_url=server_url,
        uploaded_at_ms=uploaded_at_ms,
        attempt_count=(
            error.attempt_count if isinstance(error, RemoteRequestError) else 1
        ),
        retryable=(error.retryable if isinstance(error, RemoteRequestError) else None),
        error_category=(
            error.category.value if isinstance(error, RemoteRequestError) else None
        ),
        error=str(error),
    )
