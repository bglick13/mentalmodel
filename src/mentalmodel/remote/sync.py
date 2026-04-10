from __future__ import annotations

import base64
import json
from pathlib import Path
from urllib import request

from mentalmodel.remote.contracts import CatalogSource, RemoteContractError, RunManifest
from mentalmodel.remote.store import RunBundleUpload, UploadedArtifact
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
) -> tuple[RunManifest, ...]:
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
    manifests: list[RunManifest] = []
    for upload in uploads:
        _post_json(f"{server_url.rstrip('/')}/api/remote/runs", upload.as_dict())
        manifests.append(upload.manifest)
    return tuple(manifests)


def _post_json(url: str, payload: dict[str, object]) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req) as response:
            raw = response.read().decode("utf-8")
    except Exception as exc:
        raise RemoteContractError(f"Failed to POST remote run bundle: {exc}") from exc
    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        raise RemoteContractError("Remote ingest response must be a JSON object.")
    return decoded
