from __future__ import annotations

import json
from urllib import request

from mentalmodel.remote.contracts import (
    RemoteContractError,
    RemoteProjectCatalogPublishRequest,
    RemoteProjectLinkRequest,
    RemoteProjectRecord,
)
from mentalmodel.remote.project_config import MentalModelProjectConfig


def link_project_to_server(
    config: MentalModelProjectConfig,
) -> RemoteProjectRecord:
    payload = config.to_link_request().as_dict()
    response = _request_json(
        f"{config.server_url.rstrip('/')}/api/remote/projects/link",
        method="POST",
        payload=payload,
        api_key=config.resolve_api_key(),
    )
    project_payload = response.get("project")
    if not isinstance(project_payload, dict):
        raise RemoteContractError("Remote project link response must include project.")
    return RemoteProjectRecord.from_dict(project_payload)


def fetch_remote_project_status(
    config: MentalModelProjectConfig,
) -> RemoteProjectRecord:
    response = _request_json(
        f"{config.server_url.rstrip('/')}/api/remote/projects/{config.project_id}",
        method="GET",
        payload=None,
        api_key=config.resolve_api_key(),
    )
    project_payload = response.get("project")
    if not isinstance(project_payload, dict):
        raise RemoteContractError("Remote project status response must include project.")
    return RemoteProjectRecord.from_dict(project_payload)


def publish_catalog_to_server(
    config: MentalModelProjectConfig,
) -> RemoteProjectRecord:
    payload = config.to_catalog_publish_request().as_dict()
    response = _request_json(
        f"{config.server_url.rstrip('/')}/api/remote/projects/{config.project_id}/catalog",
        method="POST",
        payload=payload,
        api_key=config.resolve_api_key(),
    )
    project_payload = response.get("project")
    if not isinstance(project_payload, dict):
        raise RemoteContractError("Remote catalog publish response must include project.")
    return RemoteProjectRecord.from_dict(project_payload)


def _request_json(
    url: str,
    *,
    method: str,
    payload: dict[str, object] | None,
    api_key: str,
) -> dict[str, object]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(req) as response:
            raw = response.read().decode("utf-8")
    except Exception as exc:
        raise RemoteContractError(f"Remote project request failed: {exc}") from exc
    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        raise RemoteContractError("Remote project response must be a JSON object.")
    return decoded


def build_link_request(
    config: MentalModelProjectConfig,
) -> RemoteProjectLinkRequest:
    return config.to_link_request()


def build_catalog_publish_request(
    config: MentalModelProjectConfig,
) -> RemoteProjectCatalogPublishRequest:
    return config.to_catalog_publish_request()
