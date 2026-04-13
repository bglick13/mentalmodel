from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Annotated

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.pagination import PageSlice
from mentalmodel.remote.backend import (
    RemoteBackendConfig,
    RemoteEventStore,
    RemoteLiveSessionStore,
    RemoteProjectStore,
    RemoteRunStore,
)
from mentalmodel.remote.contracts import (
    ProjectCatalog,
    RemoteLiveSessionStartRequest,
    RemoteLiveSessionUpdateRequest,
    RemoteProjectCatalogPublishRequest,
    RemoteProjectLinkRequest,
    RemoteRunUploadReceipt,
)
from mentalmodel.remote.events import (
    RemoteOperationEvent,
    RemoteOperationKind,
    RemoteOperationStatus,
)
from mentalmodel.remote.store import FileRemoteRunStore, RunBundleUpload
from mentalmodel.ui.catalog import DashboardCatalogEntry
from mentalmodel.ui.service import DashboardService


def create_dashboard_app(
    *,
    runs_dir: Path | None = None,
    frontend_dist: Path | None = None,
    catalog_entries: tuple[DashboardCatalogEntry, ...] | None = None,
    project_catalogs: tuple[ProjectCatalog, ...] | None = None,
    remote_backend_config: RemoteBackendConfig | None = None,
    remote_run_store: RemoteRunStore | None = None,
    remote_project_store: RemoteProjectStore | None = None,
    remote_live_session_store: RemoteLiveSessionStore | None = None,
    remote_event_store: RemoteEventStore | None = None,
    remote_api_key: str | None = None,
) -> FastAPI:
    """Create the Phase 26 dashboard API and optional static frontend host."""

    configured_remote_store = (
        remote_run_store
        if remote_run_store is not None
        else (
            None
            if remote_backend_config is None
            else RemoteRunStore.from_config(remote_backend_config)
        )
    )
    configured_remote_project_store = (
        remote_project_store
        if remote_project_store is not None
        else (
            None
            if remote_backend_config is None
            else RemoteProjectStore.from_config(remote_backend_config)
        )
    )
    configured_remote_live_store = (
        remote_live_session_store
        if remote_live_session_store is not None
        else (
            None
            if remote_backend_config is None
            else RemoteLiveSessionStore.from_config(remote_backend_config)
        )
    )
    configured_remote_event_store = (
        remote_event_store
        if remote_event_store is not None
        else (
            None
            if remote_backend_config is None
            else RemoteEventStore.from_config(remote_backend_config)
        )
    )
    service = DashboardService(
        runs_dir=runs_dir,
        catalog_entries=catalog_entries,
        project_catalogs=project_catalogs,
        remote_run_store=configured_remote_store,
        remote_project_store=configured_remote_project_store,
        remote_live_session_store=configured_remote_live_store,
        remote_event_store=configured_remote_event_store,
    )
    ingest_store = (
        configured_remote_store
        if configured_remote_store is not None
        else (None if runs_dir is None else FileRemoteRunStore(root_dir=runs_dir))
    )
    app = FastAPI(title="mentalmodel dashboard", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    required_api_key = remote_api_key or os.environ.get("MENTALMODEL_REMOTE_API_KEY")

    def require_remote_auth(
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        if required_api_key in (None, ""):
            return
        expected = f"Bearer {required_api_key}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="Unauthorized remote API request.")

    @app.get("/api/health")
    def health() -> object:
        return {"status": "ok"}

    @app.get("/api/catalog", response_model=None)
    def list_catalog() -> object:
        return {"entries": _catalog_entries_to_json(service.list_catalog())}

    @app.get("/api/projects", response_model=None)
    def list_projects() -> object:
        return {"projects": list(service.list_projects())}

    @app.post("/api/remote/projects/link", response_model=None)
    def link_remote_project(
        payload: Annotated[dict[str, object], Body()],
        _auth: None = Depends(require_remote_auth),
    ) -> object:
        if configured_remote_project_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote project link requires remote backend configuration.",
            )
        try:
            request_payload = RemoteProjectLinkRequest.from_dict(payload)
            project = configured_remote_project_store.link_project(request_payload)
            service.invalidate_remote_project_catalog()
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.PROJECT_LINK,
                status=RemoteOperationStatus.SUCCEEDED,
                project_id=request_payload.project_id,
                metadata={"catalog_entry_count": project.catalog_entry_count},
            )
        except Exception as exc:  # pragma: no cover - thin API wrapper
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.PROJECT_LINK,
                status=RemoteOperationStatus.FAILED,
                project_id=_optional_payload_str(payload, "project_id"),
                error=exc,
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"project": project.as_dict(include_catalog_snapshot=True)}

    @app.get("/api/remote/projects", response_model=None)
    def list_remote_projects(
        _auth: None = Depends(require_remote_auth),
    ) -> object:
        if configured_remote_project_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote project listing requires remote backend configuration.",
            )
        return {
            "projects": [
                project.as_dict(include_catalog_snapshot=False)
                for project in configured_remote_project_store.list_projects()
            ]
        }

    @app.get("/api/remote/projects/{project_id}", response_model=None)
    def get_remote_project(
        project_id: str,
        _auth: None = Depends(require_remote_auth),
    ) -> object:
        if configured_remote_project_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote project status requires remote backend configuration.",
            )
        try:
            project = configured_remote_project_store.get_project(project_id=project_id)
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"project": project.as_dict(include_catalog_snapshot=True)}

    @app.post("/api/remote/projects/{project_id}/catalog", response_model=None)
    def publish_remote_catalog(
        project_id: str,
        payload: Annotated[dict[str, object], Body()],
        _auth: None = Depends(require_remote_auth),
    ) -> object:
        if configured_remote_project_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote catalog publish requires remote backend configuration.",
            )
        try:
            request_payload = RemoteProjectCatalogPublishRequest.from_dict(payload)
            if request_payload.project_id != project_id:
                raise ValueError("Catalog publish path project_id does not match payload.")
            project = configured_remote_project_store.publish_catalog(request_payload)
            service.invalidate_remote_project_catalog()
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.CATALOG_PUBLISH,
                status=RemoteOperationStatus.SUCCEEDED,
                project_id=request_payload.project_id,
                metadata={"catalog_entry_count": project.catalog_entry_count},
            )
        except Exception as exc:  # pragma: no cover - thin API wrapper
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.CATALOG_PUBLISH,
                status=RemoteOperationStatus.FAILED,
                project_id=project_id,
                error=exc,
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"project": project.as_dict(include_catalog_snapshot=True)}

    @app.post("/api/remote/runs", response_model=None)
    def ingest_remote_run(
        payload: Annotated[dict[str, object], Body()],
        _auth: None = Depends(require_remote_auth),
    ) -> object:
        if ingest_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote ingest requires runs_dir to be configured on the server.",
            )
        try:
            upload = RunBundleUpload.from_dict(payload)
            if (
                configured_remote_project_store is not None
                and upload.manifest.project_id is not None
            ):
                configured_remote_project_store.get_project(
                    project_id=upload.manifest.project_id
                )
            run_dir = ingest_store.ingest(upload)
            service.invalidate_remote_run(
                graph_id=upload.manifest.graph_id,
                run_id=upload.manifest.run_id,
            )
            uploaded_at_ms = int(time.time() * 1000)
            if (
                configured_remote_project_store is not None
                and upload.manifest.project_id is not None
            ):
                configured_remote_project_store.record_completed_run_upload(
                    project_id=upload.manifest.project_id,
                    graph_id=upload.manifest.graph_id,
                    run_id=upload.manifest.run_id,
                    invocation_name=upload.manifest.invocation_name,
                    uploaded_at_ms=uploaded_at_ms,
                )
            if configured_remote_live_store is not None:
                configured_remote_live_store.mark_bundle_committed(
                    graph_id=upload.manifest.graph_id,
                    run_id=upload.manifest.run_id,
                    committed_at_ms=uploaded_at_ms,
                )
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.RUN_UPLOAD,
                status=RemoteOperationStatus.SUCCEEDED,
                project_id=upload.manifest.project_id,
                graph_id=upload.manifest.graph_id,
                run_id=upload.manifest.run_id,
                invocation_name=upload.manifest.invocation_name,
                metadata={"artifact_count": len(upload.manifest.artifacts)},
            )
            if configured_remote_live_store is not None:
                _record_remote_event(
                    configured_remote_event_store,
                    kind=RemoteOperationKind.LIVE_COMMIT,
                    status=RemoteOperationStatus.SUCCEEDED,
                    project_id=upload.manifest.project_id,
                    graph_id=upload.manifest.graph_id,
                    run_id=upload.manifest.run_id,
                    invocation_name=upload.manifest.invocation_name,
                )
        except Exception as exc:  # pragma: no cover - thin API wrapper
            graph_id = None
            run_id = None
            project_id = None
            invocation_name = None
            if "upload" in locals():
                graph_id = upload.manifest.graph_id
                run_id = upload.manifest.run_id
                project_id = upload.manifest.project_id
                invocation_name = upload.manifest.invocation_name
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.RUN_UPLOAD,
                status=RemoteOperationStatus.FAILED,
                project_id=project_id,
                graph_id=graph_id,
                run_id=run_id,
                invocation_name=invocation_name,
                error=exc,
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return RemoteRunUploadReceipt(
            graph_id=upload.manifest.graph_id,
            run_id=upload.manifest.run_id,
            uploaded_at_ms=uploaded_at_ms,
            run_dir=str(run_dir),
            project_id=upload.manifest.project_id,
        ).as_dict()

    @app.post("/api/remote/live/sessions/start", response_model=None)
    def start_remote_live_session(
        payload: Annotated[dict[str, object], Body()],
        _auth: None = Depends(require_remote_auth),
    ) -> object:
        if configured_remote_live_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote live session start requires remote backend configuration.",
            )
        try:
            request_payload = RemoteLiveSessionStartRequest.from_dict(payload)
            if (
                configured_remote_project_store is not None
                and request_payload.project_id is not None
            ):
                configured_remote_project_store.get_project(
                    project_id=request_payload.project_id
                )
            session = configured_remote_live_store.start_session(request_payload)
            service.invalidate_remote_run(
                graph_id=request_payload.graph_id,
                run_id=request_payload.run_id,
            )
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.LIVE_START,
                status=RemoteOperationStatus.SUCCEEDED,
                project_id=request_payload.project_id,
                graph_id=request_payload.graph_id,
                run_id=request_payload.run_id,
                invocation_name=request_payload.invocation_name,
                metadata={"node_count": _payload_list_length(request_payload.graph, "nodes")},
            )
        except Exception as exc:  # pragma: no cover - thin API wrapper
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.LIVE_START,
                status=RemoteOperationStatus.FAILED,
                project_id=_optional_payload_str(payload, "project_id"),
                graph_id=_optional_payload_str(payload, "graph_id"),
                run_id=_optional_payload_str(payload, "run_id"),
                invocation_name=_optional_payload_str(payload, "invocation_name"),
                error=exc,
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"session": session.as_dict()}

    @app.post("/api/remote/live/sessions/{run_id}", response_model=None)
    def update_remote_live_session(
        run_id: str,
        payload: Annotated[dict[str, object], Body()],
        _auth: None = Depends(require_remote_auth),
    ) -> object:
        if configured_remote_live_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote live session update requires remote backend configuration.",
            )
        try:
            request_payload = RemoteLiveSessionUpdateRequest.from_dict(payload)
            if request_payload.run_id != run_id:
                raise ValueError("Live session path run_id does not match payload.")
            session = configured_remote_live_store.apply_update(request_payload)
            service.invalidate_remote_run(
                graph_id=request_payload.graph_id,
                run_id=request_payload.run_id,
            )
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.LIVE_UPDATE,
                status=RemoteOperationStatus.SUCCEEDED,
                project_id=session.project_id,
                graph_id=request_payload.graph_id,
                run_id=request_payload.run_id,
                invocation_name=session.invocation_name,
                metadata={
                    "record_count": len(request_payload.records),
                    "span_count": len(request_payload.spans),
                    "status": (
                        None
                        if request_payload.status is None
                        else request_payload.status.value
                    ),
                },
            )
        except Exception as exc:  # pragma: no cover - thin API wrapper
            _record_remote_event(
                configured_remote_event_store,
                kind=RemoteOperationKind.LIVE_UPDATE,
                status=RemoteOperationStatus.FAILED,
                graph_id=_optional_payload_str(payload, "graph_id"),
                run_id=run_id,
                error=exc,
            )
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"session": session.as_dict()}

    @app.get("/api/remote/events", response_model=None)
    def list_remote_events(
        project_id: Annotated[str | None, Query()] = None,
        graph_id: Annotated[str | None, Query()] = None,
        run_id: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        _auth: None = Depends(require_remote_auth),
    ) -> object:
        if configured_remote_event_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote event listing requires remote backend configuration.",
            )
        return {
            "events": list(
                service.list_remote_events(
                    project_id=project_id,
                    graph_id=graph_id,
                    run_id=run_id,
                    limit=limit,
                )
            )
        }

    @app.post("/api/catalog/from-path", response_model=None)
    def catalog_from_path(
        payload: Annotated[dict[str, str], Body()],
    ) -> object:
        raw = payload.get("spec_path")
        if not raw:
            raise HTTPException(
                status_code=400,
                detail="spec_path is required (absolute path to a verify TOML).",
            )
        try:
            entry = service.register_spec_path(Path(raw))
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"entry": _as_json_object(entry.as_dict())}

    @app.get("/api/catalog/{spec_id}/graph", response_model=None)
    def catalog_graph(spec_id: str) -> object:
        try:
            return service.load_catalog_graph(spec_id)
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/executions", response_model=None)
    def start_execution(
        payload: Annotated[dict[str, str], Body()],
    ) -> object:
        spec_id = payload.get("spec_id")
        spec_path = payload.get("spec_path")
        if spec_path:
            if spec_id:
                raise HTTPException(
                    status_code=400,
                    detail="Send only one of spec_id or spec_path.",
                )
            try:
                session = service.start_execution_from_path(Path(spec_path))
            except Exception as exc:  # pragma: no cover - thin API wrapper
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return session.snapshot()
        if spec_id:
            try:
                session = service.start_execution(spec_id)
            except Exception as exc:  # pragma: no cover - thin API wrapper
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return session.snapshot()
        raise HTTPException(
            status_code=400,
            detail="spec_id or spec_path is required.",
        )

    @app.get("/api/executions/{execution_id}", response_model=None)
    def get_execution(
        execution_id: str,
        after_sequence: Annotated[int, Query(ge=0)] = 0,
    ) -> object:
        try:
            return service.get_execution(execution_id, after_sequence=after_sequence)
        except KeyError as exc:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown execution {execution_id!r}.",
            ) from exc

    @app.get("/api/runs", response_model=None)
    def list_runs(
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> object:
        return {
            "runs": list(
                service.list_runs(
                    graph_id=graph_id,
                    invocation_name=invocation_name,
                )
            )
        }

    @app.get("/api/analytics/timeseries", response_model=None)
    def analytics_timeseries(
        graph_id: Annotated[str, Query()],
        invocation_name: Annotated[str, Query()],
        since_ms: Annotated[int, Query()],
        until_ms: Annotated[int, Query()],
        rollup_ms: Annotated[int, Query()] = 60_000,
        run_id: Annotated[str | None, Query()] = None,
        node_id: Annotated[str | None, Query()] = None,
    ) -> object:
        try:
            return service.aggregate_record_timeseries(
                graph_id=graph_id,
                invocation_name=invocation_name,
                since_ms=since_ms,
                until_ms=until_ms,
                rollup_ms=rollup_ms,
                run_id=run_id,
                node_id=node_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/runs/{graph_id}/{run_id}/overview", response_model=None)
    def run_overview(graph_id: str, run_id: str) -> object:
        try:
            return service.get_run_overview(graph_id=graph_id, run_id=run_id)
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/catalog/{spec_id}/runs/{run_id}/views/{view_id}", response_model=None)
    def run_custom_view(
        spec_id: str,
        run_id: str,
        view_id: str,
    ) -> object:
        try:
            return service.get_run_custom_view(
                spec_id=spec_id,
                run_id=run_id,
                view_id=view_id,
            )
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/catalog/{spec_id}/runs/{run_id}/metrics", response_model=None)
    def run_metric_groups(
        spec_id: str,
        run_id: str,
        step_start: Annotated[int | None, Query()] = None,
        step_end: Annotated[int | None, Query()] = None,
        max_points: Annotated[int, Query(ge=10, le=1000)] = 160,
        node_id: Annotated[str | None, Query()] = None,
        frame_id: Annotated[str | None, Query()] = None,
    ) -> object:
        try:
            return service.get_run_metric_groups(
                spec_id=spec_id,
                run_id=run_id,
                step_start=step_start,
                step_end=step_end,
                max_points=max_points,
                node_id=node_id,
                frame_id=frame_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runs/{graph_id}/{run_id}/graph", response_model=None)
    def run_graph(graph_id: str, run_id: str) -> object:
        try:
            return service.get_run_graph(graph_id=graph_id, run_id=run_id)
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runs/{graph_id}/{run_id}/records", response_model=None)
    def run_records(
        graph_id: str,
        run_id: str,
        node_id: Annotated[str | None, Query()] = None,
        frame_id: Annotated[str | None, Query()] = None,
        cursor: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 250,
        include_payload: Annotated[bool, Query()] = False,
    ) -> object:
        try:
            page = service.get_run_records_page(
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
                cursor=cursor,
                limit=limit,
                include_payload=include_payload,
            )
            return _page_to_json(page)
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runs/{graph_id}/{run_id}/spans", response_model=None)
    def run_spans(
        graph_id: str,
        run_id: str,
        node_id: Annotated[str | None, Query()] = None,
        frame_id: Annotated[str | None, Query()] = None,
        cursor: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    ) -> object:
        try:
            page = service.get_run_spans_page(
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
                cursor=cursor,
                limit=limit,
            )
            return _page_to_json(page)
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runs/{graph_id}/{run_id}/replay", response_model=None)
    def run_replay(
        graph_id: str,
        run_id: str,
        loop_node_id: str | None = None,
    ) -> object:
        try:
            return service.get_run_replay(
                graph_id=graph_id,
                run_id=run_id,
                loop_node_id=loop_node_id,
            )
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runs/{graph_id}/{run_id}/nodes/{node_id}", response_model=None)
    def node_detail(
        graph_id: str,
        run_id: str,
        node_id: str,
        frame_id: str | None = None,
    ) -> object:
        try:
            return service.get_node_detail(
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
            )
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    assets_dir = None if frontend_dist is None else frontend_dist / "assets"
    if (
        frontend_dist is not None
        and frontend_dist.exists()
        and assets_dir is not None
        and assets_dir.exists()
    ):
        app.mount(
            "/assets",
            StaticFiles(directory=assets_dir),
            name="dashboard-assets",
        )

        @app.get("/{full_path:path}")
        def dashboard_shell(full_path: str) -> FileResponse:
            del full_path
            return FileResponse(frontend_dist / "index.html")

    return app


def _record_remote_event(
    event_store: RemoteEventStore | None,
    *,
    kind: RemoteOperationKind,
    status: RemoteOperationStatus,
    project_id: str | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    error: Exception | None = None,
    metadata: dict[str, JsonValue] | None = None,
) -> None:
    if event_store is None:
        return
    error_message = None if error is None else str(error)
    error_category = None if error is None else type(error).__name__
    event_store.record_event(
        RemoteOperationEvent(
            event_id=f"evt-{time.time_ns()}",
            occurred_at_ms=int(time.time() * 1000),
            kind=kind,
            status=status,
            project_id=project_id,
            graph_id=graph_id,
            run_id=run_id,
            invocation_name=invocation_name,
            error_category=error_category,
            error_message=error_message,
            metadata={} if metadata is None else dict(metadata),
        )
    )


def _optional_payload_str(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) else None


def _payload_list_length(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    return len(value) if isinstance(value, list) else 0


def _catalog_entries_to_json(
    entries: tuple[DashboardCatalogEntry, ...],
) -> list[JsonValue]:
    payload: list[JsonValue] = []
    for entry in entries:
        payload.append(_as_json_object(entry.as_dict()))
    return payload


def _page_to_json(page: PageSlice[dict[str, JsonValue]]) -> dict[str, JsonValue]:
    return {
        "items": [_as_json_object(item) for item in page.items],
        "next_cursor": _as_json_value(page.next_cursor),
        "total_count": _as_json_value(page.total_count),
        "has_more": _as_json_value(page.has_more),
    }


def _as_json_object(value: object) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise TypeError("Expected JSON object value.")
    payload: dict[str, JsonValue] = {}
    for key, item in value.items():
        payload[str(key)] = _as_json_value(item)
    return payload


def _as_json_value(value: object) -> JsonValue:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_as_json_value(item) for item in value]
    if isinstance(value, dict):
        return _as_json_object(value)
    raise TypeError(f"Unsupported JSON value type {type(value).__name__}.")
