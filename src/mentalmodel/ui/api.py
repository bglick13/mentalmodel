from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.remote import (
    FileRemoteRunStore,
    ProjectCatalog,
    RemoteBackendConfig,
    RemoteRunStore,
    RunBundleUpload,
)
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
    service = DashboardService(
        runs_dir=runs_dir,
        catalog_entries=catalog_entries,
        project_catalogs=project_catalogs,
        remote_run_store=configured_remote_store,
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

    @app.get("/api/health")
    def health() -> object:
        return {"status": "ok"}

    @app.get("/api/catalog", response_model=None)
    def list_catalog() -> object:
        return {"entries": _catalog_entries_to_json(service.list_catalog())}

    @app.get("/api/projects", response_model=None)
    def list_projects() -> object:
        return {"projects": list(service.list_projects())}

    @app.post("/api/remote/runs", response_model=None)
    def ingest_remote_run(
        payload: Annotated[dict[str, object], Body()],
    ) -> object:
        if ingest_store is None:
            raise HTTPException(
                status_code=400,
                detail="Remote ingest requires runs_dir to be configured on the server.",
            )
        try:
            upload = RunBundleUpload.from_dict(payload)
            run_dir = ingest_store.ingest(upload)
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "status": "ok",
            "graph_id": upload.manifest.graph_id,
            "run_id": upload.manifest.run_id,
            "run_dir": str(run_dir),
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
        node_id: str | None = None,
    ) -> object:
        try:
            return {
                "records": list(
                    service.get_run_records(
                        graph_id=graph_id,
                        run_id=run_id,
                        node_id=node_id,
                    )
                )
            }
        except Exception as exc:  # pragma: no cover - thin API wrapper
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runs/{graph_id}/{run_id}/spans", response_model=None)
    def run_spans(
        graph_id: str,
        run_id: str,
        node_id: str | None = None,
    ) -> object:
        try:
            return {
                "spans": list(
                    service.get_run_spans(
                        graph_id=graph_id,
                        run_id=run_id,
                        node_id=node_id,
                    )
                )
            }
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


def _catalog_entries_to_json(
    entries: tuple[DashboardCatalogEntry, ...],
) -> list[JsonValue]:
    payload: list[JsonValue] = []
    for entry in entries:
        payload.append(_as_json_object(entry.as_dict()))
    return payload


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
