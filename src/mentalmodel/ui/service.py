from __future__ import annotations

import hashlib
import json
import subprocess
import threading
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import cast

from mentalmodel.analysis import AnalysisReport, run_analysis
from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.invocation import (
    load_runtime_environment_subject,
    load_workflow_subject,
    read_verify_invocation_spec,
)
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.lowering import lower_program
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.dashboard_metrics import (
    IndexedMetricRow,
    evaluate_metric_groups,
    metric_rows_from_live_records,
    metric_rows_from_outputs_payload,
)
from mentalmodel.observability.export import execution_record_to_json
from mentalmodel.pagination import PageSlice, paginate_descending_sequence
from mentalmodel.remote.backend import (
    RemoteCompletedRunSink,
    RemoteEventStore,
    RemoteLiveSessionStore,
    RemoteProjectStore,
    RemoteRunStore,
)
from mentalmodel.remote.contracts import (
    ProjectCatalog,
    RemoteLiveSessionRecord,
    RemoteProjectRecord,
)
from mentalmodel.remote.workspace import (
    ProjectRunTarget,
    build_project_run_target,
    find_project_registration_for_path,
)
from mentalmodel.runtime.replay import build_replay_report
from mentalmodel.runtime.runs import (
    RunSummary,
    list_run_summaries,
    load_run_graph,
    load_run_node_inputs,
    load_run_node_output,
    load_run_node_trace,
    load_run_payload,
    load_run_records,
    load_run_records_page,
    load_run_spans,
    load_run_spans_page,
    load_run_summary,
    resolve_run_summary,
)
from mentalmodel.testing import VerificationReport, run_verification
from mentalmodel.ui.catalog import (
    DashboardCatalogEntry,
    DashboardCatalogError,
    catalog_entry_from_spec_path,
    default_dashboard_catalog,
    resolve_catalog_entry,
    validate_dashboard_catalog,
)
from mentalmodel.ui.custom_views import (
    DashboardCustomView,
    evaluate_custom_view,
    evaluate_custom_view_from_records,
)
from mentalmodel.ui.execution_worker import (
    ProjectExecutionWorker,
    SubprocessProjectExecutionWorker,
    WorkerExecutionEvent,
)
from mentalmodel.ui.run_handles import (
    DashboardRunAvailability,
    DashboardRunHandle,
    persisted_run_handle,
)
from mentalmodel.ui.workspace import flatten_project_catalogs

REMOTE_PROJECT_CACHE_TTL_MS = 2_000
RUN_QUERY_CACHE_TTL_MS = 1_000
TIMESERIES_CACHE_TTL_MS = 2_000
METRIC_GROUPS_CACHE_TTL_MS = 2_000
REMOTE_LIVE_SESSION_CACHE_TTL_MS = 1_000
RUN_OVERVIEW_CACHE_TTL_MS = 1_000
CATALOG_GRAPH_CACHE_TTL_MS = 30_000
RUN_DETAIL_CACHE_TTL_MS = 1_000


def _merge_catalog_entries(
    *groups: Sequence[DashboardCatalogEntry],
) -> tuple[DashboardCatalogEntry, ...]:
    merged: dict[str, DashboardCatalogEntry] = {}
    for group in groups:
        for entry in group:
            merged[entry.spec_id] = entry
    return tuple(merged.values())


@dataclass(slots=True)
class DashboardExecutionSession:
    """In-memory live execution state for one launched dashboard run."""

    execution_id: str
    spec: DashboardCatalogEntry
    status: str = "pending"
    started_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    finished_at_ms: int | None = None
    error: str | None = None
    run_id: str | None = None
    run_artifacts_dir: str | None = None
    live_execution_delivery: dict[str, JsonValue] | None = None
    records: list[dict[str, JsonValue]] = field(default_factory=list)
    spans: list[dict[str, JsonValue]] = field(default_factory=list)
    messages: list[dict[str, JsonValue]] = field(default_factory=list)
    _next_sequence: int = field(default=1, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def on_record(self, record: ExecutionRecord) -> None:
        payload = execution_record_to_json(record)
        self.on_record_payload(payload)

    def on_record_payload(self, payload: dict[str, JsonValue]) -> None:
        record_payload = dict(payload)
        with self._lock:
            record_run_id = record_payload.get("run_id")
            if self.run_id is None and isinstance(record_run_id, str) and record_run_id:
                self.run_id = record_run_id
            sequence = _record_sequence(record_payload)
            if sequence <= 0:
                sequence = self._next_sequence
                record_payload["sequence"] = sequence
            self._next_sequence = max(self._next_sequence, sequence + 1)
            self.records.append(record_payload)
            if self.status == "pending":
                self.status = "running"

    def add_message(
        self,
        *,
        level: str,
        message: str,
        source: str = "external-process",
    ) -> None:
        with self._lock:
            payload: dict[str, JsonValue] = {
                "sequence": self._next_sequence,
                "timestamp_ms": int(time.time() * 1000),
                "level": level,
                "message": message,
                "source": source,
            }
            self._next_sequence += 1
            self.messages.append(payload)
            if self.status == "pending":
                self.status = "running"

    def on_span_payload(self, payload: dict[str, JsonValue]) -> None:
        span_payload = dict(payload)
        with self._lock:
            sequence = _span_sequence(span_payload)
            if sequence <= 0:
                sequence = self._next_sequence
                span_payload["sequence"] = sequence
            self._next_sequence = max(self._next_sequence, sequence + 1)
            self.spans.append(span_payload)
            if self.status == "pending":
                self.status = "running"

    def mark_running(self) -> None:
        with self._lock:
            if self.status == "pending":
                self.status = "running"

    def mark_completed(self, report: VerificationReport) -> None:
        with self._lock:
            self.status = "succeeded" if report.success else "failed"
            self.finished_at_ms = int(time.time() * 1000)
            self.run_id = report.runtime.run_id
            self.run_artifacts_dir = report.runtime.run_artifacts_dir
            self.live_execution_delivery = (
                None
                if report.runtime.live_execution_delivery is None
                else _as_json_object(report.runtime.live_execution_delivery.as_dict())
            )
            if report.runtime.error is not None:
                self.error = report.runtime.error

    def mark_completed_from_payload(self, payload: dict[str, object]) -> None:
        runtime = payload.get("runtime")
        if not isinstance(runtime, dict):
            raise DashboardCatalogError("External verification payload must include runtime data.")
        run_id = runtime.get("run_id")
        run_artifacts_dir = runtime.get("run_artifacts_dir")
        runtime_error = runtime.get("error")
        success = payload.get("success")
        with self._lock:
            self.status = "succeeded" if success is True else "failed"
            self.finished_at_ms = int(time.time() * 1000)
            if isinstance(run_id, str):
                self.run_id = run_id
            self.run_artifacts_dir = (
                run_artifacts_dir if isinstance(run_artifacts_dir, str) else None
            )
            live_execution_delivery = runtime.get("live_execution_delivery")
            self.live_execution_delivery = (
                _as_json_object(live_execution_delivery)
                if isinstance(live_execution_delivery, dict)
                else None
            )
            if isinstance(runtime_error, str):
                self.error = runtime_error

    def mark_failed(self, message: str) -> None:
        with self._lock:
            self.status = "failed"
            self.finished_at_ms = int(time.time() * 1000)
            self.error = message

    def snapshot(self, *, after_sequence: int = 0) -> dict[str, JsonValue]:
        with self._lock:
            new_records = [
                record
                for record in self.records
                if _record_sequence(record) > after_sequence
            ]
            new_messages = [
                message
                for message in self.messages
                if _message_sequence(message) > after_sequence
            ]
            latest_sequence = max(
                max((_record_sequence(record) for record in self.records), default=0),
                max((_span_sequence(span) for span in self.spans), default=0),
                max((_message_sequence(message) for message in self.messages), default=0),
            )
            return {
                "execution_id": self.execution_id,
                "spec": _as_json_object(self.spec.as_dict()),
                "status": self.status,
                "started_at_ms": self.started_at_ms,
                "finished_at_ms": self.finished_at_ms,
                "error": self.error,
                "run_id": self.run_id,
                "run_artifacts_dir": self.run_artifacts_dir,
                "latest_sequence": latest_sequence,
                "records": _as_json_list(new_records),
                "spans": _as_json_list(
                    [
                        span
                        for span in self.spans
                        if _span_sequence(span) > after_sequence
                    ]
                ),
                "messages": _as_json_list(new_messages),
                "live_execution_delivery": self.live_execution_delivery,
            }


class DashboardService:
    """Shared backend service for the hosted dashboard surface."""

    def __init__(
        self,
        *,
        runs_dir: Path | None = None,
        catalog_entries: Sequence[DashboardCatalogEntry] | None = None,
        project_catalogs: Sequence[ProjectCatalog] | None = None,
        remote_run_store: RemoteRunStore | None = None,
        remote_project_store: RemoteProjectStore | None = None,
        remote_live_session_store: RemoteLiveSessionStore | None = None,
        remote_event_store: RemoteEventStore | None = None,
        project_execution_worker: ProjectExecutionWorker | None = None,
    ) -> None:
        self.runs_dir = runs_dir
        self.remote_run_store = remote_run_store
        self.remote_project_store = remote_project_store
        self.remote_live_session_store = remote_live_session_store
        self.remote_event_store = remote_event_store
        self._project_execution_worker = (
            project_execution_worker
            if project_execution_worker is not None
            else SubprocessProjectExecutionWorker()
        )
        self._project_catalogs = tuple(project_catalogs or ())
        self._project_catalog_by_id = {
            catalog.project.project_id: catalog
            for catalog in self._project_catalogs
        }
        base_entries = (
            tuple(catalog_entries)
            if catalog_entries is not None
            else (
                ()
                if remote_project_store is not None and not self._project_catalogs
                else default_dashboard_catalog()
            )
        )
        project_entries = flatten_project_catalogs(self._project_catalogs)
        self._static_catalog = validate_dashboard_catalog(base_entries + project_entries)
        self._dynamic_catalog: dict[str, DashboardCatalogEntry] = {}
        self._sessions: dict[str, DashboardExecutionSession] = {}
        self._lock = threading.Lock()
        self._remote_projects_cache: tuple[int, tuple[RemoteProjectRecord, ...]] | None = None
        self._remote_catalog_cache: tuple[int, tuple[DashboardCatalogEntry, ...]] | None = None
        self._project_list_cache: tuple[int, tuple[dict[str, JsonValue], ...]] | None = None
        self._catalog_cache: tuple[int, tuple[DashboardCatalogEntry, ...]] | None = None
        self._run_list_cache: dict[object, tuple[int, tuple[dict[str, JsonValue], ...]]] = {}
        self._metric_groups_cache: dict[object, tuple[int, dict[str, JsonValue]]] = {}
        self._timeseries_cache: dict[object, tuple[int, dict[str, JsonValue]]] = {}
        self._catalog_graph_cache: dict[str, tuple[int, dict[str, JsonValue]]] = {}
        self._run_overview_cache: dict[tuple[str, str], tuple[int, dict[str, JsonValue]]] = {}
        self._run_records_page_cache: dict[object, tuple[int, PageSlice[dict[str, JsonValue]]]] = {}
        self._run_spans_page_cache: dict[object, tuple[int, PageSlice[dict[str, JsonValue]]]] = {}
        self._node_detail_cache: dict[object, tuple[int, dict[str, JsonValue]]] = {}
        self._remote_live_session_cache: dict[
            tuple[str, str, bool], tuple[int, RemoteLiveSessionRecord]
        ] = {}

    def list_catalog(self) -> tuple[DashboardCatalogEntry, ...]:
        now_ms = int(time.time() * 1000)
        cached = self._catalog_cache
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        merged = validate_dashboard_catalog(
            _merge_catalog_entries(
                self._static_catalog,
                self._remote_catalog_entries(),
                tuple(self._dynamic_catalog.values()),
            )
        )
        self._catalog_cache = (now_ms + REMOTE_PROJECT_CACHE_TTL_MS, merged)
        return merged

    def list_projects(self) -> tuple[dict[str, JsonValue], ...]:
        now_ms = int(time.time() * 1000)
        cached = self._project_list_cache
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        projects_by_id: dict[str, dict[str, JsonValue]] = {}
        for project_catalog in self._project_catalogs:
            projects_by_id[project_catalog.project.project_id] = {
                "project_id": project_catalog.project.project_id,
                "label": project_catalog.project.label,
                "root_dir": str(project_catalog.project.root_dir),
                "runs_dir": (
                    None
                    if project_catalog.project.runs_dir is None
                    else str(project_catalog.project.runs_dir)
                ),
                "description": project_catalog.description
                or project_catalog.project.description,
                "catalog_entry_count": len(project_catalog.entries),
                "default_entry_id": project_catalog.default_entry_id,
                "tags": list(project_catalog.project.tags),
                "enabled": project_catalog.project.enabled,
                "source": "workspace",
                "default_environment": project_catalog.project.default_environment,
                "catalog_provider": project_catalog.project.catalog_provider,
                "catalog_published": True,
                "catalog_published_at_ms": None,
                "catalog_version": None,
                "remote_health": None,
            }
        if self.remote_project_store is not None:
            for project in self._list_remote_projects():
                remote_health = (
                    None
                    if self.remote_event_store is None
                    else _as_json_object(
                        self.remote_event_store.summarize_project(
                            project_id=project.project_id,
                            since_ms=int(time.time() * 1000) - 86_400_000,
                        ).as_dict()
                    )
                )
                projects_by_id[project.project_id] = {
                    "project_id": project.project_id,
                    "label": project.label,
                    "root_dir": None,
                    "runs_dir": project.default_runs_dir,
                    "description": project.description,
                    "catalog_entry_count": project.catalog_entry_count,
                    "default_entry_id": (
                        None
                        if project.catalog_snapshot is None
                        else project.catalog_snapshot.default_entry_id
                    ),
                    "tags": [],
                    "enabled": True,
                    "source": "remote",
                    "default_environment": project.default_environment,
                    "catalog_provider": project.catalog_provider,
                    "catalog_published": project.catalog_published,
                    "catalog_published_at_ms": project.catalog_published_at_ms,
                    "catalog_version": project.catalog_version,
                    "linked_at_ms": project.linked_at_ms,
                    "updated_at_ms": project.updated_at_ms,
                    "default_verify_spec": project.default_verify_spec,
                    "remote_health": remote_health,
                }
        result = tuple(projects_by_id.values())
        self._project_list_cache = (now_ms + REMOTE_PROJECT_CACHE_TTL_MS, result)
        return result

    def list_remote_events(
        self,
        *,
        project_id: str | None = None,
        graph_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> tuple[dict[str, JsonValue], ...]:
        if self.remote_event_store is None:
            return ()
        return tuple(
            _as_json_object(event.as_dict())
            for event in self.remote_event_store.list_events(
                project_id=project_id,
                graph_id=graph_id,
                run_id=run_id,
                limit=limit,
            )
        )

    def _remote_catalog_entries(self) -> tuple[DashboardCatalogEntry, ...]:
        if self.remote_project_store is None:
            return ()
        now_ms = int(time.time() * 1000)
        cached = self._remote_catalog_cache
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        entries: list[DashboardCatalogEntry] = []
        for project in self._list_remote_projects():
            if project.project_id in self._project_catalog_by_id:
                continue
            snapshot = project.catalog_snapshot
            if snapshot is None:
                continue
            for raw_entry in snapshot.entries:
                entry = DashboardCatalogEntry.from_dict(raw_entry, launch_enabled=False)
                entries.append(
                    replace(
                        entry,
                        project_id=project.project_id,
                        project_label=project.label,
                        catalog_source="remote-snapshot",
                    )
                )
        result = tuple(entries)
        self._remote_catalog_cache = (now_ms + REMOTE_PROJECT_CACHE_TTL_MS, result)
        return result

    def _resolve_entry(self, spec_id: str) -> DashboardCatalogEntry:
        for entry in self.list_catalog():
            if entry.spec_id == spec_id:
                return entry
        raise DashboardCatalogError(f"Unknown dashboard catalog entry {spec_id!r}.")

    def register_spec_path(self, spec_path: Path) -> DashboardCatalogEntry:
        """Parse a verify TOML on disk and register it for graph preview and launch."""

        entry = self._catalog_entry_from_path(spec_path)
        self._dynamic_catalog[entry.spec_id] = entry
        self.invalidate_remote_cache()
        return entry

    def invalidate_remote_cache(self) -> None:
        self._remote_projects_cache = None
        self._remote_catalog_cache = None
        self._project_list_cache = None
        self._catalog_cache = None
        self._run_list_cache.clear()
        self._metric_groups_cache.clear()
        self._timeseries_cache.clear()
        self._catalog_graph_cache.clear()
        self._run_overview_cache.clear()
        self._run_records_page_cache.clear()
        self._run_spans_page_cache.clear()
        self._node_detail_cache.clear()
        self._remote_live_session_cache.clear()

    def invalidate_remote_project_catalog(self) -> None:
        self._remote_projects_cache = None
        self._remote_catalog_cache = None
        self._project_list_cache = None
        self._catalog_cache = None
        self._run_list_cache.clear()

    def invalidate_remote_run(
        self,
        *,
        graph_id: str,
        run_id: str,
    ) -> None:
        self._run_list_cache = {
            key: value
            for key, value in self._run_list_cache.items()
            if not (isinstance(key, tuple) and key and key[0] == graph_id)
        }
        self._metric_groups_cache = {
            key: value
            for key, value in self._metric_groups_cache.items()
            if not (isinstance(key, tuple) and len(key) > 1 and key[1] == run_id)
        }
        self._timeseries_cache = {
            key: value
            for key, value in self._timeseries_cache.items()
            if not (
                isinstance(key, tuple)
                and key
                and key[0] == graph_id
                and (len(key) < 6 or key[5] in (None, run_id))
            )
        }
        self._run_overview_cache = {
            key: value
            for key, value in self._run_overview_cache.items()
            if key != (graph_id, run_id)
        }
        self._run_records_page_cache = {
            key: value
            for key, value in self._run_records_page_cache.items()
            if not (isinstance(key, tuple) and key[:2] == (graph_id, run_id))
        }
        self._run_spans_page_cache = {
            key: value
            for key, value in self._run_spans_page_cache.items()
            if not (isinstance(key, tuple) and key[:2] == (graph_id, run_id))
        }
        self._node_detail_cache = {
            key: value
            for key, value in self._node_detail_cache.items()
            if not (isinstance(key, tuple) and key[:2] == (graph_id, run_id))
        }
        self._remote_live_session_cache = {
            key: value
            for key, value in self._remote_live_session_cache.items()
            if not (isinstance(key, tuple) and key[:2] == (graph_id, run_id))
        }

    def _list_remote_projects(self) -> tuple[RemoteProjectRecord, ...]:
        if self.remote_project_store is None:
            return ()
        now_ms = int(time.time() * 1000)
        cached = self._remote_projects_cache
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        projects = tuple(self.remote_project_store.list_projects())
        self._remote_projects_cache = (
            now_ms + REMOTE_PROJECT_CACHE_TTL_MS,
            projects,
        )
        return projects

    def start_execution_from_path(self, spec_path: Path) -> DashboardExecutionSession:
        """Register the spec (if needed) and start verification in a background thread."""

        entry = self.register_spec_path(spec_path)
        return self._start_execution_with_entry(entry)

    def load_catalog_graph(self, spec_id: str) -> dict[str, JsonValue]:
        now_ms = int(time.time() * 1000)
        cached = self._catalog_graph_cache.get(spec_id)
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        entry = self._resolve_entry(spec_id)
        if not entry.launch_enabled:
            result = {
                "catalog_entry": _as_json_object(entry.as_dict()),
                **self._remote_catalog_graph_payload(entry),
            }
            self._catalog_graph_cache[spec_id] = (
                now_ms + CATALOG_GRAPH_CACHE_TTL_MS,
                result,
            )
            return result
        external_project = self._external_project_for_entry(entry)
        if external_project is not None:
            payload = self._load_external_catalog_graph(entry, external_project.project.root_dir)
            result = {
                "catalog_entry": _as_json_object(entry.as_dict()),
                "graph": _as_json_object(payload["graph"]),
                "analysis": _as_json_object(payload["analysis"]),
            }
            self._catalog_graph_cache[spec_id] = (
                now_ms + CATALOG_GRAPH_CACHE_TTL_MS,
                result,
            )
            return result
        invocation = read_verify_invocation_spec(entry.spec_path)
        _, program = load_workflow_subject(invocation.program)
        graph = lower_program(program)
        analysis = run_analysis(graph)
        result = {
            "catalog_entry": _as_json_object(entry.as_dict()),
            "graph": _graph_to_payload(graph),
            "analysis": _analysis_to_payload(analysis),
        }
        self._catalog_graph_cache[spec_id] = (
            now_ms + CATALOG_GRAPH_CACHE_TTL_MS,
            result,
        )
        return result

    def _start_execution_with_entry(
        self,
        entry: DashboardCatalogEntry,
    ) -> DashboardExecutionSession:
        if not entry.launch_enabled:
            raise DashboardCatalogError(
                f"Dashboard execution is not available for catalog entry {entry.spec_id!r}."
            )
        session = DashboardExecutionSession(
            execution_id=f"exec-{uuid.uuid4().hex}",
            spec=entry,
        )
        with self._lock:
            self._sessions[session.execution_id] = session
        thread = threading.Thread(
            target=self._run_session,
            args=(session,),
            daemon=True,
            name=f"mentalmodel-dashboard-{session.execution_id}",
        )
        thread.start()
        return session

    def start_execution(self, spec_id: str) -> DashboardExecutionSession:
        entry = self._resolve_entry(spec_id)
        return self._start_execution_with_entry(entry)

    def get_execution(self, execution_id: str, *, after_sequence: int = 0) -> dict[str, JsonValue]:
        session = self._require_session(execution_id)
        snapshot = session.snapshot(after_sequence=after_sequence)
        handle = self._active_run_handle(session)
        if handle is not None:
            snapshot["run_handle"] = handle.as_dict()
        if session.run_id is not None:
            try:
                if (
                    session.run_artifacts_dir is not None
                    and Path(session.run_artifacts_dir).exists()
                ):
                    summary = load_run_summary(Path(session.run_artifacts_dir))
                    snapshot["run_summary"] = {
                        "summary": persisted_run_handle(summary).as_dict()
                    }
                else:
                    snapshot["run_summary"] = self.get_run_overview(
                        graph_id=session.spec.graph_id,
                        run_id=session.run_id,
                    )
            except RunInspectionError:
                pass
        return snapshot

    def list_runs(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[dict[str, JsonValue], ...]:
        cache_key = (graph_id, invocation_name)
        now_ms = int(time.time() * 1000)
        cached = self._run_list_cache.get(cache_key)
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        summaries = (
            self.remote_run_store.list_run_summaries(
                graph_id=graph_id,
                invocation_name=invocation_name,
            )
            if self.remote_run_store is not None
            else list_run_summaries(
                runs_dir=self.runs_dir,
                graph_id=graph_id,
                invocation_name=invocation_name,
            )
        )
        persisted = {
            summary.run_id: persisted_run_handle(summary)
            for summary in summaries
        }
        remote_live = {
            handle.run_id: handle
            for handle in (
                self._remote_live_run_handle(session)
                for session in self._matching_remote_live_sessions(
                    graph_id=graph_id,
                    invocation_name=invocation_name,
                )
            )
            if handle is not None
        }
        active = {
            handle.run_id: handle
            for handle in (
                self._active_run_handle(session)
                for session in self._matching_sessions(
                    graph_id=graph_id,
                    invocation_name=invocation_name,
                )
            )
            if handle is not None
        }
        merged: list[DashboardRunHandle] = []
        seen: set[str] = set()
        for summary in summaries:
            merged.append(persisted[summary.run_id])
            seen.add(summary.run_id)
        for run_id, handle in remote_live.items():
            if run_id not in seen:
                merged.append(handle)
                seen.add(run_id)
        for run_id, handle in active.items():
            if run_id not in seen:
                merged.append(handle)
        merged.sort(key=lambda handle: (handle.created_at_ms, handle.run_id), reverse=True)
        result = tuple(handle.as_dict() for handle in merged)
        self._run_list_cache[cache_key] = (now_ms + RUN_QUERY_CACHE_TTL_MS, result)
        self._prune_query_caches()
        return result

    def get_run_graph(self, *, graph_id: str, run_id: str) -> dict[str, JsonValue]:
        session = self._session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            return self._graph_payload_for_entry(session.spec)
        remote_live = self._remote_live_session_for_run(
            graph_id=graph_id,
            run_id=run_id,
            include_payloads=False,
        )
        if (
            remote_live is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            return _graph_payload_from_live_session(remote_live)
        history_runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        graph = load_run_graph(
            runs_dir=history_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
        )
        return _graph_to_payload(graph)

    def get_run_overview(self, *, graph_id: str, run_id: str) -> dict[str, JsonValue]:
        now_ms = int(time.time() * 1000)
        cache_key = (graph_id, run_id)
        cached = self._run_overview_cache.get(cache_key)
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        session = self._session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            handle = self._active_run_handle(session)
            if handle is None:
                raise RunInspectionError(f"Run {run_id!r} is not available.")
            active_result: dict[str, JsonValue] = {
                "summary": handle.as_dict(),
                "verification": None,
                "verification_success": None,
                "runtime_error": session.error,
                "graph": self._graph_payload_for_entry(session.spec),
                "metrics": [],
                "invariants": [],
                "remote_delivery": None,
            }
            self._run_overview_cache[cache_key] = (
                now_ms + RUN_OVERVIEW_CACHE_TTL_MS,
                active_result,
            )
            self._prune_query_caches()
            return active_result
        remote_live = self._remote_live_session_for_run(
            graph_id=graph_id,
            run_id=run_id,
            include_payloads=False,
        )
        if (
            remote_live is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            assert self.remote_live_session_store is not None
            live_result: dict[str, JsonValue] = {
                "summary": self._remote_live_run_handle(remote_live).as_dict(),
                "verification": None,
                "verification_success": None,
                "runtime_error": remote_live.error,
                "graph": _graph_payload_from_live_session(remote_live),
                "metrics": [],
                "invariants": _as_json_list(
                    self.remote_live_session_store.list_invariants(
                        graph_id=graph_id,
                        run_id=run_id,
                    )
                ),
                "remote_delivery": (
                    None
                    if self.remote_event_store is None
                    else _as_json_object(
                        self.remote_event_store.summarize_run(
                            graph_id=graph_id,
                            run_id=run_id,
                            since_ms=int(time.time() * 1000) - 86_400_000,
                        ).as_dict()
                    )
                ),
            }
            self._run_overview_cache[cache_key] = (
                now_ms + RUN_OVERVIEW_CACHE_TTL_MS,
                live_result,
            )
            self._prune_query_caches()
            return live_result
        history_runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        summary = resolve_run_summary(
            runs_dir=history_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
        )
        verification = _safe_load_payload(
            runs_dir=history_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            filename="verification.json",
        )
        graph = self.get_run_graph(graph_id=graph_id, run_id=run_id)
        verification_success = (
            verification.get("success")
            if isinstance(verification, dict)
            and isinstance(verification.get("success"), bool)
            else None
        )
        persisted_result: dict[str, JsonValue] = {
            "summary": persisted_run_handle(summary).as_dict(),
            "verification": verification,
            "verification_success": verification_success,
            "runtime_error": _runtime_error_from_verification(verification),
            "graph": graph,
            "metrics": [],
            "invariants": _as_json_list(
                self._load_persisted_invariants(graph_id=graph_id, run_id=run_id)
            ),
            "remote_delivery": (
                None
                if self.remote_event_store is None
                else _as_json_object(
                    self.remote_event_store.summarize_run(
                        graph_id=graph_id,
                        run_id=run_id,
                        since_ms=int(time.time() * 1000) - 86_400_000,
                    ).as_dict()
                )
            ),
        }
        self._run_overview_cache[cache_key] = (
            now_ms + RUN_OVERVIEW_CACHE_TTL_MS,
            persisted_result,
        )
        self._prune_query_caches()
        return persisted_result

    def get_run_custom_view(
        self,
        *,
        spec_id: str,
        run_id: str,
        view_id: str,
    ) -> dict[str, JsonValue]:
        entry = self._resolve_entry(spec_id)
        view = _resolve_custom_view(entry, view_id)
        session = self._session_for_run(graph_id=entry.graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=entry.graph_id, run_id=run_id)
        ):
            evaluated = evaluate_custom_view_from_records(
                records=cast(list[dict[str, object]], session.records),
                run_id=run_id,
                view=view,
            )
            return _as_json_object(evaluated.as_dict())
        remote_live = self._remote_live_session_for_run(graph_id=entry.graph_id, run_id=run_id)
        if (
            remote_live is not None
            and not self._has_persisted_history(graph_id=entry.graph_id, run_id=run_id)
        ):
            evaluated = evaluate_custom_view_from_records(
                records=list(remote_live.records),
                run_id=run_id,
                view=view,
            )
            return _as_json_object(evaluated.as_dict())
        history_runs_dir = self._require_history_runs_dir(
            graph_id=entry.graph_id,
            run_id=run_id,
        )
        evaluated = evaluate_custom_view(
            runs_dir=history_runs_dir,
            graph_id=entry.graph_id,
            run_id=run_id,
            view=view,
        )
        return _as_json_object(evaluated.as_dict())

    def get_run_metric_groups(
        self,
        *,
        spec_id: str,
        run_id: str,
        step_start: int | None,
        step_end: int | None,
        max_points: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> dict[str, JsonValue]:
        cache_key = (
            spec_id,
            run_id,
            step_start,
            step_end,
            max_points,
            node_id,
            frame_id,
        )
        now_ms = int(time.time() * 1000)
        cached = self._metric_groups_cache.get(cache_key)
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        entry = resolve_catalog_entry(spec_id, self.list_catalog())
        session = self._session_for_run(graph_id=entry.graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=entry.graph_id, run_id=run_id)
        ):
            groups = evaluate_metric_groups(
                groups=entry.metric_groups,
                metric_rows=metric_rows_from_live_records(
                    cast(Sequence[dict[str, object]], session.records)
                ),
                step_start=step_start,
                step_end=step_end,
                node_id=node_id,
                frame_id=frame_id,
                max_points=max_points,
            )
            live_session_result: dict[str, JsonValue] = {
                "graph_id": entry.graph_id,
                "run_id": run_id,
                "spec_id": entry.spec_id,
                "groups": _as_json_list(groups),
            }
            self._metric_groups_cache[cache_key] = (
                now_ms + METRIC_GROUPS_CACHE_TTL_MS,
                live_session_result,
            )
            self._prune_query_caches()
            return live_session_result
        remote_live = self._remote_live_session_for_run(
            graph_id=entry.graph_id,
            run_id=run_id,
            include_payloads=False,
        )
        if (
            remote_live is not None
            and not self._has_persisted_history(graph_id=entry.graph_id, run_id=run_id)
        ):
            assert self.remote_live_session_store is not None
            groups = evaluate_metric_groups(
                groups=entry.metric_groups,
                metric_rows=self.remote_live_session_store.list_metrics(
                    graph_id=entry.graph_id,
                    run_id=run_id,
                    step_start=step_start,
                    step_end=step_end,
                    node_id=node_id,
                    frame_id=frame_id,
                    path_prefixes=tuple(
                        prefix
                        for group in entry.metric_groups
                        for prefix in group.metric_path_prefixes
                    ),
                ),
                step_start=step_start,
                step_end=step_end,
                node_id=node_id,
                frame_id=frame_id,
                max_points=max_points,
            )
            remote_live_result: dict[str, JsonValue] = {
                "graph_id": entry.graph_id,
                "run_id": run_id,
                "spec_id": entry.spec_id,
                "groups": _as_json_list(groups),
            }
            self._metric_groups_cache[cache_key] = (
                now_ms + METRIC_GROUPS_CACHE_TTL_MS,
                remote_live_result,
            )
            self._prune_query_caches()
            return remote_live_result
        metric_rows = self._load_persisted_metric_rows(
            graph_id=entry.graph_id,
            run_id=run_id,
            step_start=step_start,
            step_end=step_end,
            node_id=node_id,
            frame_id=frame_id,
            path_prefixes=tuple(
                prefix
                for group in entry.metric_groups
                for prefix in group.metric_path_prefixes
            ),
        )
        groups = evaluate_metric_groups(
            groups=entry.metric_groups,
            metric_rows=metric_rows,
            step_start=step_start,
            step_end=step_end,
            node_id=node_id,
            frame_id=frame_id,
            max_points=max_points,
        )
        persisted_result: dict[str, JsonValue] = {
            "graph_id": entry.graph_id,
            "run_id": run_id,
            "spec_id": entry.spec_id,
            "groups": _as_json_list(groups),
        }
        self._metric_groups_cache[cache_key] = (
            now_ms + METRIC_GROUPS_CACHE_TTL_MS,
            persisted_result,
        )
        self._prune_query_caches()
        return persisted_result

    def get_run_records(
        self,
        *,
        graph_id: str,
        run_id: str,
        node_id: str | None = None,
    ) -> tuple[dict[str, JsonValue], ...]:
        session = self._session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            return tuple(
                record
                for record in session.records
                if node_id is None or record.get("node_id") == node_id
            )
        remote_live = self._remote_live_session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            remote_live is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            return tuple(
                cast(dict[str, JsonValue], record)
                for record in remote_live.records
                if node_id is None or record.get("node_id") == node_id
            )
        history_runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        return load_run_records(
            runs_dir=history_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            node_id=node_id,
        )

    def get_run_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
        include_payload: bool = True,
    ) -> PageSlice[dict[str, JsonValue]]:
        cache_key = (graph_id, run_id, cursor, limit, node_id, frame_id, include_payload)
        now_ms = int(time.time() * 1000)
        cached = self._run_records_page_cache.get(cache_key)
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        session = self._session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            filtered = tuple(
                record
                for record in session.records
                if (node_id is None or record.get("node_id") == node_id)
                and (frame_id is None or record.get("frame_id") == frame_id)
            )
            items = (
                filtered
                if include_payload
                else tuple(_record_without_payload(record) for record in filtered)
            )
            result = paginate_descending_sequence(
                items,
                sequence_for=_record_sequence,
                cursor=cursor,
                limit=limit,
            )
            self._run_records_page_cache[cache_key] = (
                now_ms + RUN_DETAIL_CACHE_TTL_MS,
                result,
            )
            self._prune_query_caches()
            return result
        remote_live = self._remote_live_session_for_run(graph_id=graph_id, run_id=run_id)
        if remote_live is not None and self.remote_live_session_store is not None:
            result = self.remote_live_session_store.get_records_page(
                graph_id=graph_id,
                run_id=run_id,
                cursor=cursor,
                limit=limit,
                node_id=node_id,
                frame_id=frame_id,
                include_payload=include_payload,
            )
            self._run_records_page_cache[cache_key] = (
                now_ms + RUN_DETAIL_CACHE_TTL_MS,
                result,
            )
            self._prune_query_caches()
            return result
        if (
            self.remote_run_store is not None
            and self.remote_run_store.contains_run(graph_id=graph_id, run_id=run_id)
        ):
            result = self.remote_run_store.get_records_page(
                graph_id=graph_id,
                run_id=run_id,
                cursor=cursor,
                limit=limit,
                node_id=node_id,
                frame_id=frame_id,
                include_payload=include_payload,
            )
            self._run_records_page_cache[cache_key] = (
                now_ms + RUN_DETAIL_CACHE_TTL_MS,
                result,
            )
            self._prune_query_caches()
            return result
        history_runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        page = load_run_records_page(
            runs_dir=history_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            node_id=node_id,
            frame_id=frame_id,
            cursor=cursor,
            limit=limit,
            include_payload=include_payload,
        )
        self._run_records_page_cache[cache_key] = (
            now_ms + RUN_DETAIL_CACHE_TTL_MS,
            page,
        )
        self._prune_query_caches()
        return page

    def get_run_spans(
        self,
        *,
        graph_id: str,
        run_id: str,
        node_id: str | None = None,
    ) -> tuple[dict[str, JsonValue], ...]:
        """Return OTel span rows from ``otel-spans.jsonl`` (optionally filtered by node)."""

        session = self._session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            return tuple(
                span
                for span in session.spans
                if node_id is None or _span_node_id(span) == node_id
            )
        remote_live = self._remote_live_session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            remote_live is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            return tuple(
                cast(dict[str, JsonValue], span)
                for span in remote_live.spans
                if node_id is None or _span_node_id(cast(dict[str, JsonValue], span)) == node_id
            )
        history_runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        return load_run_spans(
            runs_dir=history_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            node_id=node_id,
        )

    def get_run_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        cache_key = (graph_id, run_id, cursor, limit, node_id, frame_id)
        now_ms = int(time.time() * 1000)
        cached = self._run_spans_page_cache.get(cache_key)
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        session = self._session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            filtered = tuple(
                span
                for span in session.spans
                if (node_id is None or _span_node_id(span) == node_id)
                and (frame_id is None or _span_frame_id(span) == frame_id)
            )
            result = paginate_descending_sequence(
                filtered,
                sequence_for=_span_sequence,
                cursor=cursor,
                limit=limit,
            )
            self._run_spans_page_cache[cache_key] = (
                now_ms + RUN_DETAIL_CACHE_TTL_MS,
                result,
            )
            self._prune_query_caches()
            return result
        remote_live = self._remote_live_session_for_run(graph_id=graph_id, run_id=run_id)
        if remote_live is not None and self.remote_live_session_store is not None:
            result = self.remote_live_session_store.get_spans_page(
                graph_id=graph_id,
                run_id=run_id,
                cursor=cursor,
                limit=limit,
                node_id=node_id,
                frame_id=frame_id,
            )
            self._run_spans_page_cache[cache_key] = (
                now_ms + RUN_DETAIL_CACHE_TTL_MS,
                result,
            )
            self._prune_query_caches()
            return result
        if (
            self.remote_run_store is not None
            and self.remote_run_store.contains_run(graph_id=graph_id, run_id=run_id)
        ):
            result = self.remote_run_store.get_spans_page(
                graph_id=graph_id,
                run_id=run_id,
                cursor=cursor,
                limit=limit,
                node_id=node_id,
                frame_id=frame_id,
            )
            self._run_spans_page_cache[cache_key] = (
                now_ms + RUN_DETAIL_CACHE_TTL_MS,
                result,
            )
            self._prune_query_caches()
            return result
        history_runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        result = load_run_spans_page(
            runs_dir=history_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            node_id=node_id,
            frame_id=frame_id,
            cursor=cursor,
            limit=limit,
        )
        self._run_spans_page_cache[cache_key] = (
            now_ms + RUN_DETAIL_CACHE_TTL_MS,
            result,
        )
        self._prune_query_caches()
        return result

    def get_run_replay(
        self,
        *,
        graph_id: str,
        run_id: str,
        loop_node_id: str | None = None,
    ) -> dict[str, JsonValue]:
        session = self._session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            return self._active_run_replay_payload(session, loop_node_id=loop_node_id)
        remote_live = self._remote_live_session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            remote_live is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            return _live_run_replay_payload(
                graph_id=graph_id,
                run_id=run_id,
                invocation_name=remote_live.invocation_name,
                success=remote_live.status.value == "succeeded",
                records=remote_live.records,
                loop_node_id=loop_node_id,
            )
        history_runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        report = build_replay_report(
            runs_dir=history_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            loop_node_id=loop_node_id,
        )
        return _as_json_object(report.as_dict())

    def get_node_detail(
        self,
        *,
        graph_id: str,
        run_id: str,
        node_id: str,
        frame_id: str | None = None,
    ) -> dict[str, JsonValue]:
        cache_key = (graph_id, run_id, node_id, frame_id)
        now_ms = int(time.time() * 1000)
        cached = self._node_detail_cache.get(cache_key)
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        session = self._session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            session is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            filtered_records = tuple(
                record
                for record in session.records
                if record.get("node_id") == node_id
                and (frame_id is None or record.get("frame_id") == frame_id)
            )
            active_result: dict[str, JsonValue] = {
                "node_id": node_id,
                "frame_id": frame_id,
                **_active_node_io_payload(
                    records=filtered_records,
                    node_id=node_id,
                ),
                "trace": {
                    "records": list(filtered_records),
                    "spans": [
                        span
                        for span in session.spans
                        if _span_node_id(span) == node_id
                        and (frame_id is None or span.get("frame_id") == frame_id)
                    ],
                },
                "available_frames": _as_json_list(
                    self._active_available_frames(
                        session=session,
                        node_id=node_id,
                    )
                ),
            }
            self._node_detail_cache[cache_key] = (
                now_ms + RUN_DETAIL_CACHE_TTL_MS,
                active_result,
            )
            self._prune_query_caches()
            return active_result
        remote_live = self._remote_live_session_for_run(graph_id=graph_id, run_id=run_id)
        if (
            remote_live is not None
            and not self._has_persisted_history(graph_id=graph_id, run_id=run_id)
        ):
            filtered_records = tuple(
                cast(dict[str, JsonValue], record)
                for record in remote_live.records
                if record.get("node_id") == node_id
                and (frame_id is None or record.get("frame_id") == frame_id)
            )
            remote_result: dict[str, JsonValue] = {
                "node_id": node_id,
                "frame_id": frame_id,
                **_active_node_io_payload(
                    records=filtered_records,
                    node_id=node_id,
                ),
                "trace": {
                    "records": list(filtered_records),
                    "spans": [
                        cast(dict[str, JsonValue], span)
                        for span in remote_live.spans
                        if _span_node_id(cast(dict[str, JsonValue], span)) == node_id
                        and (
                            frame_id is None
                            or span.get("frame_id") == frame_id
                        )
                    ],
                },
                "available_frames": _as_json_list(
                    _available_frames_from_records(records=filtered_records)
                ),
            }
            self._node_detail_cache[cache_key] = (
                now_ms + RUN_DETAIL_CACHE_TTL_MS,
                remote_result,
            )
            self._prune_query_caches()
            return remote_result
        history_runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        detail: dict[str, JsonValue] = {
            "node_id": node_id,
            "frame_id": frame_id,
        }
        try:
            detail["inputs"] = load_run_node_inputs(
                runs_dir=history_runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
            )
        except RunInspectionError as exc:
            detail["inputs_error"] = str(exc)
        try:
            detail["output"] = load_run_node_output(
                runs_dir=history_runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
            )
        except RunInspectionError as exc:
            detail["output_error"] = str(exc)
        try:
            trace = load_run_node_trace(
                runs_dir=history_runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
            )
            detail["trace"] = {
                "records": list(trace.records),
                "spans": list(trace.spans),
            }
        except RunInspectionError as exc:
            detail["trace_error"] = str(exc)
        detail["available_frames"] = _as_json_list(
            self._available_frames(
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
            )
        )
        self._node_detail_cache[cache_key] = (
            now_ms + RUN_DETAIL_CACHE_TTL_MS,
            detail,
        )
        self._prune_query_caches()
        return detail

    def _matching_sessions(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[DashboardExecutionSession, ...]:
        with self._lock:
            sessions = tuple(self._sessions.values())
        return tuple(
            session
            for session in sessions
            if (graph_id is None or session.spec.graph_id == graph_id)
            and (
                invocation_name is None
                or session.spec.invocation_name == invocation_name
            )
        )

    def _matching_remote_live_sessions(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[RemoteLiveSessionRecord, ...]:
        if self.remote_live_session_store is None:
            return ()
        return self.remote_live_session_store.list_sessions(
            graph_id=graph_id,
            invocation_name=invocation_name,
        )

    def _session_for_run(
        self,
        *,
        graph_id: str,
        run_id: str,
    ) -> DashboardExecutionSession | None:
        for session in self._matching_sessions(graph_id=graph_id):
            if session.run_id == run_id:
                return session
        return None

    def _remote_live_session_for_run(
        self,
        *,
        graph_id: str,
        run_id: str,
        include_payloads: bool = True,
    ) -> RemoteLiveSessionRecord | None:
        if self.remote_live_session_store is None:
            return None
        now_ms = int(time.time() * 1000)
        cache_key = (graph_id, run_id, include_payloads)
        cached = self._remote_live_session_cache.get(cache_key)
        if cached is not None and cached[0] > now_ms:
            return cached[1]
        try:
            session = self.remote_live_session_store.get_session(
                graph_id=graph_id,
                run_id=run_id,
                include_payloads=include_payloads,
            )
            self._remote_live_session_cache[cache_key] = (
                now_ms + REMOTE_LIVE_SESSION_CACHE_TTL_MS,
                session,
            )
            self._prune_query_caches()
            return session
        except RunInspectionError:
            return None

    def _active_run_handle(
        self,
        session: DashboardExecutionSession,
    ) -> DashboardRunHandle | None:
        if session.run_id is None:
            return None
        output_count = sum(
            1
            for record in session.records
            if record.get("event_type") == "node.succeeded"
        )
        return DashboardRunHandle(
            schema_version=0,
            graph_id=session.spec.graph_id,
            run_id=session.run_id,
            created_at_ms=session.started_at_ms,
            status=session.status,
            success=(
                True
                if session.status == "succeeded"
                else False if session.status == "failed" else None
            ),
            node_count=0,
            edge_count=0,
            record_count=len(session.records),
            output_count=output_count,
            state_count=0,
            invocation_name=session.spec.invocation_name,
            runtime_default_profile_name=None,
            runtime_profile_names=(),
            trace_mode="live",
            trace_service_name="mentalmodel",
            run_dir=session.run_artifacts_dir or "",
            source="active",
            execution_id=session.execution_id,
            availability=DashboardRunAvailability(
                summary=True,
                records=bool(session.records),
                spans=bool(session.spans),
                replay=bool(session.records),
                custom_views=False,
            ),
        )

    def _remote_live_run_handle(
        self,
        session: RemoteLiveSessionRecord,
    ) -> DashboardRunHandle:
        return DashboardRunHandle(
            schema_version=0,
            graph_id=session.graph_id,
            run_id=session.run_id,
            created_at_ms=session.started_at_ms,
            status=session.status.value,
            success=(
                True
                if session.status.value == "succeeded"
                else False if session.status.value == "failed" else None
            ),
            node_count=_graph_node_count(session.graph),
            edge_count=_graph_edge_count(session.graph),
            record_count=len(session.records),
            output_count=_remote_live_output_count(session.records),
            state_count=0,
            invocation_name=session.invocation_name,
            runtime_default_profile_name=session.runtime_default_profile_name,
            runtime_profile_names=session.runtime_profile_names,
            trace_mode="live",
            trace_service_name="mentalmodel",
            run_dir="",
            source="remote-live",
            availability=DashboardRunAvailability(
                summary=True,
                records=bool(session.records),
                spans=bool(session.spans),
                replay=bool(session.records),
                custom_views=False,
            ),
        )

    def _has_persisted_history(self, *, graph_id: str, run_id: str) -> bool:
        runs_root = (
            self.runs_dir
            if self.remote_run_store is None
            else self.remote_run_store.runs_root
        )
        try:
            self._history_runs_dir(graph_id=graph_id, run_id=run_id)
            resolve_run_summary(
                runs_dir=runs_root,
                graph_id=graph_id,
                run_id=run_id,
            )
            return True
        except RunInspectionError:
            return False

    def _graph_payload_for_entry(
        self,
        entry: DashboardCatalogEntry,
    ) -> dict[str, JsonValue]:
        external_project = self._external_project_for_entry(entry)
        if external_project is not None:
            payload = self._load_external_catalog_graph(entry, external_project.project.root_dir)
            return _as_json_object(payload["graph"])
        invocation = read_verify_invocation_spec(entry.spec_path)
        _, program = load_workflow_subject(invocation.program)
        return _graph_to_payload(lower_program(program))

    def _remote_catalog_graph_payload(
        self,
        entry: DashboardCatalogEntry,
    ) -> dict[str, JsonValue]:
        summary = self._latest_summary_for_entry(entry)
        if summary is None:
            return {
                "graph": {
                    "graph_id": entry.graph_id,
                    "metadata": {},
                    "nodes": [],
                    "edges": [],
                },
                "analysis": {
                    "error_count": 0,
                    "warning_count": 0,
                    "findings": [],
                },
            }
        return {
            "graph": self.get_run_graph(graph_id=summary.graph_id, run_id=summary.run_id),
            "analysis": {
                "error_count": 0,
                "warning_count": 0,
                "findings": [],
            },
        }

    def _latest_summary_for_entry(
        self,
        entry: DashboardCatalogEntry,
    ) -> RunSummary | None:
        summaries = (
            self.remote_run_store.list_run_summaries(
                graph_id=entry.graph_id,
                invocation_name=entry.invocation_name,
            )
            if self.remote_run_store is not None
            else list_run_summaries(
                runs_dir=self.runs_dir,
                graph_id=entry.graph_id,
                invocation_name=entry.invocation_name,
            )
        )
        if not summaries:
            return None
        return max(summaries, key=lambda summary: (summary.created_at_ms, summary.run_id))

    def _active_run_replay_payload(
        self,
        session: DashboardExecutionSession,
        *,
        loop_node_id: str | None,
    ) -> dict[str, JsonValue]:
        records = [
            record
            for record in session.records
            if loop_node_id is None or record.get("loop_node_id") == loop_node_id
        ]
        seen_frames: list[str] = []
        node_summaries: dict[tuple[str, str], dict[str, JsonValue]] = {}
        for record in records:
            raw_frame_id = record.get("frame_id")
            frame_id = raw_frame_id if isinstance(raw_frame_id, str) else "root"
            if frame_id not in seen_frames:
                seen_frames.append(frame_id)
            raw_node_id = record.get("node_id")
            node_id = raw_node_id if isinstance(raw_node_id, str) else "unknown"
            raw_event_type = record.get("event_type")
            event_type = raw_event_type if isinstance(raw_event_type, str) else "unknown"
            key = (node_id, frame_id)
            node_summaries[key] = {
                "node_id": node_id,
                "frame_id": frame_id,
                "loop_node_id": record.get("loop_node_id"),
                "iteration_index": record.get("iteration_index"),
                "succeeded": False,
                "failed": False,
                "invariant_status": None,
                "invariant_passed": None,
                "invariant_severity": None,
                "last_event_type": event_type,
            }
        return {
            "graph_id": session.spec.graph_id,
            "run_id": session.run_id,
            "invocation_name": session.spec.invocation_name,
            "success": session.status == "succeeded",
            "event_count": len(records),
            "node_count": len({record.get("node_id") for record in records}),
            "frame_ids": _as_json_list(seen_frames),
            "events": _as_json_list(records),
            "node_summaries": _as_json_list(list(node_summaries.values())),
        }

    def _active_available_frames(
        self,
        *,
        session: DashboardExecutionSession,
        node_id: str,
    ) -> list[dict[str, JsonValue]]:
        seen: list[dict[str, JsonValue]] = []
        seen_keys: set[tuple[str, str | None, int | None]] = set()
        for record in session.records:
            if record.get("node_id") != node_id:
                continue
            frame_id = record.get("frame_id")
            if not isinstance(frame_id, str):
                continue
            loop_node_id = record.get("loop_node_id")
            iteration_index = record.get("iteration_index")
            key = (
                frame_id,
                loop_node_id if isinstance(loop_node_id, str) else None,
                iteration_index if isinstance(iteration_index, int) else None,
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            seen.append(
                {
                    "frame_id": frame_id,
                    "loop_node_id": key[1],
                    "iteration_index": key[2],
                }
            )
        return seen

    def _run_session(self, session: DashboardExecutionSession) -> None:
        try:
            invocation = read_verify_invocation_spec(session.spec.spec_path)
            external_project = self._external_project_for_entry(session.spec)
            run_target = self._run_target_for_entry(
                session.spec,
                spec_runs_dir=invocation.runs_dir,
            )
            if external_project is not None:
                session.mark_running()
                external_report = self._run_external_verification(
                    session.spec,
                    external_project.project.root_dir,
                    session=session,
                    run_target=run_target,
                )
                self._publish_completed_external_run(
                    external_report,
                    run_target=run_target,
                )
                session.mark_completed_from_payload(external_report)
                return
            module, program = load_workflow_subject(invocation.program)
            environment = None
            if invocation.environment is not None:
                _, environment = load_runtime_environment_subject(invocation.environment)
            verification_report = run_verification(
                program,
                module=module,
                runs_dir=run_target.runs_dir,
                environment=environment,
                invocation_name=invocation.invocation_name,
                record_listeners=(session.on_record,),
                completed_run_sink=self._completed_run_sink(run_target),
            )
            session.mark_completed(verification_report)
        except Exception as exc:  # pragma: no cover - guarded by API tests
            session.mark_failed(f"{type(exc).__name__}: {exc}")

    def _external_project_for_entry(self, entry: DashboardCatalogEntry) -> ProjectCatalog | None:
        if entry.project_id is None:
            return None
        project_catalog = self._project_catalog_by_id.get(entry.project_id)
        if project_catalog is None:
            return None
        current_root = Path(__file__).resolve().parents[3]
        if project_catalog.project.root_dir.resolve() == current_root.resolve():
            return None
        return project_catalog

    def _project_for_spec_path(self, spec_path: Path) -> ProjectCatalog | None:
        project = find_project_registration_for_path(
            tuple(project_catalog.project for project_catalog in self._project_catalogs),
            spec_path,
        )
        if project is None:
            return None
        return self._project_catalog_by_id.get(project.project_id)

    def _catalog_entry_from_path(self, spec_path: Path) -> DashboardCatalogEntry:
        resolved = spec_path.expanduser().resolve()
        project_catalog = self._project_for_spec_path(resolved)
        if project_catalog is None:
            return catalog_entry_from_spec_path(resolved)
        external_project = self._external_project_for_entry(
            DashboardCatalogEntry(
                spec_id="project-probe",
                label=resolved.stem,
                description=str(resolved),
                spec_path=resolved,
                graph_id="probe",
                invocation_name="probe",
                project_id=project_catalog.project.project_id,
                project_label=project_catalog.project.label,
            )
        )
        if external_project is None:
            entry = catalog_entry_from_spec_path(resolved)
        else:
            metadata = self._load_external_spec_metadata(
                resolved,
                external_project.project.root_dir,
            )
            graph_id = metadata.get("graph_id")
            invocation_name = metadata.get("invocation_name")
            if not isinstance(graph_id, str):
                raise DashboardCatalogError("External spec metadata must include graph_id.")
            if invocation_name is not None and not isinstance(invocation_name, str):
                raise DashboardCatalogError(
                    "External spec metadata invocation_name must be a string when present."
                )
            digest = hashlib.sha256(str(resolved).encode()).hexdigest()[:12]
            entry = DashboardCatalogEntry(
                spec_id=f"path-{digest}",
                label=resolved.stem,
                description=str(resolved),
                spec_path=resolved,
                graph_id=graph_id,
                invocation_name=invocation_name or "verify",
                category="custom",
                tags=("spec-path",),
                catalog_source="spec-path",
            )
        return replace(
            entry,
            project_id=project_catalog.project.project_id,
            project_label=project_catalog.project.label,
            catalog_source=entry.catalog_source or "spec-path",
        )

    def _load_external_catalog_graph(
        self,
        entry: DashboardCatalogEntry,
        root_dir: Path,
    ) -> dict[str, dict[str, object]]:
        payload = self._run_external_python(
            root_dir=root_dir,
            script=_EXTERNAL_GRAPH_SCRIPT,
            args=(str(entry.spec_path),),
        )
        graph = payload.get("graph")
        analysis = payload.get("analysis")
        if not isinstance(graph, dict) or not isinstance(analysis, dict):
            raise DashboardCatalogError(
                "External catalog graph helper must return graph and analysis objects."
            )
        return {
            "graph": cast(dict[str, object], graph),
            "analysis": cast(dict[str, object], analysis),
        }

    def _run_external_verification(
        self,
        entry: DashboardCatalogEntry,
        root_dir: Path,
        *,
        session: DashboardExecutionSession,
        run_target: ProjectRunTarget,
    ) -> dict[str, object]:
        session.add_message(
            level="info",
            message=f"Launching external verification in {root_dir}",
        )
        result = self._project_execution_worker.execute(
            spec_path=entry.spec_path,
            root_dir=root_dir,
            run_target=run_target,
            on_event=lambda event: self._apply_worker_event(
                session=session,
                event=event,
            ),
        )
        return cast(dict[str, object], result.payload)

    def _run_target_for_entry(
        self,
        entry: DashboardCatalogEntry,
        *,
        spec_runs_dir: Path | None,
    ) -> ProjectRunTarget:
        project = (
            None
            if entry.project_id is None
            else (
                None
                if entry.project_id not in self._project_catalog_by_id
                else self._project_catalog_by_id[entry.project_id].project
            )
        )
        fallback_runs_dir = self.runs_dir or spec_runs_dir
        if fallback_runs_dir is None and self.remote_run_store is not None:
            fallback_runs_dir = self.remote_run_store.cache_dir
        return build_project_run_target(
            project=project,
            fallback_runs_dir=fallback_runs_dir,
            catalog_entry_id=entry.spec_id,
            catalog_source=entry.catalog_source,
        )

    def _run_target_for_spec_path(self, spec_path: Path) -> ProjectRunTarget:
        project_catalog = self._project_for_spec_path(spec_path)
        project = None if project_catalog is None else project_catalog.project
        invocation = read_verify_invocation_spec(spec_path)
        fallback_runs_dir = self.runs_dir or invocation.runs_dir
        if fallback_runs_dir is None and self.remote_run_store is not None:
            fallback_runs_dir = self.remote_run_store.cache_dir
        return build_project_run_target(
            project=project,
            fallback_runs_dir=fallback_runs_dir,
            catalog_source="spec-path",
        )

    def _completed_run_sink(
        self,
        run_target: ProjectRunTarget,
    ) -> RemoteCompletedRunSink | None:
        if self.remote_run_store is None:
            return None
        return RemoteCompletedRunSink(
            self.remote_run_store,
            project_id=run_target.project_id,
            project_label=run_target.project_label,
            environment_name=run_target.environment_name,
            catalog_entry_id=run_target.catalog_entry_id,
            catalog_source=run_target.catalog_source,
        )

    def _publish_completed_external_run(
        self,
        payload: dict[str, object],
        *,
        run_target: ProjectRunTarget,
    ) -> None:
        runtime = payload.get("runtime")
        if not isinstance(runtime, dict):
            return
        run_artifacts_dir = runtime.get("run_artifacts_dir")
        if not isinstance(run_artifacts_dir, str) or not run_artifacts_dir:
            return
        sink = self._completed_run_sink(run_target)
        if sink is None:
            return
        sink.publish_run_dir(Path(run_artifacts_dir))

    def _load_external_spec_metadata(
        self,
        spec_path: Path,
        root_dir: Path,
    ) -> dict[str, object]:
        return self._run_external_python(
            root_dir=root_dir,
            script=_EXTERNAL_SPEC_METADATA_SCRIPT,
            args=(str(spec_path),),
        )

    def _run_external_python(
        self,
        *,
        root_dir: Path,
        script: str,
        args: tuple[str, ...],
    ) -> dict[str, object]:
        command = [
            "uv",
            "run",
            "--directory",
            str(root_dir),
            "python",
            "-c",
            script,
            *args,
        ]
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            message = completed.stderr.strip() or completed.stdout.strip() or (
                f"External project command failed with exit code {completed.returncode}."
            )
            raise DashboardCatalogError(message)
        decoded = json.loads(completed.stdout)
        if not isinstance(decoded, dict):
            raise DashboardCatalogError("External project helper must return a JSON object.")
        return cast(dict[str, object], decoded)

    def _load_persisted_metric_rows(
        self,
        *,
        graph_id: str,
        run_id: str,
        step_start: int | None,
        step_end: int | None,
        node_id: str | None,
        frame_id: str | None,
        path_prefixes: Sequence[str],
    ) -> tuple[IndexedMetricRow, ...]:
        if (
            self.remote_run_store is not None
            and self.remote_run_store.contains_run(graph_id=graph_id, run_id=run_id)
        ):
            return self.remote_run_store.list_metrics(
                graph_id=graph_id,
                run_id=run_id,
                step_start=step_start,
                step_end=step_end,
                node_id=node_id,
                frame_id=frame_id,
                path_prefixes=path_prefixes,
            )
        outputs_payload = load_run_payload(
            runs_dir=self._history_runs_dir(graph_id=graph_id, run_id=run_id),
            graph_id=graph_id,
            run_id=run_id,
            filename="outputs.json",
        )
        rows = metric_rows_from_outputs_payload(outputs_payload)
        return tuple(
            row
            for row in rows
            if (node_id is None or row.node_id == node_id)
            and (frame_id is None or row.frame_id == frame_id)
            and (
                step_start is None
                or row.iteration_index is None
                or row.iteration_index >= step_start
            )
            and (
                step_end is None
                or row.iteration_index is None
                or row.iteration_index <= step_end
            )
            and (
                not path_prefixes
                or any(
                    row.path.startswith(prefix)
                    or row.metric_node_path.startswith(prefix)
                    or row.label.startswith(prefix)
                    or row.normalized_label.startswith(prefix)
                    for prefix in path_prefixes
                )
            )
        )

    def _load_persisted_invariants(
        self,
        *,
        graph_id: str,
        run_id: str,
    ) -> list[dict[str, JsonValue]]:
        if self.remote_run_store is not None and self.remote_run_store.contains_run(
            graph_id=graph_id,
            run_id=run_id,
        ):
            try:
                return _invariants_from_live_records(
                    self.remote_run_store.list_invariants(
                        graph_id=graph_id,
                        run_id=run_id,
                    )
                )
            except RunInspectionError:
                pass
        records = load_run_records(
            runs_dir=self._history_runs_dir(graph_id=graph_id, run_id=run_id),
            graph_id=graph_id,
            run_id=run_id,
        )
        return _invariants_from_live_records(records)

    def _apply_worker_event(
        self,
        *,
        session: DashboardExecutionSession,
        event: WorkerExecutionEvent,
    ) -> None:
        if event.kind == "record":
            session.on_record_payload(event.payload)
            return
        if event.kind == "span":
            session.on_span_payload(event.payload)
            return
        if event.kind == "message":
            message = event.payload.get("message")
            if not isinstance(message, str) or not message:
                return
            level = event.payload.get("level")
            source = event.payload.get("source")
            session.add_message(
                level=level if isinstance(level, str) else "info",
                message=message,
                source=source if isinstance(source, str) else "project-worker",
            )
            return
        if event.kind == "lifecycle":
            status = event.payload.get("status")
            if isinstance(status, str) and status:
                session.add_message(
                    level="info",
                    message=f"External run status: {status}",
                    source="project-worker",
                )
            return

    def _available_frames(
        self,
        *,
        graph_id: str,
        run_id: str,
        node_id: str,
    ) -> list[dict[str, JsonValue]]:
        replay = build_replay_report(
            runs_dir=self._require_history_runs_dir(graph_id=graph_id, run_id=run_id),
            graph_id=graph_id,
            run_id=run_id,
        )
        frames: list[dict[str, JsonValue]] = []
        for summary in replay.node_summaries:
            if summary.node_id != node_id:
                continue
            if summary.frame_id == "root":
                continue
            frames.append(
                {
                    "frame_id": summary.frame_id,
                    "loop_node_id": summary.loop_node_id,
                    "iteration_index": summary.iteration_index,
                }
            )
        return frames

    def aggregate_record_timeseries(
        self,
        *,
        graph_id: str,
        invocation_name: str,
        since_ms: int,
        until_ms: int,
        rollup_ms: int,
        run_id: str | None = None,
        node_id: str | None = None,
    ) -> dict[str, JsonValue]:
        """Bucket semantic records into time slices with Datadog-style rates (per second).

        Counts records whose ``timestamp_ms`` falls in ``[since_ms, until_ms)``.
        ``loop_events`` counts records with a non-null ``iteration_index``.
        ``unique_nodes`` is the count of distinct ``node_id`` values per bucket (not a rate
        of new nodes; displayed as nodes active in that interval).
        """

        if self.runs_dir is None and self.remote_run_store is None:
            return _empty_timeseries(
                graph_id=graph_id,
                invocation_name=invocation_name,
                since_ms=since_ms,
                until_ms=until_ms,
                rollup_ms=rollup_ms,
            )
        if since_ms >= until_ms or rollup_ms <= 0:
            raise ValueError("since_ms must be < until_ms and rollup_ms must be positive.")
        cache_key = (
            graph_id,
            invocation_name,
            since_ms,
            until_ms,
            rollup_ms,
            run_id,
            node_id,
        )
        now_ms = int(time.time() * 1000)
        cached = self._timeseries_cache.get(cache_key)
        if cached is not None and cached[0] > now_ms:
            return cached[1]

        span = until_ms - since_ms
        max_buckets = 500
        effective_rollup = rollup_ms
        num_buckets = max(1, (span + effective_rollup - 1) // effective_rollup)
        if num_buckets > max_buckets:
            effective_rollup = max(rollup_ms, (span + max_buckets - 1) // max_buckets)
            num_buckets = max(1, (span + effective_rollup - 1) // effective_rollup)
            num_buckets = min(num_buckets, max_buckets)
        rollup_ms = effective_rollup

        summaries = (
            self.remote_run_store.list_run_summaries(
                graph_id=graph_id,
                invocation_name=invocation_name,
            )
            if self.remote_run_store is not None
            else list_run_summaries(
                runs_dir=self.runs_dir,
                graph_id=graph_id,
                invocation_name=invocation_name,
            )
        )
        if run_id is not None:
            summaries = tuple(s for s in summaries if s.run_id == run_id)
        else:
            # Approximate which bundles may contain events in [since_ms, until_ms)
            # by run start time.
            windowed = tuple(
                s for s in summaries if since_ms <= s.created_at_ms < until_ms
            )
            if windowed:
                summaries = windowed[:200]
            else:
                summaries = summaries[: min(100, len(summaries))]

        record_counts = [0] * num_buckets
        loop_counts = [0] * num_buckets
        unique_node_counts = [0] * num_buckets

        if self.remote_run_store is not None and any(
            self.remote_run_store.contains_run(graph_id=summary.graph_id, run_id=summary.run_id)
            for summary in summaries
        ):
            bucket_rows = self.remote_run_store.aggregate_record_timeseries(
                graph_id=graph_id,
                invocation_name=invocation_name,
                since_ms=since_ms,
                until_ms=until_ms,
                rollup_ms=rollup_ms,
                run_id=run_id,
                node_id=node_id,
            )
            for bucket_index, record_count, loop_count, unique_nodes in bucket_rows:
                if bucket_index < 0 or bucket_index >= num_buckets:
                    continue
                record_counts[bucket_index] = record_count
                loop_counts[bucket_index] = loop_count
                unique_node_counts[bucket_index] = unique_nodes
        else:
            node_sets: list[set[str]] = [set() for _ in range(num_buckets)]
            for summary in summaries:
                if self.remote_run_store is not None:
                    self.remote_run_store.materialize_run(
                        graph_id=summary.graph_id,
                        run_id=summary.run_id,
                    )
                records_path = summary.run_dir / "records.jsonl"
                if not records_path.is_file():
                    continue
                with records_path.open(encoding="utf-8") as handle:
                    for line in handle:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(row, dict):
                            continue
                        ts = row.get("timestamp_ms")
                        if not isinstance(ts, int):
                            continue
                        if ts < since_ms or ts >= until_ms:
                            continue
                        nid = row.get("node_id")
                        if node_id is not None:
                            if nid != node_id:
                                continue
                        bi = (ts - since_ms) // rollup_ms
                        if bi < 0 or bi >= num_buckets:
                            continue
                        record_counts[bi] += 1
                        if isinstance(nid, str):
                            node_sets[bi].add(nid)
                        it = row.get("iteration_index")
                        if isinstance(it, int):
                            loop_counts[bi] += 1
            unique_node_counts = [len(node_set) for node_set in node_sets]

        secs = rollup_ms / 1000.0
        buckets: list[dict[str, JsonValue]] = []
        for i in range(num_buckets):
            start = since_ms + i * rollup_ms
            end = min(start + rollup_ms, until_ms)
            rc = record_counts[i]
            lc = loop_counts[i]
            un = unique_node_counts[i]
            buckets.append(
                {
                    "start_ms": start,
                    "end_ms": end,
                    "records_per_sec": rc / secs if secs else 0.0,
                    "loop_events_per_sec": lc / secs if secs else 0.0,
                    "unique_nodes": un,
                    "unique_nodes_per_sec": un / secs if secs else 0.0,
                }
            )

        result: dict[str, JsonValue] = {
            "rollup_ms": rollup_ms,
            "since_ms": since_ms,
            "until_ms": until_ms,
            "graph_id": graph_id,
            "invocation_name": invocation_name,
            "buckets": _as_json_list(buckets),
            "runs_scanned": len(summaries),
        }
        self._timeseries_cache[cache_key] = (
            now_ms + TIMESERIES_CACHE_TTL_MS,
            result,
        )
        self._prune_query_caches()
        return result

    def _require_session(self, execution_id: str) -> DashboardExecutionSession:
        with self._lock:
            session = self._sessions.get(execution_id)
        if session is None:
            raise KeyError(execution_id)
        return session

    def _prune_query_caches(self) -> None:
        now_ms = int(time.time() * 1000)
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._run_list_cache),
            now_ms,
        )
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._metric_groups_cache),
            now_ms,
        )
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._timeseries_cache),
            now_ms,
        )
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._catalog_graph_cache),
            now_ms,
        )
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._run_overview_cache),
            now_ms,
        )
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._run_records_page_cache),
            now_ms,
        )
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._run_spans_page_cache),
            now_ms,
        )
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._node_detail_cache),
            now_ms,
        )
        self._prune_cache_mapping(
            cast(dict[object, tuple[int, object]], self._remote_live_session_cache),
            now_ms,
        )

    def _prune_cache_mapping(
        self,
        cache: dict[object, tuple[int, object]],
        now_ms: int,
    ) -> None:
        expired_keys = [key for key, value in cache.items() if value[0] <= now_ms]
        for key in expired_keys:
            del cache[key]
        if len(cache) > 256:
            for key in sorted(cache, key=lambda item: cache[item][0])[: len(cache) - 256]:
                del cache[key]

    def _history_runs_dir(self, *, graph_id: str, run_id: str) -> Path | None:
        if self.remote_run_store is None:
            return self.runs_dir
        self.remote_run_store.materialize_run(graph_id=graph_id, run_id=run_id)
        return self.remote_run_store.runs_root

    def _require_history_runs_dir(self, *, graph_id: str, run_id: str) -> Path:
        runs_dir = self._history_runs_dir(graph_id=graph_id, run_id=run_id)
        if runs_dir is None:
            raise DashboardCatalogError(
                f"No runs directory is available for {graph_id!r}/{run_id!r}."
            )
        return runs_dir


def _graph_to_payload(graph: IRGraph) -> dict[str, JsonValue]:
    from mentalmodel.ir.serialization import ir_graph_to_json

    return ir_graph_to_json(graph)


def _analysis_to_payload(report: AnalysisReport) -> dict[str, JsonValue]:
    return {
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "findings": [
            {
                "code": finding.code,
                "severity": finding.severity,
                "message": finding.message,
                "node_id": finding.node_id,
            }
            for finding in report.findings
        ],
    }


def _empty_timeseries(
    *,
    graph_id: str,
    invocation_name: str,
    since_ms: int,
    until_ms: int,
    rollup_ms: int,
) -> dict[str, JsonValue]:
    """Return an empty timeseries when no runs dir or no data."""

    span = max(0, until_ms - since_ms)
    max_buckets = 500
    effective_rollup = rollup_ms if rollup_ms > 0 else 60_000
    num_buckets = max(1, (span + effective_rollup - 1) // effective_rollup)
    if num_buckets > max_buckets:
        effective_rollup = max(effective_rollup, (span + max_buckets - 1) // max_buckets)
        num_buckets = max(1, (span + effective_rollup - 1) // effective_rollup)
        num_buckets = min(num_buckets, max_buckets)
    rollup_ms = effective_rollup
    buckets: list[dict[str, JsonValue]] = []
    for i in range(num_buckets):
        start = since_ms + i * rollup_ms
        end = min(start + rollup_ms, until_ms)
        buckets.append(
            {
                "start_ms": start,
                "end_ms": end,
                "records_per_sec": 0.0,
                "loop_events_per_sec": 0.0,
                "unique_nodes": 0,
                "unique_nodes_per_sec": 0.0,
            }
        )
    return {
        "rollup_ms": rollup_ms,
        "since_ms": since_ms,
        "until_ms": until_ms,
        "graph_id": graph_id,
        "invocation_name": invocation_name,
        "buckets": _as_json_list(buckets),
        "runs_scanned": 0,
    }

def _safe_load_payload(
    *,
    runs_dir: Path | None,
    graph_id: str,
    run_id: str,
    filename: str,
) -> dict[str, JsonValue] | None:
    try:
        return load_run_payload(
            runs_dir=runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            filename=filename,
        )
    except RunInspectionError:
        return None


def _runtime_error_from_verification(
    verification: dict[str, JsonValue] | None,
) -> str | None:
    if not isinstance(verification, dict):
        return None
    runtime = verification.get("runtime")
    if not isinstance(runtime, dict):
        return None
    error = runtime.get("error")
    return error if isinstance(error, str) and error else None

def _record_sequence(record: dict[str, JsonValue]) -> int:
    value = record.get("sequence")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _message_sequence(message: dict[str, JsonValue]) -> int:
    value = message.get("sequence")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _span_sequence(span: dict[str, JsonValue]) -> int:
    value = span.get("sequence")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _span_node_id(span: dict[str, JsonValue]) -> str | None:
    attributes = span.get("attributes")
    if not isinstance(attributes, dict):
        return None
    value = attributes.get("mentalmodel.node.id")
    return value if isinstance(value, str) else None


def _span_frame_id(span: dict[str, JsonValue]) -> str | None:
    value = span.get("frame_id")
    if isinstance(value, str):
        return value
    attributes = span.get("attributes")
    if not isinstance(attributes, dict):
        return None
    attr_value = attributes.get("mentalmodel.frame.id")
    return attr_value if isinstance(attr_value, str) else None


def _graph_node_count(graph: dict[str, object]) -> int:
    nodes = graph.get("nodes")
    return len(nodes) if isinstance(nodes, list) else 0


def _graph_edge_count(graph: dict[str, object]) -> int:
    edges = graph.get("edges")
    return len(edges) if isinstance(edges, list) else 0


def _remote_live_output_count(records: Sequence[dict[str, object]]) -> int:
    return sum(1 for record in records if record.get("event_type") == "node.succeeded")


def _graph_payload_from_live_session(
    session: RemoteLiveSessionRecord,
) -> dict[str, JsonValue]:
    return _as_json_object(session.graph)


def _live_run_replay_payload(
    *,
    graph_id: str,
    run_id: str,
    invocation_name: str | None,
    success: bool,
    records: Sequence[dict[str, object]],
    loop_node_id: str | None,
) -> dict[str, JsonValue]:
    filtered_records = [
        cast(dict[str, JsonValue], record)
        for record in records
        if loop_node_id is None or record.get("loop_node_id") == loop_node_id
    ]
    seen_frames: list[str] = []
    node_summaries: dict[tuple[str, str], dict[str, JsonValue]] = {}
    for record in filtered_records:
        raw_frame_id = record.get("frame_id")
        frame_id = raw_frame_id if isinstance(raw_frame_id, str) else "root"
        if frame_id not in seen_frames:
            seen_frames.append(frame_id)
        raw_node_id = record.get("node_id")
        node_id = raw_node_id if isinstance(raw_node_id, str) else "unknown"
        raw_event_type = record.get("event_type")
        event_type = raw_event_type if isinstance(raw_event_type, str) else "unknown"
        key = (node_id, frame_id)
        summary = node_summaries.setdefault(
            key,
            {
                "node_id": node_id,
                "frame_id": frame_id,
                "loop_node_id": record.get("loop_node_id"),
                "iteration_index": record.get("iteration_index"),
                "succeeded": False,
                "failed": False,
                "invariant_status": None,
                "invariant_passed": None,
                "invariant_severity": None,
                "last_event_type": event_type,
            },
        )
        summary["last_event_type"] = event_type
        if event_type == "node.succeeded":
            summary["succeeded"] = True
        elif event_type == "node.failed":
            summary["failed"] = True
        elif event_type == "invariant.checked":
            payload = record.get("payload")
            if isinstance(payload, dict):
                passed = payload.get("passed")
                severity = payload.get("severity")
                summary["invariant_passed"] = passed if isinstance(passed, bool) else None
                summary["invariant_severity"] = (
                    severity if isinstance(severity, str) else None
                )
                summary["invariant_status"] = (
                    "pass"
                    if passed is True
                    else "fail"
                    if passed is False
                    else None
                )
    return {
        "graph_id": graph_id,
        "run_id": run_id,
        "invocation_name": invocation_name,
        "success": success,
        "event_count": len(filtered_records),
        "node_count": len({record.get("node_id") for record in filtered_records}),
        "frame_ids": _as_json_list(seen_frames),
        "events": _as_json_list(filtered_records),
        "node_summaries": _as_json_list(list(node_summaries.values())),
    }


def _invariants_from_live_records(
    records: Sequence[dict[str, JsonValue]],
) -> list[dict[str, JsonValue]]:
    invariants: list[dict[str, JsonValue]] = []
    for record in records:
        if record.get("event_type") != "invariant.checked":
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        passed = payload.get("passed")
        severity = payload.get("severity")
        node_id = record.get("node_id")
        frame_id = record.get("frame_id")
        loop_node_id = record.get("loop_node_id")
        iteration_index = record.get("iteration_index")
        invariants.append(
            {
                "node_id": node_id if isinstance(node_id, str) else None,
                "frame_id": frame_id if isinstance(frame_id, str) else None,
                "loop_node_id": (
                    loop_node_id if isinstance(loop_node_id, str) else None
                ),
                "iteration_index": (
                    iteration_index if isinstance(iteration_index, int) else None
                ),
                "status": (
                    "pass"
                    if passed is True
                    else "fail"
                    if passed is False
                    else None
                ),
                "passed": passed if isinstance(passed, bool) else None,
                "severity": severity if isinstance(severity, str) else None,
            }
        )
    return invariants


def _available_frames_from_records(
    *,
    records: Sequence[dict[str, JsonValue]],
) -> list[dict[str, JsonValue]]:
    seen: list[dict[str, JsonValue]] = []
    seen_keys: set[tuple[str, str | None, int | None]] = set()
    for record in records:
        frame_id = record.get("frame_id")
        if not isinstance(frame_id, str):
            continue
        loop_node_id = record.get("loop_node_id")
        iteration_index = record.get("iteration_index")
        key = (
            frame_id,
            loop_node_id if isinstance(loop_node_id, str) else None,
            iteration_index if isinstance(iteration_index, int) else None,
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        seen.append(
            {
                "frame_id": frame_id,
                "loop_node_id": key[1],
                "iteration_index": key[2],
            }
        )
    return seen


def _active_node_io_payload(
    *,
    records: Sequence[dict[str, JsonValue]],
    node_id: str,
) -> dict[str, JsonValue]:
    inputs: JsonValue | None = None
    output: JsonValue | None = None
    for record in records:
        event_type = record.get("event_type")
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        if event_type == "node.inputs_resolved":
            record_inputs = payload.get("inputs")
            if record_inputs is not None:
                inputs = record_inputs
        if event_type == "node.succeeded":
            record_output = payload.get("output")
            if record_output is not None:
                output = record_output
    detail: dict[str, JsonValue] = {}
    if inputs is None:
        detail["inputs_error"] = (
            f"Resolved inputs for node {node_id!r} have not streamed yet."
        )
    else:
        detail["inputs"] = inputs
    if output is None:
        detail["output_error"] = (
            f"Node output for {node_id!r} is not available until the node succeeds."
        )
    else:
        detail["output"] = output
    return detail


def _resolve_custom_view(
    entry: DashboardCatalogEntry,
    view_id: str,
) -> DashboardCustomView:
    for view in entry.custom_views:
        if view.view_id == view_id:
            return view
    raise DashboardCatalogError(
        f"Unknown custom view {view_id!r} for dashboard entry {entry.spec_id!r}."
    )


def _as_json_object(value: object) -> dict[str, JsonValue]:
    json_value = _as_json_value(value)
    if not isinstance(json_value, dict):
        raise TypeError("Expected JSON object value.")
    return json_value


def _as_json_list(values: Sequence[object]) -> list[JsonValue]:
    return [_as_json_value(value) for value in values]


def _record_without_payload(row: dict[str, JsonValue]) -> dict[str, JsonValue]:
    return {key: value for key, value in row.items() if key != "payload"}


def _as_json_value(value: object) -> JsonValue:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_as_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _as_json_value(item) for key, item in value.items()}
    raise TypeError(f"Unsupported JSON value type {type(value).__name__}.")


_EXTERNAL_GRAPH_SCRIPT = """
import json
import sys
from pathlib import Path

from mentalmodel.analysis import run_analysis
from mentalmodel.invocation import load_workflow_subject, read_verify_invocation_spec
from mentalmodel.ir.lowering import lower_program
from mentalmodel.ir.serialization import ir_graph_to_json

spec_path = Path(sys.argv[1])
invocation = read_verify_invocation_spec(spec_path)
_, program = load_workflow_subject(invocation.program)
graph = lower_program(program)
report = run_analysis(graph)
print(
    json.dumps(
        {
            "graph": ir_graph_to_json(graph),
            "analysis": {
                "error_count": report.error_count,
                "warning_count": report.warning_count,
                "findings": [
                    {
                        "code": finding.code,
                        "severity": finding.severity,
                        "message": finding.message,
                        "node_id": finding.node_id,
                    }
                    for finding in report.findings
                ],
            },
        }
    )
)
""".strip()


_EXTERNAL_SPEC_METADATA_SCRIPT = """
import json
import sys
from pathlib import Path

from mentalmodel.invocation import load_workflow_subject, read_verify_invocation_spec
from mentalmodel.ir.lowering import lower_program

spec_path = Path(sys.argv[1])
invocation = read_verify_invocation_spec(spec_path)
_, program = load_workflow_subject(invocation.program)
graph = lower_program(program)
print(
    json.dumps(
        {
            "graph_id": graph.graph_id,
            "invocation_name": invocation.invocation_name,
        }
    )
)
""".strip()
