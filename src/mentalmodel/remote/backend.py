from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol, cast

import boto3  # type: ignore[import-untyped]
import psycopg

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.pagination import (
    PageSlice,
    decode_sequence_cursor,
    encode_sequence_cursor,
    paginate_descending_sequence,
)
from mentalmodel.remote.contracts import (
    CatalogSource,
    ProjectCatalogSnapshot,
    RemoteContractError,
    RemoteLiveSessionRecord,
    RemoteLiveSessionStartRequest,
    RemoteLiveSessionStatus,
    RemoteLiveSessionUpdateRequest,
    RemoteProjectCatalogPublishRequest,
    RemoteProjectLinkRequest,
    RemoteProjectRecord,
    RunManifest,
    RunManifestStatus,
)
from mentalmodel.remote.events import (
    RemoteDeliveryHealthSummary,
    RemoteOperationEvent,
    RemoteOperationKind,
    RemoteOperationStatus,
)
from mentalmodel.remote.schema import (
    REMOTE_EVENT_MIGRATIONS,
    REMOTE_LIVE_MIGRATIONS,
    REMOTE_PROJECT_MIGRATIONS,
    REMOTE_RUNS_MIGRATIONS,
    apply_schema_migrations,
)
from mentalmodel.remote.sinks import CompletedRunPublishResult, CompletedRunSink
from mentalmodel.remote.store import RunBundleUpload
from mentalmodel.remote.sync import build_run_bundle_upload_from_run_dir
from mentalmodel.runtime.runs import (
    RunSummary,
    cast_json_value,
    load_run_records_page,
    load_run_spans_page,
    normalize_summary_payload,
)


_REMOTE_LIVE_SPAN_INSERT_SQL = """
insert into remote_live_spans (
    graph_id,
    run_id,
    span_id,
    sequence,
    start_time_ns,
    node_id,
    frame_id,
    loop_node_id,
    iteration_index,
    runtime_profile,
    payload_json
)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
on conflict (graph_id, run_id, span_id) do nothing
"""


@dataclass(slots=True, frozen=True)
class RemoteBackendConfig:
    """Connection details for the minimal remote Phase 2 backend."""

    database_url: str
    object_store_bucket: str
    object_store_endpoint: str | None = None
    object_store_region: str | None = None
    object_store_access_key: str | None = None
    object_store_secret_key: str | None = None
    object_store_secure: bool = True
    cache_dir: Path | None = None

    def __post_init__(self) -> None:
        if not self.database_url:
            raise RemoteContractError("RemoteBackendConfig.database_url cannot be empty.")
        if not self.object_store_bucket:
            raise RemoteContractError("RemoteBackendConfig.object_store_bucket cannot be empty.")

    @classmethod
    def from_env(cls) -> RemoteBackendConfig | None:
        return cls.from_mapping(os.environ)

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, str],
    ) -> RemoteBackendConfig | None:
        database_url = values.get("MENTALMODEL_REMOTE_DATABASE_URL")
        bucket = values.get("MENTALMODEL_REMOTE_OBJECT_STORE_BUCKET")
        if not database_url or not bucket:
            return None
        secure_raw = values.get("MENTALMODEL_REMOTE_OBJECT_STORE_SECURE")
        cache_dir_raw = values.get("MENTALMODEL_REMOTE_CACHE_DIR")
        return cls(
            database_url=database_url,
            object_store_bucket=bucket,
            object_store_endpoint=values.get("MENTALMODEL_REMOTE_OBJECT_STORE_ENDPOINT"),
            object_store_region=values.get("MENTALMODEL_REMOTE_OBJECT_STORE_REGION"),
            object_store_access_key=values.get("MENTALMODEL_REMOTE_OBJECT_STORE_ACCESS_KEY"),
            object_store_secret_key=values.get("MENTALMODEL_REMOTE_OBJECT_STORE_SECRET_KEY"),
            object_store_secure=False if secure_raw == "false" else True,
            cache_dir=None if not cache_dir_raw else Path(cache_dir_raw).expanduser().resolve(),
        )


@dataclass(slots=True, frozen=True)
class IndexedRemoteRun:
    """Manifest row plus summary payload stored in the remote read model."""

    manifest: RunManifest
    summary_payload: dict[str, JsonValue]
    artifact_prefix: str


class ArtifactStore(Protocol):
    def put_artifact(self, *, key: str, content: bytes, content_type: str) -> str: ...

    def get_artifact(self, *, key: str) -> bytes: ...


class ManifestIndex(Protocol):
    def upsert_indexed_run(
        self,
        *,
        manifest: RunManifest,
        summary_payload: dict[str, JsonValue],
        artifact_prefix: str,
    ) -> None: ...

    def get_run(self, *, graph_id: str, run_id: str) -> IndexedRemoteRun: ...

    def list_runs(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[IndexedRemoteRun, ...]: ...


class ProjectIndex(Protocol):
    def upsert_project(
        self,
        payload: RemoteProjectLinkRequest,
    ) -> RemoteProjectRecord: ...

    def publish_catalog(
        self,
        payload: RemoteProjectCatalogPublishRequest,
    ) -> RemoteProjectRecord: ...

    def record_completed_run_upload(
        self,
        *,
        project_id: str,
        graph_id: str,
        run_id: str,
        invocation_name: str | None,
        uploaded_at_ms: int,
    ) -> RemoteProjectRecord: ...

    def get_project(self, *, project_id: str) -> RemoteProjectRecord: ...

    def list_projects(self) -> tuple[RemoteProjectRecord, ...]: ...


class LiveSessionIndex(Protocol):
    def upsert_session_start(
        self,
        payload: RemoteLiveSessionStartRequest,
    ) -> RemoteLiveSessionRecord: ...

    def apply_session_update(
        self,
        payload: RemoteLiveSessionUpdateRequest,
    ) -> RemoteLiveSessionRecord: ...

    def mark_bundle_committed(
        self,
        *,
        graph_id: str,
        run_id: str,
        committed_at_ms: int,
    ) -> RemoteLiveSessionRecord | None: ...

    def get_session(self, *, graph_id: str, run_id: str) -> RemoteLiveSessionRecord: ...

    def list_sessions(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[RemoteLiveSessionRecord, ...]: ...

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]: ...

    def get_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]: ...


class EventIndex(Protocol):
    def record_event(self, event: RemoteOperationEvent) -> RemoteOperationEvent: ...

    def list_events(
        self,
        *,
        project_id: str | None = None,
        graph_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> tuple[RemoteOperationEvent, ...]: ...

    def summarize_project(
        self,
        *,
        project_id: str,
        since_ms: int | None = None,
    ) -> RemoteDeliveryHealthSummary: ...

    def summarize_run(
        self,
        *,
        graph_id: str,
        run_id: str,
        since_ms: int | None = None,
    ) -> RemoteDeliveryHealthSummary: ...


class PersistedRunIndex(Protocol):
    def has_indexed_run(self, *, graph_id: str, run_id: str) -> bool: ...

    def replace_run_payloads(
        self,
        *,
        graph_id: str,
        run_id: str,
        records: Sequence[dict[str, JsonValue]],
        spans: Sequence[dict[str, JsonValue]],
    ) -> None: ...

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]: ...

    def get_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]: ...

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
    ) -> tuple[tuple[int, int, int, int], ...]: ...


class InMemoryArtifactStore:
    """Deterministic artifact store for tests."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put_artifact(self, *, key: str, content: bytes, content_type: str) -> str:
        del content_type
        self._objects[key] = content
        return f"memory://{key}"

    def get_artifact(self, *, key: str) -> bytes:
        try:
            return self._objects[key]
        except KeyError as exc:  # pragma: no cover - defensive
            raise RemoteContractError(f"Unknown object-store key {key!r}.") from exc


class InMemoryManifestIndex:
    """Deterministic manifest index for tests."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], IndexedRemoteRun] = {}

    def upsert_indexed_run(
        self,
        *,
        manifest: RunManifest,
        summary_payload: dict[str, JsonValue],
        artifact_prefix: str,
    ) -> None:
        self._rows[(manifest.graph_id, manifest.run_id)] = IndexedRemoteRun(
            manifest=manifest,
            summary_payload=summary_payload,
            artifact_prefix=artifact_prefix,
        )

    def get_run(self, *, graph_id: str, run_id: str) -> IndexedRemoteRun:
        try:
            return self._rows[(graph_id, run_id)]
        except KeyError as exc:
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.") from exc

    def list_runs(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[IndexedRemoteRun, ...]:
        rows = list(self._rows.values())
        if graph_id is not None:
            rows = [row for row in rows if row.manifest.graph_id == graph_id]
        if invocation_name is not None:
            rows = [row for row in rows if row.manifest.invocation_name == invocation_name]
        rows.sort(
            key=lambda row: (row.manifest.created_at_ms, row.manifest.run_id),
            reverse=True,
        )
        return tuple(rows)


class InMemoryProjectIndex:
    """Deterministic remote project registry for tests."""

    def __init__(self) -> None:
        self._rows: dict[str, RemoteProjectRecord] = {}

    def upsert_project(
        self,
        payload: RemoteProjectLinkRequest,
    ) -> RemoteProjectRecord:
        now_ms = int(time.time() * 1000)
        existing = self._rows.get(payload.project_id)
        linked_at_ms = now_ms if existing is None else existing.linked_at_ms
        record = RemoteProjectRecord(
            project_id=payload.project_id,
            label=payload.label,
            linked_at_ms=linked_at_ms,
            updated_at_ms=now_ms,
            description=payload.description,
            default_environment=payload.default_environment,
            catalog_provider=payload.catalog_provider,
            default_runs_dir=payload.default_runs_dir,
            default_verify_spec=payload.default_verify_spec,
            catalog_snapshot=payload.catalog_snapshot,
            last_completed_run_upload_at_ms=(
                None
                if existing is None
                else existing.last_completed_run_upload_at_ms
            ),
            last_completed_run_graph_id=(
                None
                if existing is None
                else existing.last_completed_run_graph_id
            ),
            last_completed_run_id=(
                None if existing is None else existing.last_completed_run_id
            ),
            last_completed_run_invocation_name=(
                None
                if existing is None
                else existing.last_completed_run_invocation_name
            ),
        )
        self._rows[payload.project_id] = record
        return record

    def get_project(self, *, project_id: str) -> RemoteProjectRecord:
        try:
            return self._rows[project_id]
        except KeyError as exc:
            raise RemoteContractError(f"Unknown remote project {project_id!r}.") from exc

    def list_projects(self) -> tuple[RemoteProjectRecord, ...]:
        return tuple(
            sorted(
                self._rows.values(),
                key=lambda record: (record.updated_at_ms, record.project_id),
                reverse=True,
            )
        )

    def publish_catalog(
        self,
        payload: RemoteProjectCatalogPublishRequest,
    ) -> RemoteProjectRecord:
        existing = self.get_project(project_id=payload.project_id)
        updated = RemoteProjectRecord(
            project_id=existing.project_id,
            label=existing.label,
            linked_at_ms=existing.linked_at_ms,
            updated_at_ms=int(time.time() * 1000),
            description=existing.description,
            default_environment=existing.default_environment,
            catalog_provider=payload.catalog_provider,
            default_runs_dir=existing.default_runs_dir,
            default_verify_spec=existing.default_verify_spec,
            catalog_snapshot=payload.catalog_snapshot,
            last_completed_run_upload_at_ms=existing.last_completed_run_upload_at_ms,
            last_completed_run_graph_id=existing.last_completed_run_graph_id,
            last_completed_run_id=existing.last_completed_run_id,
            last_completed_run_invocation_name=existing.last_completed_run_invocation_name,
        )
        self._rows[payload.project_id] = updated
        return updated

    def record_completed_run_upload(
        self,
        *,
        project_id: str,
        graph_id: str,
        run_id: str,
        invocation_name: str | None,
        uploaded_at_ms: int,
    ) -> RemoteProjectRecord:
        existing = self.get_project(project_id=project_id)
        updated = RemoteProjectRecord(
            project_id=existing.project_id,
            label=existing.label,
            linked_at_ms=existing.linked_at_ms,
            updated_at_ms=uploaded_at_ms,
            description=existing.description,
            default_environment=existing.default_environment,
            catalog_provider=existing.catalog_provider,
            default_runs_dir=existing.default_runs_dir,
            default_verify_spec=existing.default_verify_spec,
            catalog_snapshot=existing.catalog_snapshot,
            last_completed_run_upload_at_ms=uploaded_at_ms,
            last_completed_run_graph_id=graph_id,
            last_completed_run_id=run_id,
            last_completed_run_invocation_name=invocation_name,
        )
        self._rows[project_id] = updated
        return updated


class InMemoryPersistedRunIndex:
    """Deterministic persisted run row index for tests."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], tuple[dict[str, JsonValue], ...]] = {}
        self._spans: dict[tuple[str, str], tuple[dict[str, JsonValue], ...]] = {}
        self._invocations: dict[tuple[str, str], str | None] = {}

    def has_indexed_run(self, *, graph_id: str, run_id: str) -> bool:
        return (graph_id, run_id) in self._records or (graph_id, run_id) in self._spans

    def replace_run_payloads(
        self,
        *,
        graph_id: str,
        run_id: str,
        records: Sequence[dict[str, JsonValue]],
        spans: Sequence[dict[str, JsonValue]],
    ) -> None:
        key = (graph_id, run_id)
        self._records[key] = tuple(records)
        self._spans[key] = tuple(spans)

    def set_invocation_name(
        self,
        *,
        graph_id: str,
        run_id: str,
        invocation_name: str | None,
    ) -> None:
        self._invocations[(graph_id, run_id)] = invocation_name

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        rows = self._records.get((graph_id, run_id))
        if rows is None:
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
        filtered = tuple(
            row
            for row in rows
            if (node_id is None or row.get("node_id") == node_id)
            and (frame_id is None or row.get("frame_id") == frame_id)
        )
        return paginate_descending_sequence(
            filtered,
            sequence_for=_live_row_sequence,
            cursor=cursor,
            limit=limit,
        )

    def get_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        rows = self._spans.get((graph_id, run_id))
        if rows is None:
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
        filtered = tuple(
            row
            for row in rows
            if (node_id is None or _span_node_id(row) == node_id)
            and (frame_id is None or _span_frame_id(row) == frame_id)
        )
        return paginate_descending_sequence(
            filtered,
            sequence_for=_live_row_sequence,
            cursor=cursor,
            limit=limit,
        )

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
    ) -> tuple[tuple[int, int, int, int], ...]:
        bucket_state: dict[int, tuple[int, int, set[str]]] = {}
        for (row_graph_id, row_run_id), rows in self._records.items():
            if row_graph_id != graph_id:
                continue
            if run_id is not None and row_run_id != run_id:
                continue
            if run_id is None:
                if self._invocations.get((row_graph_id, row_run_id)) != invocation_name:
                    continue
            for row in rows:
                timestamp = row.get("timestamp_ms")
                if not isinstance(timestamp, int):
                    continue
                if timestamp < since_ms or timestamp >= until_ms:
                    continue
                if node_id is not None and row.get("node_id") != node_id:
                    continue
                bucket_index = (timestamp - since_ms) // rollup_ms
                record_count, loop_count, node_set = bucket_state.get(
                    bucket_index,
                    (0, 0, set()),
                )
                node_value = row.get("node_id")
                if isinstance(node_value, str):
                    node_set.add(node_value)
                iteration_index = row.get("iteration_index")
                bucket_state[bucket_index] = (
                    record_count + 1,
                    loop_count + (1 if isinstance(iteration_index, int) else 0),
                    node_set,
                )
        return tuple(
            (
                bucket_index,
                record_count,
                loop_count,
                len(node_set),
            )
            for bucket_index, (record_count, loop_count, node_set) in sorted(
                bucket_state.items()
            )
        )


class InMemoryLiveSessionIndex:
    """Deterministic live-session registry for tests and local service use."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], RemoteLiveSessionRecord] = {}

    def upsert_session_start(
        self,
        payload: RemoteLiveSessionStartRequest,
    ) -> RemoteLiveSessionRecord:
        key = (payload.graph_id, payload.run_id)
        existing = self._rows.get(key)
        record = RemoteLiveSessionRecord(
            graph_id=payload.graph_id,
            run_id=payload.run_id,
            started_at_ms=(
                payload.started_at_ms
                if existing is None
                else existing.started_at_ms
            ),
            updated_at_ms=payload.started_at_ms,
            status=(
                RemoteLiveSessionStatus.RUNNING
                if existing is None
                else existing.status
            ),
            graph=dict(payload.graph),
            analysis=dict(payload.analysis),
            project_id=payload.project_id if existing is None else existing.project_id,
            invocation_name=(
                payload.invocation_name
                if existing is None
                else existing.invocation_name
            ),
            environment_name=(
                payload.environment_name
                if existing is None
                else existing.environment_name
            ),
            catalog_entry_id=(
                payload.catalog_entry_id
                if existing is None
                else existing.catalog_entry_id
            ),
            catalog_source=(
                payload.catalog_source
                if existing is None
                else existing.catalog_source
            ),
            runtime_default_profile_name=payload.runtime_default_profile_name,
            runtime_profile_names=payload.runtime_profile_names,
            records=tuple() if existing is None else existing.records,
            spans=tuple() if existing is None else existing.spans,
            error=None if existing is None else existing.error,
            finished_at_ms=None if existing is None else existing.finished_at_ms,
            bundle_committed_at_ms=(
                None if existing is None else existing.bundle_committed_at_ms
            ),
        )
        self._rows[key] = record
        return record

    def apply_session_update(
        self,
        payload: RemoteLiveSessionUpdateRequest,
    ) -> RemoteLiveSessionRecord:
        existing = self.get_session(graph_id=payload.graph_id, run_id=payload.run_id)
        merged_records = _merge_live_rows(
            existing.records,
            payload.records,
            id_key="record_id",
            order_key="sequence",
        )
        merged_spans = _merge_live_rows(
            existing.spans,
            payload.spans,
            id_key="span_id",
            order_key="sequence",
        )
        status = payload.status or existing.status
        finished_at_ms = (
            payload.updated_at_ms
            if status in {
                RemoteLiveSessionStatus.SUCCEEDED,
                RemoteLiveSessionStatus.FAILED,
            }
            else existing.finished_at_ms
        )
        updated = RemoteLiveSessionRecord(
            graph_id=existing.graph_id,
            run_id=existing.run_id,
            started_at_ms=existing.started_at_ms,
            updated_at_ms=max(existing.updated_at_ms, payload.updated_at_ms),
            status=status,
            graph=existing.graph,
            analysis=existing.analysis,
            project_id=existing.project_id,
            invocation_name=existing.invocation_name,
            environment_name=existing.environment_name,
            catalog_entry_id=existing.catalog_entry_id,
            catalog_source=existing.catalog_source,
            runtime_default_profile_name=existing.runtime_default_profile_name,
            runtime_profile_names=existing.runtime_profile_names,
            records=merged_records,
            spans=merged_spans,
            error=payload.error or existing.error,
            finished_at_ms=finished_at_ms,
            bundle_committed_at_ms=existing.bundle_committed_at_ms,
        )
        self._rows[(existing.graph_id, existing.run_id)] = updated
        return updated

    def mark_bundle_committed(
        self,
        *,
        graph_id: str,
        run_id: str,
        committed_at_ms: int,
    ) -> RemoteLiveSessionRecord | None:
        existing = self._rows.get((graph_id, run_id))
        if existing is None:
            return None
        updated = RemoteLiveSessionRecord(
            graph_id=existing.graph_id,
            run_id=existing.run_id,
            started_at_ms=existing.started_at_ms,
            updated_at_ms=max(existing.updated_at_ms, committed_at_ms),
            status=existing.status,
            graph=existing.graph,
            analysis=existing.analysis,
            project_id=existing.project_id,
            invocation_name=existing.invocation_name,
            environment_name=existing.environment_name,
            catalog_entry_id=existing.catalog_entry_id,
            catalog_source=existing.catalog_source,
            runtime_default_profile_name=existing.runtime_default_profile_name,
            runtime_profile_names=existing.runtime_profile_names,
            records=existing.records,
            spans=existing.spans,
            error=existing.error,
            finished_at_ms=existing.finished_at_ms,
            bundle_committed_at_ms=committed_at_ms,
        )
        self._rows[(graph_id, run_id)] = updated
        return updated

    def get_session(self, *, graph_id: str, run_id: str) -> RemoteLiveSessionRecord:
        try:
            return self._rows[(graph_id, run_id)]
        except KeyError as exc:
            raise RunInspectionError(
                f"Remote live session {graph_id}/{run_id} was not found."
            ) from exc

    def list_sessions(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[RemoteLiveSessionRecord, ...]:
        rows = list(self._rows.values())
        if graph_id is not None:
            rows = [row for row in rows if row.graph_id == graph_id]
        if invocation_name is not None:
            rows = [row for row in rows if row.invocation_name == invocation_name]
        rows.sort(key=lambda row: (row.started_at_ms, row.run_id), reverse=True)
        return tuple(rows)

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        session = self.get_session(graph_id=graph_id, run_id=run_id)
        filtered = tuple(
            cast(dict[str, JsonValue], record)
            for record in session.records
            if (node_id is None or record.get("node_id") == node_id)
            and (frame_id is None or record.get("frame_id") == frame_id)
        )
        return paginate_descending_sequence(
            filtered,
            sequence_for=_live_row_sequence,
            cursor=cursor,
            limit=limit,
        )

    def get_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        session = self.get_session(graph_id=graph_id, run_id=run_id)
        filtered = tuple(
            cast(dict[str, JsonValue], span)
            for span in session.spans
            if (node_id is None or _span_node_id(cast(dict[str, JsonValue], span)) == node_id)
            and (frame_id is None or _span_frame_id(cast(dict[str, JsonValue], span)) == frame_id)
        )
        return paginate_descending_sequence(
            filtered,
            sequence_for=_live_row_sequence,
            cursor=cursor,
            limit=limit,
        )


class InMemoryEventIndex:
    """Deterministic remote operation event log for tests and local service use."""

    def __init__(self) -> None:
        self._rows: list[RemoteOperationEvent] = []

    def record_event(self, event: RemoteOperationEvent) -> RemoteOperationEvent:
        self._rows.append(event)
        self._rows.sort(key=lambda row: (row.occurred_at_ms, row.event_id), reverse=True)
        return event

    def list_events(
        self,
        *,
        project_id: str | None = None,
        graph_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> tuple[RemoteOperationEvent, ...]:
        rows = [
            row
            for row in self._rows
            if (project_id is None or row.project_id == project_id)
            and (graph_id is None or row.graph_id == graph_id)
            and (run_id is None or row.run_id == run_id)
        ]
        return tuple(rows[: max(1, limit)])

    def summarize_project(
        self,
        *,
        project_id: str,
        since_ms: int | None = None,
    ) -> RemoteDeliveryHealthSummary:
        return _summarize_events(
            self.list_events(project_id=project_id, limit=500),
            since_ms=since_ms,
        )

    def summarize_run(
        self,
        *,
        graph_id: str,
        run_id: str,
        since_ms: int | None = None,
    ) -> RemoteDeliveryHealthSummary:
        return _summarize_events(
            self.list_events(graph_id=graph_id, run_id=run_id, limit=500),
            since_ms=since_ms,
        )


class S3ArtifactStore:
    """S3-compatible object store adapter for run artifacts."""

    def __init__(self, config: RemoteBackendConfig) -> None:
        session = boto3.session.Session()
        self.bucket = config.object_store_bucket
        self.region = config.object_store_region
        self._client = session.client(
            "s3",
            endpoint_url=config.object_store_endpoint,
            region_name=config.object_store_region,
            aws_access_key_id=config.object_store_access_key,
            aws_secret_access_key=config.object_store_secret_key,
            use_ssl=config.object_store_secure,
        )
        self._ensure_bucket()

    def put_artifact(self, *, key: str, content: bytes, content_type: str) -> str:
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
        )
        return f"s3://{self.bucket}/{key}"

    def get_artifact(self, *, key: str) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"].read()
        if not isinstance(body, bytes):  # pragma: no cover - boto3 always returns bytes
            raise RemoteContractError("Object-store body was not bytes.")
        return body

    def _ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket)
            return
        except Exception:
            pass
        create_kwargs: dict[str, object] = {"Bucket": self.bucket}
        if self.region not in {None, "", "us-east-1"}:
            create_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": self.region
            }
        self._client.create_bucket(**create_kwargs)


class PostgresManifestIndex:
    """Postgres-backed manifest index for the Phase 2 remote read model."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._schema_lock = threading.Lock()
        self._schema_ready = False

    def upsert_indexed_run(
        self,
        *,
        manifest: RunManifest,
        summary_payload: dict[str, JsonValue],
        artifact_prefix: str,
    ) -> None:
        self._ensure_schema()
        now_ms = int(time.time() * 1000)
        with psycopg.connect(self.database_url) as conn:
            conn.execute(
                """
                insert into remote_runs (
                    graph_id,
                    run_id,
                    created_at_ms,
                    completed_at_ms,
                    status,
                    success,
                    invocation_name,
                    project_id,
                    project_label,
                    environment_name,
                    catalog_entry_id,
                    catalog_source,
                    runtime_default_profile_name,
                    runtime_profile_names,
                    run_schema_version,
                    record_schema_version,
                    records_indexed_at_ms,
                    spans_indexed_at_ms,
                    manifest_json,
                    summary_json,
                    artifact_prefix,
                    updated_at_ms
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s
                )
                on conflict (graph_id, run_id) do update
                set
                    created_at_ms = excluded.created_at_ms,
                    completed_at_ms = excluded.completed_at_ms,
                    status = excluded.status,
                    success = excluded.success,
                    invocation_name = excluded.invocation_name,
                    project_id = excluded.project_id,
                    project_label = excluded.project_label,
                    environment_name = excluded.environment_name,
                    catalog_entry_id = excluded.catalog_entry_id,
                    catalog_source = excluded.catalog_source,
                    runtime_default_profile_name = excluded.runtime_default_profile_name,
                    runtime_profile_names = excluded.runtime_profile_names,
                    run_schema_version = excluded.run_schema_version,
                    record_schema_version = excluded.record_schema_version,
                    records_indexed_at_ms = excluded.records_indexed_at_ms,
                    spans_indexed_at_ms = excluded.spans_indexed_at_ms,
                    manifest_json = excluded.manifest_json,
                    summary_json = excluded.summary_json,
                    artifact_prefix = excluded.artifact_prefix,
                    updated_at_ms = excluded.updated_at_ms
                """,
                (
                    manifest.graph_id,
                    manifest.run_id,
                    manifest.created_at_ms,
                    manifest.completed_at_ms,
                    manifest.status.value,
                    manifest.success,
                    manifest.invocation_name,
                    manifest.project_id,
                    manifest.project_label,
                    manifest.environment_name,
                    manifest.catalog_entry_id,
                    None if manifest.catalog_source is None else manifest.catalog_source.value,
                    manifest.runtime_default_profile_name,
                    json.dumps(list(manifest.runtime_profile_names)),
                    manifest.run_schema_version,
                    manifest.record_schema_version,
                    None,
                    None,
                    json.dumps(manifest.as_dict(), sort_keys=True),
                    json.dumps(summary_payload, sort_keys=True),
                    artifact_prefix,
                    now_ms,
                ),
            )
            conn.commit()

    def get_run(self, *, graph_id: str, run_id: str) -> IndexedRemoteRun:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            row = conn.execute(
                """
                select manifest_json::text, summary_json::text, artifact_prefix
                from remote_runs
                where graph_id = %s and run_id = %s
                """,
                (graph_id, run_id),
            ).fetchone()
        if row is None:
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
        return _indexed_run_from_row(
            manifest_json=cast(str, row[0]),
            summary_json=cast(str, row[1]),
            artifact_prefix=cast(str, row[2]),
        )

    def list_runs(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[IndexedRemoteRun, ...]:
        self._ensure_schema()
        clauses: list[str] = []
        params: list[object] = []
        if graph_id is not None:
            clauses.append("graph_id = %s")
            params.append(graph_id)
        if invocation_name is not None:
            clauses.append("invocation_name = %s")
            params.append(invocation_name)
        where_sql = ""
        if clauses:
            where_sql = " where " + " and ".join(clauses)
        query = (
            "select manifest_json::text, summary_json::text, artifact_prefix "
            "from remote_runs"
            f"{where_sql} "
            "order by created_at_ms desc, run_id desc"
        )
        with psycopg.connect(self.database_url) as conn:
            rows = conn.execute(query, params).fetchall()
        return tuple(
            _indexed_run_from_row(
                manifest_json=cast(str, row[0]),
                summary_json=cast(str, row[1]),
                artifact_prefix=cast(str, row[2]),
            )
            for row in rows
        )

    def _ensure_schema(self) -> None:
        with self._schema_lock:
            if self._schema_ready:
                return
            with psycopg.connect(self.database_url) as conn:
                apply_schema_migrations(conn, REMOTE_RUNS_MIGRATIONS)
                conn.commit()
            self._schema_ready = True


class PostgresPersistedRunIndex:
    """Postgres-backed persisted run row index for hosted run inspection."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._schema_lock = threading.Lock()
        self._schema_ready = False

    def has_indexed_run(self, *, graph_id: str, run_id: str) -> bool:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            row = conn.execute(
                """
                select records_indexed_at_ms, spans_indexed_at_ms
                from remote_runs
                where graph_id = %s and run_id = %s
                """,
                (graph_id, run_id),
            ).fetchone()
        if row is None:
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
        return row[0] is not None or row[1] is not None

    def replace_run_payloads(
        self,
        *,
        graph_id: str,
        run_id: str,
        records: Sequence[dict[str, JsonValue]],
        spans: Sequence[dict[str, JsonValue]],
    ) -> None:
        self._ensure_schema()
        now_ms = int(time.time() * 1000)
        with psycopg.connect(self.database_url) as conn:
            conn.execute(
                "delete from remote_run_records where graph_id = %s and run_id = %s",
                (graph_id, run_id),
            )
            conn.execute(
                "delete from remote_run_spans where graph_id = %s and run_id = %s",
                (graph_id, run_id),
            )
            if records:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        insert into remote_run_records (
                            graph_id,
                            run_id,
                            record_id,
                            sequence,
                            timestamp_ms,
                            node_id,
                            frame_id,
                            loop_node_id,
                            iteration_index,
                            event_type,
                            payload_json
                        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        [
                            (
                                graph_id,
                                run_id,
                                _persisted_record_id(record),
                                _required_live_row_int(record, "sequence"),
                                _required_live_row_int(record, "timestamp_ms"),
                                _required_live_row_str(record, "node_id"),
                                _required_live_row_str(record, "frame_id"),
                                _optional_live_row_str(record, "loop_node_id"),
                                _optional_live_row_int(record, "iteration_index"),
                                _required_live_row_str(record, "event_type"),
                                json.dumps(record, sort_keys=True),
                            )
                            for record in records
                        ],
                    )
            if spans:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        insert into remote_run_spans (
                            graph_id,
                            run_id,
                            span_id,
                            sequence,
                            start_time_ns,
                            node_id,
                            frame_id,
                            loop_node_id,
                            iteration_index,
                            runtime_profile,
                            payload_json
                        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        [
                            (
                                graph_id,
                                run_id,
                                _persisted_span_id(span),
                                _required_live_row_int(span, "sequence"),
                                _required_live_row_int(span, "start_time_ns"),
                                _optional_span_node_id(span),
                                _optional_span_frame_id(span),
                                _optional_span_loop_node_id(span),
                                _optional_span_iteration_index(span),
                                _optional_span_runtime_profile(span),
                                json.dumps(span, sort_keys=True),
                            )
                            for span in spans
                        ],
                    )
            conn.execute(
                """
                update remote_runs
                set records_indexed_at_ms = %s,
                    spans_indexed_at_ms = %s,
                    updated_at_ms = %s
                where graph_id = %s and run_id = %s
                """,
                (now_ms, now_ms, now_ms, graph_id, run_id),
            )
            conn.commit()

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        self._ensure_schema()
        after_sequence = decode_sequence_cursor(cursor)
        clauses = ["graph_id = %s", "run_id = %s"]
        params: list[object] = [graph_id, run_id]
        if node_id is not None:
            clauses.append("node_id = %s")
            params.append(node_id)
        if frame_id is not None:
            clauses.append("frame_id = %s")
            params.append(frame_id)
        if after_sequence is not None:
            clauses.append("sequence < %s")
            params.append(after_sequence)
        where_sql = " and ".join(clauses)
        with psycopg.connect(self.database_url) as conn:
            count_result = conn.execute(
                f"select count(*) from remote_run_records where {where_sql}",
                tuple(params),
            ).fetchone()
            rows = conn.execute(
                f"""
                select payload_json, sequence
                from remote_run_records
                where {where_sql}
                order by sequence desc, timestamp_ms desc, record_id desc
                limit %s
                """,
                tuple([*params, limit + 1]),
            ).fetchall()
        total_count = 0 if count_result is None else _int_from_db_scalar(count_result[0])
        return _page_from_json_rows(rows, limit=limit, total_count=total_count)

    def get_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        self._ensure_schema()
        after_sequence = decode_sequence_cursor(cursor)
        clauses = ["graph_id = %s", "run_id = %s"]
        params: list[object] = [graph_id, run_id]
        if node_id is not None:
            clauses.append("node_id = %s")
            params.append(node_id)
        if frame_id is not None:
            clauses.append("frame_id = %s")
            params.append(frame_id)
        if after_sequence is not None:
            clauses.append("sequence < %s")
            params.append(after_sequence)
        where_sql = " and ".join(clauses)
        with psycopg.connect(self.database_url) as conn:
            count_result = conn.execute(
                f"select count(*) from remote_run_spans where {where_sql}",
                tuple(params),
            ).fetchone()
            rows = conn.execute(
                f"""
                select payload_json, sequence
                from remote_run_spans
                where {where_sql}
                order by sequence desc, start_time_ns desc, span_id desc
                limit %s
                """,
                tuple([*params, limit + 1]),
            ).fetchall()
        total_count = 0 if count_result is None else _int_from_db_scalar(count_result[0])
        return _page_from_json_rows(rows, limit=limit, total_count=total_count)

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
    ) -> tuple[tuple[int, int, int, int], ...]:
        self._ensure_schema()
        params: list[object] = [since_ms, rollup_ms]
        clauses = [
            "records.graph_id = %s",
            "records.timestamp_ms >= %s",
            "records.timestamp_ms < %s",
        ]
        params.extend([graph_id, since_ms, until_ms])
        join_sql = ""
        if run_id is not None:
            clauses.append("records.run_id = %s")
            params.append(run_id)
        else:
            join_sql = (
                " join remote_runs runs"
                " on runs.graph_id = records.graph_id and runs.run_id = records.run_id"
            )
            clauses.append("runs.invocation_name = %s")
            params.append(invocation_name)
        if node_id is not None:
            clauses.append("records.node_id = %s")
            params.append(node_id)
        where_sql = " and ".join(clauses)
        query = f"""
            select
                floor((records.timestamp_ms - %s)::numeric / %s)::bigint as bucket_index,
                count(*)::bigint as record_count,
                count(*) filter (where records.iteration_index is not null)::bigint as loop_count,
                count(distinct records.node_id)::bigint as unique_nodes
            from remote_run_records records
            {join_sql}
            where {where_sql}
            group by bucket_index
            order by bucket_index asc
        """
        with psycopg.connect(self.database_url) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return tuple(
            (
                _int_from_db_scalar(row[0]),
                _int_from_db_scalar(row[1]),
                _int_from_db_scalar(row[2]),
                _int_from_db_scalar(row[3]),
            )
            for row in rows
        )

    def _ensure_schema(self) -> None:
        with self._schema_lock:
            if self._schema_ready:
                return
            with psycopg.connect(self.database_url) as conn:
                apply_schema_migrations(conn, REMOTE_RUNS_MIGRATIONS)
                conn.commit()
            self._schema_ready = True


class PostgresProjectIndex:
    """Postgres-backed remote project registry for the hosted service path."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._schema_lock = threading.Lock()
        self._schema_ready = False

    def upsert_project(
        self,
        payload: RemoteProjectLinkRequest,
    ) -> RemoteProjectRecord:
        self._ensure_schema()
        now_ms = int(time.time() * 1000)
        snapshot_json = (
            None
            if payload.catalog_snapshot is None
            else json.dumps(payload.catalog_snapshot.as_dict(), sort_keys=True)
        )
        with psycopg.connect(self.database_url) as conn:
            row = conn.execute(
                """
                insert into remote_projects (
                    project_id,
                    label,
                    description,
                    default_environment,
                    catalog_provider,
                    default_runs_dir,
                    default_verify_spec,
                    linked_at_ms,
                    updated_at_ms,
                    catalog_snapshot_json,
                    catalog_entry_count,
                    catalog_published_at_ms,
                    catalog_version,
                    last_completed_run_upload_at_ms,
                    last_completed_run_graph_id,
                    last_completed_run_id,
                    last_completed_run_invocation_name
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s
                )
                on conflict (project_id) do update
                set
                    label = excluded.label,
                    description = excluded.description,
                    default_environment = excluded.default_environment,
                    catalog_provider = excluded.catalog_provider,
                    default_runs_dir = excluded.default_runs_dir,
                    default_verify_spec = excluded.default_verify_spec,
                    updated_at_ms = excluded.updated_at_ms,
                    catalog_snapshot_json = excluded.catalog_snapshot_json,
                    catalog_entry_count = excluded.catalog_entry_count,
                    catalog_published_at_ms = excluded.catalog_published_at_ms,
                    catalog_version = excluded.catalog_version
                returning
                    project_id,
                    label,
                    description,
                    default_environment,
                    catalog_provider,
                    default_runs_dir,
                    default_verify_spec,
                    linked_at_ms,
                    updated_at_ms,
                    catalog_snapshot_json::text,
                    last_completed_run_upload_at_ms,
                    last_completed_run_graph_id,
                    last_completed_run_id,
                    last_completed_run_invocation_name
                """,
                (
                    payload.project_id,
                    payload.label,
                    payload.description,
                    payload.default_environment,
                    payload.catalog_provider,
                    payload.default_runs_dir,
                    payload.default_verify_spec,
                    now_ms,
                    now_ms,
                    snapshot_json,
                    (
                        0
                        if payload.catalog_snapshot is None
                        else payload.catalog_snapshot.entry_count
                    ),
                    (
                        None
                        if payload.catalog_snapshot is None
                        else payload.catalog_snapshot.published_at_ms
                    ),
                    (
                        None
                        if payload.catalog_snapshot is None
                        else payload.catalog_snapshot.version
                    ),
                    None,
                    None,
                    None,
                    None,
                ),
            ).fetchone()
            conn.commit()
        assert row is not None
        return _remote_project_from_row(
            project_id=cast(str, row[0]),
            label=cast(str, row[1]),
            description=cast(str, row[2]),
            default_environment=cast(str | None, row[3]),
            catalog_provider=cast(str | None, row[4]),
            default_runs_dir=cast(str | None, row[5]),
            default_verify_spec=cast(str | None, row[6]),
            linked_at_ms=cast(int, row[7]),
            updated_at_ms=cast(int, row[8]),
            catalog_snapshot_json=cast(str | None, row[9]),
            last_completed_run_upload_at_ms=cast(int | None, row[10]),
            last_completed_run_graph_id=cast(str | None, row[11]),
            last_completed_run_id=cast(str | None, row[12]),
            last_completed_run_invocation_name=cast(str | None, row[13]),
        )

    def get_project(self, *, project_id: str) -> RemoteProjectRecord:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            row = conn.execute(
                """
                select
                    project_id,
                    label,
                    description,
                    default_environment,
                    catalog_provider,
                    default_runs_dir,
                    default_verify_spec,
                    linked_at_ms,
                    updated_at_ms,
                    catalog_snapshot_json::text,
                    last_completed_run_upload_at_ms,
                    last_completed_run_graph_id,
                    last_completed_run_id,
                    last_completed_run_invocation_name
                from remote_projects
                where project_id = %s
                """,
                (project_id,),
            ).fetchone()
        if row is None:
            raise RemoteContractError(f"Unknown remote project {project_id!r}.")
        return _remote_project_from_row(
            project_id=cast(str, row[0]),
            label=cast(str, row[1]),
            description=cast(str, row[2]),
            default_environment=cast(str | None, row[3]),
            catalog_provider=cast(str | None, row[4]),
            default_runs_dir=cast(str | None, row[5]),
            default_verify_spec=cast(str | None, row[6]),
            linked_at_ms=cast(int, row[7]),
            updated_at_ms=cast(int, row[8]),
            catalog_snapshot_json=cast(str | None, row[9]),
            last_completed_run_upload_at_ms=cast(int | None, row[10]),
            last_completed_run_graph_id=cast(str | None, row[11]),
            last_completed_run_id=cast(str | None, row[12]),
            last_completed_run_invocation_name=cast(str | None, row[13]),
        )

    def publish_catalog(
        self,
        payload: RemoteProjectCatalogPublishRequest,
    ) -> RemoteProjectRecord:
        self._ensure_schema()
        now_ms = int(time.time() * 1000)
        snapshot_json = json.dumps(payload.catalog_snapshot.as_dict(), sort_keys=True)
        with psycopg.connect(self.database_url) as conn:
            row = conn.execute(
                """
                update remote_projects
                set
                    catalog_provider = %s,
                    updated_at_ms = %s,
                    catalog_snapshot_json = %s::jsonb,
                    catalog_entry_count = %s,
                    catalog_published_at_ms = %s,
                    catalog_version = %s
                where project_id = %s
                returning
                    project_id,
                    label,
                    description,
                    default_environment,
                    catalog_provider,
                    default_runs_dir,
                    default_verify_spec,
                    linked_at_ms,
                    updated_at_ms,
                    catalog_snapshot_json::text,
                    last_completed_run_upload_at_ms,
                    last_completed_run_graph_id,
                    last_completed_run_id,
                    last_completed_run_invocation_name
                """,
                (
                    payload.catalog_provider,
                    now_ms,
                    snapshot_json,
                    payload.catalog_snapshot.entry_count,
                    payload.catalog_snapshot.published_at_ms,
                    payload.catalog_snapshot.version,
                    payload.project_id,
                ),
            ).fetchone()
            conn.commit()
        if row is None:
            raise RemoteContractError(f"Unknown remote project {payload.project_id!r}.")
        return _remote_project_from_row(
            project_id=cast(str, row[0]),
            label=cast(str, row[1]),
            description=cast(str, row[2]),
            default_environment=cast(str | None, row[3]),
            catalog_provider=cast(str | None, row[4]),
            default_runs_dir=cast(str | None, row[5]),
            default_verify_spec=cast(str | None, row[6]),
            linked_at_ms=cast(int, row[7]),
            updated_at_ms=cast(int, row[8]),
            catalog_snapshot_json=cast(str | None, row[9]),
            last_completed_run_upload_at_ms=cast(int | None, row[10]),
            last_completed_run_graph_id=cast(str | None, row[11]),
            last_completed_run_id=cast(str | None, row[12]),
            last_completed_run_invocation_name=cast(str | None, row[13]),
        )

    def list_projects(self) -> tuple[RemoteProjectRecord, ...]:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            rows = conn.execute(
                """
                select
                    project_id,
                    label,
                    description,
                    default_environment,
                    catalog_provider,
                    default_runs_dir,
                    default_verify_spec,
                    linked_at_ms,
                    updated_at_ms,
                    catalog_snapshot_json::text,
                    last_completed_run_upload_at_ms,
                    last_completed_run_graph_id,
                    last_completed_run_id,
                    last_completed_run_invocation_name
                from remote_projects
                order by updated_at_ms desc, project_id desc
                """
            ).fetchall()
        return tuple(
            _remote_project_from_row(
                project_id=cast(str, row[0]),
                label=cast(str, row[1]),
                description=cast(str, row[2]),
                default_environment=cast(str | None, row[3]),
                catalog_provider=cast(str | None, row[4]),
                default_runs_dir=cast(str | None, row[5]),
                default_verify_spec=cast(str | None, row[6]),
                linked_at_ms=cast(int, row[7]),
                updated_at_ms=cast(int, row[8]),
                catalog_snapshot_json=cast(str | None, row[9]),
                last_completed_run_upload_at_ms=cast(int | None, row[10]),
                last_completed_run_graph_id=cast(str | None, row[11]),
                last_completed_run_id=cast(str | None, row[12]),
                last_completed_run_invocation_name=cast(str | None, row[13]),
            )
            for row in rows
        )

    def record_completed_run_upload(
        self,
        *,
        project_id: str,
        graph_id: str,
        run_id: str,
        invocation_name: str | None,
        uploaded_at_ms: int,
    ) -> RemoteProjectRecord:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            row = conn.execute(
                """
                update remote_projects
                set
                    updated_at_ms = %s,
                    last_completed_run_upload_at_ms = %s,
                    last_completed_run_graph_id = %s,
                    last_completed_run_id = %s,
                    last_completed_run_invocation_name = %s
                where project_id = %s
                returning
                    project_id,
                    label,
                    description,
                    default_environment,
                    catalog_provider,
                    default_runs_dir,
                    default_verify_spec,
                    linked_at_ms,
                    updated_at_ms,
                    catalog_snapshot_json::text,
                    last_completed_run_upload_at_ms,
                    last_completed_run_graph_id,
                    last_completed_run_id,
                    last_completed_run_invocation_name
                """,
                (
                    uploaded_at_ms,
                    uploaded_at_ms,
                    graph_id,
                    run_id,
                    invocation_name,
                    project_id,
                ),
            ).fetchone()
            conn.commit()
        if row is None:
            raise RemoteContractError(f"Unknown remote project {project_id!r}.")
        return _remote_project_from_row(
            project_id=cast(str, row[0]),
            label=cast(str, row[1]),
            description=cast(str, row[2]),
            default_environment=cast(str | None, row[3]),
            catalog_provider=cast(str | None, row[4]),
            default_runs_dir=cast(str | None, row[5]),
            default_verify_spec=cast(str | None, row[6]),
            linked_at_ms=cast(int, row[7]),
            updated_at_ms=cast(int, row[8]),
            catalog_snapshot_json=cast(str | None, row[9]),
            last_completed_run_upload_at_ms=cast(int | None, row[10]),
            last_completed_run_graph_id=cast(str | None, row[11]),
            last_completed_run_id=cast(str | None, row[12]),
            last_completed_run_invocation_name=cast(str | None, row[13]),
        )

    def _ensure_schema(self) -> None:
        with self._schema_lock:
            if self._schema_ready:
                return
            with psycopg.connect(self.database_url) as conn:
                apply_schema_migrations(conn, REMOTE_PROJECT_MIGRATIONS)
                conn.commit()
            self._schema_ready = True


class PostgresLiveSessionIndex:
    """Postgres-backed live session store for hosted in-progress run visibility."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._schema_lock = threading.Lock()
        self._schema_ready = False

    def upsert_session_start(
        self,
        payload: RemoteLiveSessionStartRequest,
    ) -> RemoteLiveSessionRecord:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            conn.execute(
                """
                insert into remote_live_sessions (
                    graph_id,
                    run_id,
                    project_id,
                    invocation_name,
                    environment_name,
                    catalog_entry_id,
                    catalog_source,
                    runtime_default_profile_name,
                    runtime_profile_names,
                    started_at_ms,
                    updated_at_ms,
                    finished_at_ms,
                    status,
                    error,
                    graph_json,
                    analysis_json,
                    bundle_committed_at_ms
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                    %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s
                )
                on conflict (graph_id, run_id) do update
                set
                    project_id = coalesce(remote_live_sessions.project_id, excluded.project_id),
                    invocation_name = coalesce(
                        remote_live_sessions.invocation_name,
                        excluded.invocation_name
                    ),
                    environment_name = coalesce(
                        remote_live_sessions.environment_name,
                        excluded.environment_name
                    ),
                    catalog_entry_id = coalesce(
                        remote_live_sessions.catalog_entry_id,
                        excluded.catalog_entry_id
                    ),
                    catalog_source = coalesce(
                        remote_live_sessions.catalog_source,
                        excluded.catalog_source
                    ),
                    runtime_default_profile_name = excluded.runtime_default_profile_name,
                    runtime_profile_names = excluded.runtime_profile_names,
                    updated_at_ms = greatest(
                        remote_live_sessions.updated_at_ms,
                        excluded.updated_at_ms
                    ),
                    graph_json = excluded.graph_json,
                    analysis_json = excluded.analysis_json
                """,
                (
                    payload.graph_id,
                    payload.run_id,
                    payload.project_id,
                    payload.invocation_name,
                    payload.environment_name,
                    payload.catalog_entry_id,
                    None if payload.catalog_source is None else payload.catalog_source.value,
                    payload.runtime_default_profile_name,
                    json.dumps(list(payload.runtime_profile_names)),
                    payload.started_at_ms,
                    payload.started_at_ms,
                    None,
                    RemoteLiveSessionStatus.RUNNING.value,
                    None,
                    json.dumps(payload.graph, sort_keys=True),
                    json.dumps(payload.analysis, sort_keys=True),
                    None,
                ),
            )
            conn.commit()
        return self.get_session(graph_id=payload.graph_id, run_id=payload.run_id)

    def apply_session_update(
        self,
        payload: RemoteLiveSessionUpdateRequest,
    ) -> RemoteLiveSessionRecord:
        self._ensure_schema()
        existing = self.get_session(graph_id=payload.graph_id, run_id=payload.run_id)
        with psycopg.connect(self.database_url) as conn:
            if payload.records:
                for row in payload.records:
                    conn.execute(
                        """
                        insert into remote_live_records (
                            graph_id,
                            run_id,
                            record_id,
                            sequence,
                            timestamp_ms,
                            node_id,
                            frame_id,
                            loop_node_id,
                            iteration_index,
                            payload_json
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        on conflict (graph_id, run_id, record_id) do nothing
                        """,
                        (
                            payload.graph_id,
                            payload.run_id,
                            _required_live_row_str(row, "record_id"),
                            _optional_live_row_int(row, "sequence") or 0,
                            _required_live_row_int(row, "timestamp_ms"),
                            _required_live_row_str(row, "node_id"),
                            _required_live_row_str(row, "frame_id"),
                            _optional_live_row_str(row, "loop_node_id"),
                            _optional_live_row_int(row, "iteration_index"),
                            json.dumps(row, sort_keys=True),
                        ),
                    )
            if payload.spans:
                for row in payload.spans:
                    conn.execute(
                        _REMOTE_LIVE_SPAN_INSERT_SQL,
                        (
                            payload.graph_id,
                            payload.run_id,
                            _required_live_row_str(row, "span_id"),
                            _optional_live_row_int(row, "sequence") or 0,
                            _required_live_row_int(row, "start_time_ns"),
                            _optional_span_node_id(row),
                            _optional_span_frame_id(row),
                            _optional_span_loop_node_id(row),
                            _optional_span_iteration_index(row),
                            _optional_span_runtime_profile(row),
                            json.dumps(row, sort_keys=True),
                        ),
                    )
            next_status = payload.status or existing.status
            finished_at_ms = (
                payload.updated_at_ms
                if next_status
                in {
                    RemoteLiveSessionStatus.SUCCEEDED,
                    RemoteLiveSessionStatus.FAILED,
                }
                else existing.finished_at_ms
            )
            conn.execute(
                """
                update remote_live_sessions
                set
                    updated_at_ms = greatest(updated_at_ms, %s),
                    status = %s,
                    error = coalesce(%s, error),
                    finished_at_ms = %s
                where graph_id = %s and run_id = %s
                """,
                (
                    payload.updated_at_ms,
                    next_status.value,
                    payload.error,
                    finished_at_ms,
                    payload.graph_id,
                    payload.run_id,
                ),
            )
            conn.commit()
        return self.get_session(graph_id=payload.graph_id, run_id=payload.run_id)

    def mark_bundle_committed(
        self,
        *,
        graph_id: str,
        run_id: str,
        committed_at_ms: int,
    ) -> RemoteLiveSessionRecord | None:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            cursor = conn.execute(
                """
                update remote_live_sessions
                set
                    updated_at_ms = greatest(updated_at_ms, %s),
                    bundle_committed_at_ms = %s
                where graph_id = %s and run_id = %s
                returning graph_id
                """,
                (committed_at_ms, committed_at_ms, graph_id, run_id),
            )
            row = cursor.fetchone()
            conn.commit()
        if row is None:
            return None
        return self.get_session(graph_id=graph_id, run_id=run_id)

    def get_session(self, *, graph_id: str, run_id: str) -> RemoteLiveSessionRecord:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            row = conn.execute(
                """
                select
                    graph_id,
                    run_id,
                    project_id,
                    invocation_name,
                    environment_name,
                    catalog_entry_id,
                    catalog_source,
                    runtime_default_profile_name,
                    runtime_profile_names,
                    started_at_ms,
                    updated_at_ms,
                    finished_at_ms,
                    status,
                    error,
                    graph_json,
                    analysis_json,
                    bundle_committed_at_ms
                from remote_live_sessions
                where graph_id = %s and run_id = %s
                """,
                (graph_id, run_id),
            ).fetchone()
            if row is None:
                raise RunInspectionError(
                    f"Remote live session {graph_id}/{run_id} was not found."
                )
            records = self._fetch_live_rows(
                conn,
                table_name="remote_live_records",
                graph_id=graph_id,
                run_id=run_id,
            )
            spans = self._fetch_live_rows(
                conn,
                table_name="remote_live_spans",
                graph_id=graph_id,
                run_id=run_id,
            )
        return _remote_live_session_from_row(
            row,
            records=records,
            spans=spans,
        )

    def list_sessions(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[RemoteLiveSessionRecord, ...]:
        self._ensure_schema()
        conditions: list[str] = []
        params: list[object] = []
        if graph_id is not None:
            conditions.append("graph_id = %s")
            params.append(graph_id)
        if invocation_name is not None:
            conditions.append("invocation_name = %s")
            params.append(invocation_name)
        where_clause = ""
        if conditions:
            where_clause = "where " + " and ".join(conditions)
        with psycopg.connect(self.database_url) as conn:
            rows = conn.execute(
                f"""
                select
                    graph_id,
                    run_id,
                    project_id,
                    invocation_name,
                    environment_name,
                    catalog_entry_id,
                    catalog_source,
                    runtime_default_profile_name,
                    runtime_profile_names,
                    started_at_ms,
                    updated_at_ms,
                    finished_at_ms,
                    status,
                    error,
                    graph_json,
                    analysis_json,
                    bundle_committed_at_ms
                from remote_live_sessions
                {where_clause}
                order by started_at_ms desc, run_id desc
                """,
                tuple(params),
            ).fetchall()
            results: list[RemoteLiveSessionRecord] = []
            for row in rows:
                session_graph_id = cast(str, row[0])
                session_run_id = cast(str, row[1])
                records = self._fetch_live_rows(
                    conn,
                    table_name="remote_live_records",
                    graph_id=session_graph_id,
                    run_id=session_run_id,
                )
                spans = self._fetch_live_rows(
                    conn,
                    table_name="remote_live_spans",
                    graph_id=session_graph_id,
                    run_id=session_run_id,
                )
                results.append(
                    _remote_live_session_from_row(
                        row,
                        records=records,
                        spans=spans,
                    )
                )
        return tuple(results)

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        self._ensure_schema()
        before_sequence = decode_sequence_cursor(cursor)
        conditions = ["graph_id = %s", "run_id = %s"]
        params: list[object] = [graph_id, run_id]
        if node_id is not None:
            conditions.append("node_id = %s")
            params.append(node_id)
        if frame_id is not None:
            conditions.append("frame_id = %s")
            params.append(frame_id)
        if before_sequence is not None:
            conditions.append("sequence < %s")
            params.append(before_sequence)
        where_clause = " and ".join(conditions)
        count_params = params[: 2 + int(node_id is not None) + int(frame_id is not None)]
        count_where_clause = " and ".join(conditions[: len(count_params)])
        with psycopg.connect(self.database_url) as conn:
            count_row = conn.execute(
                f"select count(*) from remote_live_records where {count_where_clause}",
                tuple(count_params),
            )
            count_result = count_row.fetchone()
            if count_result is None:
                raise RunInspectionError(
                    f"Remote live session {graph_id}/{run_id} was not found."
                )
            total_count = cast(int, count_result[0])
            rows = conn.execute(
                f"""
                select payload_json, sequence
                from remote_live_records
                where {where_clause}
                order by sequence desc, timestamp_ms desc, record_id desc
                limit %s
                """,
                (*params, limit + 1),
            ).fetchall()
        items = tuple(
            cast(
                dict[str, JsonValue],
                _json_object_from_db_payload(
                    row[0], "remote_live_records.payload_json"
                ),
            )
            for row in rows[:limit]
        )
        next_cursor = (
            encode_sequence_cursor(cast(int, rows[limit - 1][1])) if len(rows) > limit else None
        )
        return PageSlice(items=items, next_cursor=next_cursor, total_count=total_count)

    def get_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        self._ensure_schema()
        before_sequence = decode_sequence_cursor(cursor)
        conditions = ["graph_id = %s", "run_id = %s"]
        params: list[object] = [graph_id, run_id]
        if node_id is not None:
            conditions.append("node_id = %s")
            params.append(node_id)
        if frame_id is not None:
            conditions.append("frame_id = %s")
            params.append(frame_id)
        if before_sequence is not None:
            conditions.append("sequence < %s")
            params.append(before_sequence)
        where_clause = " and ".join(conditions)
        count_params = params[: 2 + int(node_id is not None) + int(frame_id is not None)]
        count_where_clause = " and ".join(conditions[: len(count_params)])
        with psycopg.connect(self.database_url) as conn:
            count_row = conn.execute(
                f"select count(*) from remote_live_spans where {count_where_clause}",
                tuple(count_params),
            )
            count_result = count_row.fetchone()
            if count_result is None:
                raise RunInspectionError(
                    f"Remote live session {graph_id}/{run_id} was not found."
                )
            total_count = cast(int, count_result[0])
            rows = conn.execute(
                f"""
                select payload_json, sequence
                from remote_live_spans
                where {where_clause}
                order by sequence desc, start_time_ns desc, span_id desc
                limit %s
                """,
                (*params, limit + 1),
            ).fetchall()
        items = tuple(
            cast(
                dict[str, JsonValue],
                _json_object_from_db_payload(
                    row[0], "remote_live_spans.payload_json"
                ),
            )
            for row in rows[:limit]
        )
        next_cursor = (
            encode_sequence_cursor(cast(int, rows[limit - 1][1])) if len(rows) > limit else None
        )
        return PageSlice(items=items, next_cursor=next_cursor, total_count=total_count)

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self._schema_lock:
            if self._schema_ready:
                return
            with psycopg.connect(self.database_url) as conn:
                apply_schema_migrations(conn, REMOTE_LIVE_MIGRATIONS)
                conn.commit()
                self._schema_ready = True

    def _fetch_live_rows(
        self,
        conn: psycopg.Connection[object],
        *,
        table_name: str,
        graph_id: str,
        run_id: str,
    ) -> list[tuple[object, ...]]:
        rows = conn.execute(
            f"""
            select payload_json
            from {table_name}
            where graph_id = %s and run_id = %s
            order by sequence asc
            """,
            (graph_id, run_id),
        ).fetchall()
        return cast(list[tuple[object, ...]], rows)


class PostgresEventIndex:
    """Postgres-backed remote operation event log for hosted operator visibility."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._schema_lock = threading.Lock()
        self._schema_ready = False

    def record_event(self, event: RemoteOperationEvent) -> RemoteOperationEvent:
        self._ensure_schema()
        with psycopg.connect(self.database_url) as conn:
            conn.execute(
                """
                insert into remote_operation_events (
                    event_id,
                    occurred_at_ms,
                    kind,
                    status,
                    project_id,
                    graph_id,
                    run_id,
                    invocation_name,
                    error_category,
                    error_message,
                    metadata_json
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                on conflict (event_id) do update
                set
                    occurred_at_ms = excluded.occurred_at_ms,
                    kind = excluded.kind,
                    status = excluded.status,
                    project_id = excluded.project_id,
                    graph_id = excluded.graph_id,
                    run_id = excluded.run_id,
                    invocation_name = excluded.invocation_name,
                    error_category = excluded.error_category,
                    error_message = excluded.error_message,
                    metadata_json = excluded.metadata_json
                """,
                (
                    event.event_id,
                    event.occurred_at_ms,
                    event.kind.value,
                    event.status.value,
                    event.project_id,
                    event.graph_id,
                    event.run_id,
                    event.invocation_name,
                    event.error_category,
                    event.error_message,
                    json.dumps(event.metadata, sort_keys=True),
                ),
            )
            conn.commit()
        return event

    def list_events(
        self,
        *,
        project_id: str | None = None,
        graph_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> tuple[RemoteOperationEvent, ...]:
        self._ensure_schema()
        conditions: list[str] = []
        params: list[object] = []
        if project_id is not None:
            conditions.append("project_id = %s")
            params.append(project_id)
        if graph_id is not None:
            conditions.append("graph_id = %s")
            params.append(graph_id)
        if run_id is not None:
            conditions.append("run_id = %s")
            params.append(run_id)
        where_clause = ""
        if conditions:
            where_clause = "where " + " and ".join(conditions)
        params.append(max(1, limit))
        with psycopg.connect(self.database_url) as conn:
            rows = conn.execute(
                f"""
                select
                    event_id,
                    occurred_at_ms,
                    kind,
                    status,
                    project_id,
                    graph_id,
                    run_id,
                    invocation_name,
                    error_category,
                    error_message,
                    metadata_json
                from remote_operation_events
                {where_clause}
                order by occurred_at_ms desc, event_id desc
                limit %s
                """,
                tuple(params),
            ).fetchall()
        return tuple(_remote_operation_event_from_row(row) for row in rows)

    def summarize_project(
        self,
        *,
        project_id: str,
        since_ms: int | None = None,
    ) -> RemoteDeliveryHealthSummary:
        return _summarize_events(
            self.list_events(project_id=project_id, limit=500),
            since_ms=since_ms,
        )

    def summarize_run(
        self,
        *,
        graph_id: str,
        run_id: str,
        since_ms: int | None = None,
    ) -> RemoteDeliveryHealthSummary:
        return _summarize_events(
            self.list_events(graph_id=graph_id, run_id=run_id, limit=500),
            since_ms=since_ms,
        )

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self._schema_lock:
            if self._schema_ready:
                return
            with psycopg.connect(self.database_url) as conn:
                apply_schema_migrations(conn, REMOTE_EVENT_MIGRATIONS)
                conn.commit()
                self._schema_ready = True


class RemoteRunStore:
    """Remote ingest + historical read adapter for the minimal Phase 2 backend."""

    def __init__(
        self,
        *,
        manifest_index: ManifestIndex,
        artifact_store: ArtifactStore,
        persisted_run_index: PersistedRunIndex | None = None,
        cache_dir: Path | None = None,
    ) -> None:
        self._manifest_index = manifest_index
        self._artifact_store = artifact_store
        self._persisted_run_index = persisted_run_index
        self.cache_dir = (
            cache_dir.expanduser().resolve()
            if cache_dir is not None
            else Path(tempfile.gettempdir()) / "mentalmodel-remote-cache"
        )

    @classmethod
    def from_config(cls, config: RemoteBackendConfig) -> RemoteRunStore:
        return cls(
            manifest_index=PostgresManifestIndex(config.database_url),
            artifact_store=S3ArtifactStore(config),
            persisted_run_index=PostgresPersistedRunIndex(config.database_url),
            cache_dir=config.cache_dir,
        )

    @property
    def runs_root(self) -> Path:
        return self.cache_dir / ".runs"

    def contains_run(self, *, graph_id: str, run_id: str) -> bool:
        try:
            self._manifest_index.get_run(graph_id=graph_id, run_id=run_id)
        except RunInspectionError:
            return False
        return True

    def ingest(self, upload: RunBundleUpload) -> Path:
        artifact_map = _validated_artifact_payloads(upload)
        summary_payload = _summary_payload_from_upload(upload, artifact_map)
        artifact_prefix = f"runs/{upload.manifest.graph_id}/{upload.manifest.run_id}"
        stored_descriptors = []
        cached_bodies: dict[str, bytes] = {}
        for descriptor in upload.manifest.artifacts:
            content = artifact_map[descriptor.relative_path]
            key = f"{artifact_prefix}/{descriptor.relative_path}"
            storage_uri = self._artifact_store.put_artifact(
                key=key,
                content=content,
                content_type=descriptor.content_type,
            )
            stored_descriptors.append(replace(descriptor, storage_uri=storage_uri))
            cached_bodies[descriptor.relative_path] = content
        stored_manifest = replace(
            upload.manifest,
            status=RunManifestStatus.INDEXED,
            artifacts=tuple(stored_descriptors),
        )
        self._manifest_index.upsert_indexed_run(
            manifest=stored_manifest,
            summary_payload=summary_payload,
            artifact_prefix=artifact_prefix,
        )
        if self._persisted_run_index is not None:
            self._persisted_run_index.replace_run_payloads(
                graph_id=stored_manifest.graph_id,
                run_id=stored_manifest.run_id,
                records=_decoded_jsonl_payloads(cached_bodies.get("records.jsonl", b"")),
                spans=_decoded_jsonl_payloads(cached_bodies.get("otel-spans.jsonl", b"")),
            )
            if isinstance(self._persisted_run_index, InMemoryPersistedRunIndex):
                self._persisted_run_index.set_invocation_name(
                    graph_id=stored_manifest.graph_id,
                    run_id=stored_manifest.run_id,
                    invocation_name=stored_manifest.invocation_name,
                )
        run_dir = self._write_materialized_bundle(
            manifest=stored_manifest,
            artifact_prefix=artifact_prefix,
            artifact_bodies=cached_bodies,
        )
        return run_dir

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        if self._persisted_run_index is not None:
            self._ensure_persisted_payload_index(graph_id=graph_id, run_id=run_id)
            return self._persisted_run_index.get_records_page(
                graph_id=graph_id,
                run_id=run_id,
                cursor=cursor,
                limit=limit,
                node_id=node_id,
                frame_id=frame_id,
            )
        self.materialize_run(graph_id=graph_id, run_id=run_id)
        return load_run_records_page(
            runs_dir=self.runs_root,
            graph_id=graph_id,
            run_id=run_id,
            node_id=node_id,
            frame_id=frame_id,
            cursor=cursor,
            limit=limit,
        )

    def get_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        if self._persisted_run_index is not None:
            self._ensure_persisted_payload_index(graph_id=graph_id, run_id=run_id)
            return self._persisted_run_index.get_spans_page(
                graph_id=graph_id,
                run_id=run_id,
                cursor=cursor,
                limit=limit,
                node_id=node_id,
                frame_id=frame_id,
            )
        self.materialize_run(graph_id=graph_id, run_id=run_id)
        return load_run_spans_page(
            runs_dir=self.runs_root,
            graph_id=graph_id,
            run_id=run_id,
            node_id=node_id,
            frame_id=frame_id,
            cursor=cursor,
            limit=limit,
        )

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
    ) -> tuple[tuple[int, int, int, int], ...]:
        if self._persisted_run_index is None:
            raise RunInspectionError("Persisted run index is not configured.")
        if run_id is not None:
            self._ensure_persisted_payload_index(graph_id=graph_id, run_id=run_id)
        else:
            for summary in self.list_run_summaries(
                graph_id=graph_id,
                invocation_name=invocation_name,
            ):
                self._ensure_persisted_payload_index(
                    graph_id=summary.graph_id,
                    run_id=summary.run_id,
                )
        return self._persisted_run_index.aggregate_record_timeseries(
            graph_id=graph_id,
            invocation_name=invocation_name,
            since_ms=since_ms,
            until_ms=until_ms,
            rollup_ms=rollup_ms,
            run_id=run_id,
            node_id=node_id,
        )

    def _ensure_persisted_payload_index(self, *, graph_id: str, run_id: str) -> None:
        if self._persisted_run_index is None:
            return
        if self._persisted_run_index.has_indexed_run(graph_id=graph_id, run_id=run_id):
            return
        run_dir = self.materialize_run(graph_id=graph_id, run_id=run_id)
        self._persisted_run_index.replace_run_payloads(
            graph_id=graph_id,
            run_id=run_id,
            records=_decoded_jsonl_payloads(_read_optional_bytes(run_dir / "records.jsonl")),
            spans=_decoded_jsonl_payloads(_read_optional_bytes(run_dir / "otel-spans.jsonl")),
        )

    def list_run_summaries(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[RunSummary, ...]:
        rows = self._manifest_index.list_runs(
            graph_id=graph_id,
            invocation_name=invocation_name,
        )
        return tuple(_run_summary_from_indexed(row, run_dir=self.runs_root) for row in rows)

    def resolve_run_summary(
        self,
        *,
        graph_id: str | None = None,
        run_id: str | None = None,
        invocation_name: str | None = None,
    ) -> RunSummary:
        summaries = self.list_run_summaries(graph_id=graph_id, invocation_name=invocation_name)
        if not summaries:
            raise RunInspectionError("No remote runs matched the requested filters.")
        selected = summaries[0] if run_id is None else next(
            (summary for summary in summaries if summary.run_id == run_id),
            None,
        )
        if selected is None:
            raise RunInspectionError(f"Remote run {run_id!r} was not found.")
        self.materialize_run(graph_id=selected.graph_id, run_id=selected.run_id)
        return _run_summary_from_indexed(
            self._manifest_index.get_run(graph_id=selected.graph_id, run_id=selected.run_id),
            run_dir=self.runs_root,
        )

    def materialize_run(self, *, graph_id: str, run_id: str) -> Path:
        indexed = self._manifest_index.get_run(graph_id=graph_id, run_id=run_id)
        return self._write_materialized_bundle(
            manifest=indexed.manifest,
            artifact_prefix=indexed.artifact_prefix,
        )

    def _write_materialized_bundle(
        self,
        *,
        manifest: RunManifest,
        artifact_prefix: str,
        artifact_bodies: dict[str, bytes] | None = None,
    ) -> Path:
        run_dir = self.runs_root / manifest.graph_id / manifest.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        bodies = artifact_bodies or {}
        for descriptor in manifest.artifacts:
            target = run_dir / descriptor.relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                continue
            content = bodies.get(descriptor.relative_path)
            if content is None:
                content = self._artifact_store.get_artifact(
                    key=f"{artifact_prefix}/{descriptor.relative_path}"
                )
            target.write_bytes(content)
        return run_dir


class RemoteProjectStore:
    """Remote project registry adapter for the hosted service path."""

    def __init__(self, *, project_index: ProjectIndex) -> None:
        self._project_index = project_index

    @classmethod
    def from_config(cls, config: RemoteBackendConfig) -> RemoteProjectStore:
        return cls(project_index=PostgresProjectIndex(config.database_url))

    def link_project(self, payload: RemoteProjectLinkRequest) -> RemoteProjectRecord:
        return self._project_index.upsert_project(payload)

    def publish_catalog(
        self,
        payload: RemoteProjectCatalogPublishRequest,
    ) -> RemoteProjectRecord:
        return self._project_index.publish_catalog(payload)

    def record_completed_run_upload(
        self,
        *,
        project_id: str,
        graph_id: str,
        run_id: str,
        invocation_name: str | None,
        uploaded_at_ms: int,
    ) -> RemoteProjectRecord:
        return self._project_index.record_completed_run_upload(
            project_id=project_id,
            graph_id=graph_id,
            run_id=run_id,
            invocation_name=invocation_name,
            uploaded_at_ms=uploaded_at_ms,
        )

    def get_project(self, *, project_id: str) -> RemoteProjectRecord:
        return self._project_index.get_project(project_id=project_id)

    def list_projects(self) -> tuple[RemoteProjectRecord, ...]:
        return self._project_index.list_projects()


class RemoteLiveSessionStore:
    """Remote adapter for live in-progress run sessions."""

    def __init__(self, *, live_session_index: LiveSessionIndex) -> None:
        self._live_session_index = live_session_index

    @classmethod
    def from_config(cls, config: RemoteBackendConfig) -> RemoteLiveSessionStore:
        return cls(live_session_index=PostgresLiveSessionIndex(config.database_url))

    def start_session(
        self,
        payload: RemoteLiveSessionStartRequest,
    ) -> RemoteLiveSessionRecord:
        return self._live_session_index.upsert_session_start(payload)

    def apply_update(
        self,
        payload: RemoteLiveSessionUpdateRequest,
    ) -> RemoteLiveSessionRecord:
        return self._live_session_index.apply_session_update(payload)

    def mark_bundle_committed(
        self,
        *,
        graph_id: str,
        run_id: str,
        committed_at_ms: int,
    ) -> RemoteLiveSessionRecord | None:
        return self._live_session_index.mark_bundle_committed(
            graph_id=graph_id,
            run_id=run_id,
            committed_at_ms=committed_at_ms,
        )

    def get_session(self, *, graph_id: str, run_id: str) -> RemoteLiveSessionRecord:
        return self._live_session_index.get_session(graph_id=graph_id, run_id=run_id)

    def list_sessions(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[RemoteLiveSessionRecord, ...]:
        return self._live_session_index.list_sessions(
            graph_id=graph_id,
            invocation_name=invocation_name,
        )

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        return self._live_session_index.get_records_page(
            graph_id=graph_id,
            run_id=run_id,
            cursor=cursor,
            limit=limit,
            node_id=node_id,
            frame_id=frame_id,
        )

    def get_spans_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
    ) -> PageSlice[dict[str, JsonValue]]:
        return self._live_session_index.get_spans_page(
            graph_id=graph_id,
            run_id=run_id,
            cursor=cursor,
            limit=limit,
            node_id=node_id,
            frame_id=frame_id,
        )


class RemoteEventStore:
    """Remote operator event log adapter for hosted diagnostics."""

    def __init__(self, *, event_index: EventIndex) -> None:
        self._event_index = event_index

    @classmethod
    def from_config(cls, config: RemoteBackendConfig) -> RemoteEventStore:
        return cls(event_index=PostgresEventIndex(config.database_url))

    def record_event(self, event: RemoteOperationEvent) -> RemoteOperationEvent:
        return self._event_index.record_event(event)

    def list_events(
        self,
        *,
        project_id: str | None = None,
        graph_id: str | None = None,
        run_id: str | None = None,
        limit: int = 100,
    ) -> tuple[RemoteOperationEvent, ...]:
        return self._event_index.list_events(
            project_id=project_id,
            graph_id=graph_id,
            run_id=run_id,
            limit=limit,
        )

    def summarize_project(
        self,
        *,
        project_id: str,
        since_ms: int | None = None,
    ) -> RemoteDeliveryHealthSummary:
        return self._event_index.summarize_project(
            project_id=project_id,
            since_ms=since_ms,
        )

    def summarize_run(
        self,
        *,
        graph_id: str,
        run_id: str,
        since_ms: int | None = None,
    ) -> RemoteDeliveryHealthSummary:
        return self._event_index.summarize_run(
            graph_id=graph_id,
            run_id=run_id,
            since_ms=since_ms,
        )


class RemoteCompletedRunSink(CompletedRunSink):
    """Completed-run sink that indexes persisted local bundles into the remote backend."""

    def __init__(
        self,
        remote_run_store: RemoteRunStore,
        *,
        project_id: str | None = None,
        project_label: str | None = None,
        environment_name: str | None = None,
        catalog_entry_id: str | None = None,
        catalog_source: CatalogSource | None = None,
    ) -> None:
        self._remote_run_store = remote_run_store
        self.project_id = project_id
        self.project_label = project_label
        self.environment_name = environment_name
        self.catalog_entry_id = catalog_entry_id
        self.catalog_source = catalog_source

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
        remote_run_dir = self._remote_run_store.ingest(upload)
        return CompletedRunPublishResult(
            transport="direct-store",
            success=True,
            graph_id=upload.manifest.graph_id,
            run_id=upload.manifest.run_id,
            project_id=upload.manifest.project_id,
            remote_run_dir=str(remote_run_dir),
            uploaded_at_ms=int(time.time() * 1000),
        )


def _validated_artifact_payloads(upload: RunBundleUpload) -> dict[str, bytes]:
    missing = upload.manifest.missing_required_artifacts()
    if missing:
        raise RemoteContractError(
            "Run upload is missing required artifacts: "
            f"{', '.join(name.value for name in missing)}."
        )
    payloads = {
        artifact.descriptor.relative_path: artifact.content_bytes()
        for artifact in upload.artifacts
    }
    expected_paths = {artifact.relative_path for artifact in upload.manifest.artifacts}
    if set(payloads) != expected_paths:
        raise RemoteContractError(
            "Uploaded artifacts must match the manifest artifact descriptors exactly."
        )
    for descriptor in upload.manifest.artifacts:
        content = payloads[descriptor.relative_path]
        if descriptor.checksum_sha256 is not None:
            import hashlib

            digest = hashlib.sha256(content).hexdigest()
            if digest != descriptor.checksum_sha256:
                raise RemoteContractError(
                    f"Checksum mismatch for artifact {descriptor.logical_name.value!r}."
                )
    return payloads


def _summary_payload_from_upload(
    upload: RunBundleUpload,
    artifact_payloads: dict[str, bytes],
) -> dict[str, JsonValue]:
    descriptor = next(
        (
            item
            for item in upload.manifest.artifacts
            if item.relative_path == "summary.json"
        ),
        None,
    )
    if descriptor is None:
        raise RemoteContractError("Run upload must include summary.json.")
    raw = json.loads(artifact_payloads["summary.json"].decode("utf-8"))
    if not isinstance(raw, dict):
        raise RemoteContractError("summary.json must decode to an object.")
    payload = {
        str(key): cast_json_value(value)
        for key, value in raw.items()
    }
    normalized = normalize_summary_payload(
        payload=payload,
        run_dir=Path("/remote") / upload.manifest.graph_id / upload.manifest.run_id,
    )
    if normalized["graph_id"] != upload.manifest.graph_id:
        raise RemoteContractError("summary.json graph_id must match the uploaded manifest.")
    if normalized["run_id"] != upload.manifest.run_id:
        raise RemoteContractError("summary.json run_id must match the uploaded manifest.")
    return normalized


def _run_summary_from_indexed(indexed: IndexedRemoteRun, *, run_dir: Path) -> RunSummary:
    normalized = normalize_summary_payload(
        payload=indexed.summary_payload,
        run_dir=run_dir / indexed.manifest.graph_id / indexed.manifest.run_id,
    )
    return RunSummary(
        schema_version=_json_int(normalized, "schema_version"),
        graph_id=_json_str(normalized, "graph_id"),
        run_id=_json_str(normalized, "run_id"),
        run_dir=run_dir / indexed.manifest.graph_id / indexed.manifest.run_id,
        created_at_ms=_json_int(normalized, "created_at_ms"),
        success=_json_bool(normalized, "success"),
        node_count=_json_int(normalized, "node_count"),
        edge_count=_json_int(normalized, "edge_count"),
        record_count=_json_int(normalized, "record_count"),
        output_count=_json_int(normalized, "output_count"),
        state_count=_json_int(normalized, "state_count"),
        trace_sink_configured=_json_bool(normalized, "trace_sink_configured"),
        trace_mode=_json_str(normalized, "trace_mode"),
        trace_otlp_endpoint=_json_optional_str(normalized, "trace_otlp_endpoint"),
        trace_mirror_to_disk=_json_bool(normalized, "trace_mirror_to_disk"),
        trace_capture_local_spans=_json_bool(normalized, "trace_capture_local_spans"),
        trace_service_name=_json_str(normalized, "trace_service_name"),
        invocation_name=_json_optional_str(normalized, "invocation_name"),
        runtime_default_profile_name=_json_optional_str(
            normalized,
            "runtime_default_profile_name",
        ),
        runtime_profile_names=_json_str_tuple(normalized, "runtime_profile_names"),
    )


def _indexed_run_from_row(
    *,
    manifest_json: str,
    summary_json: str,
    artifact_prefix: str,
) -> IndexedRemoteRun:
    manifest_payload = json.loads(manifest_json)
    summary_payload = json.loads(summary_json)
    if not isinstance(manifest_payload, dict):
        raise RemoteContractError("Stored manifest row must be an object.")
    if not isinstance(summary_payload, dict):
        raise RemoteContractError("Stored summary row must be an object.")
    return IndexedRemoteRun(
        manifest=RunManifest.from_dict(cast(dict[str, object], manifest_payload)),
        summary_payload={
            str(key): cast_json_value(value)
            for key, value in summary_payload.items()
        },
        artifact_prefix=artifact_prefix,
    )


def _remote_project_from_row(
    *,
    project_id: str,
    label: str,
    description: str,
    default_environment: str | None,
    catalog_provider: str | None,
    default_runs_dir: str | None,
    default_verify_spec: str | None,
    linked_at_ms: int,
    updated_at_ms: int,
    catalog_snapshot_json: str | None,
    last_completed_run_upload_at_ms: int | None,
    last_completed_run_graph_id: str | None,
    last_completed_run_id: str | None,
    last_completed_run_invocation_name: str | None,
) -> RemoteProjectRecord:
    snapshot = None
    if catalog_snapshot_json not in (None, ""):
        raw_snapshot = cast(str, catalog_snapshot_json)
        decoded = json.loads(raw_snapshot)
        if not isinstance(decoded, dict):
            raise RemoteContractError("Stored project catalog snapshot must be an object.")
        snapshot = ProjectCatalogSnapshot.from_dict(cast(dict[str, object], decoded))
    return RemoteProjectRecord(
        project_id=project_id,
        label=label,
        description=description,
        default_environment=default_environment,
        catalog_provider=catalog_provider,
        default_runs_dir=default_runs_dir,
        default_verify_spec=default_verify_spec,
        linked_at_ms=linked_at_ms,
        updated_at_ms=updated_at_ms,
        catalog_snapshot=snapshot,
        last_completed_run_upload_at_ms=last_completed_run_upload_at_ms,
        last_completed_run_graph_id=last_completed_run_graph_id,
        last_completed_run_id=last_completed_run_id,
        last_completed_run_invocation_name=last_completed_run_invocation_name,
    )


def _remote_live_session_from_row(
    row: Sequence[object],
    *,
    records: Sequence[Sequence[object]],
    spans: Sequence[Sequence[object]],
) -> RemoteLiveSessionRecord:
    graph_json = _json_object_from_db_payload(row[14], "remote_live_sessions.graph_json")
    analysis_json = _json_object_from_db_payload(
        row[15], "remote_live_sessions.analysis_json"
    )
    normalized_records = tuple(
        _json_object_from_db_payload(record_row[0], "remote_live_records.payload_json")
        for record_row in records
    )
    normalized_spans = tuple(
        _json_object_from_db_payload(span_row[0], "remote_live_spans.payload_json")
        for span_row in spans
    )
    runtime_profile_names = _json_list_from_db_payload(
        row[8], "remote_live_sessions.runtime_profile_names"
    )
    return RemoteLiveSessionRecord(
        graph_id=cast(str, row[0]),
        run_id=cast(str, row[1]),
        project_id=cast(str | None, row[2]),
        invocation_name=cast(str | None, row[3]),
        environment_name=cast(str | None, row[4]),
        catalog_entry_id=cast(str | None, row[5]),
        catalog_source=(
            None if row[6] is None else CatalogSource(cast(str, row[6]))
        ),
        runtime_default_profile_name=cast(str | None, row[7]),
        runtime_profile_names=tuple(
            item for item in runtime_profile_names if isinstance(item, str)
        ),
        started_at_ms=cast(int, row[9]),
        updated_at_ms=cast(int, row[10]),
        finished_at_ms=cast(int | None, row[11]),
        status=RemoteLiveSessionStatus(cast(str, row[12])),
        error=cast(str | None, row[13]),
        graph=graph_json,
        analysis=analysis_json,
        bundle_committed_at_ms=cast(int | None, row[16]),
        records=normalized_records,
        spans=normalized_spans,
    )


def _merge_live_rows(
    existing: Sequence[dict[str, object]],
    incoming: Sequence[dict[str, object]],
    *,
    id_key: str,
    order_key: str,
) -> tuple[dict[str, object], ...]:
    merged: dict[str, dict[str, object]] = {}
    anonymous: list[dict[str, object]] = []
    for row in (*existing, *incoming):
        raw_id = row.get(id_key)
        if isinstance(raw_id, str) and raw_id:
            merged[raw_id] = dict(row)
            continue
        anonymous.append(dict(row))
    ordered = sorted(
        merged.values(),
        key=lambda row: (
            _optional_live_row_int(row, order_key) or 0,
            json.dumps(row, sort_keys=True),
        ),
    )
    ordered.extend(
        sorted(
            anonymous,
            key=lambda row: (
                _optional_live_row_int(row, order_key) or 0,
                json.dumps(row, sort_keys=True),
            ),
        )
    )
    return tuple(ordered)


def _required_live_row_str(row: Mapping[str, object], key: str) -> str:
    value = row.get(key)
    if isinstance(value, str) and value:
        return value
    raise RemoteContractError(f"Live row {key!r} must be a non-empty string.")


def _required_live_row_int(row: Mapping[str, object], key: str) -> int:
    value = row.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise RemoteContractError(f"Live row {key!r} must be an integer.")


def _optional_live_row_int(row: Mapping[str, object], key: str) -> int | None:
    value = row.get(key)
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise RemoteContractError(f"Live row {key!r} must be an integer when present.")


def _optional_live_row_str(row: Mapping[str, object], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
    raise RemoteContractError(f"Live row {key!r} must be a string when present.")


def _optional_span_node_id(row: Mapping[str, object]) -> str | None:
    attributes = row.get("attributes")
    if not isinstance(attributes, dict):
        return None
    value = attributes.get("mentalmodel.node.id")
    return value if isinstance(value, str) else None


def _optional_span_frame_id(row: Mapping[str, object]) -> str | None:
    value = row.get("frame_id")
    if isinstance(value, str) and value:
        return value
    attributes = row.get("attributes")
    if not isinstance(attributes, dict):
        return None
    attr = attributes.get("mentalmodel.frame.id")
    return attr if isinstance(attr, str) and attr else None


def _optional_span_loop_node_id(row: Mapping[str, object]) -> str | None:
    value = row.get("loop_node_id")
    if isinstance(value, str) and value:
        return value
    attributes = row.get("attributes")
    if not isinstance(attributes, dict):
        return None
    attr = attributes.get("mentalmodel.loop.node_id")
    return attr if isinstance(attr, str) and attr else None


def _optional_span_iteration_index(row: Mapping[str, object]) -> int | None:
    value = row.get("iteration_index")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    attributes = row.get("attributes")
    if not isinstance(attributes, dict):
        return None
    attr = attributes.get("mentalmodel.loop.iteration_index")
    if isinstance(attr, int) and not isinstance(attr, bool):
        return attr
    if isinstance(attr, str) and attr:
        try:
            return int(attr)
        except ValueError as exc:
            raise RemoteContractError(
                "Live span mentalmodel.loop.iteration_index must be an integer when present."
            ) from exc
    return None


def _optional_span_runtime_profile(row: Mapping[str, object]) -> str | None:
    attributes = row.get("attributes")
    if not isinstance(attributes, dict):
        return None
    for key in ("mentalmodel.runtime.profile", "mentalmodel.runtime.context"):
        value = attributes.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _span_node_id(span: dict[str, JsonValue]) -> str | None:
    attributes = span.get("attributes")
    if isinstance(attributes, dict):
        value = attributes.get("mentalmodel.node.id")
        if isinstance(value, str):
            return value
    value = span.get("node_id")
    return value if isinstance(value, str) else None


def _span_frame_id(span: dict[str, JsonValue]) -> str | None:
    value = span.get("frame_id")
    return value if isinstance(value, str) else None


def _live_row_sequence(row: dict[str, JsonValue]) -> int:
    value = row.get("sequence")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _persisted_record_id(row: dict[str, JsonValue]) -> str:
    value = row.get("record_id")
    if isinstance(value, str) and value:
        return value
    sequence = row.get("sequence")
    node_id = row.get("node_id")
    if isinstance(sequence, int) and isinstance(node_id, str) and node_id:
        return f"{node_id}:{sequence}"
    raise RemoteContractError("Persisted record rows must include record_id or sequence+node_id.")


def _persisted_span_id(row: dict[str, JsonValue]) -> str:
    value = row.get("span_id")
    if isinstance(value, str) and value:
        return value
    sequence = row.get("sequence")
    name = row.get("name")
    if isinstance(sequence, int) and isinstance(name, str) and name:
        return f"{name}:{sequence}"
    raise RemoteContractError("Persisted span rows must include span_id or sequence+name.")


def _decoded_jsonl_payloads(content: bytes) -> tuple[dict[str, JsonValue], ...]:
    if not content:
        return ()
    rows: list[dict[str, JsonValue]] = []
    for raw_line in content.decode("utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        decoded = json.loads(line)
        if not isinstance(decoded, dict):
            raise RemoteContractError("Persisted JSONL rows must decode to objects.")
        rows.append(
            {
                str(key): cast_json_value(value)
                for key, value in decoded.items()
            }
        )
    return tuple(rows)


def _read_optional_bytes(path: Path) -> bytes:
    return path.read_bytes() if path.is_file() else b""


def _page_from_json_rows(
    rows: Sequence[tuple[object, ...]],
    *,
    limit: int,
    total_count: int,
) -> PageSlice[dict[str, JsonValue]]:
    has_more = len(rows) > limit
    page_rows = rows[:limit]
    items = tuple(
        cast(dict[str, JsonValue], _json_object_from_db_payload(row[0], "payload_json"))
        for row in page_rows
    )
    next_cursor = None
    if has_more and page_rows:
        next_cursor = encode_sequence_cursor(_int_from_db_scalar(page_rows[-1][1]))
    return PageSlice(
        items=items,
        next_cursor=next_cursor,
        total_count=total_count,
    )


def _json_value_from_db_payload(raw: object, field_name: str) -> JsonValue:
    if isinstance(raw, (str, bytes, bytearray)):
        payload = json.loads(raw)
    else:
        payload = raw
    try:
        return cast_json_value(payload)
    except TypeError as exc:
        raise RemoteContractError(
            f"Stored {field_name} must decode to valid JSON-compatible data."
        ) from exc


def _int_from_db_scalar(value: object) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value:
        return int(value)
    raise RemoteContractError("Stored database scalar must be coercible to int.")


def _json_object_from_db_payload(raw: object, field_name: str) -> dict[str, object]:
    payload = _json_value_from_db_payload(raw, field_name)
    if not isinstance(payload, dict):
        raise RemoteContractError(f"Stored {field_name} must decode to an object.")
    return cast(dict[str, object], payload)


def _json_list_from_db_payload(raw: object, field_name: str) -> list[object]:
    payload = _json_value_from_db_payload(raw, field_name)
    if not isinstance(payload, list):
        raise RemoteContractError(f"Stored {field_name} must decode to a list.")
    return cast(list[object], payload)


def _remote_operation_event_from_row(row: Sequence[object]) -> RemoteOperationEvent:
    metadata = _json_object_from_db_payload(row[10], "remote_operation_events.metadata_json")
    return RemoteOperationEvent(
        event_id=cast(str, row[0]),
        occurred_at_ms=cast(int, row[1]),
        kind=RemoteOperationKind(cast(str, row[2])),
        status=RemoteOperationStatus(cast(str, row[3])),
        project_id=cast(str | None, row[4]),
        graph_id=cast(str | None, row[5]),
        run_id=cast(str | None, row[6]),
        invocation_name=cast(str | None, row[7]),
        error_category=cast(str | None, row[8]),
        error_message=cast(str | None, row[9]),
        metadata={str(key): cast_json_value(value) for key, value in metadata.items()},
    )


def _summarize_events(
    events: Sequence[RemoteOperationEvent],
    *,
    since_ms: int | None,
) -> RemoteDeliveryHealthSummary:
    recent_events = (
        tuple(event for event in events if since_ms is None or event.occurred_at_ms >= since_ms)
    )
    latest = next(iter(events), None)
    return RemoteDeliveryHealthSummary(
        last_event_at_ms=None if latest is None else latest.occurred_at_ms,
        last_status=None if latest is None else latest.status,
        last_kind=None if latest is None else latest.kind,
        last_error_message=None if latest is None else latest.error_message,
        recent_success_count=sum(
            1
            for event in recent_events
            if event.status is RemoteOperationStatus.SUCCEEDED
        ),
        recent_failure_count=sum(
            1
            for event in recent_events
            if event.status is RemoteOperationStatus.FAILED
        ),
    )


def _json_str(payload: dict[str, JsonValue], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, str):
        return value
    raise RemoteContractError(f"Expected {key!r} to be a string.")


def _json_optional_str(payload: dict[str, JsonValue], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise RemoteContractError(f"Expected {key!r} to be a string when present.")


def _json_int(payload: dict[str, JsonValue], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise RemoteContractError(f"Expected {key!r} to be an integer.")


def _json_bool(payload: dict[str, JsonValue], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    raise RemoteContractError(f"Expected {key!r} to be a boolean.")


def _json_str_tuple(payload: dict[str, JsonValue], key: str) -> tuple[str, ...]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise RemoteContractError(f"Expected {key!r} to be a list.")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise RemoteContractError(f"Expected every value in {key!r} to be a string.")
        items.append(item)
    return tuple(items)
