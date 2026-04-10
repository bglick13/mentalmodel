from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol, cast

import boto3  # type: ignore[import-untyped]
import psycopg

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.remote.contracts import (
    CatalogSource,
    ProjectCatalogSnapshot,
    RemoteContractError,
    RemoteProjectLinkRequest,
    RemoteProjectRecord,
    RunManifest,
    RunManifestStatus,
)
from mentalmodel.remote.sinks import CompletedRunSink
from mentalmodel.remote.store import RunBundleUpload
from mentalmodel.remote.sync import build_run_bundle_upload_from_run_dir
from mentalmodel.runtime.runs import (
    RunSummary,
    cast_json_value,
    normalize_summary_payload,
)


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

    def get_project(self, *, project_id: str) -> RemoteProjectRecord: ...

    def list_projects(self) -> tuple[RemoteProjectRecord, ...]: ...


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
                    manifest_json,
                    summary_json,
                    artifact_prefix,
                    updated_at_ms
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, %s, %s
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
                conn.execute(
                    """
                    create table if not exists remote_runs (
                        graph_id text not null,
                        run_id text not null,
                        created_at_ms bigint not null,
                        completed_at_ms bigint,
                        status text not null,
                        success boolean,
                        invocation_name text,
                        project_id text,
                        project_label text,
                        environment_name text,
                        catalog_entry_id text,
                        catalog_source text,
                        runtime_default_profile_name text,
                        runtime_profile_names jsonb not null,
                        run_schema_version integer not null,
                        record_schema_version integer,
                        manifest_json jsonb not null,
                        summary_json jsonb not null,
                        artifact_prefix text not null,
                        updated_at_ms bigint not null,
                        primary key (graph_id, run_id)
                    )
                    """
                )
                conn.execute(
                    "create index if not exists idx_remote_runs_created_at "
                    "on remote_runs (created_at_ms desc)"
                )
                conn.execute(
                    "create index if not exists idx_remote_runs_project_id "
                    "on remote_runs (project_id)"
                )
                conn.execute(
                    "create index if not exists idx_remote_runs_invocation_name "
                    "on remote_runs (invocation_name)"
                )
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
                    catalog_version
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
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
                    catalog_snapshot_json::text
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
                    catalog_snapshot_json::text
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
                    catalog_snapshot_json::text
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
            )
            for row in rows
        )

    def _ensure_schema(self) -> None:
        with self._schema_lock:
            if self._schema_ready:
                return
            with psycopg.connect(self.database_url) as conn:
                conn.execute(
                    """
                    create table if not exists remote_projects (
                        project_id text primary key,
                        label text not null,
                        description text not null,
                        default_environment text,
                        catalog_provider text,
                        default_runs_dir text,
                        default_verify_spec text,
                        linked_at_ms bigint not null,
                        updated_at_ms bigint not null,
                        catalog_snapshot_json jsonb,
                        catalog_entry_count integer not null default 0,
                        catalog_published_at_ms bigint,
                        catalog_version integer
                    )
                    """
                )
                conn.execute(
                    "create index if not exists idx_remote_projects_updated_at "
                    "on remote_projects (updated_at_ms desc)"
                )
                conn.commit()
            self._schema_ready = True


class RemoteRunStore:
    """Remote ingest + historical read adapter for the minimal Phase 2 backend."""

    def __init__(
        self,
        *,
        manifest_index: ManifestIndex,
        artifact_store: ArtifactStore,
        cache_dir: Path | None = None,
    ) -> None:
        self._manifest_index = manifest_index
        self._artifact_store = artifact_store
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
            cache_dir=config.cache_dir,
        )

    @property
    def runs_root(self) -> Path:
        return self.cache_dir / ".runs"

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
        run_dir = self._write_materialized_bundle(
            manifest=stored_manifest,
            artifact_prefix=artifact_prefix,
            artifact_bodies=cached_bodies,
        )
        return run_dir

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

    def get_project(self, *, project_id: str) -> RemoteProjectRecord:
        return self._project_index.get_project(project_id=project_id)

    def list_projects(self) -> tuple[RemoteProjectRecord, ...]:
        return self._project_index.list_projects()


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

    def publish(self, *, manifest: RunManifest, run_dir: Path) -> None:
        del manifest
        self.publish_run_dir(run_dir)

    def publish_run_dir(self, run_dir: Path) -> Path:
        upload = build_run_bundle_upload_from_run_dir(
            run_dir=run_dir,
            project_id=self.project_id,
            project_label=self.project_label,
            environment_name=self.environment_name,
            catalog_entry_id=self.catalog_entry_id,
            catalog_source=self.catalog_source,
        )
        return self._remote_run_store.ingest(upload)


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
