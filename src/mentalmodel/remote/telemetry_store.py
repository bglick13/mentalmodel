from __future__ import annotations

import base64
import json
import threading
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, cast

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.observability.dashboard_metrics import IndexedMetricRow
from mentalmodel.pagination import PageSlice, decode_sequence_cursor, encode_sequence_cursor
from mentalmodel.remote.contracts import RemoteContractError


@dataclass(slots=True, frozen=True)
class TelemetryRunRecord:
    """Canonical hosted query-model row for one run."""

    graph_id: str
    run_id: str
    created_at_ms: int
    updated_at_ms: int
    status: str
    success: bool | None
    invocation_name: str | None = None
    project_id: str | None = None
    project_label: str | None = None
    environment_name: str | None = None
    catalog_entry_id: str | None = None
    catalog_source: str | None = None
    runtime_default_profile_name: str | None = None
    runtime_profile_names: tuple[str, ...] = ()
    graph: dict[str, JsonValue] | None = None
    analysis: dict[str, JsonValue] | None = None
    error_message: str | None = None
    record_count: int = 0
    span_count: int = 0
    metric_count: int = 0
    output_count: int = 0


@dataclass(slots=True, frozen=True)
class TelemetryRecordRow:
    graph_id: str
    run_id: str
    record_id: str
    sequence: int
    timestamp_ms: int
    event_type: str
    node_id: str
    frame_id: str
    frame_path: JsonValue
    payload: JsonValue
    loop_node_id: str | None = None
    iteration_index: int | None = None
    invocation_name: str | None = None
    runtime_profile_name: str | None = None

    def as_dict(self, *, include_payload: bool = True) -> dict[str, JsonValue]:
        payload: dict[str, JsonValue] = {
            "record_id": self.record_id,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "frame_id": self.frame_id,
            "frame_path": self.frame_path,
            "loop_node_id": self.loop_node_id,
            "iteration_index": self.iteration_index,
            "event_type": self.event_type,
            "sequence": self.sequence,
            "timestamp_ms": self.timestamp_ms,
        }
        if include_payload:
            payload["payload"] = self.payload
        return payload


@dataclass(slots=True, frozen=True)
class TelemetrySpanRow:
    graph_id: str
    run_id: str
    span_key: str
    sequence: int
    name: str
    start_time_ns: int
    end_time_ns: int
    frame_id: str
    attributes: dict[str, JsonValue]
    trace_id: str | None = None
    otel_span_id: str | None = None
    parent_span_id: str | None = None
    loop_node_id: str | None = None
    iteration_index: int | None = None
    error_type: str | None = None
    error_message: str | None = None

    def as_dict(self) -> dict[str, JsonValue]:
        payload: dict[str, JsonValue] = {
            "span_id": self.span_key,
            "sequence": self.sequence,
            "name": self.name,
            "start_time_ns": self.start_time_ns,
            "end_time_ns": self.end_time_ns,
            "duration_ns": max(0, self.end_time_ns - self.start_time_ns),
            "attributes": self.attributes,
            "frame_id": self.frame_id,
            "loop_node_id": self.loop_node_id,
            "iteration_index": self.iteration_index,
            "error_type": self.error_type,
            "error_message": self.error_message,
        }
        if self.trace_id is not None:
            payload["trace_id"] = self.trace_id
        if self.otel_span_id is not None:
            payload["otel_span_id"] = self.otel_span_id
        if self.parent_span_id is not None:
            payload["parent_span_id"] = self.parent_span_id
        return payload


@dataclass(slots=True, frozen=True)
class TelemetryMetricPointRow:
    graph_id: str
    run_id: str
    point_key: str
    metric_name: str
    metric_kind: str
    value: float
    unit: str | None
    timestamp_unix_ns: int | None
    node_id: str | None = None
    frame_id: str | None = None
    loop_node_id: str | None = None
    iteration_index: int | None = None
    invocation_name: str | None = None
    runtime_profile_name: str | None = None
    attributes: dict[str, JsonValue] = field(default_factory=dict)


class TelemetryStore(Protocol):
    """Hosted query-model storage for both live and completed telemetry."""

    def replace_run(self, run: TelemetryRunRecord) -> None: ...

    def replace_records(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[TelemetryRecordRow],
    ) -> None: ...

    def append_records(self, rows: Sequence[TelemetryRecordRow]) -> None: ...

    def replace_spans(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[TelemetrySpanRow],
    ) -> None: ...

    def append_spans(self, rows: Sequence[TelemetrySpanRow]) -> None: ...

    def replace_metric_rows(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[IndexedMetricRow],
    ) -> None: ...

    def append_metric_rows(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[IndexedMetricRow],
    ) -> None: ...

    def append_metric_points(self, rows: Sequence[TelemetryMetricPointRow]) -> None: ...

    def contains_run(self, *, graph_id: str, run_id: str) -> bool: ...

    def list_runs(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[TelemetryRunRecord, ...]: ...

    def get_run(self, *, graph_id: str, run_id: str) -> TelemetryRunRecord: ...

    def get_records_page(
        self,
        *,
        graph_id: str,
        run_id: str,
        cursor: str | None,
        limit: int,
        node_id: str | None = None,
        frame_id: str | None = None,
        include_payload: bool = True,
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

    def list_metrics(
        self,
        *,
        graph_id: str,
        run_id: str,
        step_start: int | None = None,
        step_end: int | None = None,
        node_id: str | None = None,
        frame_id: str | None = None,
        path_prefixes: Sequence[str] = (),
    ) -> tuple[IndexedMetricRow, ...]: ...

    def list_invariants(
        self,
        *,
        graph_id: str,
        run_id: str,
    ) -> tuple[dict[str, JsonValue], ...]: ...


class InMemoryTelemetryStore:
    """Deterministic query-model implementation for tests."""

    def __init__(self) -> None:
        self._runs: dict[tuple[str, str], TelemetryRunRecord] = {}
        self._records: dict[tuple[str, str], dict[str, TelemetryRecordRow]] = {}
        self._spans: dict[tuple[str, str], dict[str, TelemetrySpanRow]] = {}
        self._metric_rows: dict[tuple[str, str], dict[str, IndexedMetricRow]] = {}
        self._metric_points: dict[tuple[str, str], dict[str, TelemetryMetricPointRow]] = {}

    def replace_run(self, run: TelemetryRunRecord) -> None:
        self._runs[(run.graph_id, run.run_id)] = run

    def replace_records(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[TelemetryRecordRow],
    ) -> None:
        self._records[(graph_id, run_id)] = {row.record_id: row for row in rows}

    def append_records(self, rows: Sequence[TelemetryRecordRow]) -> None:
        for row in rows:
            self._records.setdefault((row.graph_id, row.run_id), {})[row.record_id] = row

    def replace_spans(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[TelemetrySpanRow],
    ) -> None:
        self._spans[(graph_id, run_id)] = {row.span_key: row for row in rows}

    def append_spans(self, rows: Sequence[TelemetrySpanRow]) -> None:
        for row in rows:
            self._spans.setdefault((row.graph_id, row.run_id), {})[row.span_key] = row

    def replace_metric_rows(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[IndexedMetricRow],
    ) -> None:
        self._metric_rows[(graph_id, run_id)] = {
            _metric_row_key(row): row for row in rows
        }

    def append_metric_rows(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[IndexedMetricRow],
    ) -> None:
        metric_rows = self._metric_rows.setdefault((graph_id, run_id), {})
        for row in rows:
            metric_rows[_metric_row_key(row)] = row

    def append_metric_points(self, rows: Sequence[TelemetryMetricPointRow]) -> None:
        for row in rows:
            self._metric_points.setdefault((row.graph_id, row.run_id), {})[row.point_key] = row

    def contains_run(self, *, graph_id: str, run_id: str) -> bool:
        return (graph_id, run_id) in self._runs

    def list_runs(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[TelemetryRunRecord, ...]:
        rows = tuple(
            self._with_counts(row)
            for row in self._runs.values()
            if (graph_id is None or row.graph_id == graph_id)
            and (invocation_name is None or row.invocation_name == invocation_name)
        )
        return tuple(sorted(rows, key=lambda row: (row.created_at_ms, row.run_id), reverse=True))

    def get_run(self, *, graph_id: str, run_id: str) -> TelemetryRunRecord:
        try:
            return self._with_counts(self._runs[(graph_id, run_id)])
        except KeyError as exc:
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.") from exc

    def get_records_page(
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
        rows = tuple(self._records.get((graph_id, run_id), {}).values())
        if not rows and not self.contains_run(graph_id=graph_id, run_id=run_id):
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
        filtered = tuple(
            row
            for row in rows
            if (node_id is None or row.node_id == node_id)
            and (frame_id is None or row.frame_id == frame_id)
        )
        return _page_from_sequence_rows(
            rows=tuple(row.as_dict(include_payload=include_payload) for row in filtered),
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
        rows = tuple(self._spans.get((graph_id, run_id), {}).values())
        if not rows and not self.contains_run(graph_id=graph_id, run_id=run_id):
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
        filtered = tuple(
            row
            for row in rows
            if (node_id is None or _span_node_id(row) == node_id)
            and (frame_id is None or row.frame_id == frame_id)
        )
        return _page_from_sequence_rows(
            rows=tuple(row.as_dict() for row in filtered),
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
            run = self._runs.get((row_graph_id, row_run_id))
            if run is None:
                continue
            if run_id is not None and row_run_id != run_id:
                continue
            if run_id is None and run.invocation_name != invocation_name:
                continue
            for row in rows.values():
                if row.timestamp_ms < since_ms or row.timestamp_ms >= until_ms:
                    continue
                if node_id is not None and row.node_id != node_id:
                    continue
                bucket_index = (row.timestamp_ms - since_ms) // rollup_ms
                record_count, loop_count, node_set = bucket_state.get(bucket_index, (0, 0, set()))
                node_set.add(row.node_id)
                bucket_state[bucket_index] = (
                    record_count + 1,
                    loop_count + (1 if row.iteration_index is not None else 0),
                    node_set,
                )
        return tuple(
            (bucket, count, loop_count, len(node_set))
            for bucket, (count, loop_count, node_set) in sorted(bucket_state.items())
        )

    def list_metrics(
        self,
        *,
        graph_id: str,
        run_id: str,
        step_start: int | None = None,
        step_end: int | None = None,
        node_id: str | None = None,
        frame_id: str | None = None,
        path_prefixes: Sequence[str] = (),
    ) -> tuple[IndexedMetricRow, ...]:
        rows = tuple(self._metric_rows.get((graph_id, run_id), {}).values())
        if not rows and not self.contains_run(graph_id=graph_id, run_id=run_id):
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
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

    def list_invariants(
        self,
        *,
        graph_id: str,
        run_id: str,
    ) -> tuple[dict[str, JsonValue], ...]:
        rows = self._records.get((graph_id, run_id), {})
        if not rows and not self.contains_run(graph_id=graph_id, run_id=run_id):
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
        return tuple(
            row.as_dict()
            for row in sorted(rows.values(), key=lambda row: row.sequence, reverse=True)
            if row.event_type == "invariant.checked"
        )

    def _with_counts(self, run: TelemetryRunRecord) -> TelemetryRunRecord:
        key = (run.graph_id, run.run_id)
        records = tuple(self._records.get(key, {}).values())
        return TelemetryRunRecord(
            graph_id=run.graph_id,
            run_id=run.run_id,
            created_at_ms=run.created_at_ms,
            updated_at_ms=run.updated_at_ms,
            status=run.status,
            success=run.success,
            invocation_name=run.invocation_name,
            project_id=run.project_id,
            project_label=run.project_label,
            environment_name=run.environment_name,
            catalog_entry_id=run.catalog_entry_id,
            catalog_source=run.catalog_source,
            runtime_default_profile_name=run.runtime_default_profile_name,
            runtime_profile_names=run.runtime_profile_names,
            graph=run.graph,
            analysis=run.analysis,
            error_message=run.error_message,
            record_count=len(records),
            span_count=len(self._spans.get(key, {})),
            metric_count=len(self._metric_rows.get(key, {})),
            output_count=sum(1 for record in records if record.event_type == "node.succeeded"),
        )


@dataclass(slots=True, frozen=True)
class ClickHouseConfig:
    endpoint: str
    database: str = "mentalmodel"
    username: str | None = None
    password: str | None = None

    def __post_init__(self) -> None:
        if not self.endpoint:
            raise RemoteContractError("ClickHouseConfig.endpoint cannot be empty.")
        if not self.database:
            raise RemoteContractError("ClickHouseConfig.database cannot be empty.")


class ClickHouseTelemetryStore:
    """ClickHouse-backed telemetry query store."""

    def __init__(self, config: ClickHouseConfig) -> None:
        self._config = config
        self._schema_lock = threading.Lock()
        self._schema_ready = False

    @classmethod
    def from_connection(
        cls,
        *,
        endpoint: str,
        database: str = "mentalmodel",
        username: str | None = None,
        password: str | None = None,
    ) -> ClickHouseTelemetryStore:
        return cls(
            ClickHouseConfig(
                endpoint=endpoint,
                database=database,
                username=username,
                password=password,
            )
        )

    def replace_run(self, run: TelemetryRunRecord) -> None:
        self._ensure_schema()
        self._insert_json_rows(
            table="telemetry_runs",
            rows=(
                {
                    "graph_id": run.graph_id,
                    "run_id": run.run_id,
                    "created_at_ms": run.created_at_ms,
                    "updated_at_ms": run.updated_at_ms,
                    "status": run.status,
                    "success": run.success,
                    "invocation_name": run.invocation_name,
                    "project_id": run.project_id,
                    "project_label": run.project_label,
                    "environment_name": run.environment_name,
                    "catalog_entry_id": run.catalog_entry_id,
                    "catalog_source": run.catalog_source,
                    "runtime_default_profile_name": run.runtime_default_profile_name,
                    "runtime_profile_names": list(run.runtime_profile_names),
                    "graph_json": _json_string(run.graph),
                    "analysis_json": _json_string(run.analysis),
                    "error_message": run.error_message,
                    "version": run.updated_at_ms,
                },
            ),
        )

    def replace_records(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[TelemetryRecordRow],
    ) -> None:
        self._ensure_schema()
        self._execute(
            f"ALTER TABLE {self._table('telemetry_records')} DELETE "
            f"WHERE graph_id = {self._quote(graph_id)} AND run_id = {self._quote(run_id)}"
        )
        self.append_records(rows)

    def append_records(self, rows: Sequence[TelemetryRecordRow]) -> None:
        self._ensure_schema()
        if not rows:
            return
        self._insert_json_rows(
            table="telemetry_records",
            rows=tuple(
                {
                    "graph_id": row.graph_id,
                    "run_id": row.run_id,
                    "record_id": row.record_id,
                    "sequence": row.sequence,
                    "timestamp_ms": row.timestamp_ms,
                    "event_type": row.event_type,
                    "node_id": row.node_id,
                    "frame_id": row.frame_id,
                    "frame_path_json": _json_string(row.frame_path),
                    "payload_json": _json_string(row.payload),
                    "loop_node_id": row.loop_node_id,
                    "iteration_index": row.iteration_index,
                    "invocation_name": row.invocation_name,
                    "runtime_profile_name": row.runtime_profile_name,
                    "version": row.timestamp_ms,
                }
                for row in rows
            ),
        )

    def replace_spans(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[TelemetrySpanRow],
    ) -> None:
        self._ensure_schema()
        self._execute(
            f"ALTER TABLE {self._table('telemetry_spans')} DELETE "
            f"WHERE graph_id = {self._quote(graph_id)} AND run_id = {self._quote(run_id)}"
        )
        self.append_spans(rows)

    def append_spans(self, rows: Sequence[TelemetrySpanRow]) -> None:
        self._ensure_schema()
        if not rows:
            return
        self._insert_json_rows(
            table="telemetry_spans",
            rows=tuple(
                {
                    "graph_id": row.graph_id,
                    "run_id": row.run_id,
                    "span_key": row.span_key,
                    "sequence": row.sequence,
                    "name": row.name,
                    "start_time_ns": row.start_time_ns,
                    "end_time_ns": row.end_time_ns,
                    "frame_id": row.frame_id,
                    "attributes_json": _json_string(row.attributes),
                    "trace_id": row.trace_id,
                    "otel_span_id": row.otel_span_id,
                    "parent_span_id": row.parent_span_id,
                    "loop_node_id": row.loop_node_id,
                    "iteration_index": row.iteration_index,
                    "error_type": row.error_type,
                    "error_message": row.error_message,
                    "node_id": _span_node_id(row),
                    "runtime_profile_name": _runtime_profile_from_span(row.attributes),
                    "version": row.end_time_ns // 1_000_000,
                }
                for row in rows
            ),
        )

    def replace_metric_rows(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[IndexedMetricRow],
    ) -> None:
        self._ensure_schema()
        self._execute(
            f"ALTER TABLE {self._table('telemetry_metric_rows')} DELETE "
            f"WHERE graph_id = {self._quote(graph_id)} AND run_id = {self._quote(run_id)}"
        )
        self.append_metric_rows(graph_id=graph_id, run_id=run_id, rows=rows)

    def append_metric_rows(
        self,
        *,
        graph_id: str,
        run_id: str,
        rows: Sequence[IndexedMetricRow],
    ) -> None:
        self._ensure_schema()
        if not rows:
            return
        self._insert_json_rows(
            table="telemetry_metric_rows",
            rows=tuple(
                {
                    "graph_id": graph_id,
                    "run_id": run_id,
                    "metric_row_key": _metric_row_key(row),
                    "node_id": row.node_id,
                    "path": row.path,
                    "label": row.label,
                    "normalized_label": row.normalized_label,
                    "metric_node_path": row.metric_node_path,
                    "frame_id": row.frame_id,
                    "loop_node_id": row.loop_node_id,
                    "iteration_index": row.iteration_index,
                    "value": row.value,
                    "unit": row.unit,
                    "semantic_kind": row.semantic_kind,
                    "version": 0 if row.iteration_index is None else row.iteration_index,
                }
                for row in rows
            ),
        )

    def append_metric_points(self, rows: Sequence[TelemetryMetricPointRow]) -> None:
        self._ensure_schema()
        if not rows:
            return
        self._insert_json_rows(
            table="telemetry_metric_points",
            rows=tuple(
                {
                    "graph_id": row.graph_id,
                    "run_id": row.run_id,
                    "point_key": row.point_key,
                    "metric_name": row.metric_name,
                    "metric_kind": row.metric_kind,
                    "value": row.value,
                    "unit": row.unit,
                    "timestamp_unix_ns": row.timestamp_unix_ns,
                    "node_id": row.node_id,
                    "frame_id": row.frame_id,
                    "loop_node_id": row.loop_node_id,
                    "iteration_index": row.iteration_index,
                    "invocation_name": row.invocation_name,
                    "runtime_profile_name": row.runtime_profile_name,
                    "attributes_json": _json_string(row.attributes),
                    "version": (
                        0 if row.timestamp_unix_ns is None else row.timestamp_unix_ns // 1_000_000
                    ),
                }
                for row in rows
            ),
        )

    def contains_run(self, *, graph_id: str, run_id: str) -> bool:
        self._ensure_schema()
        count = self._query_count(
            f"""
            SELECT count() AS count
            FROM {self._table('telemetry_runs')} FINAL
            WHERE graph_id = {self._quote(graph_id)} AND run_id = {self._quote(run_id)}
            """
        )
        return count > 0

    def list_runs(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[TelemetryRunRecord, ...]:
        self._ensure_schema()
        where = ["1"]
        if graph_id is not None:
            where.append(f"graph_id = {self._quote(graph_id)}")
        if invocation_name is not None:
            where.append(f"invocation_name = {self._quote(invocation_name)}")
        rows = self._query_rows(
            f"""
            SELECT
              graph_id,
              run_id,
              created_at_ms,
              updated_at_ms,
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
              graph_json,
              analysis_json,
              error_message,
              (
                SELECT count()
                FROM {self._table('telemetry_records')} FINAL AS records
                WHERE records.graph_id = runs.graph_id AND records.run_id = runs.run_id
              ) AS record_count,
              (
                SELECT count()
                FROM {self._table('telemetry_spans')} FINAL AS spans
                WHERE spans.graph_id = runs.graph_id AND spans.run_id = runs.run_id
              ) AS span_count,
              (
                SELECT count()
                FROM {self._table('telemetry_metric_rows')} FINAL AS metrics
                WHERE metrics.graph_id = runs.graph_id AND metrics.run_id = runs.run_id
              ) AS metric_count,
              (
                SELECT count()
                FROM {self._table('telemetry_records')} FINAL AS outputs
                WHERE outputs.graph_id = runs.graph_id
                  AND outputs.run_id = runs.run_id
                  AND outputs.event_type = 'node.succeeded'
              ) AS output_count
            FROM {self._table('telemetry_runs')} FINAL AS runs
            WHERE {' AND '.join(where)}
            ORDER BY created_at_ms DESC, run_id DESC
            """
        )
        return tuple(_run_record_from_ch_row(row) for row in rows)

    def get_run(self, *, graph_id: str, run_id: str) -> TelemetryRunRecord:
        self._ensure_schema()
        rows = self._query_rows(
            f"""
            SELECT
              graph_id,
              run_id,
              created_at_ms,
              updated_at_ms,
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
              graph_json,
              analysis_json,
              error_message,
              (
                SELECT count()
                FROM {self._table('telemetry_records')} FINAL AS records
                WHERE records.graph_id = runs.graph_id AND records.run_id = runs.run_id
              ) AS record_count,
              (
                SELECT count()
                FROM {self._table('telemetry_spans')} FINAL AS spans
                WHERE spans.graph_id = runs.graph_id AND spans.run_id = runs.run_id
              ) AS span_count,
              (
                SELECT count()
                FROM {self._table('telemetry_metric_rows')} FINAL AS metrics
                WHERE metrics.graph_id = runs.graph_id AND metrics.run_id = runs.run_id
              ) AS metric_count,
              (
                SELECT count()
                FROM {self._table('telemetry_records')} FINAL AS outputs
                WHERE outputs.graph_id = runs.graph_id
                  AND outputs.run_id = runs.run_id
                  AND outputs.event_type = 'node.succeeded'
              ) AS output_count
            FROM {self._table('telemetry_runs')} FINAL AS runs
            WHERE graph_id = {self._quote(graph_id)} AND run_id = {self._quote(run_id)}
            LIMIT 1
            """
        )
        if not rows:
            raise RunInspectionError(f"Remote run {graph_id}/{run_id} was not found.")
        return _run_record_from_ch_row(rows[0])

    def get_records_page(
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
        self._ensure_schema()
        before = decode_sequence_cursor(cursor)
        filters = [
            f"graph_id = {self._quote(graph_id)}",
            f"run_id = {self._quote(run_id)}",
        ]
        if node_id is not None:
            filters.append(f"node_id = {self._quote(node_id)}")
        if frame_id is not None:
            filters.append(f"frame_id = {self._quote(frame_id)}")
        if before is not None:
            filters.append(f"sequence < {before}")
        payload_field = "payload_json" if include_payload else "'' AS payload_json"
        rows = self._query_rows(
            f"""
            SELECT
              record_id,
              run_id,
              node_id,
              frame_id,
              frame_path_json,
              loop_node_id,
              iteration_index,
              event_type,
              sequence,
              timestamp_ms,
              {payload_field}
            FROM {self._table('telemetry_records')} FINAL
            WHERE {' AND '.join(filters)}
            ORDER BY sequence DESC
            LIMIT {limit + 1}
            """
        )
        count = self._query_count(
            f"""
            SELECT count() AS count
            FROM {self._table('telemetry_records')} FINAL
            WHERE graph_id = {self._quote(graph_id)} AND run_id = {self._quote(run_id)}
            """
        )
        items = tuple(
            _record_row_from_ch_row(row, include_payload=include_payload) for row in rows
        )
        page_items = items[:limit]
        next_cursor = None
        if len(items) > limit and page_items:
            next_cursor = encode_sequence_cursor(_required_int(page_items[-1], "sequence"))
        return PageSlice(
            items=page_items,
            next_cursor=next_cursor,
            total_count=count,
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
        self._ensure_schema()
        before = decode_sequence_cursor(cursor)
        filters = [
            f"graph_id = {self._quote(graph_id)}",
            f"run_id = {self._quote(run_id)}",
        ]
        if node_id is not None:
            filters.append(f"node_id = {self._quote(node_id)}")
        if frame_id is not None:
            filters.append(f"frame_id = {self._quote(frame_id)}")
        if before is not None:
            filters.append(f"sequence < {before}")
        rows = self._query_rows(
            f"""
            SELECT
              span_key,
              sequence,
              name,
              start_time_ns,
              end_time_ns,
              frame_id,
              loop_node_id,
              iteration_index,
              error_type,
              error_message,
              trace_id,
              otel_span_id,
              parent_span_id,
              attributes_json
            FROM {self._table('telemetry_spans')} FINAL
            WHERE {' AND '.join(filters)}
            ORDER BY sequence DESC
            LIMIT {limit + 1}
            """
        )
        count = self._query_count(
            f"""
            SELECT count() AS count
            FROM {self._table('telemetry_spans')} FINAL
            WHERE graph_id = {self._quote(graph_id)} AND run_id = {self._quote(run_id)}
            """
        )
        items = tuple(_span_row_from_ch_row(row) for row in rows)
        page_items = items[:limit]
        next_cursor = None
        if len(items) > limit and page_items:
            next_cursor = encode_sequence_cursor(_required_int(page_items[-1], "sequence"))
        return PageSlice(
            items=page_items,
            next_cursor=next_cursor,
            total_count=count,
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
        self._ensure_schema()
        run_filter = (
            f"records.run_id = {self._quote(run_id)}"
            if run_id is not None
            else f"runs.invocation_name = {self._quote(invocation_name)}"
        )
        node_filter = "" if node_id is None else f" AND records.node_id = {self._quote(node_id)}"
        rows = self._query_rows(
            f"""
            SELECT
              intDiv(records.timestamp_ms - {since_ms}, {rollup_ms}) AS bucket_index,
              count() AS record_count,
              countIf(records.iteration_index IS NOT NULL) AS loop_count,
              uniqExact(records.node_id) AS node_count
            FROM {self._table('telemetry_records')} FINAL AS records
            INNER JOIN {self._table('telemetry_runs')} FINAL AS runs
              ON records.graph_id = runs.graph_id AND records.run_id = runs.run_id
            WHERE records.graph_id = {self._quote(graph_id)}
              AND {run_filter}
              AND records.timestamp_ms >= {since_ms}
              AND records.timestamp_ms < {until_ms}
              {node_filter}
            GROUP BY bucket_index
            ORDER BY bucket_index ASC
            """
        )
        return tuple(
            (
                _required_int(row, "bucket_index"),
                _required_int(row, "record_count"),
                _required_int(row, "loop_count"),
                _required_int(row, "node_count"),
            )
            for row in rows
        )

    def list_metrics(
        self,
        *,
        graph_id: str,
        run_id: str,
        step_start: int | None = None,
        step_end: int | None = None,
        node_id: str | None = None,
        frame_id: str | None = None,
        path_prefixes: Sequence[str] = (),
    ) -> tuple[IndexedMetricRow, ...]:
        self._ensure_schema()
        filters = [
            f"graph_id = {self._quote(graph_id)}",
            f"run_id = {self._quote(run_id)}",
        ]
        if node_id is not None:
            filters.append(f"node_id = {self._quote(node_id)}")
        if frame_id is not None:
            filters.append(f"frame_id = {self._quote(frame_id)}")
        if step_start is not None:
            filters.append(f"(iteration_index IS NULL OR iteration_index >= {step_start})")
        if step_end is not None:
            filters.append(f"(iteration_index IS NULL OR iteration_index <= {step_end})")
        if path_prefixes:
            prefix_clauses = []
            for prefix in path_prefixes:
                quoted = self._quote(f"{prefix}%")
                prefix_clauses.extend(
                    [
                        f"path LIKE {quoted}",
                        f"metric_node_path LIKE {quoted}",
                        f"label LIKE {quoted}",
                        f"normalized_label LIKE {quoted}",
                    ]
                )
            filters.append(f"({' OR '.join(prefix_clauses)})")
        rows = self._query_rows(
            f"""
            SELECT
              node_id,
              path,
              label,
              normalized_label,
              metric_node_path,
              frame_id,
              loop_node_id,
              iteration_index,
              value,
              unit,
              semantic_kind
            FROM {self._table('telemetry_metric_rows')} FINAL
            WHERE {' AND '.join(filters)}
            ORDER BY node_id ASC, path ASC, coalesce(iteration_index, -1) ASC
            """
        )
        return tuple(_indexed_metric_row_from_ch_row(row) for row in rows)

    def list_invariants(
        self,
        *,
        graph_id: str,
        run_id: str,
    ) -> tuple[dict[str, JsonValue], ...]:
        self._ensure_schema()
        rows = self._query_rows(
            f"""
            SELECT
              record_id,
              run_id,
              node_id,
              frame_id,
              frame_path_json,
              loop_node_id,
              iteration_index,
              event_type,
              sequence,
              timestamp_ms,
              payload_json
            FROM {self._table('telemetry_records')} FINAL
            WHERE graph_id = {self._quote(graph_id)}
              AND run_id = {self._quote(run_id)}
              AND event_type = 'invariant.checked'
            ORDER BY sequence DESC
            """
        )
        return tuple(_record_row_from_ch_row(row, include_payload=True) for row in rows)

    def _ensure_schema(self) -> None:
        with self._schema_lock:
            if self._schema_ready:
                return
            for statement in _CLICKHOUSE_DDL:
                self._execute(statement)
            self._schema_ready = True

    def _insert_json_rows(self, *, table: str, rows: Sequence[Mapping[str, object]]) -> None:
        encoded = "\n".join(json.dumps(dict(row), sort_keys=True) for row in rows)
        if encoded:
            encoded += "\n"
        self._execute(
            f"INSERT INTO {self._table(table)} FORMAT JSONEachRow",
            body=encoded.encode("utf-8"),
            content_type="application/x-ndjson",
        )

    def _query_rows(self, query: str) -> tuple[dict[str, object], ...]:
        raw = self._execute(f"{query}\nFORMAT JSON")
        decoded = json.loads(raw.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise RemoteContractError("ClickHouse query did not return a JSON object.")
        data = decoded.get("data")
        if not isinstance(data, list):
            raise RemoteContractError("ClickHouse JSON response is missing data rows.")
        rows: list[dict[str, object]] = []
        for item in data:
            if not isinstance(item, dict):
                raise RemoteContractError("ClickHouse JSON row must be an object.")
            rows.append({str(key): value for key, value in item.items()})
        return tuple(rows)

    def _query_count(self, query: str) -> int:
        rows = self._query_rows(query)
        if not rows:
            return 0
        return _required_int(rows[0], "count")

    def _execute(
        self,
        query: str,
        *,
        body: bytes | None = None,
        content_type: str = "text/plain; charset=utf-8",
    ) -> bytes:
        url = _clickhouse_query_url(self._config)
        payload = query.encode("utf-8") if body is None else query.encode("utf-8") + b"\n" + body
        request = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": content_type,
                **_clickhouse_auth_headers(self._config),
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return cast(bytes, response.read())

    def _table(self, name: str) -> str:
        return f"`{self._config.database}`.`{name}`"

    def _quote(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _page_from_sequence_rows(
    *,
    rows: Sequence[dict[str, JsonValue]],
    cursor: str | None,
    limit: int,
) -> PageSlice[dict[str, JsonValue]]:
    if limit < 1:
        raise ValueError("Page size limit must be positive.")
    before = decode_sequence_cursor(cursor)
    ordered = sorted(rows, key=lambda row: _required_int(row, "sequence"), reverse=True)
    if before is not None:
        ordered = [row for row in ordered if _required_int(row, "sequence") < before]
    page_items = tuple(ordered[:limit])
    next_cursor = None
    if len(ordered) > limit and page_items:
        next_cursor = encode_sequence_cursor(_required_int(page_items[-1], "sequence"))
    return PageSlice(items=page_items, next_cursor=next_cursor, total_count=len(rows))


def _metric_row_key(row: IndexedMetricRow) -> str:
    return "::".join(
        [
            row.node_id,
            row.path,
            row.frame_id or "root",
            row.loop_node_id or "none",
            "" if row.iteration_index is None else str(row.iteration_index),
        ]
    )


def _span_node_id(row: TelemetrySpanRow) -> str | None:
    value = row.attributes.get("mentalmodel.node_id")
    return value if isinstance(value, str) else None


def _runtime_profile_from_span(attributes: Mapping[str, JsonValue]) -> str | None:
    value = attributes.get("mentalmodel.runtime_profile")
    return value if isinstance(value, str) else None


def _json_string(value: JsonValue | dict[str, JsonValue] | None) -> str:
    if value is None:
        return "{}"
    return json.dumps(value, sort_keys=True)


def _json_value(value: object) -> JsonValue:
    encoded = json.dumps(value, sort_keys=True)
    return cast(JsonValue, json.loads(encoded))


def _run_record_from_ch_row(row: Mapping[str, object]) -> TelemetryRunRecord:
    runtime_profiles_value = row.get("runtime_profile_names", [])
    runtime_profiles = (
        runtime_profiles_value if isinstance(runtime_profiles_value, list) else []
    )
    return TelemetryRunRecord(
        graph_id=cast(str, row["graph_id"]),
        run_id=cast(str, row["run_id"]),
        created_at_ms=_required_int(row, "created_at_ms"),
        updated_at_ms=_required_int(row, "updated_at_ms"),
        status=cast(str, row["status"]),
        success=cast(bool | None, row["success"]),
        invocation_name=cast(str | None, row.get("invocation_name")),
        project_id=cast(str | None, row.get("project_id")),
        project_label=cast(str | None, row.get("project_label")),
        environment_name=cast(str | None, row.get("environment_name")),
        catalog_entry_id=cast(str | None, row.get("catalog_entry_id")),
        catalog_source=cast(str | None, row.get("catalog_source")),
        runtime_default_profile_name=cast(str | None, row.get("runtime_default_profile_name")),
        runtime_profile_names=tuple(
            item for item in runtime_profiles if isinstance(item, str)
        ),
        graph=_decoded_json_object_field(row.get("graph_json")),
        analysis=_decoded_json_object_field(row.get("analysis_json")),
        error_message=cast(str | None, row.get("error_message")),
        record_count=_optional_int_value(row.get("record_count")),
        span_count=_optional_int_value(row.get("span_count")),
        metric_count=_optional_int_value(row.get("metric_count")),
        output_count=_optional_int_value(row.get("output_count")),
    )


def _record_row_from_ch_row(
    row: Mapping[str, object],
    *,
    include_payload: bool,
) -> dict[str, JsonValue]:
    payload: dict[str, JsonValue] = {
        "record_id": cast(str, row["record_id"]),
        "run_id": cast(str, row["run_id"]),
        "node_id": cast(str, row["node_id"]),
        "frame_id": cast(str, row["frame_id"]),
        "frame_path": _decoded_json_field(row.get("frame_path_json"), default=[]),
        "loop_node_id": cast(str | None, row.get("loop_node_id")),
        "iteration_index": cast(int | None, row.get("iteration_index")),
        "event_type": cast(str, row["event_type"]),
        "sequence": _required_int(row, "sequence"),
        "timestamp_ms": _required_int(row, "timestamp_ms"),
    }
    if include_payload:
        payload["payload"] = _decoded_json_field(row.get("payload_json"), default={})
    return payload


def _span_row_from_ch_row(row: Mapping[str, object]) -> dict[str, JsonValue]:
    attributes = _decoded_json_field(row.get("attributes_json"), default={})
    payload: dict[str, JsonValue] = {
        "span_id": cast(str, row["span_key"]),
        "sequence": _required_int(row, "sequence"),
        "name": cast(str, row["name"]),
        "start_time_ns": _required_int(row, "start_time_ns"),
        "end_time_ns": _required_int(row, "end_time_ns"),
        "duration_ns": max(
            0,
            _required_int(row, "end_time_ns") - _required_int(row, "start_time_ns"),
        ),
        "attributes": attributes,
        "frame_id": cast(str, row["frame_id"]),
        "loop_node_id": cast(str | None, row.get("loop_node_id")),
        "iteration_index": cast(int | None, row.get("iteration_index")),
        "error_type": cast(str | None, row.get("error_type")),
        "error_message": cast(str | None, row.get("error_message")),
    }
    if row.get("trace_id") is not None:
        payload["trace_id"] = cast(str, row["trace_id"])
    if row.get("otel_span_id") is not None:
        payload["otel_span_id"] = cast(str, row["otel_span_id"])
    if row.get("parent_span_id") is not None:
        payload["parent_span_id"] = cast(str, row["parent_span_id"])
    return payload


def _indexed_metric_row_from_ch_row(row: Mapping[str, object]) -> IndexedMetricRow:
    return IndexedMetricRow(
        node_id=cast(str, row["node_id"]),
        path=cast(str, row["path"]),
        label=cast(str, row["label"]),
        normalized_label=cast(str, row["normalized_label"]),
        metric_node_path=cast(str, row["metric_node_path"]),
        frame_id=cast(str | None, row.get("frame_id")),
        loop_node_id=cast(str | None, row.get("loop_node_id")),
        iteration_index=cast(int | None, row.get("iteration_index")),
        value=float(cast(int | float | str, row["value"])),
        unit=cast(str, row["unit"]),
        semantic_kind=cast(str, row["semantic_kind"]),
    )


def _required_int(row: Mapping[str, object], key: str) -> int:
    value = row.get(key)
    if isinstance(value, bool):
        raise RemoteContractError(f"{key} must be an integer value.")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    raise RemoteContractError(f"{key} must be an integer value.")


def _optional_int_value(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        raise RemoteContractError("Expected integer-compatible count value.")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    raise RemoteContractError("Expected integer-compatible count value.")


def _decoded_json_field(value: object, *, default: JsonValue | None = None) -> JsonValue:
    if value in (None, ""):
        return {} if default is None else default
    if isinstance(value, str):
        decoded = json.loads(value)
        return _json_value(decoded)
    return _json_value(value)


def _decoded_json_object_field(value: object) -> dict[str, JsonValue] | None:
    decoded = _decoded_json_field(value)
    return decoded if isinstance(decoded, dict) else None


def _clickhouse_query_url(config: ClickHouseConfig) -> str:
    parsed = urllib.parse.urlparse(config.endpoint)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    query["database"] = [config.database]
    encoded_query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=encoded_query))


def _clickhouse_auth_headers(config: ClickHouseConfig) -> dict[str, str]:
    if config.username is None:
        return {}
    token = base64.b64encode(
        f"{config.username}:{config.password or ''}".encode()
    ).decode("ascii")
    return {"Authorization": f"Basic {token}"}


_CLICKHOUSE_DDL: tuple[str, ...] = (
    """
    CREATE DATABASE IF NOT EXISTS mentalmodel
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS mentalmodel.telemetry_runs (
        graph_id String,
        run_id String,
        created_at_ms Int64,
        updated_at_ms Int64,
        status String,
        success Nullable(Bool),
        invocation_name Nullable(String),
        project_id Nullable(String),
        project_label Nullable(String),
        environment_name Nullable(String),
        catalog_entry_id Nullable(String),
        catalog_source Nullable(String),
        runtime_default_profile_name Nullable(String),
        runtime_profile_names Array(String),
        graph_json String,
        analysis_json String,
        error_message Nullable(String),
        version Int64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (graph_id, run_id)
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS mentalmodel.telemetry_records (
        graph_id String,
        run_id String,
        record_id String,
        sequence Int64,
        timestamp_ms Int64,
        event_type String,
        node_id String,
        frame_id String,
        frame_path_json String,
        payload_json String,
        loop_node_id Nullable(String),
        iteration_index Nullable(Int64),
        invocation_name Nullable(String),
        runtime_profile_name Nullable(String),
        version Int64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (graph_id, run_id, record_id)
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS mentalmodel.telemetry_spans (
        graph_id String,
        run_id String,
        span_key String,
        sequence Int64,
        name String,
        start_time_ns Int64,
        end_time_ns Int64,
        frame_id String,
        node_id Nullable(String),
        loop_node_id Nullable(String),
        iteration_index Nullable(Int64),
        trace_id Nullable(String),
        otel_span_id Nullable(String),
        parent_span_id Nullable(String),
        error_type Nullable(String),
        error_message Nullable(String),
        runtime_profile_name Nullable(String),
        attributes_json String,
        version Int64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (graph_id, run_id, span_key)
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS mentalmodel.telemetry_metric_rows (
        graph_id String,
        run_id String,
        metric_row_key String,
        node_id String,
        path String,
        label String,
        normalized_label String,
        metric_node_path String,
        frame_id Nullable(String),
        loop_node_id Nullable(String),
        iteration_index Nullable(Int64),
        value Float64,
        unit String,
        semantic_kind String,
        version Int64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (graph_id, run_id, metric_row_key)
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS mentalmodel.telemetry_metric_points (
        graph_id String,
        run_id String,
        point_key String,
        metric_name String,
        metric_kind String,
        value Float64,
        unit Nullable(String),
        timestamp_unix_ns Nullable(Int64),
        node_id Nullable(String),
        frame_id Nullable(String),
        loop_node_id Nullable(String),
        iteration_index Nullable(Int64),
        invocation_name Nullable(String),
        runtime_profile_name Nullable(String),
        attributes_json String,
        version Int64
    ) ENGINE = ReplacingMergeTree(version)
    ORDER BY (graph_id, run_id, point_key)
    """.strip(),
)
