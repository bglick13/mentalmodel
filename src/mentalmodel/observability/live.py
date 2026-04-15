from __future__ import annotations

import json
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, InstrumentationScope, KeyValue
from opentelemetry.proto.metrics.v1 import metrics_pb2
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

from mentalmodel.analysis import AnalysisReport
from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import LiveDeliveryCapacityError, LiveIngestionError
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.ir.serialization import ir_graph_to_json
from mentalmodel.observability.config import TracingConfig, TracingMode, load_tracing_config
from mentalmodel.observability.metrics import MetricKind, MetricObservation
from mentalmodel.observability.semantic_conventions import (
    EVENT_TYPE,
    GRAPH_ID,
    RUN_ID,
    TelemetryAttributeValue,
)
from mentalmodel.observability.telemetry import (
    OtelLogRecord,
    OtelMetric,
    OtelMetricPoint,
    OtelSpan,
    TelemetryMapper,
    TelemetryResourceContext,
)
from mentalmodel.observability.tracing import RecordedSpan
from mentalmodel.remote.sinks import LiveExecutionPublishResult, LiveExecutionSink

_OUTBOX_SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    available_at_ms INTEGER NOT NULL,
    claimed_by TEXT,
    lease_expires_at_ms INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS telemetry_outbox_available_idx
ON telemetry_outbox (available_at_ms, lease_expires_at_ms, id);
CREATE INDEX IF NOT EXISTS telemetry_outbox_claimed_idx
ON telemetry_outbox (claimed_by);
"""

_INSTRUMENTATION_SCOPE = InstrumentationScope(name="mentalmodel.live_ingestion")
_LEASE_DURATION_MS = 15_000
_REQUEST_TIMEOUT_SECONDS = 10


EnvelopeKind = Literal[
    "semantic_log",
    "trace_span",
    "metric_point",
    "run_lifecycle",
    "delivery_health",
]


@dataclass(slots=True, frozen=True)
class LiveIngestionConfig:
    """Producer-side config for durable async live telemetry export."""

    otlp_endpoint: str
    outbox_dir: Path
    max_outbox_bytes: int = 64 * 1024 * 1024
    max_batch_events: int = 256
    max_batch_bytes: int = 512 * 1024
    flush_interval_ms: int = 1_000
    shutdown_flush_timeout_ms: int = 5_000
    require_live_delivery: bool = False
    otlp_headers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.otlp_endpoint:
            raise LiveIngestionError("LiveIngestionConfig.otlp_endpoint cannot be empty.")
        if self.max_outbox_bytes < 1:
            raise LiveIngestionError(
                "LiveIngestionConfig.max_outbox_bytes must be positive."
            )
        if self.max_batch_events < 1:
            raise LiveIngestionError(
                "LiveIngestionConfig.max_batch_events must be positive."
            )
        if self.max_batch_bytes < 1:
            raise LiveIngestionError(
                "LiveIngestionConfig.max_batch_bytes must be positive."
            )
        if self.flush_interval_ms < 1:
            raise LiveIngestionError(
                "LiveIngestionConfig.flush_interval_ms must be positive."
            )
        if self.shutdown_flush_timeout_ms < 1:
            raise LiveIngestionError(
                "LiveIngestionConfig.shutdown_flush_timeout_ms must be positive."
            )


@dataclass(slots=True, frozen=True)
class OutboxStats:
    depth: int
    bytes: int
    oldest_event_age_ms: int | None


@dataclass(slots=True, frozen=True)
class LiveDeliveryStats:
    accepted_log_count: int = 0
    accepted_span_count: int = 0
    accepted_metric_count: int = 0
    exported_log_count: int = 0
    exported_span_count: int = 0
    exported_metric_count: int = 0
    retry_count: int = 0
    last_batch_size: int = 0
    last_batch_latency_ms: int | None = None
    degraded: bool = False
    failed_open: bool = False
    accepting_events: bool = True
    last_error: str | None = None


@dataclass(slots=True, frozen=True)
class TelemetryEnvelope:
    kind: EnvelopeKind
    payload: dict[str, JsonValue]
    created_at_ms: int

    def to_json(self) -> str:
        return json.dumps(
            {
                "kind": self.kind,
                "payload": self.payload,
                "created_at_ms": self.created_at_ms,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, raw: str) -> TelemetryEnvelope:
        decoded = json.loads(raw)
        if not isinstance(decoded, dict):
            raise LiveIngestionError("TelemetryEnvelope payload must decode to an object.")
        kind = decoded.get("kind")
        payload = decoded.get("payload")
        created_at_ms = decoded.get("created_at_ms")
        if not isinstance(kind, str):
            raise LiveIngestionError("TelemetryEnvelope.kind must be a string.")
        if not isinstance(payload, dict):
            raise LiveIngestionError("TelemetryEnvelope.payload must be an object.")
        if not isinstance(created_at_ms, int):
            raise LiveIngestionError("TelemetryEnvelope.created_at_ms must be an integer.")
        if kind not in {
            "semantic_log",
            "trace_span",
            "metric_point",
            "run_lifecycle",
            "delivery_health",
        }:
            raise LiveIngestionError(f"Unsupported telemetry envelope kind {kind!r}.")
        return cls(
            kind=cast(EnvelopeKind, kind),
            payload=payload,
            created_at_ms=created_at_ms,
        )


@dataclass(slots=True, frozen=True)
class ClaimedEnvelope:
    id: int
    attempt_count: int
    envelope: TelemetryEnvelope
    size_bytes: int
    created_at_ms: int


@dataclass(slots=True, frozen=True)
class ClaimedBatch:
    token: str
    envelopes: tuple[ClaimedEnvelope, ...]
    total_bytes: int


class DurableOutbox:
    """SQLite-backed durable queue with lease-based replay semantics."""

    def __init__(
        self,
        *,
        root: Path,
        max_bytes: int,
    ) -> None:
        self._root = root.expanduser().resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        self._path = self._root / "live-outbox.sqlite3"
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._initialize()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, envelopes: Sequence[TelemetryEnvelope]) -> OutboxStats:
        if not envelopes:
            return self.stats()
        encoded = tuple(envelope.to_json() for envelope in envelopes)
        sizes = tuple(len(item.encode("utf-8")) for item in encoded)
        with self._lock:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current_bytes = _query_int(
                    connection,
                    "SELECT COALESCE(SUM(size_bytes), 0) FROM telemetry_outbox",
                )
                projected_bytes = current_bytes + sum(sizes)
                if projected_bytes > self._max_bytes:
                    raise LiveDeliveryCapacityError(
                        "Durable live telemetry outbox exceeded its configured hard cap."
                    )
                rows = [
                    (
                        envelope.kind,
                        payload,
                        size_bytes,
                        envelope.created_at_ms,
                        envelope.created_at_ms,
                    )
                    for envelope, payload, size_bytes in zip(
                        envelopes,
                        encoded,
                        sizes,
                        strict=True,
                    )
                ]
                connection.executemany(
                    """
                    INSERT INTO telemetry_outbox (
                        kind,
                        payload_json,
                        size_bytes,
                        created_at_ms,
                        available_at_ms
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                connection.commit()
                return self._stats(connection)

    def claim_batch(
        self,
        *,
        max_events: int,
        max_bytes: int,
        now_ms: int | None = None,
        lease_ms: int = _LEASE_DURATION_MS,
    ) -> ClaimedBatch | None:
        resolved_now_ms = _now_ms() if now_ms is None else now_ms
        batch_token = uuid4().hex
        with self._lock:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                rows = connection.execute(
                    """
                    SELECT id, payload_json, size_bytes, created_at_ms, attempt_count
                    FROM telemetry_outbox
                    WHERE available_at_ms <= ?
                    AND (claimed_by IS NULL OR lease_expires_at_ms <= ?)
                    ORDER BY id ASC
                    """,
                    (resolved_now_ms, resolved_now_ms),
                ).fetchall()
                claimed_rows: list[tuple[int, str, int, int, int]] = []
                total_bytes = 0
                for row in rows:
                    row_size = int(row[2])
                    if claimed_rows and total_bytes + row_size > max_bytes:
                        break
                    claimed_rows.append(
                        (
                            int(row[0]),
                            str(row[1]),
                            row_size,
                            int(row[3]),
                            int(row[4]),
                        )
                    )
                    total_bytes += row_size
                    if len(claimed_rows) >= max_events:
                        break
                if not claimed_rows:
                    connection.rollback()
                    return None
                ids = tuple(row[0] for row in claimed_rows)
                placeholders = ",".join("?" for _ in ids)
                connection.execute(
                    f"""
                    UPDATE telemetry_outbox
                    SET claimed_by = ?, lease_expires_at_ms = ?, attempt_count = attempt_count + 1
                    WHERE id IN ({placeholders})
                    """,
                    (batch_token, resolved_now_ms + lease_ms, *ids),
                )
                connection.commit()
                return ClaimedBatch(
                    token=batch_token,
                    envelopes=tuple(
                        ClaimedEnvelope(
                            id=row_id,
                            attempt_count=attempt_count + 1,
                            envelope=TelemetryEnvelope.from_json(payload_json),
                            size_bytes=size_bytes,
                            created_at_ms=created_at_ms,
                        )
                        for (
                            row_id,
                            payload_json,
                            size_bytes,
                            created_at_ms,
                            attempt_count,
                        ) in claimed_rows
                    ),
                    total_bytes=total_bytes,
                )

    def acknowledge(self, token: str) -> OutboxStats:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    "DELETE FROM telemetry_outbox WHERE claimed_by = ?",
                    (token,),
                )
                connection.commit()
                return self._stats(connection)

    def retry(self, token: str, *, error: str, now_ms: int | None = None) -> OutboxStats:
        resolved_now_ms = _now_ms() if now_ms is None else now_ms
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE telemetry_outbox
                    SET claimed_by = NULL,
                        lease_expires_at_ms = NULL,
                        available_at_ms = ?,
                        last_error = ?
                    WHERE claimed_by = ?
                    """,
                    (resolved_now_ms, error, token),
                )
                connection.commit()
                return self._stats(connection)

    def stats(self) -> OutboxStats:
        with self._lock:
            with self._connect() as connection:
                return self._stats(connection)

    def _stats(self, connection: sqlite3.Connection) -> OutboxStats:
        row = connection.execute(
            """
            SELECT
                COUNT(*),
                COALESCE(SUM(size_bytes), 0),
                MIN(created_at_ms)
            FROM telemetry_outbox
            """
        ).fetchone()
        if row is None:
            return OutboxStats(depth=0, bytes=0, oldest_event_age_ms=None)
        depth = int(row[0])
        total_bytes = int(row[1])
        oldest_created_at_ms = None if row[2] is None else int(row[2])
        oldest_age_ms = None
        if oldest_created_at_ms is not None:
            oldest_age_ms = max(0, _now_ms() - oldest_created_at_ms)
        return OutboxStats(
            depth=depth,
            bytes=total_bytes,
            oldest_event_age_ms=oldest_age_ms,
        )

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(_OUTBOX_SCHEMA)
            connection.commit()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection


class AsyncLiveExporter(LiveExecutionSink):
    """Durable, non-blocking producer sink for live OTLP telemetry."""

    def __init__(
        self,
        *,
        config: LiveIngestionConfig,
        run_id: str,
        invocation_name: str | None,
        resource_context: TelemetryResourceContext,
        runtime_default_profile_name: str | None = None,
        runtime_profile_names: tuple[str, ...] = (),
        tracing_config: TracingConfig | None = None,
    ) -> None:
        self._config = config
        self._run_id = run_id
        self._invocation_name = invocation_name
        self._base_resource_context = resource_context
        self._runtime_default_profile_name = runtime_default_profile_name
        self._runtime_profile_names = runtime_profile_names
        self._runtime_tracing_config = _live_runtime_tracing_config(
            tracing_config=tracing_config,
        )
        self._outbox = DurableOutbox(
            root=config.outbox_dir,
            max_bytes=config.max_outbox_bytes,
        )
        self._flush_event = threading.Event()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._sender_loop,
            name=f"mentalmodel-live-exporter-{run_id}",
            daemon=True,
        )
        self._lock = threading.Lock()
        self._mapper: TelemetryMapper | None = None
        self._graph_id: str | None = None
        self._stats = LiveDeliveryStats()
        self._completed = False
        self._thread.start()

    def runtime_tracing_config(self) -> TracingConfig | None:
        return self._runtime_tracing_config

    def start(self, *, graph: IRGraph, analysis: AnalysisReport) -> None:
        resource_context = TelemetryResourceContext(
            graph_id=graph.graph_id,
            project_id=self._base_resource_context.project_id,
            project_label=self._base_resource_context.project_label,
            environment_name=self._base_resource_context.environment_name,
            catalog_entry_id=self._base_resource_context.catalog_entry_id,
            catalog_source=self._base_resource_context.catalog_source,
            service_name=self._base_resource_context.service_name,
            service_namespace=self._base_resource_context.service_namespace,
            service_version=self._base_resource_context.service_version,
        )
        self._graph_id = graph.graph_id
        self._mapper = TelemetryMapper(resource_context=resource_context)
        self._append(
            (
                _run_lifecycle_envelope(
                    graph_id=graph.graph_id,
                    run_id=self._run_id,
                    invocation_name=self._invocation_name,
                    event="started",
                    success=None,
                    error=None,
                    runtime_default_profile_name=self._runtime_default_profile_name,
                    runtime_profile_names=self._runtime_profile_names,
                    graph_payload=ir_graph_to_json(graph),
                    analysis_payload=_analysis_payload(analysis),
                ),
            )
        )

    def emit_record(self, record: ExecutionRecord) -> None:
        mapper = self._require_mapper()
        self._append((_semantic_log_envelope(mapper.execution_record_to_log(record)),))
        with self._lock:
            self._stats = _replace_stats(
                self._stats,
                accepted_log_count=self._stats.accepted_log_count + 1,
            )

    def emit_span(self, span: RecordedSpan) -> None:
        mapper = self._require_mapper()
        self._append((_trace_span_envelope(mapper.recorded_span_to_span(span)),))
        with self._lock:
            self._stats = _replace_stats(
                self._stats,
                accepted_span_count=self._stats.accepted_span_count + 1,
            )

    def emit_metrics(self, observations: Sequence[MetricObservation]) -> None:
        mapper = self._require_mapper()
        if not observations:
            return
        timestamp_unix_ns = time.time_ns()
        envelopes = tuple(
            _metric_envelope(
                mapper.metric_observation_to_metric(
                    observation,
                    timestamp_unix_ns=timestamp_unix_ns,
                )
            )
            for observation in observations
        )
        self._append(envelopes)
        with self._lock:
            self._stats = _replace_stats(
                self._stats,
                accepted_metric_count=self._stats.accepted_metric_count + len(observations),
            )

    def complete(self, *, success: bool, error: str | None = None) -> None:
        if self._graph_id is not None:
            self._append(
                (
                    _run_lifecycle_envelope(
                        graph_id=self._graph_id,
                        run_id=self._run_id,
                        invocation_name=self._invocation_name,
                        event="completed",
                        success=success,
                        error=error,
                        runtime_default_profile_name=self._runtime_default_profile_name,
                        runtime_profile_names=self._runtime_profile_names,
                    ),
                )
            )
        self._completed = True
        self._flush_event.set()
        deadline = time.time() + (self._config.shutdown_flush_timeout_ms / 1000)
        while time.time() < deadline:
            try:
                if self._outbox.stats().depth == 0:
                    break
            except sqlite3.OperationalError:
                break
            time.sleep(0.05)
        self._stop_event.set()
        self._flush_event.set()
        self._thread.join(timeout=self._config.shutdown_flush_timeout_ms / 1000)

    def delivery_result(self) -> LiveExecutionPublishResult | None:
        if self._graph_id is None:
            return None
        outbox = self._outbox.stats()
        with self._lock:
            stats = self._stats
        success = (
            not stats.failed_open
            and outbox.depth == 0
            and (stats.last_error is None or not stats.degraded)
        )
        return LiveExecutionPublishResult(
            transport="otlp-http",
            delivery_mode="durable-outbox",
            success=success,
            graph_id=self._graph_id,
            run_id=self._run_id,
            project_id=self._base_resource_context.project_id,
            otlp_endpoint=self._config.otlp_endpoint,
            required=self._config.require_live_delivery,
            accepted_log_count=stats.accepted_log_count,
            accepted_span_count=stats.accepted_span_count,
            accepted_metric_count=stats.accepted_metric_count,
            exported_log_count=stats.exported_log_count,
            exported_span_count=stats.exported_span_count,
            exported_metric_count=stats.exported_metric_count,
            outbox_depth=outbox.depth,
            outbox_bytes=outbox.bytes,
            ack_lag_ms=outbox.oldest_event_age_ms,
            retry_count=stats.retry_count,
            last_batch_size=stats.last_batch_size,
            last_batch_latency_ms=stats.last_batch_latency_ms,
            degraded=stats.degraded,
            failed_open=stats.failed_open,
            accepting_events=stats.accepting_events,
            completed=self._completed,
            last_error=stats.last_error,
        )

    def _append(self, envelopes: Sequence[TelemetryEnvelope]) -> None:
        with self._lock:
            if not self._stats.accepting_events:
                return
        try:
            self._outbox.append(envelopes)
        except LiveDeliveryCapacityError as exc:
            with self._lock:
                if self._config.require_live_delivery:
                    self._stats = _replace_stats(
                        self._stats,
                        last_error=str(exc),
                        degraded=True,
                    )
                    raise
                self._stats = _replace_stats(
                    self._stats,
                    last_error=str(exc),
                    degraded=True,
                    failed_open=True,
                    accepting_events=False,
                )
            return
        self._flush_event.set()

    def _sender_loop(self) -> None:
        while True:
            self._flush_event.wait(self._config.flush_interval_ms / 1000)
            self._flush_event.clear()
            try:
                if self._stop_event.is_set() and self._outbox.stats().depth == 0:
                    return
                batch = self._outbox.claim_batch(
                    max_events=self._config.max_batch_events,
                    max_bytes=self._config.max_batch_bytes,
                )
            except sqlite3.OperationalError as exc:
                if self._stop_event.is_set():
                    return
                with self._lock:
                    self._stats = _replace_stats(
                        self._stats,
                        degraded=True,
                        last_error=str(exc),
                    )
                time.sleep(self._config.flush_interval_ms / 1000)
                continue
            if batch is None:
                if self._stop_event.is_set():
                    return
                continue
            start = time.time()
            try:
                export_counts = _post_otlp_batch(
                    endpoint=self._config.otlp_endpoint,
                    headers=self._config.otlp_headers,
                    envelopes=tuple(claimed.envelope for claimed in batch.envelopes),
                )
            except Exception as exc:  # pragma: no cover - exercised through integration tests
                self._outbox.retry(batch.token, error=str(exc))
                with self._lock:
                    self._stats = _replace_stats(
                        self._stats,
                        retry_count=self._stats.retry_count + 1,
                        degraded=True,
                        last_error=str(exc),
                        last_batch_size=len(batch.envelopes),
                        last_batch_latency_ms=int((time.time() - start) * 1000),
                    )
                continue
            self._outbox.acknowledge(batch.token)
            with self._lock:
                self._stats = _replace_stats(
                    self._stats,
                    exported_log_count=self._stats.exported_log_count + export_counts[0],
                    exported_span_count=self._stats.exported_span_count + export_counts[1],
                    exported_metric_count=self._stats.exported_metric_count + export_counts[2],
                    degraded=False,
                    last_error=None,
                    last_batch_size=len(batch.envelopes),
                    last_batch_latency_ms=int((time.time() - start) * 1000),
                )

    def _require_mapper(self) -> TelemetryMapper:
        mapper = self._mapper
        if mapper is None:
            raise LiveIngestionError("AsyncLiveExporter.start() must be called before emission.")
        return mapper


def _semantic_log_envelope(log: OtelLogRecord) -> TelemetryEnvelope:
    return TelemetryEnvelope(
        kind="semantic_log",
        payload={
            "timestamp_unix_ns": log.timestamp_unix_ns,
            "observed_timestamp_unix_ns": log.observed_timestamp_unix_ns,
            "body": log.body,
            "attributes": _json_attributes(log.attributes),
            "resource_attributes": _json_attributes(log.resource_attributes),
            "severity_text": log.severity_text,
        },
        created_at_ms=_now_ms(),
    )


def _trace_span_envelope(span: OtelSpan) -> TelemetryEnvelope:
    return TelemetryEnvelope(
        kind="trace_span",
        payload={
            "trace_id": span.trace_id,
            "span_id": span.span_id,
            "parent_span_id": span.parent_span_id,
            "name": span.name,
            "start_time_unix_ns": span.start_time_unix_ns,
            "end_time_unix_ns": span.end_time_unix_ns,
            "attributes": _json_attributes(span.attributes),
            "resource_attributes": _json_attributes(span.resource_attributes),
            "error_type": span.error_type,
            "error_message": span.error_message,
        },
        created_at_ms=_now_ms(),
    )


def _metric_envelope(metric: OtelMetric) -> TelemetryEnvelope:
    return TelemetryEnvelope(
        kind="metric_point",
        payload={
            "name": metric.name,
            "kind": metric.kind,
            "description": metric.description,
            "unit": metric.unit,
            "resource_attributes": _json_attributes(metric.resource_attributes),
            "points": [
                {
                    "value": point.value,
                    "attributes": _json_attributes(point.attributes),
                    "timestamp_unix_ns": point.timestamp_unix_ns,
                }
                for point in metric.points
            ],
        },
        created_at_ms=_now_ms(),
    )


def _run_lifecycle_envelope(
    *,
    graph_id: str,
    run_id: str,
    invocation_name: str | None,
    event: str,
    success: bool | None,
    error: str | None,
    runtime_default_profile_name: str | None,
    runtime_profile_names: tuple[str, ...],
    graph_payload: dict[str, JsonValue] | None = None,
    analysis_payload: dict[str, JsonValue] | None = None,
) -> TelemetryEnvelope:
    attributes: dict[str, JsonValue] = {
        GRAPH_ID: graph_id,
        RUN_ID: run_id,
        EVENT_TYPE: "mentalmodel.run.lifecycle",
        "mentalmodel.run.lifecycle.event": event,
    }
    if invocation_name is not None:
        attributes["mentalmodel.invocation_name"] = invocation_name
    if runtime_default_profile_name is not None:
        attributes["mentalmodel.runtime_default_profile"] = runtime_default_profile_name
    if success is not None:
        attributes["mentalmodel.run.success"] = success
    if error is not None:
        attributes["mentalmodel.error_message"] = error
    return TelemetryEnvelope(
        kind="run_lifecycle",
        payload={
            "timestamp_unix_ns": time.time_ns(),
            "body": {
                "summary": f"run {event}",
                "event": event,
                "success": success,
                "error": error,
                "runtime_profile_names": list(runtime_profile_names),
                "graph": graph_payload,
                "analysis": analysis_payload,
            },
            "attributes": attributes,
            "resource_attributes": {GRAPH_ID: graph_id},
            "severity_text": "INFO" if success is not False else "ERROR",
        },
        created_at_ms=_now_ms(),
    )


def _post_otlp_batch(
    *,
    endpoint: str,
    headers: Mapping[str, str],
    envelopes: Sequence[TelemetryEnvelope],
) -> tuple[int, int, int]:
    log_kinds = {"semantic_log", "run_lifecycle", "delivery_health"}
    logs = [
        _envelope_to_log(envelope)
        for envelope in envelopes
        if envelope.kind in log_kinds
    ]
    spans = [
        _envelope_to_span(envelope)
        for envelope in envelopes
        if envelope.kind == "trace_span"
    ]
    metrics = [
        _envelope_to_metric(envelope)
        for envelope in envelopes
        if envelope.kind == "metric_point"
    ]
    if logs:
        _post_protobuf(
            _logs_endpoint(endpoint),
            _serialize_logs(logs),
            headers=headers,
        )
    if spans:
        _post_protobuf(
            _traces_endpoint(endpoint),
            _serialize_spans(spans),
            headers=headers,
        )
    if metrics:
        _post_protobuf(
            _metrics_endpoint(endpoint),
            _serialize_metrics(metrics),
            headers=headers,
        )
    return (len(logs), len(spans), len(metrics))


def _analysis_payload(report: AnalysisReport) -> dict[str, JsonValue]:
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


def _post_protobuf(url: str, payload: bytes, *, headers: Mapping[str, str]) -> None:
    request = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/x-protobuf",
            **dict(headers),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
            if response.status >= 400:
                raise LiveIngestionError(
                    f"OTLP export to {url!r} failed with HTTP {response.status}."
                )
    except urllib.error.HTTPError as exc:
        raise LiveIngestionError(
            f"OTLP export to {url!r} failed with HTTP {exc.code}: {exc.reason}"
        ) from exc
    except urllib.error.URLError as exc:
        raise LiveIngestionError(f"OTLP export to {url!r} failed: {exc.reason}") from exc


def _serialize_logs(logs: Sequence[OtelLogRecord]) -> bytes:
    request = ExportLogsServiceRequest()
    resource_logs = request.resource_logs.add()
    _set_resource_attributes(resource_logs.resource, logs[0].resource_attributes)
    scope_logs = resource_logs.scope_logs.add()
    scope_logs.scope.CopyFrom(_INSTRUMENTATION_SCOPE)
    for log in logs:
        record = scope_logs.log_records.add()
        record.time_unix_nano = log.timestamp_unix_ns
        record.observed_time_unix_nano = log.observed_timestamp_unix_ns
        if log.severity_text is not None:
            record.severity_text = log.severity_text
        record.body.CopyFrom(_any_value(log.body))
        record.attributes.extend(_key_values(log.attributes))
    return bytes(request.SerializeToString())


def _serialize_spans(spans: Sequence[OtelSpan]) -> bytes:
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    _set_resource_attributes(resource_spans.resource, spans[0].resource_attributes)
    scope_spans = resource_spans.scope_spans.add()
    scope_spans.scope.CopyFrom(_INSTRUMENTATION_SCOPE)
    for span in spans:
        message = scope_spans.spans.add()
        message.name = span.name
        message.start_time_unix_nano = span.start_time_unix_ns
        message.end_time_unix_nano = span.end_time_unix_ns
        if span.trace_id is not None:
            message.trace_id = bytes.fromhex(span.trace_id)
        if span.span_id is not None:
            message.span_id = bytes.fromhex(span.span_id)
        if span.parent_span_id is not None:
            message.parent_span_id = bytes.fromhex(span.parent_span_id)
        message.attributes.extend(_key_values(span.attributes))
    return bytes(request.SerializeToString())


def _serialize_metrics(metrics: Sequence[OtelMetric]) -> bytes:
    request = ExportMetricsServiceRequest()
    grouped: dict[tuple[tuple[str, JsonValue], ...], list[OtelMetric]] = {}
    for metric in metrics:
        key = tuple(sorted(metric.resource_attributes.items()))
        grouped.setdefault(key, []).append(metric)
    for _, resource_metrics_payload in grouped.items():
        resource_metrics = request.resource_metrics.add()
        _set_resource_attributes(
            resource_metrics.resource,
            resource_metrics_payload[0].resource_attributes,
        )
        scope_metrics = resource_metrics.scope_metrics.add()
        scope_metrics.scope.CopyFrom(_INSTRUMENTATION_SCOPE)
        for metric in resource_metrics_payload:
            metric_message = scope_metrics.metrics.add()
            metric_message.name = metric.name
            metric_message.description = metric.description
            if metric.unit is not None:
                metric_message.unit = metric.unit
            if metric.kind == MetricKind.COUNTER.value:
                metric_sum = metric_message.sum
                metric_sum.aggregation_temporality = (
                    metrics_pb2.AGGREGATION_TEMPORALITY_CUMULATIVE
                )
                metric_sum.is_monotonic = True
                for point in metric.points:
                    data_point = metric_sum.data_points.add()
                    _set_number_data_point(data_point, point)
            elif metric.kind == "gauge":
                metric_gauge = metric_message.gauge
                for point in metric.points:
                    data_point = metric_gauge.data_points.add()
                    _set_number_data_point(data_point, point)
            else:
                metric_histogram = metric_message.histogram
                metric_histogram.aggregation_temporality = (
                    metrics_pb2.AGGREGATION_TEMPORALITY_DELTA
                )
                for point in metric.points:
                    data_point = metric_histogram.data_points.add()
                    _set_histogram_data_point(data_point, point)
    return bytes(request.SerializeToString())


def _set_number_data_point(message: NumberDataPoint, point: OtelMetricPoint) -> None:
    if point.timestamp_unix_ns is not None:
        message.time_unix_nano = point.timestamp_unix_ns
    message.attributes.extend(_key_values(point.attributes))
    if isinstance(point.value, int) and not isinstance(point.value, bool):
        message.as_int = point.value
    else:
        message.as_double = float(point.value)


def _set_histogram_data_point(message: HistogramDataPoint, point: OtelMetricPoint) -> None:
    if point.timestamp_unix_ns is not None:
        message.time_unix_nano = point.timestamp_unix_ns
    message.attributes.extend(_key_values(point.attributes))
    numeric = float(point.value)
    message.count = 1
    message.sum = numeric
    message.bucket_counts.extend([0, 1])
    message.explicit_bounds.extend([numeric])
    message.min = numeric
    message.max = numeric


def _set_resource_attributes(resource: Resource, attributes: Mapping[str, JsonValue]) -> None:
    resource.attributes.extend(_key_values(attributes))


def _key_values(attributes: Mapping[str, JsonValue]) -> list[KeyValue]:
    values: list[KeyValue] = []
    for key, value in sorted(attributes.items()):
        key_value = KeyValue()
        key_value.key = key
        key_value.value.CopyFrom(_any_value(value))
        values.append(key_value)
    return values


def _any_value(value: JsonValue) -> AnyValue:
    any_value = AnyValue()
    if value is None:
        any_value.string_value = "null"
        return any_value
    if isinstance(value, bool):
        any_value.bool_value = value
        return any_value
    if isinstance(value, int) and not isinstance(value, bool):
        any_value.int_value = value
        return any_value
    if isinstance(value, float):
        any_value.double_value = value
        return any_value
    if isinstance(value, str):
        any_value.string_value = value
        return any_value
    if isinstance(value, list):
        any_value.array_value.values.extend(_any_value(item) for item in value)
        return any_value
    if isinstance(value, dict):
        for item_key, item_value in sorted(value.items()):
            entry = any_value.kvlist_value.values.add()
            entry.key = item_key
            entry.value.CopyFrom(_any_value(item_value))
        return any_value
    any_value.string_value = str(value)
    return any_value


def _envelope_to_log(envelope: TelemetryEnvelope) -> OtelLogRecord:
    payload = envelope.payload
    timestamp_unix_ns = _required_int(payload, "timestamp_unix_ns")
    observed_timestamp_unix_ns = (
        timestamp_unix_ns
        if "observed_timestamp_unix_ns" not in payload
        else _required_int(payload, "observed_timestamp_unix_ns")
    )
    return OtelLogRecord(
        timestamp_unix_ns=timestamp_unix_ns,
        observed_timestamp_unix_ns=observed_timestamp_unix_ns,
        body=_required_json_value(payload, "body"),
        attributes=_coerce_attributes(_required_dict(payload, "attributes")),
        resource_attributes=_coerce_attributes(_required_dict(payload, "resource_attributes")),
        severity_text=_optional_str(payload.get("severity_text")),
    )


def _envelope_to_span(envelope: TelemetryEnvelope) -> OtelSpan:
    payload = envelope.payload
    attributes = _coerce_attributes(payload["attributes"])
    return OtelSpan(
        trace_id=_optional_str(payload.get("trace_id")),
        span_id=_optional_str(payload.get("span_id")),
        parent_span_id=_optional_str(payload.get("parent_span_id")),
        name=_required_str(payload, "name"),
        start_time_unix_ns=_required_int(payload, "start_time_unix_ns"),
        end_time_unix_ns=_required_int(payload, "end_time_unix_ns"),
        attributes=attributes,
        resource_attributes=_coerce_attributes(_required_dict(payload, "resource_attributes")),
        source_attributes=attributes,
        synthetic_span_id=_optional_str(payload.get("span_id")) or uuid4().hex,
        sequence=0,
        error_type=_optional_str(payload.get("error_type")),
        error_message=_optional_str(payload.get("error_message")),
    )


def _envelope_to_metric(envelope: TelemetryEnvelope) -> OtelMetric:
    payload = envelope.payload
    return OtelMetric(
        name=_required_str(payload, "name"),
        kind=_required_str(payload, "kind"),
        description=_optional_str(payload.get("description")) or "",
        unit=_optional_str(payload.get("unit")),
        resource_attributes=_coerce_attributes(_required_dict(payload, "resource_attributes")),
        points=tuple(
            _metric_point_from_payload(point)
            for point in _required_list(payload, "points")
        ),
    )


def _json_attributes(
    attributes: Mapping[str, TelemetryAttributeValue],
) -> dict[str, JsonValue]:
    return {key: value for key, value in attributes.items()}


def _coerce_attributes(payload: object) -> dict[str, TelemetryAttributeValue]:
    if not isinstance(payload, dict):
        raise LiveIngestionError("Telemetry attributes must decode to an object.")
    values: dict[str, TelemetryAttributeValue] = {}
    for key, value in payload.items():
        if isinstance(value, bool):
            values[key] = value
            continue
        if isinstance(value, int):
            values[key] = value
            continue
        if isinstance(value, float):
            values[key] = value
            continue
        if isinstance(value, str):
            values[key] = value
            continue
        raise LiveIngestionError(f"Unsupported telemetry attribute value for {key!r}: {value!r}")
    return values


def _metric_point_from_payload(point: JsonValue) -> OtelMetricPoint:
    payload = _as_dict(point, subject="metric point")
    numeric_value = payload.get("value")
    if isinstance(numeric_value, bool) or not isinstance(numeric_value, (int, float)):
        raise LiveIngestionError("Metric point value must be numeric.")
    return OtelMetricPoint(
        value=numeric_value,
        attributes=_coerce_attributes(_required_dict(payload, "attributes")),
        timestamp_unix_ns=_optional_int(payload, "timestamp_unix_ns", default=None),
    )


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _required_int(payload: Mapping[str, JsonValue], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise LiveIngestionError(f"Expected integer payload field {key!r}.")
    return value


def _optional_int(
    payload: Mapping[str, JsonValue],
    key: str,
    *,
    default: int | None,
) -> int | None:
    value = payload.get(key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise LiveIngestionError(f"Expected integer payload field {key!r}.")
    return value


def _required_str(payload: Mapping[str, JsonValue], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise LiveIngestionError(f"Expected string payload field {key!r}.")
    return value


def _required_dict(
    payload: Mapping[str, JsonValue],
    key: str,
) -> dict[str, JsonValue]:
    return _as_dict(payload.get(key), subject=key)


def _required_list(
    payload: Mapping[str, JsonValue],
    key: str,
) -> list[JsonValue]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise LiveIngestionError(f"Expected list payload field {key!r}.")
    return value


def _required_json_value(payload: Mapping[str, JsonValue], key: str) -> JsonValue:
    if key not in payload:
        raise LiveIngestionError(f"Expected payload field {key!r}.")
    return payload[key]


def _as_dict(value: JsonValue | None, *, subject: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise LiveIngestionError(f"Expected {subject} to decode to an object.")
    return value


def _query_int(connection: sqlite3.Connection, query: str) -> int:
    row = connection.execute(query).fetchone()
    if row is None:
        return 0
    return int(row[0])


def _replace_stats(stats: LiveDeliveryStats, **changes: object) -> LiveDeliveryStats:
    return LiveDeliveryStats(
        accepted_log_count=cast(
            int,
            changes.get("accepted_log_count", stats.accepted_log_count),
        ),
        accepted_span_count=cast(
            int,
            changes.get("accepted_span_count", stats.accepted_span_count),
        ),
        accepted_metric_count=cast(
            int,
            changes.get("accepted_metric_count", stats.accepted_metric_count),
        ),
        exported_log_count=cast(
            int,
            changes.get("exported_log_count", stats.exported_log_count),
        ),
        exported_span_count=cast(
            int,
            changes.get("exported_span_count", stats.exported_span_count),
        ),
        exported_metric_count=cast(
            int,
            changes.get("exported_metric_count", stats.exported_metric_count),
        ),
        retry_count=cast(int, changes.get("retry_count", stats.retry_count)),
        last_batch_size=cast(int, changes.get("last_batch_size", stats.last_batch_size)),
        last_batch_latency_ms=cast(
            int | None,
            changes.get("last_batch_latency_ms", stats.last_batch_latency_ms),
        ),
        degraded=cast(bool, changes.get("degraded", stats.degraded)),
        failed_open=cast(bool, changes.get("failed_open", stats.failed_open)),
        accepting_events=cast(
            bool,
            changes.get("accepting_events", stats.accepting_events),
        ),
        last_error=cast(str | None, changes.get("last_error", stats.last_error)),
    )


def _live_runtime_tracing_config(
    *,
    tracing_config: TracingConfig | None,
) -> TracingConfig:
    base = tracing_config or load_tracing_config()
    return TracingConfig(
        service_name=base.service_name,
        service_namespace=base.service_namespace,
        service_version=base.service_version,
        mode=TracingMode.DISK,
        otlp_endpoint=None,
        otlp_headers={},
        otlp_insecure=False,
        mirror_to_disk=True,
        capture_local_spans=True,
    )


def _logs_endpoint(base: str) -> str:
    return _otlp_endpoint(base, suffix="/v1/logs")


def _traces_endpoint(base: str) -> str:
    return _otlp_endpoint(base, suffix="/v1/traces")


def _metrics_endpoint(base: str) -> str:
    return _otlp_endpoint(base, suffix="/v1/metrics")


def _otlp_endpoint(base: str, *, suffix: str) -> str:
    normalized = base.rstrip("/")
    if (
        normalized.endswith("/v1/logs")
        or normalized.endswith("/v1/traces")
        or normalized.endswith("/v1/metrics")
    ):
        normalized = normalized.rsplit("/", 1)[0]
    return f"{normalized}{suffix}"


def _now_ms() -> int:
    return int(time.time() * 1000)
