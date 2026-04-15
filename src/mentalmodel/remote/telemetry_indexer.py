from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.metrics.v1.metrics_pb2 import Metric, NumberDataPoint

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.observability.dashboard_metrics import (
    metric_rows_from_live_records,
    metric_rows_from_outputs_payload,
)
from mentalmodel.observability.semantic_conventions import (
    CATALOG_ENTRY_ID,
    CATALOG_SOURCE,
    ENVIRONMENT_NAME,
    EVENT_TYPE,
    FRAME_ID,
    GRAPH_ID,
    INVOCATION_NAME,
    ITERATION_INDEX,
    LOOP_NODE_ID,
    NODE_ID,
    PROJECT_ID,
    PROJECT_LABEL,
    RUN_ID,
    RUNTIME_PROFILE,
)
from mentalmodel.observability.telemetry import (
    OtelLogRecord,
    OtelSpan,
    execution_record_json_from_log,
    recorded_span_json_from_span,
)
from mentalmodel.remote.contracts import RunManifest
from mentalmodel.remote.telemetry_store import (
    TelemetryMetricPointRow,
    TelemetryRecordRow,
    TelemetryRunRecord,
    TelemetrySpanRow,
    TelemetryStore,
)

TOPIC_LOGS = "mentalmodel.telemetry.logs"
TOPIC_TRACES = "mentalmodel.telemetry.traces"
TOPIC_METRICS = "mentalmodel.telemetry.metrics"


@dataclass(slots=True, frozen=True)
class TelemetryConsumerConfig:
    brokers: tuple[str, ...]
    group_id: str = "mentalmodel-clickhouse-indexer"
    logs_topic: str = TOPIC_LOGS
    traces_topic: str = TOPIC_TRACES
    metrics_topic: str = TOPIC_METRICS
    poll_timeout_ms: int = 1_000
    max_batch_messages: int = 256

    def __post_init__(self) -> None:
        if not self.brokers:
            raise ValueError("TelemetryConsumerConfig.brokers cannot be empty.")


class TelemetryIndexer:
    """Map completed bundles and OTLP batches into the hosted query model."""

    def __init__(self, store: TelemetryStore) -> None:
        self._store = store

    def index_completed_run(
        self,
        *,
        manifest: RunManifest,
        artifact_map: Mapping[str, bytes],
    ) -> None:
        records = _decode_jsonl_rows(artifact_map.get("records.jsonl", b""))
        spans = _decode_jsonl_rows(artifact_map.get("otel-spans.jsonl", b""))
        outputs_payload = _decode_json_object(artifact_map.get("outputs.json", b"{}"))
        graph_payload = _decode_json_object(artifact_map.get("graph.json", b"{}"))
        verification_payload = _decode_json_object(
            artifact_map.get("verification.json", b"{}")
        )
        success = manifest.success
        error_message = None
        runtime = verification_payload.get("runtime")
        if isinstance(runtime, dict):
            runtime_error = runtime.get("error")
            if isinstance(runtime_error, str):
                error_message = runtime_error
        status = "succeeded" if success else "failed"
        updated_at_ms = manifest.completed_at_ms or manifest.created_at_ms
        self._store.replace_run(
            TelemetryRunRecord(
                graph_id=manifest.graph_id,
                run_id=manifest.run_id,
                created_at_ms=manifest.created_at_ms,
                updated_at_ms=updated_at_ms,
                status=status,
                success=success,
                invocation_name=manifest.invocation_name,
                project_id=manifest.project_id,
                project_label=manifest.project_label,
                environment_name=manifest.environment_name,
                catalog_entry_id=manifest.catalog_entry_id,
                catalog_source=(
                    None if manifest.catalog_source is None else manifest.catalog_source.value
                ),
                runtime_default_profile_name=manifest.runtime_default_profile_name,
                runtime_profile_names=manifest.runtime_profile_names,
                graph=graph_payload,
                analysis=None,
                error_message=error_message,
            )
        )
        self._store.replace_records(
            graph_id=manifest.graph_id,
            run_id=manifest.run_id,
            rows=tuple(
                _telemetry_record_row_from_json(
                    graph_id=manifest.graph_id,
                    run_id=manifest.run_id,
                    row=row,
                    invocation_name=manifest.invocation_name,
                )
                for row in records
            ),
        )
        self._store.replace_spans(
            graph_id=manifest.graph_id,
            run_id=manifest.run_id,
            rows=tuple(
                _telemetry_span_row_from_json(
                    graph_id=manifest.graph_id,
                    run_id=manifest.run_id,
                    row=row,
                )
                for row in spans
            ),
        )
        self._store.replace_metric_rows(
            graph_id=manifest.graph_id,
            run_id=manifest.run_id,
            rows=metric_rows_from_outputs_payload(outputs_payload),
        )

    def index_otlp_logs(self, payload: bytes) -> None:
        request = ExportLogsServiceRequest()
        request.ParseFromString(payload)
        for resource_log in request.resource_logs:
            resource_attributes = _attribute_map(resource_log.resource.attributes)
            for scope_log in resource_log.scope_logs:
                for log_record in scope_log.log_records:
                    self._index_log_record(
                        resource_attributes=resource_attributes,
                        log_record=log_record,
                    )

    def index_otlp_traces(self, payload: bytes) -> None:
        request = ExportTraceServiceRequest()
        request.ParseFromString(payload)
        for resource_span in request.resource_spans:
            resource_attributes = _attribute_map(resource_span.resource.attributes)
            for scope_span in resource_span.scope_spans:
                for span in scope_span.spans:
                    attributes = _attribute_map(span.attributes)
                    identity = _merged_identity(resource_attributes, attributes)
                    graph_id = _required_str(identity, GRAPH_ID)
                    run_id = _required_str(identity, RUN_ID)
                    self._ensure_run(
                        graph_id=graph_id,
                        run_id=run_id,
                        timestamp_ms=span.start_time_unix_nano // 1_000_000,
                        identity=identity,
                    )
                    otel_span = OtelSpan(
                        trace_id=_hex_bytes(span.trace_id),
                        span_id=_hex_bytes(span.span_id),
                        parent_span_id=_hex_bytes(span.parent_span_id),
                        name=span.name,
                        start_time_unix_ns=span.start_time_unix_nano,
                        end_time_unix_ns=span.end_time_unix_nano,
                        attributes=cast(dict[str, str | bool | int | float], attributes),
                        resource_attributes=cast(
                            dict[str, str | bool | int | float], resource_attributes
                        ),
                        source_attributes=cast(
                            dict[str, str | bool | int | float], attributes
                        ),
                        synthetic_span_id=_hex_bytes(span.span_id)
                        or _stable_key(
                            graph_id,
                            run_id,
                            span.name,
                            str(span.start_time_unix_nano),
                            str(span.end_time_unix_nano),
                        ),
                        sequence=_sequence_from_span_attributes(attributes),
                        error_type=_optional_str(attributes, "exception.type"),
                        error_message=_optional_str(attributes, "exception.message"),
                    )
                    span_json = recorded_span_json_from_span(otel_span)
                    self._store.append_spans(
                        (
                            _telemetry_span_row_from_json(
                                graph_id=graph_id,
                                run_id=run_id,
                                row=span_json,
                            ),
                        )
                    )

    def index_otlp_metrics(self, payload: bytes) -> None:
        request = ExportMetricsServiceRequest()
        request.ParseFromString(payload)
        metric_points: list[TelemetryMetricPointRow] = []
        for resource_metric in request.resource_metrics:
            resource_attributes = _attribute_map(resource_metric.resource.attributes)
            for scope_metric in resource_metric.scope_metrics:
                for metric in scope_metric.metrics:
                    rows = _metric_point_rows_from_otel_metric(
                        resource_attributes=resource_attributes,
                        metric=metric,
                    )
                    metric_points.extend(rows)
        if metric_points:
            self._store.append_metric_points(tuple(metric_points))

    def consume_forever(
        self,
        *,
        config: TelemetryConsumerConfig,
        stop_after_idle_ms: int | None = None,
        max_messages: int | None = None,
    ) -> int:
        from kafka import KafkaConsumer  # type: ignore[import-untyped]

        consumer = KafkaConsumer(
            config.logs_topic,
            config.traces_topic,
            config.metrics_topic,
            bootstrap_servers=list(config.brokers),
            group_id=config.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            consumer_timeout_ms=config.poll_timeout_ms,
            value_deserializer=lambda value: cast(bytes, value),
            key_deserializer=lambda value: cast(bytes, value),
        )
        processed = 0
        last_message_at_ms = int(time.time() * 1000)
        try:
            while True:
                batch = consumer.poll(
                    timeout_ms=config.poll_timeout_ms,
                    max_records=config.max_batch_messages,
                )
                if not batch:
                    if (
                        stop_after_idle_ms is not None
                        and int(time.time() * 1000) - last_message_at_ms >= stop_after_idle_ms
                    ):
                        return processed
                    continue
                for records in batch.values():
                    for message in records:
                        processed += 1
                        last_message_at_ms = int(time.time() * 1000)
                        if message.topic == config.logs_topic:
                            self.index_otlp_logs(message.value)
                        elif message.topic == config.traces_topic:
                            self.index_otlp_traces(message.value)
                        elif message.topic == config.metrics_topic:
                            self.index_otlp_metrics(message.value)
                        else:  # pragma: no cover - kafka subscription constrains topics
                            raise ValueError(f"Unsupported telemetry topic {message.topic!r}.")
                        if max_messages is not None and processed >= max_messages:
                            consumer.commit()
                            return processed
                consumer.commit()
        finally:
            consumer.close()

    def _index_log_record(
        self,
        *,
        resource_attributes: Mapping[str, JsonValue],
        log_record: object,
    ) -> None:
        from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord

        typed = cast(LogRecord, log_record)
        attributes = _attribute_map(typed.attributes)
        body = _any_value_to_json(typed.body)
        identity = _merged_identity(resource_attributes, attributes)
        graph_id = _required_str(identity, GRAPH_ID)
        run_id = _required_str(identity, RUN_ID)
        timestamp_ms = typed.time_unix_nano // 1_000_000
        event_type = _optional_str(attributes, EVENT_TYPE)
        self._ensure_run(
            graph_id=graph_id,
            run_id=run_id,
            timestamp_ms=timestamp_ms,
            identity=identity,
        )
        if event_type == "mentalmodel.run.lifecycle":
            self._index_lifecycle_log(
                graph_id=graph_id,
                run_id=run_id,
                timestamp_ms=timestamp_ms,
                attributes=attributes,
                body=body if isinstance(body, dict) else {},
                identity=identity,
            )
            return
        otel_log = OtelLogRecord(
            timestamp_unix_ns=typed.time_unix_nano,
            observed_timestamp_unix_ns=typed.observed_time_unix_nano or typed.time_unix_nano,
            body=body,
            attributes=cast(dict[str, str | bool | int | float], attributes),
            resource_attributes=cast(
                dict[str, str | bool | int | float], resource_attributes
            ),
            severity_text=typed.severity_text or None,
        )
        record_json = execution_record_json_from_log(otel_log)
        record_row = _telemetry_record_row_from_json(
            graph_id=graph_id,
            run_id=run_id,
            row=record_json,
            invocation_name=_optional_str(identity, INVOCATION_NAME),
            runtime_profile_name=_optional_str(identity, RUNTIME_PROFILE),
        )
        self._store.append_records((record_row,))
        metric_rows = metric_rows_from_live_records(
            cast(Sequence[dict[str, object]], (record_json,))
        )
        if metric_rows:
            self._store.append_metric_rows(
                graph_id=graph_id,
                run_id=run_id,
                rows=metric_rows,
            )

    def _index_lifecycle_log(
        self,
        *,
        graph_id: str,
        run_id: str,
        timestamp_ms: int,
        attributes: Mapping[str, JsonValue],
        body: Mapping[str, JsonValue],
        identity: Mapping[str, JsonValue],
    ) -> None:
        event_value = attributes.get("mentalmodel.run.lifecycle.event")
        event = event_value if isinstance(event_value, str) else "started"
        try:
            current = self._store.get_run(graph_id=graph_id, run_id=run_id)
        except Exception:
            current = None
        summary = body.get("summary")
        error_message = attributes.get("mentalmodel.error_message")
        success_value = attributes.get("mentalmodel.run.success")
        success = success_value if isinstance(success_value, bool) else None
        graph_payload = body.get("graph")
        analysis_payload = body.get("analysis")
        runtime_profile_names = body.get("runtime_profile_names")
        run = TelemetryRunRecord(
            graph_id=graph_id,
            run_id=run_id,
            created_at_ms=(
                timestamp_ms
                if current is None
                else min(current.created_at_ms, timestamp_ms)
            ),
            updated_at_ms=timestamp_ms,
            status=(
                "running"
                if event == "started"
                else "succeeded"
                if success is True
                else "failed"
                if success is False
                else (current.status if current is not None else "running")
            ),
            success=(
                success
                if success is not None
                else (None if current is None else current.success)
            ),
            invocation_name=_optional_str(identity, INVOCATION_NAME)
            or (None if current is None else current.invocation_name),
            project_id=_optional_str(identity, PROJECT_ID)
            or (None if current is None else current.project_id),
            project_label=_optional_str(identity, PROJECT_LABEL)
            or (None if current is None else current.project_label),
            environment_name=_optional_str(identity, ENVIRONMENT_NAME)
            or (None if current is None else current.environment_name),
            catalog_entry_id=_optional_str(identity, CATALOG_ENTRY_ID)
            or (None if current is None else current.catalog_entry_id),
            catalog_source=_optional_str(identity, CATALOG_SOURCE)
            or (None if current is None else current.catalog_source),
            runtime_default_profile_name=(
                _optional_str(attributes, "mentalmodel.runtime_default_profile")
                or (None if current is None else current.runtime_default_profile_name)
            ),
            runtime_profile_names=(
                tuple(item for item in runtime_profile_names if isinstance(item, str))
                if isinstance(runtime_profile_names, list)
                else (() if current is None else current.runtime_profile_names)
            ),
            graph=(
                graph_payload
                if isinstance(graph_payload, dict)
                else (None if current is None else current.graph)
            ),
            analysis=(
                analysis_payload
                if isinstance(analysis_payload, dict)
                else (None if current is None else current.analysis)
            ),
            error_message=(
                error_message
                if isinstance(error_message, str)
                else (
                    summary
                    if event == "failed" and isinstance(summary, str)
                    else (None if current is None else current.error_message)
                )
            ),
        )
        self._store.replace_run(run)

    def _ensure_run(
        self,
        *,
        graph_id: str,
        run_id: str,
        timestamp_ms: int,
        identity: Mapping[str, JsonValue],
    ) -> None:
        if self._store.contains_run(graph_id=graph_id, run_id=run_id):
            return
        self._store.replace_run(
            TelemetryRunRecord(
                graph_id=graph_id,
                run_id=run_id,
                created_at_ms=timestamp_ms,
                updated_at_ms=timestamp_ms,
                status="running",
                success=None,
                invocation_name=_optional_str(identity, INVOCATION_NAME),
                project_id=_optional_str(identity, PROJECT_ID),
                project_label=_optional_str(identity, PROJECT_LABEL),
                environment_name=_optional_str(identity, ENVIRONMENT_NAME),
                catalog_entry_id=_optional_str(identity, CATALOG_ENTRY_ID),
                catalog_source=_optional_str(identity, CATALOG_SOURCE),
                runtime_default_profile_name=None,
                runtime_profile_names=(),
                graph=None,
                analysis=None,
                error_message=None,
            )
        )


def _telemetry_record_row_from_json(
    *,
    graph_id: str,
    run_id: str,
    row: Mapping[str, object],
    invocation_name: str | None,
    runtime_profile_name: str | None = None,
) -> TelemetryRecordRow:
    payload = row.get("payload", {})
    return TelemetryRecordRow(
        graph_id=graph_id,
        run_id=run_id,
        record_id=cast(str, row["record_id"]),
        sequence=_required_int(row, "sequence"),
        timestamp_ms=_required_int(row, "timestamp_ms"),
        event_type=cast(str, row["event_type"]),
        node_id=cast(str, row["node_id"]),
        frame_id=cast(str, row["frame_id"]),
        frame_path=_json_value(row.get("frame_path", [])),
        payload=_json_value(payload),
        loop_node_id=cast(str | None, row.get("loop_node_id")),
        iteration_index=cast(int | None, row.get("iteration_index")),
        invocation_name=invocation_name,
        runtime_profile_name=runtime_profile_name,
    )


def _telemetry_span_row_from_json(
    *,
    graph_id: str,
    run_id: str,
    row: Mapping[str, object],
) -> TelemetrySpanRow:
    attributes = row.get("attributes", {})
    return TelemetrySpanRow(
        graph_id=graph_id,
        run_id=run_id,
        span_key=cast(str, row["span_id"]),
        sequence=_required_int(row, "sequence"),
        name=cast(str, row["name"]),
        start_time_ns=_required_int(row, "start_time_ns"),
        end_time_ns=_required_int(row, "end_time_ns"),
        frame_id=cast(str, row["frame_id"]),
        attributes=cast(dict[str, JsonValue], _json_value(attributes)),
        trace_id=cast(str | None, row.get("trace_id")),
        otel_span_id=cast(str | None, row.get("otel_span_id")),
        parent_span_id=cast(str | None, row.get("parent_span_id")),
        loop_node_id=cast(str | None, row.get("loop_node_id")),
        iteration_index=cast(int | None, row.get("iteration_index")),
        error_type=cast(str | None, row.get("error_type")),
        error_message=cast(str | None, row.get("error_message")),
    )


def _metric_point_rows_from_otel_metric(
    *,
    resource_attributes: Mapping[str, JsonValue],
    metric: Metric,
) -> list[TelemetryMetricPointRow]:
    rows: list[TelemetryMetricPointRow] = []
    if metric.HasField("sum"):
        rows.extend(
            _metric_point_rows_from_number_points(
                resource_attributes=resource_attributes,
                metric_name=metric.name,
                metric_kind="sum",
                unit=metric.unit or None,
                points=metric.sum.data_points,
            )
        )
    if metric.HasField("gauge"):
        rows.extend(
            _metric_point_rows_from_number_points(
                resource_attributes=resource_attributes,
                metric_name=metric.name,
                metric_kind="gauge",
                unit=metric.unit or None,
                points=metric.gauge.data_points,
            )
        )
    if metric.HasField("histogram"):
        for point in metric.histogram.data_points:
            attrs = _merged_identity(resource_attributes, _attribute_map(point.attributes))
            graph_id = _required_str(attrs, GRAPH_ID)
            run_id = _required_str(attrs, RUN_ID)
            value = float(point.sum)
            rows.append(
                TelemetryMetricPointRow(
                    graph_id=graph_id,
                    run_id=run_id,
                    point_key=_stable_key(
                        graph_id,
                        run_id,
                        metric.name,
                        str(point.time_unix_nano),
                        json.dumps(attrs, sort_keys=True),
                    ),
                    metric_name=metric.name,
                    metric_kind="histogram",
                    value=value,
                    unit=metric.unit or None,
                    timestamp_unix_ns=point.time_unix_nano or None,
                    node_id=_optional_str(attrs, NODE_ID),
                    frame_id=_optional_str(attrs, FRAME_ID),
                    loop_node_id=_optional_str(attrs, LOOP_NODE_ID),
                    iteration_index=_optional_int(attrs, ITERATION_INDEX),
                    invocation_name=_optional_str(attrs, INVOCATION_NAME),
                    runtime_profile_name=_optional_str(attrs, RUNTIME_PROFILE),
                    attributes=dict(attrs),
                )
            )
    return rows


def _metric_point_rows_from_number_points(
    *,
    resource_attributes: Mapping[str, JsonValue],
    metric_name: str,
    metric_kind: str,
    unit: str | None,
    points: Sequence[NumberDataPoint],
) -> list[TelemetryMetricPointRow]:
    rows: list[TelemetryMetricPointRow] = []
    for point in points:
        attrs = _merged_identity(resource_attributes, _attribute_map(point.attributes))
        graph_id = _required_str(attrs, GRAPH_ID)
        run_id = _required_str(attrs, RUN_ID)
        value = (
            float(point.as_double)
            if point.HasField("as_double")
            else float(point.as_int)
        )
        rows.append(
            TelemetryMetricPointRow(
                graph_id=graph_id,
                run_id=run_id,
                point_key=_stable_key(
                    graph_id,
                    run_id,
                    metric_name,
                    str(point.time_unix_nano),
                    json.dumps(attrs, sort_keys=True),
                    str(value),
                ),
                metric_name=metric_name,
                metric_kind=metric_kind,
                value=value,
                unit=unit,
                timestamp_unix_ns=point.time_unix_nano or None,
                node_id=_optional_str(attrs, NODE_ID),
                frame_id=_optional_str(attrs, FRAME_ID),
                loop_node_id=_optional_str(attrs, LOOP_NODE_ID),
                iteration_index=_optional_int(attrs, ITERATION_INDEX),
                invocation_name=_optional_str(attrs, INVOCATION_NAME),
                runtime_profile_name=_optional_str(attrs, RUNTIME_PROFILE),
                attributes=dict(attrs),
            )
        )
    return rows


def _decode_json_object(payload: bytes) -> dict[str, JsonValue]:
    if not payload:
        return {}
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("Expected JSON object payload.")
    return cast(dict[str, JsonValue], decoded)


def _decode_jsonl_rows(payload: bytes) -> tuple[dict[str, JsonValue], ...]:
    rows: list[dict[str, JsonValue]] = []
    for raw_line in payload.decode("utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        decoded = json.loads(line)
        if not isinstance(decoded, dict):
            raise ValueError("Expected JSON object in jsonl payload.")
        rows.append(cast(dict[str, JsonValue], decoded))
    return tuple(rows)


def _attribute_map(attributes: Sequence[KeyValue]) -> dict[str, JsonValue]:
    payload: dict[str, JsonValue] = {}
    for attribute in attributes:
        payload[attribute.key] = _any_value_to_json(attribute.value)
    return payload


def _any_value_to_json(value: AnyValue) -> JsonValue:
    field = value.WhichOneof("value")
    if field == "string_value":
        return value.string_value
    if field == "bool_value":
        return value.bool_value
    if field == "int_value":
        return int(value.int_value)
    if field == "double_value":
        return float(value.double_value)
    if field == "bytes_value":
        return value.bytes_value.hex()
    if field == "array_value":
        return [_any_value_to_json(item) for item in value.array_value.values]
    if field == "kvlist_value":
        return {
            item.key: _any_value_to_json(item.value)
            for item in value.kvlist_value.values
        }
    return {}


def _merged_identity(
    resource_attributes: Mapping[str, JsonValue],
    attributes: Mapping[str, JsonValue],
) -> dict[str, JsonValue]:
    identity = dict(resource_attributes)
    identity.update(attributes)
    return identity


def _required_str(attributes: Mapping[str, JsonValue], key: str) -> str:
    value = attributes.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"OTLP telemetry record is missing required attribute {key!r}.")
    return value


def _optional_str(attributes: Mapping[str, JsonValue], key: str) -> str | None:
    value = attributes.get(key)
    return value if isinstance(value, str) and value else None


def _optional_int(attributes: Mapping[str, JsonValue], key: str) -> int | None:
    value = attributes.get(key)
    return value if isinstance(value, int) else None


def _sequence_from_span_attributes(attributes: Mapping[str, JsonValue]) -> int:
    value = attributes.get("mentalmodel.sequence")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return 0


def _hex_bytes(value: bytes) -> str | None:
    return None if not value else value.hex()


def _stable_key(*parts: str) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(part.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _json_value(value: object) -> JsonValue:
    encoded = json.dumps(value, sort_keys=True)
    return cast(JsonValue, json.loads(encoded))


def _required_int(row: Mapping[str, object], key: str) -> int:
    value = row.get(key)
    if isinstance(value, bool):
        raise ValueError(f"{key} must be an integer.")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    raise ValueError(f"{key} must be an integer.")
