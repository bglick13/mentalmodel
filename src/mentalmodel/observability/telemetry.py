from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.metrics import MetricObservation
from mentalmodel.observability.semantic_conventions import (
    CATALOG_ENTRY_ID,
    CATALOG_SOURCE,
    ENVIRONMENT_NAME,
    ERROR_MESSAGE,
    ERROR_TYPE,
    EVENT_TYPE,
    FRAME_ID,
    GRAPH_ID,
    INVOCATION_NAME,
    ITERATION_INDEX,
    LOOP_NODE_ID,
    NODE_ID,
    NODE_KIND,
    PAYLOAD_ATTRIBUTE_PREFIX,
    PROJECT_ID,
    PROJECT_LABEL,
    RECORD_ID,
    RUN_ID,
    RUNTIME_CONTEXT,
    RUNTIME_PROFILE,
    SEQUENCE,
    SERVICE_NAME,
    SERVICE_NAMESPACE,
    SERVICE_VERSION,
    TelemetryAttributeValue,
    canonicalize_attributes,
    prefixed_payload_attribute,
    with_legacy_trace_aliases,
)
from mentalmodel.observability.serialization import serialize_runtime_value
from mentalmodel.observability.tracing import RecordedSpan


@dataclass(slots=True, frozen=True)
class TelemetryResourceContext:
    """Shared resource identity attached to mapped telemetry rows."""

    graph_id: str | None = None
    project_id: str | None = None
    project_label: str | None = None
    environment_name: str | None = None
    catalog_entry_id: str | None = None
    catalog_source: str | None = None
    service_name: str | None = None
    service_namespace: str | None = None
    service_version: str | None = None

    def attributes(self) -> dict[str, TelemetryAttributeValue]:
        resource: dict[str, TelemetryAttributeValue] = {}
        if self.graph_id is not None:
            resource[GRAPH_ID] = self.graph_id
        if self.project_id is not None:
            resource[PROJECT_ID] = self.project_id
        if self.project_label is not None:
            resource[PROJECT_LABEL] = self.project_label
        if self.environment_name is not None:
            resource[ENVIRONMENT_NAME] = self.environment_name
        if self.catalog_entry_id is not None:
            resource[CATALOG_ENTRY_ID] = self.catalog_entry_id
        if self.catalog_source is not None:
            resource[CATALOG_SOURCE] = self.catalog_source
        if self.service_name is not None:
            resource[SERVICE_NAME] = self.service_name
        if self.service_namespace is not None:
            resource[SERVICE_NAMESPACE] = self.service_namespace
        if self.service_version is not None:
            resource[SERVICE_VERSION] = self.service_version
        return resource


@dataclass(slots=True, frozen=True)
class OtelLogRecord:
    """Canonical OTel-aligned log record for semantic execution events."""

    timestamp_unix_ns: int
    observed_timestamp_unix_ns: int
    body: JsonValue
    attributes: dict[str, TelemetryAttributeValue]
    resource_attributes: dict[str, TelemetryAttributeValue]
    severity_text: str | None = None


@dataclass(slots=True, frozen=True)
class OtelSpan:
    """Canonical OTel-aligned span representation."""

    trace_id: str | None
    span_id: str | None
    parent_span_id: str | None
    name: str
    start_time_unix_ns: int
    end_time_unix_ns: int
    attributes: dict[str, TelemetryAttributeValue]
    resource_attributes: dict[str, TelemetryAttributeValue]
    source_attributes: dict[str, TelemetryAttributeValue]
    synthetic_span_id: str
    sequence: int
    error_type: str | None = None
    error_message: str | None = None


@dataclass(slots=True, frozen=True)
class OtelMetricPoint:
    """One mapped OTel-aligned metric datapoint."""

    value: int | float
    attributes: dict[str, TelemetryAttributeValue]
    timestamp_unix_ns: int | None = None


@dataclass(slots=True, frozen=True)
class OtelMetric:
    """Canonical OTel-aligned metric instrument plus datapoints."""

    name: str
    kind: str
    description: str
    unit: str | None
    resource_attributes: dict[str, TelemetryAttributeValue]
    points: tuple[OtelMetricPoint, ...]


@dataclass(slots=True, frozen=True)
class TelemetryMapper:
    """Map runtime domain objects into one canonical telemetry contract."""

    resource_context: TelemetryResourceContext = TelemetryResourceContext()

    def execution_record_to_log(self, record: ExecutionRecord) -> OtelLogRecord:
        attributes: dict[str, TelemetryAttributeValue] = {
            RECORD_ID: record.record_id,
            RUN_ID: record.run_id,
            NODE_ID: record.node_id,
            FRAME_ID: record.frame.frame_id,
            EVENT_TYPE: record.event_type,
            SEQUENCE: record.sequence,
        }
        if record.frame.loop_node_id is not None:
            attributes[LOOP_NODE_ID] = record.frame.loop_node_id
        if record.frame.iteration_index is not None:
            attributes[ITERATION_INDEX] = record.frame.iteration_index
        attributes.update(_promoted_payload_attributes(record.payload))
        body = {
            "summary": _record_summary(record),
            "event_type": record.event_type,
            "frame_path": serialize_runtime_value(record.frame.path),
            "payload": serialize_runtime_value(record.payload),
        }
        timestamp_unix_ns = record.timestamp_ms * 1_000_000
        return OtelLogRecord(
            timestamp_unix_ns=timestamp_unix_ns,
            observed_timestamp_unix_ns=timestamp_unix_ns,
            body=body,
            attributes=attributes,
            resource_attributes=self.resource_context.attributes(),
        )

    def recorded_span_to_span(self, span: RecordedSpan) -> OtelSpan:
        source_attributes = canonicalize_attributes(span.attributes)
        attributes = dict(source_attributes)
        attributes.setdefault(SEQUENCE, span.sequence)
        attributes.setdefault(FRAME_ID, span.frame_id)
        if span.loop_node_id is not None:
            attributes.setdefault(LOOP_NODE_ID, span.loop_node_id)
        if span.iteration_index is not None:
            attributes.setdefault(ITERATION_INDEX, span.iteration_index)
        if span.error_type is not None:
            attributes.setdefault(ERROR_TYPE, span.error_type)
        if span.error_message is not None:
            attributes.setdefault(ERROR_MESSAGE, span.error_message)
        return OtelSpan(
            trace_id=span.trace_id,
            span_id=span.otel_span_id,
            parent_span_id=span.parent_span_id,
            name=span.name,
            start_time_unix_ns=span.start_time_ns,
            end_time_unix_ns=span.end_time_ns,
            attributes=attributes,
            resource_attributes=self.resource_context.attributes(),
            source_attributes=source_attributes,
            synthetic_span_id=span.span_id,
            sequence=span.sequence,
            error_type=span.error_type,
            error_message=span.error_message,
        )

    def metric_observation_to_metric(
        self,
        observation: MetricObservation,
        *,
        timestamp_unix_ns: int | None = None,
    ) -> OtelMetric:
        resource_attributes = self.resource_context.attributes()
        point_attributes: dict[str, TelemetryAttributeValue] = {}
        for key, value in observation.attributes.items():
            if key == "service_name" and isinstance(value, str):
                resource_attributes.setdefault(SERVICE_NAME, value)
                continue
            if key == "graph_id" and isinstance(value, str):
                resource_attributes.setdefault(GRAPH_ID, value)
                continue
            canonical_key = _metric_attribute_key(key)
            if canonical_key is None:
                continue
            canonical_value = _metric_attribute_value(value)
            if canonical_value is not None:
                point_attributes[canonical_key] = canonical_value
        return OtelMetric(
            name=observation.definition.name,
            kind=observation.definition.kind.value,
            description=observation.definition.description,
            unit=observation.definition.unit,
            resource_attributes=resource_attributes,
            points=(
                OtelMetricPoint(
                    value=observation.value,
                    attributes=point_attributes,
                    timestamp_unix_ns=timestamp_unix_ns,
                ),
            ),
        )


def execution_record_json_from_log(log: OtelLogRecord) -> dict[str, JsonValue]:
    """Render the legacy execution-record JSON shape from the canonical log."""

    body = _log_body_dict(log.body)
    attributes = log.attributes
    timestamp_ms = _nanoseconds_to_milliseconds(log.timestamp_unix_ns)
    return {
        "record_id": _required_str(attributes, RECORD_ID),
        "run_id": _required_str(attributes, RUN_ID),
        "node_id": _required_str(attributes, NODE_ID),
        "frame_id": _required_str(attributes, FRAME_ID),
        "frame_path": _json_value(body.get("frame_path", [])),
        "loop_node_id": _optional_str(attributes, LOOP_NODE_ID),
        "iteration_index": _optional_int(attributes, ITERATION_INDEX),
        "event_type": _required_str(attributes, EVENT_TYPE),
        "sequence": _required_int(attributes, SEQUENCE),
        "timestamp_ms": timestamp_ms,
        "payload": _json_value(body.get("payload", {})),
    }


def recorded_span_json_from_span(span: OtelSpan) -> dict[str, JsonValue]:
    """Render the legacy recorded-span JSON shape from the canonical span."""

    attributes = with_legacy_trace_aliases(span.source_attributes or span.attributes)
    duration_ns = max(0, span.end_time_unix_ns - span.start_time_unix_ns)
    payload: dict[str, JsonValue] = {
        "span_id": span.synthetic_span_id,
        "sequence": span.sequence,
        "name": span.name,
        "start_time_ns": span.start_time_unix_ns,
        "end_time_ns": span.end_time_unix_ns,
        "duration_ns": duration_ns,
        "attributes": _json_value(attributes),
        "frame_id": _required_str(span.attributes, FRAME_ID),
        "loop_node_id": _optional_str(span.attributes, LOOP_NODE_ID),
        "iteration_index": _optional_int(span.attributes, ITERATION_INDEX),
        "error_type": span.error_type,
        "error_message": span.error_message,
    }
    if span.trace_id is not None:
        payload["trace_id"] = span.trace_id
    if span.span_id is not None:
        payload["otel_span_id"] = span.span_id
    if span.parent_span_id is not None:
        payload["parent_span_id"] = span.parent_span_id
    return payload


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    """Write one JSON document with stable formatting."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {key: serialize_runtime_value(value) for key, value in payload.items()},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def write_jsonl(path: Path, rows: Iterable[Mapping[str, object]]) -> None:
    """Write newline-delimited JSON with stable formatting."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded_rows = [
        json.dumps(
            {key: serialize_runtime_value(value) for key, value in row.items()},
            sort_keys=True,
        )
        for row in rows
    ]
    content = "\n".join(encoded_rows)
    if content:
        content += "\n"
    path.write_text(content, encoding="utf-8")


def _record_summary(record: ExecutionRecord) -> str:
    return f"{record.node_id} {record.event_type}"


def _promoted_payload_attributes(
    payload: Mapping[str, JsonValue],
) -> dict[str, TelemetryAttributeValue]:
    promoted: dict[str, TelemetryAttributeValue] = {}
    for key, value in sorted(payload.items()):
        if key == "runtime_profile":
            if isinstance(value, str):
                promoted[RUNTIME_PROFILE] = value
            continue
        if key == "invocation_name":
            if isinstance(value, str):
                promoted[INVOCATION_NAME] = value
            continue
        if isinstance(value, (str, bool, int, float)):
            promoted[prefixed_payload_attribute(key)] = value
    return promoted


def _metric_attribute_key(key: str) -> str | None:
    mapping = {
        "run_id": RUN_ID,
        "node_id": NODE_ID,
        "node_kind": NODE_KIND,
        "runtime_context": RUNTIME_CONTEXT,
        "runtime_profile": RUNTIME_PROFILE,
        "invocation_name": INVOCATION_NAME,
        "frame_id": FRAME_ID,
        "loop_node_id": LOOP_NODE_ID,
        "iteration_index": ITERATION_INDEX,
    }
    if key in mapping:
        return mapping[key]
    if key.startswith(PAYLOAD_ATTRIBUTE_PREFIX):
        return key
    return None


def _metric_attribute_value(value: object) -> TelemetryAttributeValue | None:
    if isinstance(value, (str, bool, int, float)):
        return value
    return None


def _log_body_dict(body: JsonValue) -> dict[str, JsonValue]:
    if isinstance(body, dict):
        return body
    return {"payload": {}, "frame_path": [], "summary": body}


def _json_value(value: object) -> JsonValue:
    return serialize_runtime_value(value)


def _required_str(attributes: Mapping[str, TelemetryAttributeValue], key: str) -> str:
    value = attributes.get(key)
    if not isinstance(value, str):
        raise ValueError(f"Telemetry attribute {key!r} is required.")
    return value


def _optional_str(attributes: Mapping[str, TelemetryAttributeValue], key: str) -> str | None:
    value = attributes.get(key)
    return value if isinstance(value, str) else None


def _required_int(attributes: Mapping[str, TelemetryAttributeValue], key: str) -> int:
    value = attributes.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise ValueError(f"Telemetry attribute {key!r} must be an integer.")


def _optional_int(attributes: Mapping[str, TelemetryAttributeValue], key: str) -> int | None:
    value = attributes.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _nanoseconds_to_milliseconds(value: int) -> int:
    return value // 1_000_000
