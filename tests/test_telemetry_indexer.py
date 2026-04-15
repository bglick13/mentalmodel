from __future__ import annotations

import json
import unittest
from collections.abc import Mapping
from typing import Protocol, cast

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
from opentelemetry.proto.metrics.v1.metrics_pb2 import AggregationTemporality

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.observability.semantic_conventions import (
    EVENT_TYPE,
    FRAME_ID,
    GRAPH_ID,
    INVOCATION_NAME,
    ITERATION_INDEX,
    LOOP_NODE_ID,
    NODE_ID,
    PROJECT_ID,
    RECORD_ID,
    RUN_ID,
    SEQUENCE,
)
from mentalmodel.remote.contracts import (
    ArtifactDescriptor,
    ArtifactName,
    CatalogSource,
    RunManifest,
    RunManifestStatus,
    RunTraceSummary,
)
from mentalmodel.remote.telemetry_indexer import TelemetryIndexer
from mentalmodel.remote.telemetry_store import InMemoryTelemetryStore, TelemetryRunRecord


class TelemetryIndexerTest(unittest.TestCase):
    def test_otlp_logs_index_lifecycle_records_and_derived_metrics(self) -> None:
        store = InMemoryTelemetryStore()
        indexer = TelemetryIndexer(store)

        indexer.index_otlp_logs(
            _logs_payload(
                resource_attributes={GRAPH_ID: "async_rl_demo", PROJECT_ID: "proj-1"},
                logs=(
                    {
                        "timestamp_unix_ns": 1_000_000_000,
                        "attributes": {
                            RUN_ID: "run-123",
                            INVOCATION_NAME: "smoke",
                            EVENT_TYPE: "mentalmodel.run.lifecycle",
                            "mentalmodel.run.lifecycle.event": "started",
                            "mentalmodel.runtime_default_profile": "real",
                        },
                        "body": {
                            "summary": "starting",
                            "graph": {
                                "graph_id": "async_rl_demo",
                                "metadata": {},
                                "nodes": [{"node_id": "trainer"}],
                                "edges": [],
                            },
                            "analysis": {
                                "error_count": 0,
                                "warning_count": 0,
                                "findings": [],
                            },
                            "runtime_profile_names": ["real"],
                        },
                    },
                    {
                        "timestamp_unix_ns": 1_100_000_000,
                        "attributes": {
                            RUN_ID: "run-123",
                            RECORD_ID: "run-123:7",
                            INVOCATION_NAME: "smoke",
                            EVENT_TYPE: "node.succeeded",
                            NODE_ID: "trainer",
                            FRAME_ID: "steps[3]",
                            LOOP_NODE_ID: "steps",
                            ITERATION_INDEX: 3,
                            SEQUENCE: 7,
                        },
                        "body": {
                            "payload": {
                                "output": {
                                    "train.prompt_count": 4,
                                    "train.reward": 1.25,
                                }
                            }
                        },
                    },
                    {
                        "timestamp_unix_ns": 1_200_000_000,
                        "attributes": {
                            RUN_ID: "run-123",
                            INVOCATION_NAME: "smoke",
                            EVENT_TYPE: "mentalmodel.run.lifecycle",
                            "mentalmodel.run.lifecycle.event": "completed",
                            "mentalmodel.run.success": True,
                        },
                        "body": {"summary": "done"},
                    },
                ),
            )
        )

        run = store.get_run(graph_id="async_rl_demo", run_id="run-123")
        self.assertEqual(run.status, "succeeded")
        self.assertTrue(run.success)
        self.assertEqual(run.project_id, "proj-1")
        self.assertEqual(run.invocation_name, "smoke")
        self.assertEqual(run.runtime_default_profile_name, "real")
        self.assertEqual(run.runtime_profile_names, ("real",))
        assert run.graph is not None
        self.assertEqual(run.graph["graph_id"], "async_rl_demo")

        records = store.get_records_page(
            graph_id="async_rl_demo",
            run_id="run-123",
            cursor=None,
            limit=10,
            include_payload=True,
        )
        self.assertEqual(records.total_count, 1)
        self.assertEqual(records.items[0]["node_id"], "trainer")
        self.assertEqual(records.items[0]["frame_id"], "steps[3]")

        metrics = store.list_metrics(
            graph_id="async_rl_demo",
            run_id="run-123",
            path_prefixes=("train.",),
        )
        self.assertEqual(len(metrics), 2)
        self.assertTrue(all(row.iteration_index == 3 for row in metrics))

    def test_otlp_traces_index_span_rows_with_canonical_identity(self) -> None:
        store = InMemoryTelemetryStore()
        indexer = TelemetryIndexer(store)

        indexer.index_otlp_traces(
            _traces_payload(
                resource_attributes={GRAPH_ID: "async_rl_demo", RUN_ID: "run-456"},
                spans=(
                    {
                        "trace_id": bytes.fromhex("11" * 16),
                        "span_id": bytes.fromhex("22" * 8),
                        "parent_span_id": bytes.fromhex("33" * 8),
                        "name": "actor:trainer",
                        "start_time_unix_nano": 100,
                        "end_time_unix_nano": 250,
                        "attributes": {
                            NODE_ID: "trainer",
                            FRAME_ID: "steps[1]",
                            LOOP_NODE_ID: "steps",
                            ITERATION_INDEX: 1,
                            SEQUENCE: 9,
                        },
                    },
                ),
            )
        )

        run = store.get_run(graph_id="async_rl_demo", run_id="run-456")
        self.assertEqual(run.status, "running")

        spans = store.get_spans_page(
            graph_id="async_rl_demo",
            run_id="run-456",
            cursor=None,
            limit=10,
        )
        self.assertEqual(spans.total_count, 1)
        self.assertEqual(spans.items[0]["name"], "actor:trainer")
        self.assertEqual(spans.items[0]["trace_id"], "11111111111111111111111111111111")
        self.assertEqual(spans.items[0]["otel_span_id"], "2222222222222222")
        self.assertEqual(spans.items[0]["parent_span_id"], "3333333333333333")
        self.assertEqual(spans.items[0]["frame_id"], "steps[1]")

    def test_otlp_metrics_index_metric_points(self) -> None:
        store = InMemoryTelemetryStore()
        indexer = TelemetryIndexer(store)

        indexer.index_otlp_metrics(
            _metrics_payload(
                resource_attributes={GRAPH_ID: "async_rl_demo", RUN_ID: "run-789"},
                metrics=(
                    {
                        "kind": "sum",
                        "name": "mentalmodel.node.duration_ms",
                        "unit": "ms",
                        "points": (
                            {
                                "time_unix_nano": 5_000,
                                "value": 12.5,
                                "attributes": {
                                    NODE_ID: "trainer",
                                    FRAME_ID: "steps[4]",
                                    LOOP_NODE_ID: "steps",
                                    ITERATION_INDEX: 4,
                                },
                            },
                        ),
                    },
                ),
            )
        )

        point_rows = store._metric_points[("async_rl_demo", "run-789")]  # noqa: SLF001
        self.assertEqual(len(point_rows), 1)
        point = next(iter(point_rows.values()))
        self.assertEqual(point.metric_name, "mentalmodel.node.duration_ms")
        self.assertEqual(point.metric_kind, "sum")
        self.assertEqual(point.value, 12.5)
        self.assertEqual(point.frame_id, "steps[4]")
        self.assertEqual(point.iteration_index, 4)

    def test_completed_run_indexing_replaces_live_rows_with_finalized_bundle(self) -> None:
        store = InMemoryTelemetryStore()
        indexer = TelemetryIndexer(store)

        store.replace_run(
            TelemetryRunRecord(
                graph_id="async_rl_demo",
                run_id="live-run",
                created_at_ms=1_000,
                updated_at_ms=1_100,
                status="running",
                success=None,
                invocation_name="smoke",
            )
        )
        store.replace_records(
            graph_id="async_rl_demo",
            run_id="live-run",
            rows=(),
        )
        manifest = RunManifest(
            run_id="live-run",
            graph_id="async_rl_demo",
            created_at_ms=1_000,
            completed_at_ms=2_000,
            status=RunManifestStatus.INDEXED,
            success=True,
            run_schema_version=4,
            trace_summary=RunTraceSummary(mode="otlp", service_name="mentalmodel"),
            artifacts=(
                ArtifactDescriptor(
                    logical_name=ArtifactName.GRAPH,
                    relative_path="graph.json",
                    content_type="application/json",
                ),
                ArtifactDescriptor(
                    logical_name=ArtifactName.RECORDS,
                    relative_path="records.jsonl",
                    content_type="application/jsonl",
                ),
                ArtifactDescriptor(
                    logical_name=ArtifactName.OUTPUTS,
                    relative_path="outputs.json",
                    content_type="application/json",
                ),
                ArtifactDescriptor(
                    logical_name=ArtifactName.SPANS,
                    relative_path="otel-spans.jsonl",
                    content_type="application/jsonl",
                    required=False,
                ),
            ),
            invocation_name="smoke",
            project_id="proj-1",
            project_label="Project 1",
            environment_name="prod",
            catalog_entry_id="smoke-spec",
            catalog_source=CatalogSource.BUILTIN,
            runtime_default_profile_name="real",
            runtime_profile_names=("real",),
        )

        indexer.index_completed_run(
            manifest=manifest,
            artifact_map={
                "graph.json": json.dumps(
                    {
                        "graph_id": "async_rl_demo",
                        "metadata": {},
                        "nodes": [{"node_id": "trainer"}],
                        "edges": [],
                    }
                ).encode("utf-8"),
                "records.jsonl": (
                    json.dumps(
                        {
                            "record_id": "live-run:1",
                            "run_id": "live-run",
                            "node_id": "trainer",
                            "frame_id": "steps[0]",
                            "frame_path": ["root", "steps[0]"],
                            "loop_node_id": "steps",
                            "iteration_index": 0,
                            "event_type": "node.succeeded",
                            "sequence": 1,
                            "timestamp_ms": 1_500,
                            "payload": {
                                "output": {
                                    "train.prompt_count": 8,
                                    "train.reward": 2.5,
                                }
                            },
                        }
                    )
                    + "\n"
                ).encode("utf-8"),
                "otel-spans.jsonl": (
                    json.dumps(
                        {
                            "span_id": "span-1",
                            "sequence": 1,
                            "name": "actor:trainer",
                            "start_time_ns": 100,
                            "end_time_ns": 200,
                            "frame_id": "steps[0]",
                            "loop_node_id": "steps",
                            "iteration_index": 0,
                            "attributes": {"mentalmodel.node_id": "trainer"},
                        }
                    )
                    + "\n"
                ).encode("utf-8"),
                "outputs.json": json.dumps(
                    {
                        "outputs": {},
                        "framed_outputs": [
                            {
                                "node_id": "trainer",
                                "frame_id": "steps[0]",
                                "loop_node_id": "steps",
                                "iteration_index": 0,
                                "value": {
                                    "train.prompt_count": 8,
                                    "train.reward": 2.5,
                                },
                            }
                        ],
                    }
                ).encode("utf-8"),
                "verification.json": json.dumps({"runtime": {}}).encode("utf-8"),
            },
        )

        run = store.get_run(graph_id="async_rl_demo", run_id="live-run")
        self.assertEqual(run.status, "succeeded")
        self.assertTrue(run.success)
        self.assertEqual(run.project_id, "proj-1")
        self.assertEqual(run.environment_name, "prod")
        self.assertEqual(run.catalog_entry_id, "smoke-spec")
        self.assertEqual(run.record_count, 1)
        self.assertEqual(run.span_count, 1)

        metrics = store.list_metrics(
            graph_id="async_rl_demo",
            run_id="live-run",
            path_prefixes=("train.",),
        )
        self.assertEqual(len(metrics), 2)
def _logs_payload(
    *,
    resource_attributes: dict[str, JsonValue],
    logs: tuple[dict[str, object], ...],
) -> bytes:
    request = ExportLogsServiceRequest()
    resource_logs = request.resource_logs.add()
    _extend_attributes(resource_logs.resource.attributes, resource_attributes)
    scope_logs = resource_logs.scope_logs.add()
    for item in logs:
        log_record = scope_logs.log_records.add()
        log_record.time_unix_nano = _require_int(item, "timestamp_unix_ns")
        log_record.observed_time_unix_nano = _require_int(item, "timestamp_unix_ns")
        _extend_attributes(
            log_record.attributes,
            _require_mapping(item, "attributes"),
        )
        log_record.body.CopyFrom(_json_any_value(_require_json_value(item, "body")))
    return cast(bytes, request.SerializeToString())


def _traces_payload(
    *,
    resource_attributes: dict[str, JsonValue],
    spans: tuple[dict[str, object], ...],
) -> bytes:
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    _extend_attributes(resource_spans.resource.attributes, resource_attributes)
    scope_spans = resource_spans.scope_spans.add()
    for item in spans:
        span = scope_spans.spans.add()
        span.trace_id = _require_bytes(item, "trace_id")
        span.span_id = _require_bytes(item, "span_id")
        span.parent_span_id = _require_bytes(item, "parent_span_id")
        span.name = _require_str(item, "name")
        span.start_time_unix_nano = _require_int(item, "start_time_unix_nano")
        span.end_time_unix_nano = _require_int(item, "end_time_unix_nano")
        _extend_attributes(span.attributes, _require_mapping(item, "attributes"))
    return cast(bytes, request.SerializeToString())


def _metrics_payload(
    *,
    resource_attributes: dict[str, JsonValue],
    metrics: tuple[dict[str, object], ...],
) -> bytes:
    request = ExportMetricsServiceRequest()
    resource_metrics = request.resource_metrics.add()
    _extend_attributes(resource_metrics.resource.attributes, resource_attributes)
    scope_metrics = resource_metrics.scope_metrics.add()
    for item in metrics:
        metric = scope_metrics.metrics.add()
        metric.name = _require_str(item, "name")
        metric.unit = _require_str(item, "unit")
        points = _require_tuple_of_mappings(item, "points")
        if item["kind"] == "sum":
            metric.sum.aggregation_temporality = (
                AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA
            )
            metric.sum.is_monotonic = False
            for point_payload in points:
                point = metric.sum.data_points.add()
                point.time_unix_nano = _require_int(point_payload, "time_unix_nano")
                point.as_double = _require_float(point_payload, "value")
                _extend_attributes(point.attributes, _require_mapping(point_payload, "attributes"))
        else:
            raise AssertionError(f"Unsupported metric kind {item['kind']!r}.")
    return cast(bytes, request.SerializeToString())

class _KeyValueSequence(Protocol):
    def add(self) -> KeyValue: ...


def _extend_attributes(target: _KeyValueSequence, attributes: Mapping[str, JsonValue]) -> None:
    for key, value in attributes.items():
        attribute = target.add()
        attribute.key = key
        attribute.value.CopyFrom(_json_any_value(value))


def _json_any_value(value: JsonValue) -> AnyValue:
    any_value = AnyValue()
    if value is None:
        return any_value
    if isinstance(value, bool):
        any_value.bool_value = value
        return any_value
    if isinstance(value, int):
        any_value.int_value = value
        return any_value
    if isinstance(value, float):
        any_value.double_value = value
        return any_value
    if isinstance(value, str):
        any_value.string_value = value
        return any_value
    if isinstance(value, list):
        for item in value:
            any_value.array_value.values.add().CopyFrom(_json_any_value(item))
        return any_value
    for key, item in value.items():
        entry = KeyValue(key=key)
        entry.value.CopyFrom(_json_any_value(item))
        any_value.kvlist_value.values.append(entry)
    return any_value


def _require_mapping(
    payload: Mapping[str, object],
    key: str,
) -> dict[str, JsonValue]:
    value = payload[key]
    if not isinstance(value, dict):
        raise AssertionError(f"{key} must be a mapping.")
    return cast(dict[str, JsonValue], value)


def _require_str(payload: Mapping[str, object], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str):
        raise AssertionError(f"{key} must be a string.")
    return value


def _require_int(payload: Mapping[str, object], key: str) -> int:
    value = payload[key]
    if not isinstance(value, int):
        raise AssertionError(f"{key} must be an integer.")
    return value


def _require_float(payload: Mapping[str, object], key: str) -> float:
    value = payload[key]
    if isinstance(value, int):
        return float(value)
    if not isinstance(value, float):
        raise AssertionError(f"{key} must be a float.")
    return value


def _require_bytes(payload: Mapping[str, object], key: str) -> bytes:
    value = payload[key]
    if not isinstance(value, bytes):
        raise AssertionError(f"{key} must be bytes.")
    return value


def _require_json_value(payload: Mapping[str, object], key: str) -> JsonValue:
    value = payload[key]
    return cast(JsonValue, value)


def _require_tuple_of_mappings(
    payload: Mapping[str, object],
    key: str,
) -> tuple[dict[str, object], ...]:
    value = payload[key]
    if not isinstance(value, tuple):
        raise AssertionError(f"{key} must be a tuple.")
    if any(not isinstance(item, dict) for item in value):
        raise AssertionError(f"{key} items must be mappings.")
    return cast(tuple[dict[str, object], ...], value)


if __name__ == "__main__":
    unittest.main()
