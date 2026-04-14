from __future__ import annotations

import unittest

from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.metrics import (
    MetricContext,
    MetricDefinition,
    MetricKind,
    MetricObservation,
)
from mentalmodel.observability.semantic_conventions import (
    EVENT_TYPE,
    FRAME_ID,
    GRAPH_ID,
    ITERATION_INDEX,
    LOOP_NODE_ID,
    NODE_ID,
    RUN_ID,
    RUNTIME_PROFILE,
    SEQUENCE,
    SERVICE_NAME,
)
from mentalmodel.observability.telemetry import (
    TelemetryMapper,
    TelemetryResourceContext,
    execution_record_json_from_log,
    recorded_span_json_from_span,
)
from mentalmodel.observability.tracing import RecordedSpan
from mentalmodel.runtime.frame import ExecutionFrame, ExecutionFrameSegment


class TelemetryMapperTest(unittest.TestCase):
    def test_execution_record_maps_to_canonical_log_and_legacy_json_losslessly(self) -> None:
        record = ExecutionRecord(
            record_id="run-123:7",
            run_id="run-123",
            node_id="sample_policy",
            event_type="node.succeeded",
            sequence=7,
            timestamp_ms=1_710_000_123_456,
            frame=ExecutionFrame(
                path=(ExecutionFrameSegment(loop_node_id="steps", iteration_index=3),),
            ),
            payload={
                "runtime_profile": "gpu",
                "output_type": "dict",
                "ok": True,
            },
        )
        mapper = TelemetryMapper(
            TelemetryResourceContext(
                graph_id="async_rl_demo",
                project_id="proj-1",
                service_name="mentalmodel-test",
            )
        )

        log = mapper.execution_record_to_log(record)

        self.assertEqual(log.attributes[RUN_ID], "run-123")
        self.assertEqual(log.attributes[NODE_ID], "sample_policy")
        self.assertEqual(log.attributes[FRAME_ID], "steps[3]")
        self.assertEqual(log.attributes[LOOP_NODE_ID], "steps")
        self.assertEqual(log.attributes[ITERATION_INDEX], 3)
        self.assertEqual(log.attributes[EVENT_TYPE], "node.succeeded")
        self.assertEqual(log.attributes[SEQUENCE], 7)
        self.assertEqual(log.attributes[RUNTIME_PROFILE], "gpu")
        self.assertEqual(log.resource_attributes[GRAPH_ID], "async_rl_demo")
        self.assertEqual(log.resource_attributes[SERVICE_NAME], "mentalmodel-test")

        legacy = execution_record_json_from_log(log)
        self.assertEqual(legacy["record_id"], "run-123:7")
        self.assertEqual(legacy["frame_id"], "steps[3]")
        self.assertEqual(legacy["iteration_index"], 3)
        self.assertEqual(
            legacy["payload"],
            {
                "runtime_profile": "gpu",
                "output_type": "dict",
                "ok": True,
            },
        )

    def test_recorded_span_mapping_canonicalizes_legacy_attributes_without_losing_compatibility(
        self,
    ) -> None:
        span = RecordedSpan(
            span_id="span-3:steps[2]:10:effect:sample_policy",
            sequence=3,
            name="effect:sample_policy",
            start_time_ns=10,
            end_time_ns=25,
            attributes={
                "mentalmodel.run_id": "run-123",
                "mentalmodel.node.id": "sample_policy",
                "mentalmodel.frame.id": "steps[2]",
                "mentalmodel.loop.node_id": "steps",
                "mentalmodel.loop.iteration_index": "2",
                "mentalmodel.runtime.profile": "gpu",
            },
            frame_id="steps[2]",
            loop_node_id="steps",
            iteration_index=2,
            trace_id="abc123",
            otel_span_id="def456",
            parent_span_id="parent000",
            error_type="RuntimeError",
            error_message="boom",
        )
        mapped = TelemetryMapper().recorded_span_to_span(span)

        self.assertEqual(mapped.attributes[RUN_ID], "run-123")
        self.assertEqual(mapped.attributes[NODE_ID], "sample_policy")
        self.assertEqual(mapped.attributes[FRAME_ID], "steps[2]")
        self.assertEqual(mapped.attributes[LOOP_NODE_ID], "steps")
        self.assertEqual(mapped.attributes[ITERATION_INDEX], 2)
        self.assertEqual(mapped.attributes[RUNTIME_PROFILE], "gpu")
        self.assertEqual(mapped.trace_id, "abc123")
        self.assertEqual(mapped.span_id, "def456")

        legacy = recorded_span_json_from_span(mapped)
        legacy_attributes = legacy["attributes"]
        assert isinstance(legacy_attributes, dict)
        self.assertEqual(legacy_attributes["mentalmodel.node.id"], "sample_policy")
        self.assertEqual(legacy_attributes["mentalmodel.node_id"], "sample_policy")
        self.assertEqual(legacy["iteration_index"], 2)
        self.assertEqual(legacy["trace_id"], "abc123")

    def test_metric_observation_mapping_preserves_frame_iteration_and_runtime_profile(self) -> None:
        context = MetricContext(
            graph_id="async_rl_demo",
            run_id="run-123",
            node_id="sample_policy",
            node_kind="effect",
            runtime_context="trainer",
            frame_id="steps[2]",
            loop_node_id="steps",
            iteration_index=2,
            service_name="mentalmodel-test",
            runtime_profile="gpu",
            invocation_name="smoke",
        )
        observation = MetricObservation(
            definition=MetricDefinition(
                name="mentalmodel.node.duration_ms",
                kind=MetricKind.HISTOGRAM,
                unit="ms",
            ),
            value=12.5,
            attributes=context.default_attributes(),
        )

        metric = TelemetryMapper().metric_observation_to_metric(observation, timestamp_unix_ns=50)

        self.assertEqual(metric.resource_attributes[GRAPH_ID], "async_rl_demo")
        self.assertEqual(metric.resource_attributes[SERVICE_NAME], "mentalmodel-test")
        self.assertEqual(metric.points[0].attributes[RUN_ID], "run-123")
        self.assertEqual(metric.points[0].attributes[NODE_ID], "sample_policy")
        self.assertEqual(metric.points[0].attributes[FRAME_ID], "steps[2]")
        self.assertEqual(metric.points[0].attributes[LOOP_NODE_ID], "steps")
        self.assertEqual(metric.points[0].attributes[ITERATION_INDEX], 2)
        self.assertEqual(metric.points[0].attributes[RUNTIME_PROFILE], "gpu")
        self.assertEqual(metric.points[0].timestamp_unix_ns, 50)


if __name__ == "__main__":
    unittest.main()
