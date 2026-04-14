from __future__ import annotations

import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

from mentalmodel.analysis import run_analysis
from mentalmodel.core import Actor, ActorHandler, ActorResult, Workflow
from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.ir.lowering import lower_program
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.live import (
    AsyncLiveExporter,
    DurableOutbox,
    LiveIngestionConfig,
    TelemetryEnvelope,
)
from mentalmodel.observability.metrics import MetricDefinition, MetricKind, MetricObservation
from mentalmodel.observability.telemetry import TelemetryResourceContext
from mentalmodel.observability.tracing import RecordedSpan
from mentalmodel.runtime.context import ExecutionContext


class _NoOpHandler(ActorHandler[dict[str, object], object, str]):
    async def handle(
        self,
        inputs: dict[str, object],
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[str, object]:
        del inputs, state, ctx
        return ActorResult(output="ok")


def _build_program() -> Workflow[NamedPrimitive]:
    return Workflow(
        name="live_ingestion_graph",
        children=[Actor(name="source", handler=_NoOpHandler())],
    )


class _CaptureServer(HTTPServer):
    def __init__(self, server_address: tuple[str, int]) -> None:
        super().__init__(server_address, _CaptureHandler)
        self.log_requests: list[ExportLogsServiceRequest] = []
        self.trace_requests: list[ExportTraceServiceRequest] = []
        self.metric_requests: list[ExportMetricsServiceRequest] = []


class _CaptureHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers["Content-Length"])
        body = self.rfile.read(length)
        if self.path.endswith("/v1/logs"):
            request = ExportLogsServiceRequest()
            request.ParseFromString(body)
            self.server.log_requests.append(request)  # type: ignore[attr-defined]
        elif self.path.endswith("/v1/traces"):
            request = ExportTraceServiceRequest()
            request.ParseFromString(body)
            self.server.trace_requests.append(request)  # type: ignore[attr-defined]
        elif self.path.endswith("/v1/metrics"):
            request = ExportMetricsServiceRequest()
            request.ParseFromString(body)
            self.server.metric_requests.append(request)  # type: ignore[attr-defined]
        else:  # pragma: no cover - defensive
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        del format, args


class LiveIngestionTest(unittest.TestCase):
    def test_durable_outbox_replays_claimed_rows_after_lease_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            outbox_root = Path(tmpdir) / "outbox"
            outbox = DurableOutbox(root=outbox_root, max_bytes=1024)
            outbox.append(
                (
                    TelemetryEnvelope(
                        kind="delivery_health",
                        payload={"message": "one"},
                        created_at_ms=100,
                    ),
                )
            )

            claimed = outbox.claim_batch(max_events=10, max_bytes=1024, now_ms=200, lease_ms=10)
            assert claimed is not None

            reopened = DurableOutbox(root=outbox_root, max_bytes=1024)
            replayed = reopened.claim_batch(
                max_events=10,
                max_bytes=1024,
                now_ms=211,
                lease_ms=10,
            )

            assert replayed is not None
            self.assertEqual(len(replayed.envelopes), 1)
            self.assertEqual(replayed.envelopes[0].envelope.payload["message"], "one")
            reopened.acknowledge(replayed.token)
            self.assertEqual(reopened.stats().depth, 0)

    def test_durable_outbox_enforces_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            outbox = DurableOutbox(root=Path(tmpdir) / "outbox", max_bytes=32)
            with self.assertRaisesRegex(Exception, "hard cap"):
                outbox.append(
                    (
                        TelemetryEnvelope(
                            kind="delivery_health",
                            payload={"message": "x" * 128},
                            created_at_ms=100,
                        ),
                    )
                )

    def test_async_live_exporter_exports_logs_spans_and_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            server = _CaptureServer(("127.0.0.1", 0))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                graph = lower_program(_build_program())
                exporter = AsyncLiveExporter(
                    config=LiveIngestionConfig(
                        otlp_endpoint=f"http://127.0.0.1:{server.server_port}",
                        outbox_dir=Path(tmpdir) / "outbox",
                        flush_interval_ms=25,
                    ),
                    run_id="run-live",
                    invocation_name="smoke",
                    resource_context=TelemetryResourceContext(
                        project_id="proj-1",
                        service_name="mentalmodel-test",
                    ),
                )
                exporter.start(graph=graph, analysis=run_analysis(graph))
                exporter.emit_record(
                    ExecutionRecord(
                        record_id="run-live:1",
                        run_id="run-live",
                        node_id="source",
                        event_type="node.started",
                        sequence=1,
                        timestamp_ms=1_710_000_000_000,
                        payload={"detail": "started"},
                    )
                )
                exporter.emit_span(
                    RecordedSpan(
                        span_id="span-1",
                        sequence=1,
                        name="actor:source",
                        start_time_ns=10,
                        end_time_ns=25,
                        attributes={
                            "mentalmodel.run_id": "run-live",
                            "mentalmodel.node_id": "source",
                            "mentalmodel.frame_id": "root",
                        },
                        frame_id="root",
                        loop_node_id=None,
                        iteration_index=None,
                        trace_id="0" * 32,
                        otel_span_id="1" * 16,
                        parent_span_id=None,
                    )
                )
                exporter.emit_metrics(
                    (
                        MetricObservation(
                            definition=MetricDefinition(
                                name="mentalmodel.node.duration_ms",
                                kind=MetricKind.HISTOGRAM,
                                unit="ms",
                            ),
                            value=12.5,
                            attributes={
                                "graph_id": graph.graph_id,
                                "run_id": "run-live",
                                "service_name": "mentalmodel-test",
                                "node_id": "source",
                                "frame_id": "root",
                            },
                        ),
                    )
                )
                exporter.complete(success=True)

                result = exporter.delivery_result()
                assert result is not None
                self.assertTrue(result.success)
                self.assertEqual(result.outbox_depth, 0)
                self.assertGreaterEqual(result.exported_log_count, 3)
                self.assertEqual(result.exported_span_count, 1)
                self.assertEqual(result.exported_metric_count, 1)
                self.assertGreaterEqual(len(server.log_requests), 1)
                self.assertGreaterEqual(len(server.trace_requests), 1)
                self.assertGreaterEqual(len(server.metric_requests), 1)
                first_log_request = server.log_requests[0]
                self.assertEqual(
                    first_log_request.resource_logs[0].resource.attributes[0].key,
                    "mentalmodel.graph_id",
                )
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
