from __future__ import annotations

import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SpanExporter,
)
from opentelemetry.trace import Span, Tracer

from mentalmodel.observability.config import TracingConfig, TracingMode, load_tracing_config


@dataclass(slots=True, frozen=True)
class RecordedSpan:
    """Serializable span data captured alongside OpenTelemetry spans."""

    span_id: str
    sequence: int
    name: str
    start_time_ns: int
    end_time_ns: int
    attributes: dict[str, str]
    frame_id: str
    loop_node_id: str | None
    iteration_index: int | None
    error_type: str | None = None
    error_message: str | None = None


SpanListener = Callable[[RecordedSpan], None]


@dataclass(slots=True)
class TracingAdapter:
    """Wrapper around an OpenTelemetry tracer plus local span capture."""

    tracer: Tracer
    provider: TracerProvider
    config: TracingConfig
    spans: list[RecordedSpan] = field(default_factory=list)
    listeners: Sequence[SpanListener] = field(default_factory=tuple)
    _sequence: int = 0

    @property
    def sink_configured(self) -> bool:
        return self.config.external_sink_configured

    @contextmanager
    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, str] | None = None,
    ) -> Iterator[Span]:
        start_time_ns = time.time_ns()
        error_type: str | None = None
        error_message: str | None = None
        attrs = dict(attributes or {})
        with self.tracer.start_as_current_span(
            name=name,
            attributes=attrs,
        ) as span:
            try:
                yield span
            except Exception as exc:
                error_type = type(exc).__name__
                error_message = str(exc)
                raise
            finally:
                if self.config.capture_local_spans:
                    self._sequence += 1
                    recorded = RecordedSpan(
                        span_id=_span_id(
                            name=name,
                            start_time_ns=start_time_ns,
                            frame_id=_span_frame_id(attrs),
                            sequence=self._sequence,
                        ),
                        sequence=self._sequence,
                        name=name,
                        start_time_ns=start_time_ns,
                        end_time_ns=time.time_ns(),
                        attributes=attrs,
                        frame_id=_span_frame_id(attrs),
                        loop_node_id=_span_loop_node_id(attrs),
                        iteration_index=_span_iteration_index(attrs),
                        error_type=error_type,
                        error_message=error_message,
                    )
                    self.spans.append(recorded)
                    for listener in self.listeners:
                        listener(recorded)

    def snapshot_spans(self) -> tuple[RecordedSpan, ...]:
        """Return an immutable copy of captured spans."""

        return tuple(self.spans)

    def flush(self) -> None:
        """Flush configured exporters."""

        self.provider.force_flush()

    def trace_summary(self) -> dict[str, str | bool | None]:
        """Return a JSON-safe summary of resolved tracing config."""

        return self.config.summary()


@dataclass(slots=True)
class TracingFactory:
    """Factory for tracer providers and exporters from typed config."""

    config: TracingConfig
    listeners: Sequence[SpanListener] = ()

    def create(self) -> TracingAdapter:
        provider = TracerProvider(resource=self._resource())
        exporter = self._build_exporter()
        if exporter is not None:
            provider.add_span_processor(BatchSpanProcessor(exporter))
        return TracingAdapter(
            tracer=provider.get_tracer("mentalmodel.runtime"),
            provider=provider,
            config=self.config,
            listeners=self.listeners,
        )

    def _resource(self) -> Resource:
        attributes = {"service.name": self.config.service_name}
        if self.config.service_namespace is not None:
            attributes["service.namespace"] = self.config.service_namespace
        if self.config.service_version is not None:
            attributes["service.version"] = self.config.service_version
        return Resource.create(attributes)

    def _build_exporter(self) -> SpanExporter | None:
        if self.config.mode is TracingMode.DISABLED:
            return None
        if self.config.mode is TracingMode.DISK:
            return None
        if self.config.mode is TracingMode.CONSOLE:
            return ConsoleSpanExporter(out=sys.stderr)
        if self.config.mode is TracingMode.OTLP_HTTP:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter as HttpOTLPSpanExporter,
            )

            return HttpOTLPSpanExporter(
                endpoint=self.config.otlp_endpoint,
                headers=self.config.otlp_headers,
            )
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter as GrpcOTLPSpanExporter,
        )

        return GrpcOTLPSpanExporter(
            endpoint=self.config.otlp_endpoint,
            headers=self.config.otlp_headers,
            insecure=self.config.otlp_insecure,
        )


def create_tracing_adapter(
    *,
    config: TracingConfig | None = None,
    service_name: str = "mentalmodel",
    listeners: Sequence[SpanListener] = (),
) -> TracingAdapter:
    """Create a tracing adapter from explicit config or environment defaults."""

    resolved = config or load_tracing_config(service_name=service_name)
    return TracingFactory(resolved, listeners=listeners).create()


def _span_frame_id(attributes: Mapping[str, str]) -> str:
    return attributes.get("mentalmodel.frame.id", "root")


def _span_loop_node_id(attributes: Mapping[str, str]) -> str | None:
    value = attributes.get("mentalmodel.loop.node_id")
    return value if value is not None else None


def _span_iteration_index(attributes: Mapping[str, str]) -> int | None:
    value = attributes.get("mentalmodel.loop.iteration_index")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _span_id(
    *,
    name: str,
    start_time_ns: int,
    frame_id: str,
    sequence: int,
) -> str:
    return f"span-{sequence}:{frame_id}:{start_time_ns}:{name}"
