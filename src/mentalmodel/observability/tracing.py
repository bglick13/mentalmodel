from __future__ import annotations

import os
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import Span, Tracer


@dataclass(slots=True, frozen=True)
class RecordedSpan:
    """Serializable span data captured alongside OpenTelemetry spans."""

    name: str
    start_time_ns: int
    end_time_ns: int
    attributes: dict[str, str]
    error_type: str | None = None
    error_message: str | None = None


@dataclass(slots=True)
class TracingAdapter:
    """Small wrapper around an OpenTelemetry tracer."""

    tracer: Tracer
    sink_configured: bool
    spans: list[RecordedSpan] = field(default_factory=list)

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
        with self.tracer.start_as_current_span(
            name=name,
            attributes=dict(attributes or {}),
        ) as span:
            try:
                yield span
            except Exception as exc:
                error_type = type(exc).__name__
                error_message = str(exc)
                raise
            finally:
                self.spans.append(
                    RecordedSpan(
                        name=name,
                        start_time_ns=start_time_ns,
                        end_time_ns=time.time_ns(),
                        attributes=dict(attributes or {}),
                        error_type=error_type,
                        error_message=error_message,
                    )
                )

    def snapshot_spans(self) -> tuple[RecordedSpan, ...]:
        """Return an immutable copy of captured spans."""

        return tuple(self.spans)


@dataclass(slots=True)
class InMemoryTracingFactory:
    """Factory for an in-process tracer provider."""

    service_name: str = "mentalmodel"
    provider: TracerProvider = field(init=False)

    def __post_init__(self) -> None:
        self.provider = TracerProvider(
            resource=Resource.create({"service.name": self.service_name})
        )

    def create(self) -> TracingAdapter:
        return TracingAdapter(
            tracer=self.provider.get_tracer("mentalmodel.runtime"),
            sink_configured=detect_external_tracing_sink(),
        )


def create_tracing_adapter(service_name: str = "mentalmodel") -> TracingAdapter:
    """Create a default tracing adapter for the runtime."""

    return InMemoryTracingFactory(service_name=service_name).create()


def detect_external_tracing_sink() -> bool:
    """Return whether the process appears to have an external trace sink configured."""

    return any(
        os.getenv(var_name)
        for var_name in (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_TRACES_EXPORTER",
        )
    )
