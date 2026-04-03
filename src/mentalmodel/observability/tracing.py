from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import Span, Tracer


@dataclass(slots=True)
class TracingAdapter:
    """Small wrapper around an OpenTelemetry tracer."""

    tracer: Tracer

    @contextmanager
    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, str] | None = None,
    ) -> Iterator[Span]:
        with self.tracer.start_as_current_span(
            name=name,
            attributes=dict(attributes or {}),
        ) as span:
            yield span


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
        )


def create_tracing_adapter(service_name: str = "mentalmodel") -> TracingAdapter:
    """Create a default tracing adapter for the runtime."""

    return InMemoryTracingFactory(service_name=service_name).create()
