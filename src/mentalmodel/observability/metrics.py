from __future__ import annotations

import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Generic, Protocol, TextIO, TypeVar, cast
from urllib.parse import urlparse, urlunparse

from opentelemetry.metrics import Counter, Histogram, Meter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource

from mentalmodel.errors import LiveDeliveryCapacityError
from mentalmodel.observability.config import TracingConfig, TracingMode, load_tracing_config
from mentalmodel.observability.semantic_conventions import TelemetryAttributeValue

MetricAttributeValue = TelemetryAttributeValue
OutputT = TypeVar("OutputT")
OutputT_contra = TypeVar("OutputT_contra", contravariant=True)

SAFE_SUMMARY_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
MAX_INFERRED_FIELDS = 8


class MetricKind(StrEnum):
    """Supported metric instrument kinds."""

    COUNTER = "counter"
    HISTOGRAM = "histogram"


@dataclass(slots=True, frozen=True)
class MetricContext:
    """Stable metric context shared across emitters and extractors."""

    graph_id: str
    run_id: str
    node_id: str | None
    node_kind: str | None
    runtime_context: str | None
    frame_id: str
    loop_node_id: str | None
    iteration_index: int | None
    service_name: str
    runtime_profile: str | None = None
    invocation_name: str | None = None

    def default_attributes(self) -> dict[str, MetricAttributeValue]:
        attributes: dict[str, MetricAttributeValue] = {
            "graph_id": self.graph_id,
            "run_id": self.run_id,
            "service_name": self.service_name,
            "frame_id": self.frame_id,
        }
        if self.node_id is not None:
            attributes["node_id"] = self.node_id
        if self.node_kind is not None:
            attributes["node_kind"] = self.node_kind
        if self.runtime_context is not None:
            attributes["runtime_context"] = self.runtime_context
        if self.loop_node_id is not None:
            attributes["loop_node_id"] = self.loop_node_id
        if self.iteration_index is not None:
            attributes["iteration_index"] = self.iteration_index
        if self.runtime_profile is not None:
            attributes["runtime_profile"] = self.runtime_profile
        if self.invocation_name is not None:
            attributes["invocation_name"] = self.invocation_name
        return attributes


@dataclass(slots=True, frozen=True)
class MetricDefinition:
    """Declared metric shape independent of exporter details."""

    name: str
    kind: MetricKind
    description: str = ""
    unit: str | None = None
    attribute_keys: tuple[str, ...] = field(default_factory=tuple)


@dataclass(slots=True, frozen=True)
class MetricObservation:
    """Concrete metric value plus stable attributes."""

    definition: MetricDefinition
    value: int | float
    attributes: dict[str, MetricAttributeValue] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class MetricFieldProjection:
    """Stable projection of one numeric field from a provider metric map."""

    source_key: str
    metric_name: str
    kind: MetricKind = MetricKind.HISTOGRAM
    unit: str | None = None
    description: str = ""
    attributes: Mapping[str, MetricAttributeValue] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class MetricMapProjection:
    """Projection policy for a stable subset of one metric map."""

    fields: tuple[MetricFieldProjection, ...]
    metric_map_key: str | None = None
    metric_name_prefix: str | None = None

    def __post_init__(self) -> None:
        if not self.fields:
            raise ValueError("MetricMapProjection requires at least one projected field.")
        emitted_names: set[str] = set()
        for projection_field in self.fields:
            if not projection_field.source_key:
                raise ValueError("MetricFieldProjection.source_key must not be empty.")
            metric_name = _projected_metric_name(
                self.metric_name_prefix,
                projection_field.metric_name,
            )
            if not metric_name:
                raise ValueError("MetricFieldProjection.metric_name must not be empty.")
            if metric_name in emitted_names:
                raise ValueError(
                    f"MetricMapProjection defines duplicate metric name {metric_name!r}."
                )
            emitted_names.add(metric_name)


class MetricExtractor(Protocol[OutputT_contra]):
    """Extract metric observations from one typed node output."""

    def extract(
        self,
        output: OutputT_contra,
        context: MetricContext,
    ) -> Sequence[MetricObservation]:
        """Extract zero or more metric observations."""


@dataclass(slots=True, frozen=True)
class OutputMetricSpec(Generic[OutputT]):
    """Metric extraction policy attached to one primitive output."""

    extractor: MetricExtractor[OutputT] | None = None
    infer_summary_metrics: bool = False
    prefix: str | None = None


MetricMapAccessor = Callable[[OutputT], Mapping[str, object] | None]


def infer_output_metrics(*, prefix: str | None = None) -> OutputMetricSpec[OutputT]:
    """Create a spec that safely infers metrics from flat numeric summaries."""

    return OutputMetricSpec(extractor=None, infer_summary_metrics=True, prefix=prefix)


def extract_output_metrics(
    extractor: MetricExtractor[OutputT],
) -> OutputMetricSpec[OutputT]:
    """Create a spec that emits metrics through one explicit extractor."""

    return OutputMetricSpec(extractor=extractor, infer_summary_metrics=False, prefix=None)


def project_metric_map(
    projection: MetricMapProjection,
    *,
    accessor: MetricMapAccessor[OutputT] | None = None,
) -> OutputMetricSpec[OutputT]:
    """Project a stable numeric subset from a typed provider metric map."""

    return extract_output_metrics(
        _MetricMapProjectionExtractor(
            projection=projection,
            accessor=accessor,
        )
    )


def project_flat_metric_map(
    *,
    prefix: str,
    fields: Sequence[str],
    metric_map_key: str | None = None,
    accessor: MetricMapAccessor[OutputT] | None = None,
    kind: MetricKind = MetricKind.HISTOGRAM,
) -> OutputMetricSpec[OutputT]:
    """Project named numeric fields under one stable metric prefix."""

    return project_metric_map(
        MetricMapProjection(
            metric_map_key=metric_map_key,
            metric_name_prefix=prefix,
            fields=tuple(
                MetricFieldProjection(
                    source_key=field_name,
                    metric_name=field_name,
                    kind=kind,
                )
                for field_name in fields
            ),
        ),
        accessor=accessor,
    )


class MetricEmitter(Protocol):
    """Exporter-neutral metric emission interface."""

    def emit(self, observations: Sequence[MetricObservation]) -> None:
        """Emit a batch of observations."""

    def flush(self) -> None:
        """Flush any buffered observations."""


@dataclass(slots=True)
class NoOpMetricEmitter:
    """Metric emitter used when external metrics are disabled."""

    def emit(self, observations: Sequence[MetricObservation]) -> None:
        del observations

    def flush(self) -> None:
        return None


@dataclass(slots=True)
class RecordingMetricEmitter:
    """In-memory metric emitter for deterministic tests and local evaluation."""

    observations: list[MetricObservation] = field(default_factory=list)
    flush_calls: int = 0

    def emit(self, observations: Sequence[MetricObservation]) -> None:
        self.observations.extend(observations)

    def flush(self) -> None:
        self.flush_calls += 1

    def snapshot(self) -> tuple[MetricObservation, ...]:
        return tuple(self.observations)


@dataclass(slots=True)
class ConsoleMetricEmitter:
    """Synchronous console metric emitter used for local debugging."""

    stream: TextIO

    def emit(self, observations: Sequence[MetricObservation]) -> None:
        for observation in observations:
            self.stream.write(
                f"{observation.definition.name} "
                f"value={observation.value!r} "
                f"attributes={observation.attributes!r}\n"
            )

    def flush(self) -> None:
        self.stream.flush()


@dataclass(slots=True)
class OTelMetricEmitter:
    """OpenTelemetry-backed metric emitter."""

    provider: MeterProvider
    meter: Meter
    counters: dict[str, Counter]
    histograms: dict[str, Histogram]

    def emit(self, observations: Sequence[MetricObservation]) -> None:
        for observation in observations:
            if observation.definition.kind is MetricKind.COUNTER:
                counter = self._counter(observation.definition)
                counter.add(observation.value, attributes=observation.attributes)
                continue
            histogram = self._histogram(observation.definition)
            histogram.record(observation.value, attributes=observation.attributes)

    def flush(self) -> None:
        self.provider.force_flush()

    def _counter(self, definition: MetricDefinition) -> Counter:
        instrument = self.counters.get(definition.name)
        if instrument is None:
            instrument = self.meter.create_counter(
                name=definition.name,
                description=definition.description,
                unit=definition.unit or "",
            )
            self.counters[definition.name] = instrument
        return instrument

    def _histogram(self, definition: MetricDefinition) -> Histogram:
        instrument = self.histograms.get(definition.name)
        if instrument is None:
            instrument = self.meter.create_histogram(
                name=definition.name,
                description=definition.description,
                unit=definition.unit or "",
            )
            self.histograms[definition.name] = instrument
        return instrument


@dataclass(slots=True, frozen=True)
class _MetricMapProjectionExtractor(Generic[OutputT]):
    projection: MetricMapProjection
    accessor: MetricMapAccessor[OutputT] | None = None

    def extract(
        self,
        output: OutputT,
        context: MetricContext,
    ) -> Sequence[MetricObservation]:
        metric_map = _resolve_metric_map(
            output=output,
            metric_map_key=self.projection.metric_map_key,
            accessor=self.accessor,
        )
        if metric_map is None:
            return tuple()
        base_attributes = context.default_attributes()
        observations: list[MetricObservation] = []
        for projection_field in self.projection.fields:
            numeric_value = _projectable_numeric(metric_map.get(projection_field.source_key))
            if numeric_value is None:
                continue
            attributes = dict(base_attributes)
            attributes.update(projection_field.attributes)
            observations.append(
                MetricObservation(
                    definition=MetricDefinition(
                        name=_projected_metric_name(
                            self.projection.metric_name_prefix,
                            projection_field.metric_name,
                        ),
                        kind=projection_field.kind,
                        description=projection_field.description,
                        unit=projection_field.unit,
                    ),
                    value=numeric_value,
                    attributes=attributes,
                )
            )
        return tuple(observations)


def create_metric_emitter(
    *,
    config: TracingConfig | None = None,
    service_name: str = "mentalmodel",
) -> MetricEmitter:
    """Create a metric emitter from resolved observability config."""

    resolved = config or load_tracing_config(service_name=service_name)
    if resolved.mode in {TracingMode.DISK, TracingMode.DISABLED}:
        return NoOpMetricEmitter()
    if resolved.mode is TracingMode.CONSOLE:
        return ConsoleMetricEmitter(stream=sys.stderr)

    reader = PeriodicExportingMetricReader(_build_metric_exporter(resolved))
    provider = MeterProvider(
        metric_readers=[reader],
        resource=_resource(resolved),
    )
    return OTelMetricEmitter(
        provider=provider,
        meter=provider.get_meter("mentalmodel.runtime"),
        counters={},
        histograms={},
    )


def _resolve_metric_map(
    *,
    output: OutputT,
    metric_map_key: str | None,
    accessor: MetricMapAccessor[OutputT] | None,
) -> Mapping[str, object] | None:
    if accessor is not None:
        return accessor(output)
    if metric_map_key is None:
        if not isinstance(output, Mapping):
            return None
        return cast(Mapping[str, object], output)
    if not isinstance(output, Mapping):
        return None
    nested_value = output.get(metric_map_key)
    if not isinstance(nested_value, Mapping):
        return None
    return cast(Mapping[str, object], nested_value)


def _projectable_numeric(value: object) -> int | float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return value


def _projected_metric_name(prefix: str | None, metric_name: str) -> str:
    if prefix is None:
        return metric_name
    return f"{prefix}.{metric_name}"


def derive_output_metrics(
    *,
    output: object,
    context: MetricContext,
    specs: Sequence[OutputMetricSpec[object]],
) -> tuple[MetricObservation, ...]:
    """Derive output metrics from attached specs and safe inference."""

    observations: list[MetricObservation] = []
    for spec in specs:
        if spec.extractor is not None:
            observations.extend(spec.extractor.extract(output, context))
        if spec.infer_summary_metrics:
            observations.extend(
                infer_output_metric_observations(
                    output=output,
                    context=context,
                    prefix=spec.prefix,
                )
            )
    return tuple(observations)


def infer_output_metric_observations(
    *,
    output: object,
    context: MetricContext,
    prefix: str | None = None,
) -> tuple[MetricObservation, ...]:
    """Infer metric observations from a flat bounded numeric mapping."""

    if not isinstance(output, Mapping):
        return tuple()
    if not output or len(output) > MAX_INFERRED_FIELDS:
        return tuple()
    items: list[tuple[str, int | float]] = []
    for key, value in output.items():
        if not isinstance(key, str) or SAFE_SUMMARY_KEY.fullmatch(key) is None:
            return tuple()
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return tuple()
        items.append((key, value))
    node_token = (
        context.node_id.replace("-", "_")
        if context.node_id is not None
        else "output"
    )
    base = prefix or f"mentalmodel.output.{node_token}"
    return tuple(
        MetricObservation(
            definition=MetricDefinition(
                name=f"{base}.{field_name}",
                kind=MetricKind.HISTOGRAM,
                description=f"Inferred numeric output field {field_name!r} from {node_token!r}.",
            ),
            value=numeric_value,
            attributes=context.default_attributes(),
        )
        for field_name, numeric_value in sorted(items)
    )


RUN_STARTED = MetricDefinition(
    name="mentalmodel.run.started",
    kind=MetricKind.COUNTER,
    description="Number of started mentalmodel runs.",
    attribute_keys=(
        "graph_id",
        "run_id",
        "service_name",
        "frame_id",
        "runtime_profile",
        "invocation_name",
    ),
)
RUN_COMPLETED = MetricDefinition(
    name="mentalmodel.run.completed",
    kind=MetricKind.COUNTER,
    description="Number of completed mentalmodel runs.",
    attribute_keys=(
        "graph_id",
        "run_id",
        "service_name",
        "frame_id",
        "runtime_profile",
        "invocation_name",
        "success",
    ),
)
NODE_EXECUTIONS = MetricDefinition(
    name="mentalmodel.node.executions",
    kind=MetricKind.COUNTER,
    description="Number of executed mentalmodel nodes.",
    attribute_keys=(
        "graph_id",
        "run_id",
        "service_name",
        "node_id",
        "node_kind",
        "frame_id",
        "loop_node_id",
        "iteration_index",
        "runtime_context",
        "runtime_profile",
        "invocation_name",
    ),
)
NODE_DURATION_MS = MetricDefinition(
    name="mentalmodel.node.duration_ms",
    kind=MetricKind.HISTOGRAM,
    description="Execution duration for mentalmodel nodes in milliseconds.",
    unit="ms",
    attribute_keys=(
        "graph_id",
        "run_id",
        "service_name",
        "node_id",
        "node_kind",
        "frame_id",
        "loop_node_id",
        "iteration_index",
        "runtime_context",
        "runtime_profile",
        "invocation_name",
        "success",
    ),
)
INVARIANT_FAILURES = MetricDefinition(
    name="mentalmodel.invariant.failures",
    kind=MetricKind.COUNTER,
    description="Number of failed runtime invariants.",
    attribute_keys=(
        "graph_id",
        "run_id",
        "service_name",
        "frame_id",
        "loop_node_id",
        "iteration_index",
        "runtime_context",
        "runtime_profile",
        "invocation_name",
        "severity",
    ),
)


def run_started_observation(context: MetricContext) -> MetricObservation:
    return MetricObservation(
        definition=RUN_STARTED,
        value=1,
        attributes=context.default_attributes(),
    )


def run_completed_observation(
    context: MetricContext,
    *,
    success: bool,
) -> MetricObservation:
    attributes = context.default_attributes()
    attributes["success"] = success
    return MetricObservation(
        definition=RUN_COMPLETED,
        value=1,
        attributes=attributes,
    )


def node_execution_observation(context: MetricContext) -> MetricObservation:
    return MetricObservation(
        definition=NODE_EXECUTIONS,
        value=1,
        attributes=context.default_attributes(),
    )


def node_duration_observation(
    context: MetricContext,
    *,
    duration_ms: float,
    success: bool,
) -> MetricObservation:
    attributes = context.default_attributes()
    attributes["success"] = success
    return MetricObservation(
        definition=NODE_DURATION_MS,
        value=duration_ms,
        attributes=attributes,
    )


def invariant_failure_observation(
    context: MetricContext,
    *,
    severity: str,
) -> MetricObservation:
    attributes = context.default_attributes()
    attributes["severity"] = severity
    return MetricObservation(
        definition=INVARIANT_FAILURES,
        value=1,
        attributes=attributes,
    )


def _resource(config: TracingConfig) -> Resource:
    attributes = {"service.name": config.service_name}
    if config.service_namespace is not None:
        attributes["service.namespace"] = config.service_namespace
    if config.service_version is not None:
        attributes["service.version"] = config.service_version
    return Resource.create(attributes)


def _build_metric_exporter(config: TracingConfig) -> MetricExporter:
    if config.mode is TracingMode.OTLP_HTTP:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter as HttpOTLPMetricExporter,
        )

        return HttpOTLPMetricExporter(
            endpoint=_metric_http_endpoint(config.otlp_endpoint),
            headers=config.otlp_headers,
        )
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
        OTLPMetricExporter as GrpcOTLPMetricExporter,
    )

    return GrpcOTLPMetricExporter(
        endpoint=config.otlp_endpoint,
        headers=config.otlp_headers,
        insecure=config.otlp_insecure,
    )


def _metric_http_endpoint(endpoint: str | None) -> str:
    if endpoint is None:
        raise ValueError("HTTP metric exporter requires an endpoint.")
    parsed = urlparse(endpoint)
    path = parsed.path.rstrip("/")
    if path.endswith("/v1/metrics"):
        return endpoint
    if path.endswith("/v1/traces"):
        path = path[: -len("/v1/traces")] + "/v1/metrics"
    elif not path:
        path = "/v1/metrics"
    else:
        path = f"{path}/v1/metrics"
    return urlunparse(parsed._replace(path=path))


def emit_metric_batch(
    emitter: MetricEmitter,
    observations: Sequence[MetricObservation],
) -> None:
    """Emit metrics without letting exporter failures break runtime execution."""

    if not observations:
        return
    try:
        emitter.emit(observations)
    except LiveDeliveryCapacityError:
        raise
    except Exception:
        return


def cast_metric_specs(
    specs: Sequence[OutputMetricSpec[OutputT]],
) -> tuple[OutputMetricSpec[object], ...]:
    """Erase output typing at the runtime boundary without using ``Any``."""

    return tuple(cast(OutputMetricSpec[object], spec) for spec in specs)
