from __future__ import annotations

from collections.abc import Mapping
from typing import Final, TypeAlias

TelemetryScalar: TypeAlias = str | bool | int | float
TelemetryAttributeValue: TypeAlias = TelemetryScalar

SERVICE_NAME: Final[str] = "service.name"
SERVICE_NAMESPACE: Final[str] = "service.namespace"
SERVICE_VERSION: Final[str] = "service.version"

GRAPH_ID: Final[str] = "mentalmodel.graph_id"
RUN_ID: Final[str] = "mentalmodel.run_id"
RECORD_ID: Final[str] = "mentalmodel.record_id"
NODE_ID: Final[str] = "mentalmodel.node_id"
NODE_KIND: Final[str] = "mentalmodel.node_kind"
FRAME_ID: Final[str] = "mentalmodel.frame_id"
LOOP_NODE_ID: Final[str] = "mentalmodel.loop_node_id"
ITERATION_INDEX: Final[str] = "mentalmodel.iteration_index"
EVENT_TYPE: Final[str] = "mentalmodel.event_type"
SEQUENCE: Final[str] = "mentalmodel.sequence"
INVOCATION_NAME: Final[str] = "mentalmodel.invocation_name"
RUNTIME_CONTEXT: Final[str] = "mentalmodel.runtime_context"
RUNTIME_PROFILE: Final[str] = "mentalmodel.runtime_profile"
PROJECT_ID: Final[str] = "mentalmodel.project_id"
PROJECT_LABEL: Final[str] = "mentalmodel.project_label"
ENVIRONMENT_NAME: Final[str] = "mentalmodel.environment_name"
CATALOG_ENTRY_ID: Final[str] = "mentalmodel.catalog_entry_id"
CATALOG_SOURCE: Final[str] = "mentalmodel.catalog_source"
ERROR_TYPE: Final[str] = "exception.type"
ERROR_MESSAGE: Final[str] = "exception.message"

PAYLOAD_ATTRIBUTE_PREFIX: Final[str] = "mentalmodel.payload."

LEGACY_TRACE_ATTRIBUTE_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    NODE_ID: ("mentalmodel.node.id",),
    FRAME_ID: ("mentalmodel.frame.id",),
    LOOP_NODE_ID: ("mentalmodel.loop.node_id",),
    ITERATION_INDEX: ("mentalmodel.loop.iteration_index",),
    INVOCATION_NAME: ("mentalmodel.invocation.name",),
    RUNTIME_CONTEXT: ("mentalmodel.runtime.context",),
    RUNTIME_PROFILE: ("mentalmodel.runtime.profile",),
}

_LEGACY_TO_CANONICAL: Final[dict[str, str]] = {
    alias: canonical
    for canonical, aliases in LEGACY_TRACE_ATTRIBUTE_ALIASES.items()
    for alias in aliases
}
_INTEGER_KEYS: Final[frozenset[str]] = frozenset({ITERATION_INDEX, SEQUENCE})


def canonical_attribute_key(key: str) -> str:
    """Return the stable canonical telemetry attribute key."""

    return _LEGACY_TO_CANONICAL.get(key, key)


def canonicalize_attributes(
    attributes: Mapping[str, object],
) -> dict[str, TelemetryAttributeValue]:
    """Normalize telemetry attributes onto the canonical naming scheme."""

    normalized: dict[str, TelemetryAttributeValue] = {}
    for raw_key, raw_value in attributes.items():
        canonical_key = canonical_attribute_key(raw_key)
        value = _coerce_value(canonical_key, raw_value)
        if value is None:
            continue
        normalized[canonical_key] = value
    return normalized


def with_legacy_trace_aliases(
    attributes: Mapping[str, TelemetryAttributeValue],
) -> dict[str, TelemetryAttributeValue]:
    """Mirror canonical span attributes under legacy keys during migration."""

    expanded = dict(attributes)
    for canonical_key, aliases in LEGACY_TRACE_ATTRIBUTE_ALIASES.items():
        value = expanded.get(canonical_key)
        if value is None:
            continue
        for alias in aliases:
            expanded.setdefault(alias, value)
    return expanded


def prefixed_payload_attribute(field_name: str) -> str:
    """Return the canonical attribute key for one promoted payload field."""

    return f"{PAYLOAD_ATTRIBUTE_PREFIX}{field_name}"


def _coerce_value(
    key: str,
    value: object,
) -> TelemetryAttributeValue | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        if key in _INTEGER_KEYS:
            try:
                return int(value)
            except ValueError:
                return None
        return value
    return None
