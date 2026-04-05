from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeVar

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.core.models import InvariantResult
from mentalmodel.ir.graph import IRGraph

Numeric = int | float
DetailT = TypeVar("DetailT", bound=JsonValue)


@dataclass(slots=True, frozen=True)
class BoundaryObservation:
    """One runtime-context crossing observed in a lowered graph."""

    producer_node_id: str
    consumer_node_id: str
    producer_runtime: str
    consumer_runtime: str

    def as_tuple(self) -> tuple[str, str]:
        return (self.producer_runtime, self.consumer_runtime)


def aligned_key_sets(*mappings: Mapping[str, object]) -> frozenset[str] | None:
    """Return the shared key set when every mapping aligns, else ``None``."""

    if not mappings:
        return frozenset()
    first = frozenset(mappings[0].keys())
    if all(frozenset(mapping.keys()) == first for mapping in mappings[1:]):
        return first
    return None


def assert_aligned_key_sets(
    *mappings: Mapping[str, object],
    expected_keys: Collection[str] | None = None,
    labels: Sequence[str] | None = None,
) -> frozenset[str]:
    """Assert that every mapping exposes the same key set."""

    shared = aligned_key_sets(*mappings)
    if shared is None:
        observed = [
            sorted(mapping.keys())
            for mapping in mappings
        ]
        named = _labelled_sets(labels=labels, observed=observed)
        raise AssertionError(f"expected aligned key sets, got {named}")
    if expected_keys is not None:
        expected = frozenset(expected_keys)
        if shared != expected:
            raise AssertionError(
                f"expected keys {sorted(expected)}, got {sorted(shared)}"
            )
    return shared


def is_monotonic_non_decreasing(values: Sequence[Numeric]) -> bool:
    """Return whether the numeric sequence never decreases."""

    return all(
        previous <= current
        for previous, current in zip(values, values[1:], strict=False)
    )


def assert_monotonic_non_decreasing(
    values: Sequence[Numeric],
    *,
    label: str = "values",
) -> None:
    """Assert that the numeric sequence never decreases."""

    if is_monotonic_non_decreasing(values):
        return
    raise AssertionError(f"expected {label} to be monotonic non-decreasing: {list(values)}")


def assert_causal_order(
    observed: int,
    current: int,
    *,
    max_lag: int,
) -> None:
    """Assert that the observed value does not exceed current and stays within lag."""

    if observed > current:
        raise AssertionError(
            f"expected observed={observed} to be <= current={current}"
        )
    lag = current - observed
    if lag > max_lag:
        raise AssertionError(
            f"expected lag={lag} to be <= max_lag={max_lag}"
        )


def collect_runtime_boundary_observations(graph: IRGraph) -> tuple[BoundaryObservation, ...]:
    """Collect data-edge runtime-context crossings from a lowered graph."""

    metadata_by_node = {node.node_id: node.metadata for node in graph.nodes}
    observations: list[BoundaryObservation] = []
    for edge in graph.edges:
        if edge.kind != "data":
            continue
        source_metadata = metadata_by_node.get(edge.source_node_id, {})
        target_metadata = metadata_by_node.get(edge.target_node_id, {})
        source_runtime = source_metadata.get("runtime_context")
        target_runtime = target_metadata.get("runtime_context")
        if source_runtime is None or target_runtime is None:
            continue
        if source_runtime == target_runtime:
            continue
        observations.append(
            BoundaryObservation(
                producer_node_id=edge.source_node_id,
                consumer_node_id=edge.target_node_id,
                producer_runtime=source_runtime,
                consumer_runtime=target_runtime,
            )
        )
    return tuple(observations)


def assert_runtime_boundary_crossings(
    observations: Sequence[BoundaryObservation],
    *,
    allowed: Collection[tuple[str, str]],
) -> None:
    """Assert that every runtime-context crossing is explicitly allowed."""

    unexpected = [
        observation
        for observation in observations
        if observation.as_tuple() not in allowed
    ]
    if not unexpected:
        return
    rendered = ", ".join(
        (
            f"{observation.producer_node_id}({observation.producer_runtime})"
            f" -> {observation.consumer_node_id}({observation.consumer_runtime})"
        )
        for observation in unexpected
    )
    raise AssertionError(f"unexpected runtime-context crossings: {rendered}")


def invariant_pass(
    *,
    details: Mapping[str, DetailT] | None = None,
) -> InvariantResult[DetailT]:
    """Build a passing invariant result with typed details."""

    return InvariantResult(passed=True, details={} if details is None else details)


def invariant_fail(
    *,
    details: Mapping[str, DetailT],
) -> InvariantResult[DetailT]:
    """Build a failing invariant result with typed details."""

    return InvariantResult(passed=False, details=details)


def _labelled_sets(
    *,
    labels: Sequence[str] | None,
    observed: Sequence[Sequence[str]],
) -> str:
    if labels is None:
        return str([list(keys) for keys in observed])
    rendered: list[str] = []
    for label, keys in zip(labels, observed, strict=False):
        rendered.append(f"{label}={list(keys)}")
    if len(observed) > len(rendered):
        rendered.extend(str(list(keys)) for keys in observed[len(rendered) :])
    return ", ".join(rendered)
