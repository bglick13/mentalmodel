from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.runtime.events import INVARIANT_CHECKED
from mentalmodel.runtime.runs import (
    RunSummary,
    load_run_payload,
    load_run_records,
    resolve_run_summary,
)


@dataclass(slots=True, frozen=True)
class ReplayEvent:
    """Normalized semantic event for persisted run replay."""

    sequence: int
    node_id: str
    frame_id: str
    frame_path: tuple[dict[str, JsonValue], ...]
    loop_node_id: str | None
    iteration_index: int | None
    event_type: str
    timestamp_ms: int
    payload: dict[str, JsonValue]

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "sequence": self.sequence,
            "node_id": self.node_id,
            "frame_id": self.frame_id,
            "frame_path": [dict(segment) for segment in self.frame_path],
            "loop_node_id": self.loop_node_id,
            "iteration_index": self.iteration_index,
            "event_type": self.event_type,
            "timestamp_ms": self.timestamp_ms,
            "payload": self.payload,
        }


@dataclass(slots=True, frozen=True)
class ReplayNodeSummary:
    """Per-node replay summary derived from semantic records."""

    node_id: str
    frame_id: str
    frame_path: tuple[dict[str, JsonValue], ...]
    loop_node_id: str | None
    iteration_index: int | None
    event_count: int
    first_sequence: int | None
    last_sequence: int | None
    last_event_type: str | None
    succeeded: bool
    failed: bool
    invariant_passed: bool | None
    invariant_severity: str | None
    invariant_status: str | None

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "node_id": self.node_id,
            "frame_id": self.frame_id,
            "frame_path": [dict(segment) for segment in self.frame_path],
            "loop_node_id": self.loop_node_id,
            "iteration_index": self.iteration_index,
            "event_count": self.event_count,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
            "last_event_type": self.last_event_type,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "invariant_passed": self.invariant_passed,
            "invariant_severity": self.invariant_severity,
            "invariant_status": self.invariant_status,
        }


@dataclass(slots=True, frozen=True)
class ReplayReport:
    """Replay projection for one persisted run bundle."""

    summary: RunSummary
    verification_success: bool | None
    runtime_error: str | None
    events: tuple[ReplayEvent, ...]
    node_summaries: tuple[ReplayNodeSummary, ...]
    frame_ids: tuple[str, ...]
    output_node_ids: tuple[str, ...]
    state_node_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "graph_id": self.summary.graph_id,
            "schema_version": self.summary.schema_version,
            "run_id": self.summary.run_id,
            "created_at_ms": self.summary.created_at_ms,
            "success": self.summary.success,
            "verification_success": self.verification_success,
            "runtime_error": self.runtime_error,
            "event_count": len(self.events),
            "node_count": len(self.node_summaries),
            "frame_ids": list(self.frame_ids),
            "output_node_ids": list(self.output_node_ids),
            "state_node_ids": list(self.state_node_ids),
            "events": [event.as_dict() for event in self.events],
            "node_summaries": [summary.as_dict() for summary in self.node_summaries],
        }


@dataclass(slots=True, frozen=True)
class NodeDiff:
    """Per-node comparison across two persisted runs."""

    node_id: str
    frame_id: str
    loop_node_id: str | None
    iteration_index: int | None
    missing_in_run_a: bool
    missing_in_run_b: bool
    events_equal: bool
    inputs_equal: bool | None
    outputs_equal: bool | None
    event_types_run_a: tuple[str, ...]
    event_types_run_b: tuple[str, ...]
    input_run_a: JsonValue | None
    input_run_b: JsonValue | None
    output_run_a: JsonValue | None
    output_run_b: JsonValue | None

    @property
    def differs(self) -> bool:
        return any(
            (
                self.missing_in_run_a,
                self.missing_in_run_b,
                not self.events_equal,
                self.inputs_equal is False,
                self.outputs_equal is False,
            )
        )

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "node_id": self.node_id,
            "frame_id": self.frame_id,
            "loop_node_id": self.loop_node_id,
            "iteration_index": self.iteration_index,
            "missing_in_run_a": self.missing_in_run_a,
            "missing_in_run_b": self.missing_in_run_b,
            "events_equal": self.events_equal,
            "inputs_equal": self.inputs_equal,
            "outputs_equal": self.outputs_equal,
            "event_types_run_a": list(self.event_types_run_a),
            "event_types_run_b": list(self.event_types_run_b),
            "input_run_a": self.input_run_a,
            "input_run_b": self.input_run_b,
            "output_run_a": self.output_run_a,
            "output_run_b": self.output_run_b,
        }


@dataclass(slots=True, frozen=True)
class InvariantDiff:
    """Focused comparison for one invariant node across two runs."""

    node_id: str
    frame_id: str
    loop_node_id: str | None
    iteration_index: int | None
    outcome_run_a: bool | None
    outcome_run_b: bool | None
    severity_run_a: str | None
    severity_run_b: str | None
    details_run_a: JsonValue | None
    details_run_b: JsonValue | None

    @property
    def differs(self) -> bool:
        return (
            self.outcome_run_a != self.outcome_run_b
            or self.severity_run_a != self.severity_run_b
            or self.details_run_a != self.details_run_b
        )

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "node_id": self.node_id,
            "frame_id": self.frame_id,
            "loop_node_id": self.loop_node_id,
            "iteration_index": self.iteration_index,
            "outcome_run_a": self.outcome_run_a,
            "outcome_run_b": self.outcome_run_b,
            "severity_run_a": self.severity_run_a,
            "severity_run_b": self.severity_run_b,
            "details_run_a": self.details_run_a,
            "details_run_b": self.details_run_b,
        }


@dataclass(slots=True, frozen=True)
class RunDiff:
    """Structured comparison result for two persisted runs."""

    graph_id: str
    run_a: RunSummary
    run_b: RunSummary
    verification_success_run_a: bool | None
    verification_success_run_b: bool | None
    runtime_error_run_a: str | None
    runtime_error_run_b: str | None
    state_equal: bool
    state_run_a: dict[tuple[str, str], JsonValue]
    state_run_b: dict[tuple[str, str], JsonValue]
    node_diffs: tuple[NodeDiff, ...]
    invariant_diffs: tuple[InvariantDiff, ...]

    @property
    def differs(self) -> bool:
        return (not self.state_equal) or any(
            node_diff.differs for node_diff in self.node_diffs
        ) or any(invariant_diff.differs for invariant_diff in self.invariant_diffs)

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "graph_id": self.graph_id,
            "differs": self.differs,
            "run_a": _summary_to_json(self.run_a),
            "run_b": _summary_to_json(self.run_b),
            "verification_success_run_a": self.verification_success_run_a,
            "verification_success_run_b": self.verification_success_run_b,
            "runtime_error_run_a": self.runtime_error_run_a,
            "runtime_error_run_b": self.runtime_error_run_b,
            "state_equal": self.state_equal,
            "state_run_a": _serialize_addressed_mapping(self.state_run_a, key_name="state_key"),
            "state_run_b": _serialize_addressed_mapping(self.state_run_b, key_name="state_key"),
            "node_diffs": [node_diff.as_dict() for node_diff in self.node_diffs],
            "invariant_diffs": [
                invariant_diff.as_dict() for invariant_diff in self.invariant_diffs
            ],
        }


def build_replay_report(
    *,
    runs_dir: Path | None = None,
    graph_id: str,
    run_id: str | None = None,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> ReplayReport:
    """Build a normalized replay report for one persisted run."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    raw_records = load_run_records(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    events = tuple(_normalize_replay_event(record) for record in raw_records)
    node_summaries = summarize_replay_events(events)
    outputs = _load_framed_node_mapping(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        filename="outputs.json",
        key="framed_outputs",
        id_key="node_id",
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    state = _load_framed_state_mapping(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        filename="state.json",
        key="framed_state",
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    verification_payload = _load_optional_payload(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        filename="verification.json",
    )
    return ReplayReport(
        summary=summary,
        verification_success=_verification_success(verification_payload),
        runtime_error=_runtime_error(verification_payload),
        events=events,
        node_summaries=node_summaries,
        frame_ids=tuple(sorted({event.frame_id for event in events})),
        output_node_ids=tuple(sorted({node_id for _, node_id in outputs.keys()})),
        state_node_ids=tuple(sorted({state_key for _, state_key in state.keys()})),
    )


def build_run_diff(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None,
    run_a: str,
    run_b: str,
    node_id: str | None = None,
    invariant: str | None = None,
) -> RunDiff:
    """Build a persisted-bundle diff for two runs from the same graph."""

    summary_a = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_a)
    summary_b = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_b)
    if summary_a.graph_id != summary_b.graph_id:
        raise RunInspectionError(
            "Run diff requires both runs to belong to the same graph."
        )

    records_a = load_run_records(
        runs_dir=runs_dir,
        graph_id=summary_a.graph_id,
        run_id=summary_a.run_id,
    )
    records_b = load_run_records(
        runs_dir=runs_dir,
        graph_id=summary_b.graph_id,
        run_id=summary_b.run_id,
    )
    outputs_a = _load_framed_node_mapping(
        runs_dir=runs_dir,
        graph_id=summary_a.graph_id,
        run_id=summary_a.run_id,
        filename="outputs.json",
        key="framed_outputs",
        id_key="node_id",
    )
    outputs_b = _load_framed_node_mapping(
        runs_dir=runs_dir,
        graph_id=summary_b.graph_id,
        run_id=summary_b.run_id,
        filename="outputs.json",
        key="framed_outputs",
        id_key="node_id",
    )
    state_a = _load_framed_state_mapping(
        runs_dir=runs_dir,
        graph_id=summary_a.graph_id,
        run_id=summary_a.run_id,
        filename="state.json",
        key="framed_state",
    )
    state_b = _load_framed_state_mapping(
        runs_dir=runs_dir,
        graph_id=summary_b.graph_id,
        run_id=summary_b.run_id,
        filename="state.json",
        key="framed_state",
    )
    verification_a = _load_optional_payload(
        runs_dir=runs_dir,
        graph_id=summary_a.graph_id,
        run_id=summary_a.run_id,
        filename="verification.json",
    )
    verification_b = _load_optional_payload(
        runs_dir=runs_dir,
        graph_id=summary_b.graph_id,
        run_id=summary_b.run_id,
        filename="verification.json",
    )

    node_diffs = diff_nodes(
        records_run_a=records_a,
        records_run_b=records_b,
        outputs_run_a=outputs_a,
        outputs_run_b=outputs_b,
        node_id=node_id,
    )
    invariant_diffs = diff_invariants(
        records_run_a=records_a,
        records_run_b=records_b,
        outputs_run_a=outputs_a,
        outputs_run_b=outputs_b,
        node_id=node_id,
        invariant=invariant,
    )
    return RunDiff(
        graph_id=summary_a.graph_id,
        run_a=summary_a,
        run_b=summary_b,
        verification_success_run_a=_verification_success(verification_a),
        verification_success_run_b=_verification_success(verification_b),
        runtime_error_run_a=_runtime_error(verification_a),
        runtime_error_run_b=_runtime_error(verification_b),
        state_equal=state_a == state_b,
        state_run_a=state_a,
        state_run_b=state_b,
        node_diffs=node_diffs,
        invariant_diffs=invariant_diffs,
    )


def summarize_replay_events(
    events: Iterable[ReplayEvent],
) -> tuple[ReplayNodeSummary, ...]:
    grouped: dict[tuple[str, str], list[ReplayEvent]] = defaultdict(list)
    for event in events:
        grouped[(event.frame_id, event.node_id)].append(event)
    summaries: list[ReplayNodeSummary] = []
    for _, node_events in sorted(grouped.items()):
        node_events = sorted(node_events, key=lambda event: event.sequence)
        first_event = node_events[0]
        invariant_passed = _invariant_outcome_from_events(node_events)
        invariant_severity = _invariant_severity_from_events(node_events)
        summaries.append(
            ReplayNodeSummary(
                node_id=first_event.node_id,
                frame_id=first_event.frame_id,
                frame_path=first_event.frame_path,
                loop_node_id=first_event.loop_node_id,
                iteration_index=first_event.iteration_index,
                event_count=len(node_events),
                first_sequence=node_events[0].sequence if node_events else None,
                last_sequence=node_events[-1].sequence if node_events else None,
                last_event_type=node_events[-1].event_type if node_events else None,
                succeeded=any(event.event_type == "node.succeeded" for event in node_events),
                failed=any(event.event_type == "node.failed" for event in node_events),
                invariant_passed=invariant_passed,
                invariant_severity=invariant_severity,
                invariant_status=_invariant_status_label(
                    invariant_passed,
                    invariant_severity,
                ),
            )
        )
    return tuple(summaries)


def diff_nodes(
    *,
    records_run_a: tuple[dict[str, JsonValue], ...],
    records_run_b: tuple[dict[str, JsonValue], ...],
    outputs_run_a: Mapping[tuple[str, str], JsonValue],
    outputs_run_b: Mapping[tuple[str, str], JsonValue],
    node_id: str | None = None,
) -> tuple[NodeDiff, ...]:
    inputs_run_a = _latest_inputs_by_address(records_run_a)
    inputs_run_b = _latest_inputs_by_address(records_run_b)
    records_by_node_run_a = _records_by_address(records_run_a)
    records_by_node_run_b = _records_by_address(records_run_b)

    addresses = tuple(
        sorted(
            {
                *records_by_node_run_a.keys(),
                *records_by_node_run_b.keys(),
                *inputs_run_a.keys(),
                *inputs_run_b.keys(),
                *outputs_run_a.keys(),
                *outputs_run_b.keys(),
            }
        )
    )

    node_diffs: list[NodeDiff] = []
    for current_frame_id, current_node_id in addresses:
        if node_id is not None and current_node_id != node_id:
            continue
        records_a = records_by_node_run_a.get((current_frame_id, current_node_id), tuple())
        records_b = records_by_node_run_b.get((current_frame_id, current_node_id), tuple())
        input_a = inputs_run_a.get((current_frame_id, current_node_id))
        input_b = inputs_run_b.get((current_frame_id, current_node_id))
        output_a = outputs_run_a.get((current_frame_id, current_node_id))
        output_b = outputs_run_b.get((current_frame_id, current_node_id))
        loop_node_id, iteration_index = _address_frame_metadata(records_a, records_b)
        node_diffs.append(
            NodeDiff(
                node_id=current_node_id,
                frame_id=current_frame_id,
                loop_node_id=loop_node_id,
                iteration_index=iteration_index,
                missing_in_run_a=not any(
                    value is not None for value in (input_a, output_a)
                )
                and not records_a,
                missing_in_run_b=not any(
                    value is not None for value in (input_b, output_b)
                )
                and not records_b,
                events_equal=_record_signatures(records_a) == _record_signatures(records_b),
                inputs_equal=None if input_a is None and input_b is None else input_a == input_b,
                outputs_equal=None
                if output_a is None and output_b is None
                else output_a == output_b,
                event_types_run_a=tuple(_event_types(records_a)),
                event_types_run_b=tuple(_event_types(records_b)),
                input_run_a=input_a,
                input_run_b=input_b,
                output_run_a=output_a,
                output_run_b=output_b,
            )
        )
    return tuple(node_diffs)


def diff_invariants(
    *,
    records_run_a: tuple[dict[str, JsonValue], ...],
    records_run_b: tuple[dict[str, JsonValue], ...],
    outputs_run_a: Mapping[tuple[str, str], JsonValue],
    outputs_run_b: Mapping[tuple[str, str], JsonValue],
    node_id: str | None = None,
    invariant: str | None = None,
) -> tuple[InvariantDiff, ...]:
    outcomes_run_a = _invariant_outcomes(records_run_a)
    outcomes_run_b = _invariant_outcomes(records_run_b)
    severities_run_a = _invariant_severities(records_run_a)
    severities_run_b = _invariant_severities(records_run_b)
    detail_nodes = {
        address
        for address, value in outputs_run_a.items()
        if _invariant_details(value) is not None
    } | {
        address
        for address, value in outputs_run_b.items()
        if _invariant_details(value) is not None
    }
    invariant_nodes = set(outcomes_run_a) | set(outcomes_run_b) | detail_nodes
    diffs: list[InvariantDiff] = []
    for current_frame_id, current_node_id in sorted(invariant_nodes):
        if node_id is not None and current_node_id != node_id:
            continue
        if invariant is not None and current_node_id != invariant:
            continue
        records_a = [
            record
            for record in records_run_a
            if _record_address(record) == (current_frame_id, current_node_id)
        ]
        records_b = [
            record
            for record in records_run_b
            if _record_address(record) == (current_frame_id, current_node_id)
        ]
        loop_node_id, iteration_index = _address_frame_metadata(
            tuple(records_a),
            tuple(records_b),
        )
        diffs.append(
            InvariantDiff(
                node_id=current_node_id,
                frame_id=current_frame_id,
                loop_node_id=loop_node_id,
                iteration_index=iteration_index,
                outcome_run_a=outcomes_run_a.get((current_frame_id, current_node_id)),
                outcome_run_b=outcomes_run_b.get((current_frame_id, current_node_id)),
                severity_run_a=severities_run_a.get((current_frame_id, current_node_id)),
                severity_run_b=severities_run_b.get((current_frame_id, current_node_id)),
                details_run_a=_invariant_details(
                    outputs_run_a.get((current_frame_id, current_node_id))
                ),
                details_run_b=_invariant_details(
                    outputs_run_b.get((current_frame_id, current_node_id))
                ),
            )
        )
    return tuple(diffs)


def _normalize_replay_event(record: dict[str, JsonValue]) -> ReplayEvent:
    payload = record.get("payload")
    if not isinstance(payload, dict):
        raise RunInspectionError("Execution record payload must be a JSON object.")
    frame_path = _frame_path(record)
    return ReplayEvent(
        sequence=_require_int(record, "sequence"),
        node_id=_require_str(record, "node_id"),
        frame_id=_frame_id(record),
        frame_path=frame_path,
        loop_node_id=_optional_str(record, "loop_node_id"),
        iteration_index=_optional_int(record, "iteration_index"),
        event_type=_require_str(record, "event_type"),
        timestamp_ms=_require_int(record, "timestamp_ms"),
        payload=payload,
    )


def _records_by_address(
    records: tuple[dict[str, JsonValue], ...],
) -> dict[tuple[str, str], tuple[dict[str, JsonValue], ...]]:
    grouped: dict[tuple[str, str], list[dict[str, JsonValue]]] = defaultdict(list)
    for record in records:
        grouped[_record_address(record)].append(record)
    return {
        address: tuple(sorted(node_records, key=lambda record: _require_int(record, "sequence")))
        for address, node_records in grouped.items()
    }


def _record_signatures(
    records: tuple[dict[str, JsonValue], ...],
) -> tuple[dict[str, JsonValue], ...]:
    return tuple(
        {
            "event_type": _require_str(record, "event_type"),
            "payload": _require_payload(record),
        }
        for record in records
    )


def _event_types(records: tuple[dict[str, JsonValue], ...]) -> list[str]:
    return [_require_str(record, "event_type") for record in records]


def _latest_inputs_by_address(
    records: tuple[dict[str, JsonValue], ...],
) -> dict[tuple[str, str], JsonValue]:
    latest: dict[tuple[str, str], tuple[int, JsonValue]] = {}
    for record in records:
        if record.get("event_type") != "node.inputs_resolved":
            continue
        payload = _require_payload(record)
        if "inputs" not in payload:
            continue
        address = _record_address(record)
        sequence = _require_int(record, "sequence")
        current = latest.get(address)
        if current is None or sequence > current[0]:
            latest[address] = (sequence, payload["inputs"])
    return {address: value for address, (_, value) in latest.items()}


def _invariant_outcomes(
    records: tuple[dict[str, JsonValue], ...],
) -> dict[tuple[str, str], bool]:
    outcomes: dict[tuple[str, str], tuple[int, bool]] = {}
    for record in records:
        if record.get("event_type") != INVARIANT_CHECKED:
            continue
        payload = _require_payload(record)
        passed = payload.get("passed")
        if not isinstance(passed, bool):
            continue
        address = _record_address(record)
        sequence = _require_int(record, "sequence")
        current = outcomes.get(address)
        if current is None or sequence > current[0]:
            outcomes[address] = (sequence, passed)
    return {address: passed for address, (_, passed) in outcomes.items()}


def _invariant_severities(
    records: tuple[dict[str, JsonValue], ...],
) -> dict[tuple[str, str], str]:
    severities: dict[tuple[str, str], tuple[int, str]] = {}
    for record in records:
        if record.get("event_type") != INVARIANT_CHECKED:
            continue
        payload = _require_payload(record)
        severity = payload.get("severity")
        if not isinstance(severity, str):
            continue
        address = _record_address(record)
        sequence = _require_int(record, "sequence")
        current = severities.get(address)
        if current is None or sequence > current[0]:
            severities[address] = (sequence, severity)
    return {address: severity for address, (_, severity) in severities.items()}


def _invariant_outcome_from_events(events: list[ReplayEvent]) -> bool | None:
    for event in reversed(events):
        if event.event_type != INVARIANT_CHECKED:
            continue
        passed = event.payload.get("passed")
        if isinstance(passed, bool):
            return passed
    return None


def _invariant_severity_from_events(events: list[ReplayEvent]) -> str | None:
    for event in reversed(events):
        if event.event_type != INVARIANT_CHECKED:
            continue
        severity = event.payload.get("severity")
        if isinstance(severity, str):
            return severity
    return None


def _invariant_status_label(
    passed: bool | None,
    severity: str | None,
) -> str | None:
    if passed is None:
        return None
    if passed:
        return "pass"
    if severity == "warning":
        return "warning_fail"
    return "error_fail"


def _invariant_details(value: JsonValue | None) -> JsonValue | None:
    if not isinstance(value, dict):
        return None
    details = value.get("details")
    return details


def _record_address(record: dict[str, JsonValue]) -> tuple[str, str]:
    return (_frame_id(record), _require_str(record, "node_id"))


def _frame_id(payload: dict[str, JsonValue]) -> str:
    value = payload.get("frame_id")
    return value if isinstance(value, str) else "root"


def _frame_path(payload: dict[str, JsonValue]) -> tuple[dict[str, JsonValue], ...]:
    value = payload.get("frame_path")
    if value is None:
        return tuple()
    if not isinstance(value, list):
        raise RunInspectionError("Frame path must be a JSON array when present.")
    normalized: list[dict[str, JsonValue]] = []
    for item in value:
        if not isinstance(item, dict):
            raise RunInspectionError("Frame path entries must be JSON objects.")
        normalized.append(item)
    return tuple(normalized)


def _address_frame_metadata(
    records_a: tuple[dict[str, JsonValue], ...],
    records_b: tuple[dict[str, JsonValue], ...],
) -> tuple[str | None, int | None]:
    for record in (*records_a, *records_b):
        loop_node_id = _optional_str(record, "loop_node_id")
        iteration_index = _optional_int(record, "iteration_index")
        if loop_node_id is not None or iteration_index is not None:
            return loop_node_id, iteration_index
    return None, None


def _load_framed_node_mapping(
    *,
    runs_dir: Path | None,
    graph_id: str,
    run_id: str,
    filename: str,
    key: str,
    id_key: str,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> dict[tuple[str, str], JsonValue]:
    payload = load_run_payload(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        filename=filename,
    )
    raw = payload.get(key)
    if raw is None:
        return _load_legacy_root_node_mapping(payload=payload, key="outputs")
    if isinstance(raw, dict):
        return {
            ("root", _require_str({"node_id": node_id}, "node_id")): value
            for node_id, value in raw.items()
        }
    if not isinstance(raw, list):
        raise RunInspectionError(
            f"Run artifact {filename!r} does not contain a JSON array for {key!r}."
        )
    loaded: dict[tuple[str, str], JsonValue] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise RunInspectionError(
                f"Run artifact {filename!r} contains a non-object entry in {key!r}."
            )
        if frame_id is not None and item.get("frame_id") != frame_id:
            continue
        if loop_node_id is not None and item.get("loop_node_id") != loop_node_id:
            continue
        if iteration_index is not None and item.get("iteration_index") != iteration_index:
            continue
        current_frame_id = _require_str(item, "frame_id")
        current_id = _require_str(item, id_key)
        loaded[(current_frame_id, current_id)] = item.get("value")
    return loaded


def _load_framed_state_mapping(
    *,
    runs_dir: Path | None,
    graph_id: str,
    run_id: str,
    filename: str,
    key: str,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> dict[tuple[str, str], JsonValue]:
    payload = load_run_payload(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        filename=filename,
    )
    raw = payload.get(key)
    if raw is None:
        return _load_legacy_root_state_mapping(payload=payload)
    if isinstance(raw, dict):
        return {
            ("root", _require_str({"state_key": state_key}, "state_key")): value
            for state_key, value in raw.items()
        }
    if not isinstance(raw, list):
        raise RunInspectionError(
            f"Run artifact {filename!r} does not contain a JSON array for {key!r}."
        )
    loaded: dict[tuple[str, str], JsonValue] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise RunInspectionError(
                f"Run artifact {filename!r} contains a non-object entry in {key!r}."
            )
        if frame_id is not None and item.get("frame_id") != frame_id:
            continue
        if loop_node_id is not None and item.get("loop_node_id") != loop_node_id:
            continue
        if iteration_index is not None and item.get("iteration_index") != iteration_index:
            continue
        current_frame_id = _require_str(item, "frame_id")
        state_key = _require_str(item, "state_key")
        loaded[(current_frame_id, state_key)] = item.get("value")
    return loaded


def _load_legacy_root_node_mapping(
    *,
    payload: dict[str, JsonValue],
    key: str,
) -> dict[tuple[str, str], JsonValue]:
    raw = payload.get(key)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise RunInspectionError(
            f"Run artifact fallback {key!r} must be a JSON object."
        )
    return {("root", str(node_id)): value for node_id, value in raw.items()}


def _load_legacy_root_state_mapping(
    *,
    payload: dict[str, JsonValue],
) -> dict[tuple[str, str], JsonValue]:
    raw = payload.get("state")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise RunInspectionError("Run artifact fallback 'state' must be a JSON object.")
    return {("root", str(state_key)): value for state_key, value in raw.items()}


def _load_optional_payload(
    *,
    runs_dir: Path | None,
    graph_id: str,
    run_id: str,
    filename: str,
) -> dict[str, JsonValue] | None:
    try:
        return load_run_payload(
            runs_dir=runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            filename=filename,
        )
    except RunInspectionError:
        return None


def _verification_success(payload: dict[str, JsonValue] | None) -> bool | None:
    if payload is None:
        return None
    success = payload.get("success")
    return success if isinstance(success, bool) else None


def _runtime_error(payload: dict[str, JsonValue] | None) -> str | None:
    if payload is None:
        return None
    runtime = payload.get("runtime")
    if not isinstance(runtime, dict):
        return None
    error = runtime.get("error")
    return error if isinstance(error, str) else None


def _summary_to_json(summary: RunSummary) -> dict[str, JsonValue]:
    return {
        "schema_version": summary.schema_version,
        "graph_id": summary.graph_id,
        "run_id": summary.run_id,
        "created_at_ms": summary.created_at_ms,
        "success": summary.success,
        "node_count": summary.node_count,
        "edge_count": summary.edge_count,
        "record_count": summary.record_count,
        "output_count": summary.output_count,
        "state_count": summary.state_count,
        "trace_sink_configured": summary.trace_sink_configured,
        "trace_mode": summary.trace_mode,
        "trace_otlp_endpoint": summary.trace_otlp_endpoint,
        "trace_mirror_to_disk": summary.trace_mirror_to_disk,
        "trace_capture_local_spans": summary.trace_capture_local_spans,
        "trace_service_name": summary.trace_service_name,
        "run_dir": str(summary.run_dir),
    }


def _serialize_addressed_mapping(
    mapping: Mapping[tuple[str, str], JsonValue],
    *,
    key_name: str,
) -> list[JsonValue]:
    return [
        {"frame_id": frame_id, key_name: key, "value": value}
        for (frame_id, key), value in sorted(mapping.items())
    ]


def _require_str(payload: Mapping[str, JsonValue], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise RunInspectionError(f"Expected {key!r} to be a string.")
    return value


def _require_int(payload: Mapping[str, JsonValue], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise RunInspectionError(f"Expected {key!r} to be an integer.")
    return value


def _optional_str(payload: Mapping[str, JsonValue], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RunInspectionError(f"Expected {key!r} to be a string when present.")
    return value


def _optional_int(payload: Mapping[str, JsonValue], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise RunInspectionError(f"Expected {key!r} to be an integer when present.")
    return value


def _require_payload(record: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    payload = record.get("payload")
    if not isinstance(payload, dict):
        raise RunInspectionError("Expected execution record payload to be a JSON object.")
    return payload
