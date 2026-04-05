from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

from mentalmodel.core.interfaces import JsonValue, RuntimeValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.export import (
    execution_record_to_json,
    recorded_span_to_json,
    serialize_runtime_value,
    write_json,
    write_jsonl,
)
from mentalmodel.observability.tracing import RecordedSpan

RUNS_DIRNAME = ".runs"
RUN_SCHEMA_VERSION = 3


@dataclass(slots=True, frozen=True)
class RunArtifacts:
    """Filesystem locations for one materialized run bundle."""

    run_dir: Path
    summary_path: Path
    records_path: Path
    outputs_path: Path
    state_path: Path
    spans_path: Path | None
    verification_path: Path | None = None


@dataclass(slots=True, frozen=True)
class RunSummary:
    """Parsed metadata from one persisted run bundle."""

    schema_version: int
    graph_id: str
    run_id: str
    run_dir: Path
    created_at_ms: int
    success: bool
    node_count: int
    edge_count: int
    record_count: int
    output_count: int
    state_count: int
    trace_sink_configured: bool
    trace_mode: str
    trace_otlp_endpoint: str | None
    trace_mirror_to_disk: bool
    trace_capture_local_spans: bool
    trace_service_name: str


@dataclass(slots=True, frozen=True)
class RunRepairAction:
    """One deterministic summary.json repair operation."""

    run_dir: Path
    graph_id: str
    run_id: str
    from_schema_version: int
    to_schema_version: int
    updates: dict[str, JsonValue]


@dataclass(slots=True, frozen=True)
class RunRepairPlan:
    """Repair plan for one set of run bundles."""

    root_dir: Path
    actions: tuple[RunRepairAction, ...]

    @property
    def has_actions(self) -> bool:
        return bool(self.actions)


@dataclass(slots=True, frozen=True)
class RunNodeTrace:
    """Resolved semantic trace for one node in one run."""

    summary: RunSummary
    node_id: str
    records: tuple[dict[str, JsonValue], ...]
    spans: tuple[dict[str, JsonValue], ...]


def default_runs_dir(*, root: Path | None = None) -> Path:
    """Return the default run-artifact root directory."""

    base = root or Path.cwd()
    if base.name == RUNS_DIRNAME:
        return base
    return base / RUNS_DIRNAME


def write_run_artifacts(
    *,
    graph: IRGraph,
    run_id: str,
    success: bool,
    records: tuple[ExecutionRecord, ...],
    outputs: dict[str, RuntimeValue],
    state: dict[str, RuntimeValue],
    spans: tuple[RecordedSpan, ...],
    runs_dir: Path | None = None,
    verification_payload: dict[str, object] | None = None,
    trace_sink_configured: bool,
    trace_summary: dict[str, str | bool | None],
) -> RunArtifacts:
    """Write one run bundle to disk."""

    run_dir = default_runs_dir(root=runs_dir) / graph.graph_id / run_id
    summary_path = run_dir / "summary.json"
    records_path = run_dir / "records.jsonl"
    outputs_path = run_dir / "outputs.json"
    state_path = run_dir / "state.json"
    trace_mirror_to_disk = _require_summary_bool(trace_summary, "trace_mirror_to_disk")
    spans_path = run_dir / "otel-spans.jsonl" if trace_mirror_to_disk else None
    verification_path = (
        None if verification_payload is None else run_dir / "verification.json"
    )
    created_at_ms = _created_at_ms(records=records)

    write_json(
        summary_path,
        {
            "schema_version": RUN_SCHEMA_VERSION,
            "graph_id": graph.graph_id,
            "run_id": run_id,
            "created_at_ms": created_at_ms,
            "success": success,
            "node_count": len(graph.nodes),
            "edge_count": len(graph.edges),
            "record_count": len(records),
            "output_count": len(outputs),
            "state_count": len(state),
            "trace_sink_configured": trace_sink_configured,
            "trace_mode": _require_summary_str(trace_summary, "trace_mode"),
            "trace_otlp_endpoint": trace_summary.get("trace_otlp_endpoint"),
            "trace_mirror_to_disk": trace_mirror_to_disk,
            "trace_capture_local_spans": _require_summary_bool(
                trace_summary, "trace_capture_local_spans"
            ),
            "trace_service_name": _require_summary_str(trace_summary, "trace_service_name"),
        },
    )
    write_jsonl(records_path, (execution_record_to_json(record) for record in records))
    write_json(
        outputs_path,
        {
            "outputs": serialize_runtime_value(outputs),
        },
    )
    write_json(
        state_path,
        {
            "state": serialize_runtime_value(state),
        },
    )
    if spans_path is not None:
        write_jsonl(spans_path, (recorded_span_to_json(span) for span in spans))
    if verification_path is not None and verification_payload is not None:
        write_json(verification_path, verification_payload)
    return RunArtifacts(
        run_dir=run_dir,
        summary_path=summary_path,
        records_path=records_path,
        outputs_path=outputs_path,
        state_path=state_path,
        spans_path=spans_path,
        verification_path=verification_path,
    )


def list_run_summaries(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
) -> tuple[RunSummary, ...]:
    """Return persisted run summaries sorted newest-first."""

    root = default_runs_dir(root=runs_dir)
    if not root.exists():
        return tuple()
    graph_dirs = [root / graph_id] if graph_id is not None else sorted(root.iterdir())
    summaries: list[RunSummary] = []
    for graph_dir in graph_dirs:
        if not graph_dir.exists() or not graph_dir.is_dir():
            continue
        for run_dir in sorted(graph_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            summary_path = run_dir / "summary.json"
            if not summary_path.exists():
                continue
            summaries.append(load_run_summary(run_dir))
    return tuple(
        sorted(
            summaries,
            key=lambda summary: (summary.created_at_ms, summary.run_id),
            reverse=True,
        )
    )


def resolve_run_summary(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
) -> RunSummary:
    """Resolve one run summary by id or return the newest matching run."""

    summaries = list_run_summaries(runs_dir=runs_dir, graph_id=graph_id)
    if not summaries:
        raise RunInspectionError(
            f"No runs found under {default_runs_dir(root=runs_dir)}."
        )
    if run_id is None:
        return summaries[0]
    for summary in summaries:
        if summary.run_id == run_id:
            return summary
    raise RunInspectionError(
        f"Run {run_id!r} was not found under {default_runs_dir(root=runs_dir)}."
    )


def load_run_summary(run_dir: Path) -> RunSummary:
    """Load one run summary from disk."""

    payload = read_json(run_dir / "summary.json")
    summary_payload = normalize_summary_payload(payload=payload, run_dir=run_dir)
    graph_id = _require_str(summary_payload, "graph_id")
    run_id = _require_str(summary_payload, "run_id")
    return RunSummary(
        schema_version=_require_int(summary_payload, "schema_version"),
        graph_id=graph_id,
        run_id=run_id,
        run_dir=run_dir,
        created_at_ms=_require_int(summary_payload, "created_at_ms"),
        success=_require_bool(summary_payload, "success"),
        node_count=_require_int(summary_payload, "node_count"),
        edge_count=_require_int(summary_payload, "edge_count"),
        record_count=_require_int(summary_payload, "record_count"),
        output_count=_require_int(summary_payload, "output_count"),
        state_count=_require_int(summary_payload, "state_count"),
        trace_sink_configured=_require_bool(summary_payload, "trace_sink_configured"),
        trace_mode=_require_str(summary_payload, "trace_mode"),
        trace_otlp_endpoint=_optional_str(summary_payload, "trace_otlp_endpoint"),
        trace_mirror_to_disk=_require_bool(summary_payload, "trace_mirror_to_disk"),
        trace_capture_local_spans=_require_bool(summary_payload, "trace_capture_local_spans"),
        trace_service_name=_require_str(summary_payload, "trace_service_name"),
    )


def load_run_payload(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    filename: str,
) -> dict[str, JsonValue]:
    """Load one JSON payload from a resolved run bundle."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    return read_json(summary.run_dir / filename)


def plan_run_repairs(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
) -> RunRepairPlan:
    """Plan deterministic repairs for one runs root or subtree."""

    root = default_runs_dir(root=runs_dir)
    actions: list[RunRepairAction] = []
    for run_dir in iter_run_dirs(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id):
        summary_path = run_dir / "summary.json"
        if not summary_path.exists():
            continue
        raw_payload = read_json(summary_path)
        current_version = resolve_schema_version(raw_payload)
        normalized = normalize_summary_payload(payload=raw_payload, run_dir=run_dir)
        updates = {
            key: value
            for key, value in normalized.items()
            if raw_payload.get(key) != value
        }
        if not updates:
            continue
        actions.append(
            RunRepairAction(
                run_dir=run_dir,
                graph_id=_require_str(normalized, "graph_id"),
                run_id=_require_str(normalized, "run_id"),
                from_schema_version=current_version,
                to_schema_version=RUN_SCHEMA_VERSION,
                updates=updates,
            )
        )
    return RunRepairPlan(root_dir=root, actions=tuple(actions))


def apply_run_repairs(plan: RunRepairPlan) -> RunRepairPlan:
    """Apply one deterministic repair plan to disk."""

    for action in plan.actions:
        summary_path = action.run_dir / "summary.json"
        payload = read_json(summary_path)
        normalized = normalize_summary_payload(payload=payload, run_dir=action.run_dir)
        write_json(summary_path, normalized)
    return plan


def load_run_records(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    node_id: str | None = None,
    event_type: str | None = None,
) -> tuple[dict[str, JsonValue], ...]:
    """Load JSONL execution records from a resolved run bundle."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    records_path = summary.run_dir / "records.jsonl"
    if not records_path.exists():
        raise RunInspectionError(f"Run {summary.run_id!r} does not contain records.jsonl.")
    loaded: list[dict[str, JsonValue]] = []
    for line in records_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise RunInspectionError(f"Malformed JSONL row in {records_path}.")
        record = {
            str(key): cast_json_value(value)
            for key, value in payload.items()
        }
        if node_id is not None and record.get("node_id") != node_id:
            continue
        if event_type is not None and record.get("event_type") != event_type:
            continue
        loaded.append(record)
    return tuple(loaded)


def load_run_spans(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    node_id: str | None = None,
) -> tuple[dict[str, JsonValue], ...]:
    """Load JSONL span records from a resolved run bundle."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    spans_path = summary.run_dir / "otel-spans.jsonl"
    if not spans_path.exists():
        return tuple()
    loaded: list[dict[str, JsonValue]] = []
    for line in spans_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise RunInspectionError(f"Malformed JSONL row in {spans_path}.")
        span = {str(key): cast_json_value(value) for key, value in payload.items()}
        if node_id is not None and _span_node_id(span) != node_id:
            continue
        loaded.append(span)
    return tuple(loaded)


def load_run_node_output(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    node_id: str,
) -> JsonValue:
    """Load one node output from a resolved run bundle."""

    payload = load_run_payload(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        filename="outputs.json",
    )
    outputs = payload.get("outputs")
    if not isinstance(outputs, dict):
        raise RunInspectionError("Run outputs.json does not contain an 'outputs' mapping.")
    if node_id not in outputs:
        raise RunInspectionError(f"Run output for node {node_id!r} was not found.")
    return outputs[node_id]


def load_run_node_inputs(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    node_id: str,
) -> JsonValue:
    """Load one node input payload from a resolved run bundle."""

    records = load_run_records(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        node_id=node_id,
        event_type="node.inputs_resolved",
    )
    if not records:
        raise RunInspectionError(
            f"Resolved inputs for node {node_id!r} were not found in the run bundle."
        )
    payload = records[-1].get("payload")
    if not isinstance(payload, dict):
        raise RunInspectionError(
            f"Resolved input payload for node {node_id!r} is missing from the run bundle."
        )
    inputs = payload.get("inputs")
    if inputs is None:
        raise RunInspectionError(
            f"Resolved input payload for node {node_id!r} is missing from the run bundle."
        )
    return inputs


def load_run_node_trace(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    node_id: str,
    event_type: str | None = None,
) -> RunNodeTrace:
    """Load the semantic trace for one node in one run."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    records = load_run_records(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        node_id=node_id,
        event_type=event_type,
    )
    spans = load_run_spans(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        node_id=node_id,
    )
    if not records and not spans:
        raise RunInspectionError(
            f"No trace data was found for node {node_id!r} in run {summary.run_id!r}."
        )
    return RunNodeTrace(summary=summary, node_id=node_id, records=records, spans=spans)


def read_json(path: Path) -> dict[str, JsonValue]:
    """Read one JSON object from disk."""

    if not path.exists():
        raise RunInspectionError(f"Run artifact {path} does not exist.")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RunInspectionError(f"Expected JSON object in {path}.")
    return {str(key): cast_json_value(value) for key, value in payload.items()}


def cast_json_value(value: object) -> JsonValue:
    """Validate and coerce a loaded JSON value into the package JsonValue alias."""

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [cast_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): cast_json_value(item) for key, item in value.items()}
    raise RunInspectionError(f"Unsupported JSON value type {type(value).__name__}.")


def _created_at_ms(*, records: tuple[ExecutionRecord, ...]) -> int:
    if records:
        return min(record.timestamp_ms for record in records)
    return int(time.time() * 1000)


def _resolve_created_at_ms(*, payload: dict[str, JsonValue], run_dir: Path) -> int:
    value = payload.get("created_at_ms")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    summary_path = run_dir / "summary.json"
    return int(summary_path.stat().st_mtime * 1000)


def resolve_schema_version(payload: dict[str, JsonValue]) -> int:
    """Resolve the effective schema version for one summary payload."""

    value = payload.get("schema_version")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return 1


def normalize_summary_payload(
    *,
    payload: dict[str, JsonValue],
    run_dir: Path,
) -> dict[str, JsonValue]:
    """Return one summary payload normalized to the current schema."""

    return {
        "schema_version": RUN_SCHEMA_VERSION,
        "graph_id": _require_str(payload, "graph_id"),
        "run_id": _require_str(payload, "run_id"),
        "created_at_ms": _resolve_created_at_ms(payload=payload, run_dir=run_dir),
        "success": _require_bool(payload, "success"),
        "node_count": _require_int(payload, "node_count"),
        "edge_count": _require_int(payload, "edge_count"),
        "record_count": _require_int(payload, "record_count"),
        "output_count": _require_int(payload, "output_count"),
        "state_count": _require_int(payload, "state_count"),
        "trace_sink_configured": _require_bool(payload, "trace_sink_configured"),
        "trace_mode": _optional_str(payload, "trace_mode") or "disk",
        "trace_otlp_endpoint": _optional_str(payload, "trace_otlp_endpoint"),
        "trace_mirror_to_disk": _optional_bool(payload, "trace_mirror_to_disk", default=True),
        "trace_capture_local_spans": _optional_bool(
            payload, "trace_capture_local_spans", default=True
        ),
        "trace_service_name": _optional_str(payload, "trace_service_name") or "mentalmodel",
    }


def iter_run_dirs(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
) -> tuple[Path, ...]:
    """Iterate run directories under one runs root with optional filters."""

    root = default_runs_dir(root=runs_dir)
    if not root.exists():
        return tuple()
    graph_dirs = [root / graph_id] if graph_id is not None else sorted(root.iterdir())
    selected: list[Path] = []
    for graph_dir in graph_dirs:
        if not graph_dir.exists() or not graph_dir.is_dir():
            continue
        for candidate in sorted(graph_dir.iterdir()):
            if not candidate.is_dir():
                continue
            if run_id is not None and candidate.name != run_id:
                continue
            selected.append(candidate)
    return tuple(selected)


def _require_str(payload: dict[str, JsonValue], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, str):
        return value
    raise RunInspectionError(f"Expected {key!r} to be a string.")


def _require_int(payload: dict[str, JsonValue], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise RunInspectionError(f"Expected {key!r} to be an integer.")


def _require_bool(payload: dict[str, JsonValue], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    raise RunInspectionError(f"Expected {key!r} to be a boolean.")


def _optional_str(payload: dict[str, JsonValue], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise RunInspectionError(f"Expected {key!r} to be a string when present.")


def _optional_bool(payload: dict[str, JsonValue], key: str, *, default: bool) -> bool:
    value = payload.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise RunInspectionError(f"Expected {key!r} to be a boolean when present.")


def _require_summary_str(summary: dict[str, str | bool | None], key: str) -> str:
    value = summary.get(key)
    if isinstance(value, str):
        return value
    raise RunInspectionError(f"Expected trace summary value {key!r} to be a string.")


def _require_summary_bool(summary: dict[str, str | bool | None], key: str) -> bool:
    value = summary.get(key)
    if isinstance(value, bool):
        return value
    raise RunInspectionError(f"Expected trace summary value {key!r} to be a boolean.")


def _span_node_id(span: dict[str, JsonValue]) -> str | None:
    attributes = span.get("attributes")
    if not isinstance(attributes, dict):
        return None
    value = attributes.get("mentalmodel.node.id")
    return value if isinstance(value, str) else None
