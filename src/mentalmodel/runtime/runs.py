from __future__ import annotations

import hashlib
import json
import time
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from mentalmodel.core.interfaces import JsonValue, RuntimeValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.ir.serialization import ir_graph_from_json, ir_graph_to_json
from mentalmodel.observability.export import (
    execution_record_to_json,
    recorded_span_to_json,
    serialize_runtime_value,
    write_json,
    write_jsonl,
)
from mentalmodel.observability.tracing import RecordedSpan
from mentalmodel.pagination import (
    PageSlice,
    decode_sequence_cursor,
    encode_sequence_cursor,
)
from mentalmodel.remote import (
    ArtifactDescriptor,
    ArtifactName,
    CatalogSource,
    RunManifest,
    RunManifestStatus,
    RunTraceSummary,
)
from mentalmodel.runtime.frame import FramedNodeValue, FramedStateValue

RUNS_DIRNAME = ".runs"
RUN_SCHEMA_VERSION = 7
EXECUTION_RECORD_SCHEMA_VERSION = 1


@dataclass(slots=True, frozen=True)
class RunArtifacts:
    """Filesystem locations for one materialized run bundle."""

    manifest: RunManifest
    run_dir: Path
    summary_path: Path
    graph_path: Path
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
    invocation_name: str | None
    runtime_default_profile_name: str | None
    runtime_profile_names: tuple[str, ...]


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


@dataclass(slots=True, frozen=True)
class RunFrameScope:
    """Optional frame filter used when inspecting persisted runs."""

    frame_id: str | None = None
    loop_node_id: str | None = None
    iteration_index: int | None = None

    @property
    def is_explicit(self) -> bool:
        return any(
            value is not None
            for value in (self.frame_id, self.loop_node_id, self.iteration_index)
        )


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
    framed_outputs: tuple[FramedNodeValue[RuntimeValue], ...],
    state: dict[str, RuntimeValue],
    framed_state: tuple[FramedStateValue[RuntimeValue], ...],
    spans: tuple[RecordedSpan, ...],
    runs_dir: Path | None = None,
    verification_payload: dict[str, object] | None = None,
    trace_sink_configured: bool,
    trace_summary: dict[str, str | bool | None],
    invocation_name: str | None,
    runtime_default_profile_name: str | None,
    runtime_profile_names: tuple[str, ...],
    project_id: str | None = None,
    project_label: str | None = None,
    environment_name: str | None = None,
    catalog_entry_id: str | None = None,
    catalog_source: CatalogSource | None = None,
) -> RunArtifacts:
    """Write one run bundle to disk."""

    run_dir = default_runs_dir(root=runs_dir) / graph.graph_id / run_id
    summary_path = run_dir / "summary.json"
    graph_path = run_dir / "graph.json"
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
            "invocation_name": invocation_name,
            "runtime_default_profile_name": runtime_default_profile_name,
            "runtime_profile_names": list(runtime_profile_names),
        },
    )
    write_json(graph_path, ir_graph_to_json(graph))
    write_jsonl(records_path, (execution_record_to_json(record) for record in records))
    write_json(
        outputs_path,
        {
            "outputs": serialize_runtime_value(outputs),
            "framed_outputs": [
                _framed_output_to_json(entry) for entry in framed_outputs
            ],
        },
    )
    write_json(
        state_path,
        {
            "state": serialize_runtime_value(state),
            "framed_state": [
                _framed_state_to_json(entry) for entry in framed_state
            ],
        },
    )
    if spans_path is not None:
        write_jsonl(spans_path, (recorded_span_to_json(span) for span in spans))
    if verification_path is not None and verification_payload is not None:
        write_json(verification_path, verification_payload)
    manifest = build_run_manifest(
        graph_id=graph.graph_id,
        run_id=run_id,
        created_at_ms=created_at_ms,
        success=success,
        summary_path=summary_path,
        graph_path=graph_path,
        records_path=records_path,
        outputs_path=outputs_path,
        state_path=state_path,
        spans_path=spans_path,
        verification_path=verification_path,
        trace_summary=trace_summary,
        invocation_name=invocation_name,
        runtime_default_profile_name=runtime_default_profile_name,
        runtime_profile_names=runtime_profile_names,
        project_id=project_id,
        project_label=project_label,
        environment_name=environment_name,
        catalog_entry_id=catalog_entry_id,
        catalog_source=catalog_source,
        completed_at_ms=_completed_at_ms(records=records, created_at_ms=created_at_ms),
    )
    return RunArtifacts(
        manifest=manifest,
        run_dir=run_dir,
        summary_path=summary_path,
        graph_path=graph_path,
        records_path=records_path,
        outputs_path=outputs_path,
        state_path=state_path,
        spans_path=spans_path,
        verification_path=verification_path,
    )


def build_run_manifest(
    *,
    graph_id: str,
    run_id: str,
    created_at_ms: int,
    completed_at_ms: int,
    success: bool,
    summary_path: Path,
    graph_path: Path,
    records_path: Path,
    outputs_path: Path,
    state_path: Path,
    trace_summary: dict[str, str | bool | None],
    invocation_name: str | None,
    runtime_default_profile_name: str | None,
    runtime_profile_names: tuple[str, ...],
    spans_path: Path | None = None,
    verification_path: Path | None = None,
    project_id: str | None = None,
    project_label: str | None = None,
    environment_name: str | None = None,
    catalog_entry_id: str | None = None,
    catalog_source: CatalogSource | None = None,
) -> RunManifest:
    """Build the canonical run manifest for one local run bundle."""
    artifacts: list[ArtifactDescriptor] = [
        _artifact_descriptor(ArtifactName.SUMMARY, summary_path, "application/json"),
        _artifact_descriptor(ArtifactName.GRAPH, graph_path, "application/json"),
        _artifact_descriptor(
            ArtifactName.RECORDS,
            records_path,
            "application/x-ndjson",
        ),
        _artifact_descriptor(ArtifactName.OUTPUTS, outputs_path, "application/json"),
        _artifact_descriptor(ArtifactName.STATE, state_path, "application/json"),
    ]
    if verification_path is not None:
        artifacts.append(
            _artifact_descriptor(
                ArtifactName.VERIFICATION,
                verification_path,
                "application/json",
                required=False,
            )
        )
    if spans_path is not None:
        artifacts.append(
            _artifact_descriptor(
                ArtifactName.SPANS,
                spans_path,
                "application/x-ndjson",
                required=False,
            )
        )
    return RunManifest(
        run_id=run_id,
        graph_id=graph_id,
        created_at_ms=created_at_ms,
        completed_at_ms=completed_at_ms,
        status=RunManifestStatus.SEALED,
        success=success,
        run_schema_version=RUN_SCHEMA_VERSION,
        record_schema_version=EXECUTION_RECORD_SCHEMA_VERSION,
        trace_summary=build_run_trace_summary(trace_summary=trace_summary),
        artifacts=tuple(artifacts),
        invocation_name=invocation_name,
        project_id=project_id,
        project_label=project_label,
        environment_name=environment_name,
        catalog_entry_id=catalog_entry_id,
        catalog_source=catalog_source,
        runtime_default_profile_name=runtime_default_profile_name,
        runtime_profile_names=runtime_profile_names,
    )


def build_run_manifest_from_summary(
    summary: RunSummary,
    *,
    project_id: str | None = None,
    project_label: str | None = None,
    environment_name: str | None = None,
    catalog_entry_id: str | None = None,
    catalog_source: CatalogSource | None = None,
) -> RunManifest:
    """Build the canonical run manifest for one already-persisted run bundle."""

    run_dir = summary.run_dir
    summary_path = run_dir / "summary.json"
    graph_path = run_dir / "graph.json"
    records_path = run_dir / "records.jsonl"
    outputs_path = run_dir / "outputs.json"
    state_path = run_dir / "state.json"
    verification_path = run_dir / "verification.json"
    spans_path = run_dir / "otel-spans.jsonl"
    return build_run_manifest(
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        created_at_ms=summary.created_at_ms,
        completed_at_ms=_completed_at_ms_for_run_dir(run_dir, default=summary.created_at_ms),
        success=summary.success,
        summary_path=summary_path,
        graph_path=graph_path,
        records_path=records_path,
        outputs_path=outputs_path,
        state_path=state_path,
        spans_path=spans_path if spans_path.exists() else None,
        verification_path=verification_path if verification_path.exists() else None,
        trace_summary={
            "trace_mode": summary.trace_mode,
            "trace_otlp_endpoint": summary.trace_otlp_endpoint,
            "trace_mirror_to_disk": summary.trace_mirror_to_disk,
            "trace_capture_local_spans": summary.trace_capture_local_spans,
            "trace_sink_configured": summary.trace_sink_configured,
            "trace_service_name": summary.trace_service_name,
            "trace_service_namespace": None,
            "trace_service_version": None,
        },
        invocation_name=summary.invocation_name,
        runtime_default_profile_name=summary.runtime_default_profile_name,
        runtime_profile_names=summary.runtime_profile_names,
        project_id=project_id,
        project_label=project_label,
        environment_name=environment_name,
        catalog_entry_id=catalog_entry_id,
        catalog_source=catalog_source,
    )


def build_run_trace_summary(
    trace_summary: dict[str, str | bool | None],
) -> RunTraceSummary:
    """Convert runtime trace summary payloads into the canonical manifest shape."""

    return RunTraceSummary(
        mode=_require_summary_str(trace_summary, "trace_mode"),
        service_name=_require_summary_str(trace_summary, "trace_service_name"),
        otlp_endpoint=_optional_summary_str(trace_summary, "trace_otlp_endpoint"),
        mirror_to_disk=_require_summary_bool(trace_summary, "trace_mirror_to_disk"),
        capture_local_spans=_require_summary_bool(
            trace_summary, "trace_capture_local_spans"
        ),
        sink_configured=_require_summary_bool(trace_summary, "trace_sink_configured"),
        service_namespace=_optional_summary_str(trace_summary, "trace_service_namespace"),
        service_version=_optional_summary_str(trace_summary, "trace_service_version"),
    )


def list_run_summaries(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    invocation_name: str | None = None,
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
            summary = load_run_summary(run_dir)
            if invocation_name is not None and summary.invocation_name != invocation_name:
                continue
            summaries.append(summary)
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
    invocation_name: str | None = None,
) -> RunSummary:
    """Resolve one run summary by id or return the newest matching run."""

    summaries = list_run_summaries(
        runs_dir=runs_dir,
        graph_id=graph_id,
        invocation_name=invocation_name,
    )
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
        invocation_name=_optional_str(summary_payload, "invocation_name"),
        runtime_default_profile_name=_optional_str(
            summary_payload, "runtime_default_profile_name"
        ),
        runtime_profile_names=_require_str_list(summary_payload, "runtime_profile_names"),
    )


def load_run_payload(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    filename: str,
) -> dict[str, JsonValue]:
    """Load one JSON payload from a resolved run bundle."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    return read_json(summary.run_dir / filename)


def load_run_graph(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
) -> IRGraph:
    """Load the persisted lowered graph for one resolved run bundle."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    payload = read_json(summary.run_dir / "graph.json")
    try:
        return ir_graph_from_json(payload)
    except TypeError as exc:
        raise RunInspectionError(f"Malformed graph.json in run {summary.run_id!r}: {exc}") from exc


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
    invocation_name: str | None = None,
    node_id: str | None = None,
    event_type: str | None = None,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> tuple[dict[str, JsonValue], ...]:
    """Load JSONL execution records from a resolved run bundle."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
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
        if not _matches_frame_scope(
            payload=record,
            scope=RunFrameScope(
                frame_id=frame_id,
                loop_node_id=loop_node_id,
                iteration_index=iteration_index,
            ),
        ):
            continue
        loaded.append(record)
    return tuple(loaded)


def load_run_records_page(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    node_id: str | None = None,
    frame_id: str | None = None,
    cursor: str | None = None,
    limit: int = 100,
    include_payload: bool = True,
) -> PageSlice[dict[str, JsonValue]]:
    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    records_path = summary.run_dir / "records.jsonl"
    if not records_path.exists():
        raise RunInspectionError(f"Run {summary.run_id!r} does not contain records.jsonl.")
    before = decode_sequence_cursor(cursor)
    total_count = 0
    page_match_count = 0
    page_items: deque[dict[str, JsonValue]] = deque(maxlen=limit)
    for record in _iter_jsonl_object_rows(records_path):
        if node_id is not None and record.get("node_id") != node_id:
            continue
        if frame_id is not None and record.get("frame_id") != frame_id:
            continue
        total_count += 1
        sequence = _require_int(record, "sequence")
        if before is not None and sequence >= before:
            continue
        page_match_count += 1
        row = record if include_payload else _record_without_payload(record)
        page_items.append(row)
    items = tuple(reversed(page_items))
    next_cursor = None
    if page_match_count > limit and items:
        next_cursor = encode_sequence_cursor(_require_int(items[-1], "sequence"))
    return PageSlice(items=items, next_cursor=next_cursor, total_count=total_count)


def load_run_spans(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    node_id: str | None = None,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> tuple[dict[str, JsonValue], ...]:
    """Load JSONL span records from a resolved run bundle."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
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
        if not _matches_frame_scope(
            payload=span,
            scope=RunFrameScope(
                frame_id=frame_id,
                loop_node_id=loop_node_id,
                iteration_index=iteration_index,
            ),
        ):
            continue
        loaded.append(span)
    return tuple(loaded)


def load_run_spans_page(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    node_id: str | None = None,
    frame_id: str | None = None,
    cursor: str | None = None,
    limit: int = 100,
) -> PageSlice[dict[str, JsonValue]]:
    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    spans_path = summary.run_dir / "otel-spans.jsonl"
    if not spans_path.exists():
        return PageSlice(items=tuple(), next_cursor=None, total_count=0)
    before = decode_sequence_cursor(cursor)
    total_count = 0
    page_match_count = 0
    page_items: deque[dict[str, JsonValue]] = deque(maxlen=limit)
    for span in _iter_jsonl_object_rows(spans_path):
        if node_id is not None and _span_node_id(span) != node_id:
            continue
        if frame_id is not None and _span_frame_id(span) != frame_id:
            continue
        total_count += 1
        sequence = _require_int(span, "sequence")
        if before is not None and sequence >= before:
            continue
        page_match_count += 1
        page_items.append(span)
    items = tuple(reversed(page_items))
    next_cursor = None
    if page_match_count > limit and items:
        next_cursor = encode_sequence_cursor(_require_int(items[-1], "sequence"))
    return PageSlice(items=items, next_cursor=next_cursor, total_count=total_count)


def load_run_node_output(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    node_id: str,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> JsonValue:
    """Load one node output from a resolved run bundle."""

    payload = load_run_payload(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
        filename="outputs.json",
    )
    scope = RunFrameScope(
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    framed_outputs = _load_framed_outputs(payload)
    if framed_outputs:
        matches = [
            entry
            for entry in framed_outputs
            if entry["node_id"] == node_id and _matches_frame_scope(entry, scope=scope)
        ]
        if not matches:
            raise RunInspectionError(f"Run output for node {node_id!r} was not found.")
        if len(matches) > 1 and not scope.is_explicit:
            raise RunInspectionError(
                f"Run output for node {node_id!r} exists in multiple frames. "
                "Specify --frame-id or loop/iteration filters."
            )
        return matches[-1]["value"]
    outputs = payload.get("outputs")
    if scope.is_explicit and frame_id != "root":
        raise RunInspectionError(
            f"Run output for node {node_id!r} is only available in the "
            "root frame for this run bundle."
        )
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
    invocation_name: str | None = None,
    node_id: str,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> JsonValue:
    """Load one node input payload from a resolved run bundle."""

    records = load_run_records(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
        node_id=node_id,
        event_type="node.inputs_resolved",
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    if not records:
        raise RunInspectionError(
            f"Resolved inputs for node {node_id!r} were not found in the run bundle."
        )
    scope = RunFrameScope(
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    if len(records) > 1 and not scope.is_explicit:
        distinct_frames = {record.get("frame_id") for record in records}
        if len(distinct_frames) > 1:
            raise RunInspectionError(
                f"Resolved inputs for node {node_id!r} exist in multiple frames. "
                "Specify --frame-id or loop/iteration filters."
            )
    if len(records) > 1:
        records = tuple(sorted(records, key=_record_sort_key))
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
    invocation_name: str | None = None,
    node_id: str,
    event_type: str | None = None,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> RunNodeTrace:
    """Load the semantic trace for one node in one run."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    records = load_run_records(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        invocation_name=summary.invocation_name,
        node_id=node_id,
        event_type=event_type,
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    spans = load_run_spans(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        invocation_name=summary.invocation_name,
        node_id=node_id,
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
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


def _iter_jsonl_object_rows(path: Path) -> Iterator[dict[str, JsonValue]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise RunInspectionError(f"Malformed JSONL row in {path}.")
            yield {str(key): cast_json_value(value) for key, value in payload.items()}


def cast_json_value(value: object) -> JsonValue:
    """Validate and coerce a loaded JSON value into the package JsonValue alias."""

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [cast_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): cast_json_value(item) for key, item in value.items()}
    raise RunInspectionError(f"Unsupported JSON value type {type(value).__name__}.")


def _load_framed_outputs(
    payload: dict[str, JsonValue],
) -> tuple[dict[str, JsonValue], ...]:
    framed_outputs = payload.get("framed_outputs")
    if framed_outputs is None:
        return tuple()
    if not isinstance(framed_outputs, list):
        raise RunInspectionError("Run outputs.json framed_outputs must be a JSON array.")
    entries: list[dict[str, JsonValue]] = []
    for item in framed_outputs:
        if not isinstance(item, dict):
            raise RunInspectionError("Run outputs.json framed_outputs rows must be objects.")
        entries.append(item)
    return tuple(entries)


def _load_framed_state(
    payload: dict[str, JsonValue],
) -> tuple[dict[str, JsonValue], ...]:
    framed_state = payload.get("framed_state")
    if framed_state is None:
        return tuple()
    if not isinstance(framed_state, list):
        raise RunInspectionError("Run state.json framed_state must be a JSON array.")
    entries: list[dict[str, JsonValue]] = []
    for item in framed_state:
        if not isinstance(item, dict):
            raise RunInspectionError("Run state.json framed_state rows must be objects.")
        entries.append(item)
    return tuple(entries)


def _framed_output_to_json(entry: FramedNodeValue[RuntimeValue]) -> dict[str, JsonValue]:
    return {
        "node_id": entry.node_id,
        "frame_id": entry.frame.frame_id,
        "frame_path": serialize_runtime_value(entry.frame.path),
        "loop_node_id": entry.frame.loop_node_id,
        "iteration_index": entry.frame.iteration_index,
        "value": serialize_runtime_value(entry.value),
    }


def _framed_state_to_json(entry: FramedStateValue[RuntimeValue]) -> dict[str, JsonValue]:
    return {
        "state_key": entry.state_key,
        "frame_id": entry.frame.frame_id,
        "frame_path": serialize_runtime_value(entry.frame.path),
        "loop_node_id": entry.frame.loop_node_id,
        "iteration_index": entry.frame.iteration_index,
        "value": serialize_runtime_value(entry.value),
    }


def _matches_frame_scope(
    payload: dict[str, JsonValue],
    scope: RunFrameScope,
) -> bool:
    if scope.frame_id is not None and payload.get("frame_id") != scope.frame_id:
        return False
    if scope.loop_node_id is not None and payload.get("loop_node_id") != scope.loop_node_id:
        return False
    if (
        scope.iteration_index is not None
        and payload.get("iteration_index") != scope.iteration_index
    ):
        return False
    return True


def _record_sort_key(record: dict[str, JsonValue]) -> tuple[int, int]:
    return (_require_int(record, "timestamp_ms"), _require_int(record, "sequence"))


def _created_at_ms(*, records: tuple[ExecutionRecord, ...]) -> int:
    if records:
        return min(record.timestamp_ms for record in records)
    return int(time.time() * 1000)


def _completed_at_ms(
    *,
    records: tuple[ExecutionRecord, ...],
    created_at_ms: int,
) -> int:
    if records:
        return max(record.timestamp_ms for record in records)
    return created_at_ms


def _completed_at_ms_for_run_dir(run_dir: Path, *, default: int) -> int:
    records_path = run_dir / "records.jsonl"
    if not records_path.exists():
        return default
    latest = default
    with records_path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                ts = row.get("timestamp_ms")
                if isinstance(ts, int) and ts > latest:
                    latest = ts
    return latest


def _artifact_descriptor(
    logical_name: ArtifactName,
    path: Path,
    content_type: str,
    *,
    required: bool = True,
) -> ArtifactDescriptor:
    digest = hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None
    return ArtifactDescriptor(
        logical_name=logical_name,
        relative_path=path.name,
        content_type=content_type,
        byte_size=path.stat().st_size if path.exists() else None,
        checksum_sha256=digest,
        required=required,
    )


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
        "invocation_name": _optional_str(payload, "invocation_name"),
        "runtime_default_profile_name": _optional_str(payload, "runtime_default_profile_name"),
        "runtime_profile_names": cast(
            JsonValue,
            _optional_str_list(payload, "runtime_profile_names"),
        ),
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


def _optional_str_list(payload: dict[str, JsonValue], key: str) -> list[str]:
    value = payload.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise RunInspectionError(f"Expected {key!r} to be a string array when present.")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise RunInspectionError(
                f"Expected every value in {key!r} to be a string."
            )
        items.append(item)
    return items


def _require_str_list(payload: dict[str, JsonValue], key: str) -> tuple[str, ...]:
    if key not in payload:
        raise RunInspectionError(f"Expected {key!r} to be present.")
    return tuple(_optional_str_list(payload, key))


def _require_summary_str(summary: dict[str, str | bool | None], key: str) -> str:
    value = summary.get(key)
    if isinstance(value, str):
        return value
    raise RunInspectionError(f"Expected trace summary value {key!r} to be a string.")


def _optional_summary_str(summary: dict[str, str | bool | None], key: str) -> str | None:
    value = summary.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise RunInspectionError(
        f"Expected trace summary value {key!r} to be a string when present."
    )


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


def _span_frame_id(span: dict[str, JsonValue]) -> str | None:
    value = span.get("frame_id")
    if isinstance(value, str):
        return value
    attributes = span.get("attributes")
    if not isinstance(attributes, dict):
        return None
    attr_value = attributes.get("mentalmodel.frame.id")
    return attr_value if isinstance(attr_value, str) else None


def _record_without_payload(row: dict[str, JsonValue]) -> dict[str, JsonValue]:
    return {key: value for key, value in row.items() if key != "payload"}
