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
) -> RunArtifacts:
    """Write one run bundle to disk."""

    run_dir = default_runs_dir(root=runs_dir) / graph.graph_id / run_id
    summary_path = run_dir / "summary.json"
    records_path = run_dir / "records.jsonl"
    outputs_path = run_dir / "outputs.json"
    state_path = run_dir / "state.json"
    spans_path = None if trace_sink_configured else run_dir / "otel-spans.jsonl"
    verification_path = (
        None if verification_payload is None else run_dir / "verification.json"
    )
    created_at_ms = _created_at_ms(records=records)

    write_json(
        summary_path,
        {
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
    graph_id = _require_str(payload, "graph_id")
    run_id = _require_str(payload, "run_id")
    return RunSummary(
        graph_id=graph_id,
        run_id=run_id,
        run_dir=run_dir,
        created_at_ms=_resolve_created_at_ms(payload=payload, run_dir=run_dir),
        success=_require_bool(payload, "success"),
        node_count=_require_int(payload, "node_count"),
        edge_count=_require_int(payload, "edge_count"),
        record_count=_require_int(payload, "record_count"),
        output_count=_require_int(payload, "output_count"),
        state_count=_require_int(payload, "state_count"),
        trace_sink_configured=_require_bool(payload, "trace_sink_configured"),
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
