from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mentalmodel.core.interfaces import RuntimeValue
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


def default_runs_dir(*, root: Path | None = None) -> Path:
    """Return the default run-artifact root directory."""

    return (root or Path.cwd()) / RUNS_DIRNAME


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

    write_json(
        summary_path,
        {
            "graph_id": graph.graph_id,
            "run_id": run_id,
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
