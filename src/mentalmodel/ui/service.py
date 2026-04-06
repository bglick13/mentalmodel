from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from mentalmodel.analysis import AnalysisReport, run_analysis
from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.invocation import (
    load_runtime_environment_subject,
    load_workflow_subject,
    read_verify_invocation_spec,
)
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.lowering import lower_program
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.export import execution_record_to_json
from mentalmodel.runtime.replay import build_replay_report
from mentalmodel.runtime.runs import (
    RunSummary,
    list_run_summaries,
    load_run_graph,
    load_run_node_inputs,
    load_run_node_output,
    load_run_node_trace,
    load_run_payload,
    load_run_records,
    resolve_run_summary,
)
from mentalmodel.testing import VerificationReport, run_verification
from mentalmodel.ui.catalog import (
    DashboardCatalogEntry,
    default_dashboard_catalog,
    resolve_catalog_entry,
)


@dataclass(slots=True)
class DashboardExecutionSession:
    """In-memory live execution state for one launched dashboard run."""

    execution_id: str
    spec: DashboardCatalogEntry
    status: str = "pending"
    started_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    finished_at_ms: int | None = None
    error: str | None = None
    run_id: str | None = None
    run_artifacts_dir: str | None = None
    records: list[dict[str, JsonValue]] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def on_record(self, record: ExecutionRecord) -> None:
        payload = execution_record_to_json(record)
        with self._lock:
            self.records.append(payload)
            if self.status == "pending":
                self.status = "running"

    def mark_completed(self, report: VerificationReport) -> None:
        with self._lock:
            self.status = "succeeded" if report.success else "failed"
            self.finished_at_ms = int(time.time() * 1000)
            self.run_id = report.runtime.run_id
            self.run_artifacts_dir = report.runtime.run_artifacts_dir
            if report.runtime.error is not None:
                self.error = report.runtime.error

    def mark_failed(self, message: str) -> None:
        with self._lock:
            self.status = "failed"
            self.finished_at_ms = int(time.time() * 1000)
            self.error = message

    def snapshot(self, *, after_sequence: int = 0) -> dict[str, JsonValue]:
        with self._lock:
            new_records = [
                record
                for record in self.records
                if _record_sequence(record) > after_sequence
            ]
            latest_sequence = max((_record_sequence(record) for record in self.records), default=0)
            return {
                "execution_id": self.execution_id,
                "spec": _as_json_object(self.spec.as_dict()),
                "status": self.status,
                "started_at_ms": self.started_at_ms,
                "finished_at_ms": self.finished_at_ms,
                "error": self.error,
                "run_id": self.run_id,
                "run_artifacts_dir": self.run_artifacts_dir,
                "latest_sequence": latest_sequence,
                "records": _as_json_list(new_records),
            }


class DashboardService:
    """Shared backend service for the hosted dashboard surface."""

    def __init__(self, *, runs_dir: Path | None = None) -> None:
        self.runs_dir = runs_dir
        self._sessions: dict[str, DashboardExecutionSession] = {}
        self._lock = threading.Lock()

    def list_catalog(self) -> tuple[DashboardCatalogEntry, ...]:
        return default_dashboard_catalog()

    def load_catalog_graph(self, spec_id: str) -> dict[str, JsonValue]:
        entry = resolve_catalog_entry(spec_id)
        invocation = read_verify_invocation_spec(entry.spec_path)
        _, program = load_workflow_subject(invocation.program)
        graph = lower_program(program)
        analysis = run_analysis(graph)
        return {
            "catalog_entry": _as_json_object(entry.as_dict()),
            "graph": _graph_to_payload(graph),
            "analysis": _analysis_to_payload(analysis),
        }

    def start_execution(self, spec_id: str) -> DashboardExecutionSession:
        entry = resolve_catalog_entry(spec_id)
        session = DashboardExecutionSession(
            execution_id=f"exec-{uuid.uuid4().hex}",
            spec=entry,
        )
        with self._lock:
            self._sessions[session.execution_id] = session
        thread = threading.Thread(
            target=self._run_session,
            args=(session,),
            daemon=True,
            name=f"mentalmodel-dashboard-{session.execution_id}",
        )
        thread.start()
        return session

    def get_execution(self, execution_id: str, *, after_sequence: int = 0) -> dict[str, JsonValue]:
        session = self._require_session(execution_id)
        snapshot = session.snapshot(after_sequence=after_sequence)
        if session.run_id is not None:
            snapshot["run_summary"] = self.get_run_overview(
                graph_id=session.spec.graph_id,
                run_id=session.run_id,
            )
        return snapshot

    def list_runs(
        self,
        *,
        graph_id: str | None = None,
        invocation_name: str | None = None,
    ) -> tuple[dict[str, JsonValue], ...]:
        return tuple(
            _summary_to_payload(summary)
            for summary in list_run_summaries(
                runs_dir=self.runs_dir,
                graph_id=graph_id,
                invocation_name=invocation_name,
            )
        )

    def get_run_graph(self, *, graph_id: str, run_id: str) -> dict[str, JsonValue]:
        graph = load_run_graph(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            run_id=run_id,
        )
        return _graph_to_payload(graph)

    def get_run_overview(self, *, graph_id: str, run_id: str) -> dict[str, JsonValue]:
        summary = resolve_run_summary(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            run_id=run_id,
        )
        verification = _safe_load_payload(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            filename="verification.json",
        )
        replay = build_replay_report(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            run_id=run_id,
        )
        graph = self.get_run_graph(graph_id=graph_id, run_id=run_id)
        metrics = self._derive_numeric_output_metrics(graph_id=graph_id, run_id=run_id)
        return {
            "summary": _summary_to_payload(summary),
            "verification": verification,
            "graph": graph,
            "metrics": _as_json_list(metrics),
            "invariants": _as_json_list([
                {
                    "node_id": node.node_id,
                    "frame_id": node.frame_id,
                    "loop_node_id": node.loop_node_id,
                    "iteration_index": node.iteration_index,
                    "status": node.invariant_status,
                    "passed": node.invariant_passed,
                    "severity": node.invariant_severity,
                }
                for node in replay.node_summaries
                if node.invariant_status is not None
            ]),
        }

    def get_run_records(
        self,
        *,
        graph_id: str,
        run_id: str,
        node_id: str | None = None,
    ) -> tuple[dict[str, JsonValue], ...]:
        return load_run_records(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            node_id=node_id,
        )

    def get_run_replay(
        self,
        *,
        graph_id: str,
        run_id: str,
        loop_node_id: str | None = None,
    ) -> dict[str, JsonValue]:
        report = build_replay_report(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            loop_node_id=loop_node_id,
        )
        return _as_json_object(report.as_dict())

    def get_node_detail(
        self,
        *,
        graph_id: str,
        run_id: str,
        node_id: str,
        frame_id: str | None = None,
    ) -> dict[str, JsonValue]:
        detail: dict[str, JsonValue] = {
            "node_id": node_id,
            "frame_id": frame_id,
        }
        try:
            detail["inputs"] = load_run_node_inputs(
                runs_dir=self.runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
            )
        except RunInspectionError as exc:
            detail["inputs_error"] = str(exc)
        try:
            detail["output"] = load_run_node_output(
                runs_dir=self.runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
            )
        except RunInspectionError as exc:
            detail["output_error"] = str(exc)
        try:
            trace = load_run_node_trace(
                runs_dir=self.runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
                frame_id=frame_id,
            )
            detail["trace"] = {
                "records": list(trace.records),
                "spans": list(trace.spans),
            }
        except RunInspectionError as exc:
            detail["trace_error"] = str(exc)
        detail["available_frames"] = _as_json_list(
            self._available_frames(
                graph_id=graph_id,
                run_id=run_id,
                node_id=node_id,
            )
        )
        return detail

    def _run_session(self, session: DashboardExecutionSession) -> None:
        try:
            invocation = read_verify_invocation_spec(session.spec.spec_path)
            module, program = load_workflow_subject(invocation.program)
            environment = None
            if invocation.environment is not None:
                _, environment = load_runtime_environment_subject(invocation.environment)
            report = run_verification(
                program,
                module=module,
                runs_dir=self.runs_dir or invocation.runs_dir,
                environment=environment,
                invocation_name=invocation.invocation_name,
                record_listeners=(session.on_record,),
            )
            session.mark_completed(report)
        except Exception as exc:  # pragma: no cover - guarded by API tests
            session.mark_failed(f"{type(exc).__name__}: {exc}")

    def _derive_numeric_output_metrics(
        self,
        *,
        graph_id: str,
        run_id: str,
    ) -> list[dict[str, JsonValue]]:
        outputs_payload = load_run_payload(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            filename="outputs.json",
        )
        outputs = outputs_payload.get("outputs")
        if not isinstance(outputs, dict):
            return []
        metrics: list[dict[str, JsonValue]] = []
        for node_id, output in outputs.items():
            if not isinstance(node_id, str):
                continue
            metrics.extend(
                {
                    "node_id": node_id,
                    "path": metric_path,
                    "value": metric_value,
                    "label": f"{node_id}.{metric_path}",
                }
                for metric_path, metric_value in _flatten_numeric_values(output)
            )
        return metrics

    def _available_frames(
        self,
        *,
        graph_id: str,
        run_id: str,
        node_id: str,
    ) -> list[dict[str, JsonValue]]:
        replay = build_replay_report(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            run_id=run_id,
        )
        frames: list[dict[str, JsonValue]] = []
        for summary in replay.node_summaries:
            if summary.node_id != node_id:
                continue
            if summary.frame_id == "root":
                continue
            frames.append(
                {
                    "frame_id": summary.frame_id,
                    "loop_node_id": summary.loop_node_id,
                    "iteration_index": summary.iteration_index,
                }
            )
        return frames

    def _require_session(self, execution_id: str) -> DashboardExecutionSession:
        with self._lock:
            session = self._sessions.get(execution_id)
        if session is None:
            raise KeyError(execution_id)
        return session


def _graph_to_payload(graph: IRGraph) -> dict[str, JsonValue]:
    from mentalmodel.ir.serialization import ir_graph_to_json

    return ir_graph_to_json(graph)


def _analysis_to_payload(report: AnalysisReport) -> dict[str, JsonValue]:
    return {
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "findings": [
            {
                "code": finding.code,
                "severity": finding.severity,
                "message": finding.message,
                "node_id": finding.node_id,
            }
            for finding in report.findings
        ],
    }


def _summary_to_payload(summary: RunSummary) -> dict[str, JsonValue]:
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
        "invocation_name": summary.invocation_name,
        "runtime_default_profile_name": summary.runtime_default_profile_name,
        "runtime_profile_names": list(summary.runtime_profile_names),
        "trace_mode": summary.trace_mode,
        "trace_service_name": summary.trace_service_name,
        "run_dir": str(summary.run_dir),
    }


def _safe_load_payload(
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


def _flatten_numeric_values(
    value: JsonValue,
    *,
    prefix: str = "",
) -> list[tuple[str, int | float]]:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return []
    if isinstance(value, (int, float)):
        return [(prefix, value)]
    if isinstance(value, list):
        return []
    flattened: list[tuple[str, int | float]] = []
    for key, inner in value.items():
        child_prefix = key if not prefix else f"{prefix}.{key}"
        flattened.extend(_flatten_numeric_values(inner, prefix=child_prefix))
    return flattened


def _record_sequence(record: dict[str, JsonValue]) -> int:
    value = record.get("sequence")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _as_json_object(value: object) -> dict[str, JsonValue]:
    json_value = _as_json_value(value)
    if not isinstance(json_value, dict):
        raise TypeError("Expected JSON object value.")
    return json_value


def _as_json_list(values: Sequence[object]) -> list[JsonValue]:
    return [_as_json_value(value) for value in values]


def _as_json_value(value: object) -> JsonValue:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_as_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _as_json_value(item) for key, item in value.items()}
    raise TypeError(f"Unsupported JSON value type {type(value).__name__}.")
