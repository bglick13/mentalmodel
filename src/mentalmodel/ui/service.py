from __future__ import annotations

import json
import hashlib
import subprocess
import threading
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import cast

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
    RunFrameScope,
    RunSummary,
    list_run_summaries,
    load_run_graph,
    load_run_node_inputs,
    load_run_node_output,
    load_run_node_trace,
    load_run_payload,
    load_run_records,
    load_run_spans,
    resolve_run_summary,
)
from mentalmodel.testing import VerificationReport, run_verification
from mentalmodel.remote import ProjectCatalog
from mentalmodel.ui.catalog import (
    DashboardCatalogEntry,
    DashboardCatalogError,
    catalog_entry_from_spec_path,
    default_dashboard_catalog,
    validate_dashboard_catalog,
)
from mentalmodel.ui.workspace import flatten_project_catalogs


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

    def mark_completed_from_payload(self, payload: dict[str, object]) -> None:
        runtime = payload.get("runtime")
        if not isinstance(runtime, dict):
            raise DashboardCatalogError("External verification payload must include runtime data.")
        run_id = runtime.get("run_id")
        run_artifacts_dir = runtime.get("run_artifacts_dir")
        runtime_error = runtime.get("error")
        success = payload.get("success")
        with self._lock:
            self.status = "succeeded" if success is True else "failed"
            self.finished_at_ms = int(time.time() * 1000)
            self.run_id = run_id if isinstance(run_id, str) else None
            self.run_artifacts_dir = (
                run_artifacts_dir if isinstance(run_artifacts_dir, str) else None
            )
            if isinstance(runtime_error, str):
                self.error = runtime_error

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

    def __init__(
        self,
        *,
        runs_dir: Path | None = None,
        catalog_entries: Sequence[DashboardCatalogEntry] | None = None,
        project_catalogs: Sequence[ProjectCatalog] | None = None,
    ) -> None:
        self.runs_dir = runs_dir
        self._project_catalogs = tuple(project_catalogs or ())
        self._project_catalog_by_id = {
            catalog.project.project_id: catalog
            for catalog in self._project_catalogs
        }
        base_entries = (
            tuple(catalog_entries)
            if catalog_entries is not None
            else default_dashboard_catalog()
        )
        project_entries = flatten_project_catalogs(self._project_catalogs)
        self._catalog = validate_dashboard_catalog(base_entries + project_entries)
        self._dynamic_catalog: dict[str, DashboardCatalogEntry] = {}
        self._sessions: dict[str, DashboardExecutionSession] = {}
        self._lock = threading.Lock()

    def list_catalog(self) -> tuple[DashboardCatalogEntry, ...]:
        return tuple(self._catalog) + tuple(self._dynamic_catalog.values())

    def list_projects(self) -> tuple[dict[str, JsonValue], ...]:
        projects: list[dict[str, JsonValue]] = []
        for project_catalog in self._project_catalogs:
            projects.append(
                {
                    "project_id": project_catalog.project.project_id,
                    "label": project_catalog.project.label,
                    "root_dir": str(project_catalog.project.root_dir),
                    "runs_dir": (
                        None
                        if project_catalog.project.runs_dir is None
                        else str(project_catalog.project.runs_dir)
                    ),
                    "description": project_catalog.description
                    or project_catalog.project.description,
                    "catalog_entry_count": len(project_catalog.entries),
                    "default_entry_id": project_catalog.default_entry_id,
                    "tags": list(project_catalog.project.tags),
                    "enabled": project_catalog.project.enabled,
                }
            )
        return tuple(projects)

    def _resolve_entry(self, spec_id: str) -> DashboardCatalogEntry:
        for entry in self._catalog:
            if entry.spec_id == spec_id:
                return entry
        if spec_id in self._dynamic_catalog:
            return self._dynamic_catalog[spec_id]
        raise DashboardCatalogError(f"Unknown dashboard catalog entry {spec_id!r}.")

    def register_spec_path(self, spec_path: Path) -> DashboardCatalogEntry:
        """Parse a verify TOML on disk and register it for graph preview and launch."""

        entry = self._catalog_entry_from_path(spec_path)
        self._dynamic_catalog[entry.spec_id] = entry
        return entry

    def start_execution_from_path(self, spec_path: Path) -> DashboardExecutionSession:
        """Register the spec (if needed) and start verification in a background thread."""

        entry = self.register_spec_path(spec_path)
        return self._start_execution_with_entry(entry)

    def load_catalog_graph(self, spec_id: str) -> dict[str, JsonValue]:
        entry = self._resolve_entry(spec_id)
        external_project = self._external_project_for_entry(entry)
        if external_project is not None:
            payload = self._load_external_catalog_graph(entry, external_project.project.root_dir)
            return {
                "catalog_entry": _as_json_object(entry.as_dict()),
                "graph": _as_json_object(payload["graph"]),
                "analysis": _as_json_object(payload["analysis"]),
            }
        invocation = read_verify_invocation_spec(entry.spec_path)
        _, program = load_workflow_subject(invocation.program)
        graph = lower_program(program)
        analysis = run_analysis(graph)
        return {
            "catalog_entry": _as_json_object(entry.as_dict()),
            "graph": _graph_to_payload(graph),
            "analysis": _analysis_to_payload(analysis),
        }

    def _start_execution_with_entry(self, entry: DashboardCatalogEntry) -> DashboardExecutionSession:
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

    def start_execution(self, spec_id: str) -> DashboardExecutionSession:
        entry = self._resolve_entry(spec_id)
        return self._start_execution_with_entry(entry)

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

    def get_run_spans(
        self,
        *,
        graph_id: str,
        run_id: str,
        node_id: str | None = None,
    ) -> tuple[dict[str, JsonValue], ...]:
        """Return OTel span rows from ``otel-spans.jsonl`` (optionally filtered by node)."""

        return load_run_spans(
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
            external_project = self._external_project_for_entry(session.spec)
            if external_project is not None:
                report = self._run_external_verification(
                    session.spec,
                    external_project.project.root_dir,
                )
                session.mark_completed_from_payload(report)
                return
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

    def _external_project_for_entry(self, entry: DashboardCatalogEntry) -> ProjectCatalog | None:
        if entry.project_id is None:
            return None
        project_catalog = self._project_catalog_by_id.get(entry.project_id)
        if project_catalog is None:
            return None
        current_root = Path(__file__).resolve().parents[3]
        if project_catalog.project.root_dir.resolve() == current_root.resolve():
            return None
        return project_catalog

    def _project_for_spec_path(self, spec_path: Path) -> ProjectCatalog | None:
        resolved = spec_path.expanduser().resolve()
        for project_catalog in self._project_catalogs:
            root_dir = project_catalog.project.root_dir.expanduser().resolve()
            try:
                resolved.relative_to(root_dir)
            except ValueError:
                continue
            return project_catalog
        return None

    def _catalog_entry_from_path(self, spec_path: Path) -> DashboardCatalogEntry:
        resolved = spec_path.expanduser().resolve()
        project_catalog = self._project_for_spec_path(resolved)
        if project_catalog is None:
            return catalog_entry_from_spec_path(resolved)
        external_project = self._external_project_for_entry(
            DashboardCatalogEntry(
                spec_id="project-probe",
                label=resolved.stem,
                description=str(resolved),
                spec_path=resolved,
                graph_id="probe",
                invocation_name="probe",
                project_id=project_catalog.project.project_id,
                project_label=project_catalog.project.label,
            )
        )
        if external_project is None:
            entry = catalog_entry_from_spec_path(resolved)
        else:
            metadata = self._load_external_spec_metadata(resolved, external_project.project.root_dir)
            graph_id = metadata.get("graph_id")
            invocation_name = metadata.get("invocation_name")
            if not isinstance(graph_id, str):
                raise DashboardCatalogError("External spec metadata must include graph_id.")
            if invocation_name is not None and not isinstance(invocation_name, str):
                raise DashboardCatalogError(
                    "External spec metadata invocation_name must be a string when present."
                )
            digest = hashlib.sha256(str(resolved).encode()).hexdigest()[:12]
            entry = DashboardCatalogEntry(
                spec_id=f"path-{digest}",
                label=resolved.stem,
                description=str(resolved),
                spec_path=resolved,
                graph_id=graph_id,
                invocation_name=invocation_name or "verify",
                category="custom",
                tags=("spec-path",),
                catalog_source="spec-path",
            )
        return replace(
            entry,
            project_id=project_catalog.project.project_id,
            project_label=project_catalog.project.label,
            catalog_source=entry.catalog_source or "spec-path",
        )

    def _load_external_catalog_graph(
        self,
        entry: DashboardCatalogEntry,
        root_dir: Path,
    ) -> dict[str, dict[str, object]]:
        payload = self._run_external_python(
            root_dir=root_dir,
            script=_EXTERNAL_GRAPH_SCRIPT,
            args=(str(entry.spec_path),),
        )
        graph = payload.get("graph")
        analysis = payload.get("analysis")
        if not isinstance(graph, dict) or not isinstance(analysis, dict):
            raise DashboardCatalogError(
                "External catalog graph helper must return graph and analysis objects."
            )
        return {
            "graph": cast(dict[str, object], graph),
            "analysis": cast(dict[str, object], analysis),
        }

    def _run_external_verification(
        self,
        entry: DashboardCatalogEntry,
        root_dir: Path,
    ) -> dict[str, object]:
        runs_dir_arg = (
            "-"
            if self.runs_dir is None
            else str(self.runs_dir.expanduser().resolve())
        )
        return self._run_external_python(
            root_dir=root_dir,
            script=_EXTERNAL_VERIFY_SCRIPT,
            args=(str(entry.spec_path), runs_dir_arg),
        )

    def _load_external_spec_metadata(
        self,
        spec_path: Path,
        root_dir: Path,
    ) -> dict[str, object]:
        return self._run_external_python(
            root_dir=root_dir,
            script=_EXTERNAL_SPEC_METADATA_SCRIPT,
            args=(str(spec_path),),
        )

    def _run_external_python(
        self,
        *,
        root_dir: Path,
        script: str,
        args: tuple[str, ...],
    ) -> dict[str, object]:
        command = [
            "uv",
            "run",
            "--directory",
            str(root_dir),
            "python",
            "-c",
            script,
            *args,
        ]
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            message = completed.stderr.strip() or completed.stdout.strip() or (
                f"External project command failed with exit code {completed.returncode}."
            )
            raise DashboardCatalogError(message)
        decoded = json.loads(completed.stdout)
        if not isinstance(decoded, dict):
            raise DashboardCatalogError("External project helper must return a JSON object.")
        return cast(dict[str, object], decoded)

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
        metrics: list[dict[str, JsonValue]] = []
        outputs = outputs_payload.get("outputs")
        if isinstance(outputs, dict):
            for node_id, output in outputs.items():
                if not isinstance(node_id, str):
                    continue
                metrics.extend(
                    _metrics_from_output(
                        node_id=node_id,
                        output=output,
                        frame_scope=RunFrameScope(frame_id="root"),
                    )
                )
        framed_outputs = outputs_payload.get("framed_outputs")
        if isinstance(framed_outputs, list):
            for item in framed_outputs:
                if not isinstance(item, dict):
                    continue
                framed_node_id: object = item.get("node_id")
                framed_output: object = item.get("value")
                frame_id: object = item.get("frame_id")
                loop_node_id: object = item.get("loop_node_id")
                iteration_index: object = item.get("iteration_index")
                if not isinstance(framed_node_id, str) or not isinstance(frame_id, str):
                    continue
                if frame_id == "root":
                    continue
                metrics.extend(
                    _metrics_from_output(
                        node_id=framed_node_id,
                        output=_as_json_value(framed_output),
                        frame_scope=RunFrameScope(
                            frame_id=frame_id,
                            loop_node_id=(
                                loop_node_id if isinstance(loop_node_id, str) else None
                            ),
                            iteration_index=(
                                iteration_index
                                if isinstance(iteration_index, int)
                                else None
                            ),
                        ),
                    )
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

    def aggregate_record_timeseries(
        self,
        *,
        graph_id: str,
        invocation_name: str,
        since_ms: int,
        until_ms: int,
        rollup_ms: int,
        run_id: str | None = None,
        node_id: str | None = None,
    ) -> dict[str, JsonValue]:
        """Bucket semantic records into time slices with Datadog-style rates (per second).

        Counts records whose ``timestamp_ms`` falls in ``[since_ms, until_ms)``.
        ``loop_events`` counts records with a non-null ``iteration_index``.
        ``unique_nodes`` is the count of distinct ``node_id`` values per bucket (not a rate
        of new nodes; displayed as nodes active in that interval).
        """

        if self.runs_dir is None:
            return _empty_timeseries(
                graph_id=graph_id,
                invocation_name=invocation_name,
                since_ms=since_ms,
                until_ms=until_ms,
                rollup_ms=rollup_ms,
            )
        if since_ms >= until_ms or rollup_ms <= 0:
            raise ValueError("since_ms must be < until_ms and rollup_ms must be positive.")

        span = until_ms - since_ms
        max_buckets = 500
        effective_rollup = rollup_ms
        num_buckets = max(1, (span + effective_rollup - 1) // effective_rollup)
        if num_buckets > max_buckets:
            effective_rollup = max(rollup_ms, (span + max_buckets - 1) // max_buckets)
            num_buckets = max(1, (span + effective_rollup - 1) // effective_rollup)
            num_buckets = min(num_buckets, max_buckets)
        rollup_ms = effective_rollup

        summaries = list_run_summaries(
            runs_dir=self.runs_dir,
            graph_id=graph_id,
            invocation_name=invocation_name,
        )
        if run_id is not None:
            summaries = tuple(s for s in summaries if s.run_id == run_id)
        else:
            # Approximate which bundles may contain events in [since_ms, until_ms) by run start time.
            windowed = tuple(
                s for s in summaries if since_ms <= s.created_at_ms < until_ms
            )
            if windowed:
                summaries = windowed[:200]
            else:
                summaries = summaries[: min(100, len(summaries))]

        record_counts = [0] * num_buckets
        loop_counts = [0] * num_buckets
        node_sets: list[set[str]] = [set() for _ in range(num_buckets)]

        for summary in summaries:
            records_path = summary.run_dir / "records.jsonl"
            if not records_path.is_file():
                continue
            with records_path.open(encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(row, dict):
                        continue
                    ts = row.get("timestamp_ms")
                    if not isinstance(ts, int):
                        continue
                    if ts < since_ms or ts >= until_ms:
                        continue
                    nid = row.get("node_id")
                    if node_id is not None:
                        if nid != node_id:
                            continue
                    bi = (ts - since_ms) // rollup_ms
                    if bi < 0 or bi >= num_buckets:
                        continue
                    record_counts[bi] += 1
                    if isinstance(nid, str):
                        node_sets[bi].add(nid)
                    it = row.get("iteration_index")
                    if isinstance(it, int):
                        loop_counts[bi] += 1

        secs = rollup_ms / 1000.0
        buckets: list[dict[str, JsonValue]] = []
        for i in range(num_buckets):
            start = since_ms + i * rollup_ms
            end = min(start + rollup_ms, until_ms)
            rc = record_counts[i]
            lc = loop_counts[i]
            un = len(node_sets[i])
            buckets.append(
                {
                    "start_ms": start,
                    "end_ms": end,
                    "records_per_sec": rc / secs if secs else 0.0,
                    "loop_events_per_sec": lc / secs if secs else 0.0,
                    "unique_nodes": un,
                    "unique_nodes_per_sec": un / secs if secs else 0.0,
                }
            )

        return {
            "rollup_ms": rollup_ms,
            "since_ms": since_ms,
            "until_ms": until_ms,
            "graph_id": graph_id,
            "invocation_name": invocation_name,
            "buckets": buckets,
            "runs_scanned": len(summaries),
        }

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


def _empty_timeseries(
    *,
    graph_id: str,
    invocation_name: str,
    since_ms: int,
    until_ms: int,
    rollup_ms: int,
) -> dict[str, JsonValue]:
    """Return an empty timeseries when no runs dir or no data."""

    span = max(0, until_ms - since_ms)
    max_buckets = 500
    effective_rollup = rollup_ms if rollup_ms > 0 else 60_000
    num_buckets = max(1, (span + effective_rollup - 1) // effective_rollup)
    if num_buckets > max_buckets:
        effective_rollup = max(effective_rollup, (span + max_buckets - 1) // max_buckets)
        num_buckets = max(1, (span + effective_rollup - 1) // effective_rollup)
        num_buckets = min(num_buckets, max_buckets)
    rollup_ms = effective_rollup
    buckets: list[dict[str, JsonValue]] = []
    for i in range(num_buckets):
        start = since_ms + i * rollup_ms
        end = min(start + rollup_ms, until_ms)
        buckets.append(
            {
                "start_ms": start,
                "end_ms": end,
                "records_per_sec": 0.0,
                "loop_events_per_sec": 0.0,
                "unique_nodes": 0,
                "unique_nodes_per_sec": 0.0,
            }
        )
    return {
        "rollup_ms": rollup_ms,
        "since_ms": since_ms,
        "until_ms": until_ms,
        "graph_id": graph_id,
        "invocation_name": invocation_name,
        "buckets": buckets,
        "runs_scanned": 0,
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


def _metrics_from_output(
    *,
    node_id: str,
    output: JsonValue,
    frame_scope: RunFrameScope,
) -> list[dict[str, JsonValue]]:
    metrics: list[dict[str, JsonValue]] = []
    for metric_path, metric_value in _flatten_numeric_values(output):
        label = f"{node_id}.{metric_path}"
        if frame_scope.frame_id is not None and frame_scope.frame_id != "root":
            label = f"{frame_scope.frame_id}.{label}"
        metrics.append(
            {
                "node_id": node_id,
                "path": metric_path,
                "value": metric_value,
                "label": label,
                "frame_id": frame_scope.frame_id,
                "loop_node_id": frame_scope.loop_node_id,
                "iteration_index": frame_scope.iteration_index,
            }
        )
    return metrics


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


_EXTERNAL_GRAPH_SCRIPT = """
import json
import sys
from pathlib import Path

from mentalmodel.analysis import run_analysis
from mentalmodel.invocation import load_workflow_subject, read_verify_invocation_spec
from mentalmodel.ir.lowering import lower_program
from mentalmodel.ir.serialization import ir_graph_to_json

spec_path = Path(sys.argv[1])
invocation = read_verify_invocation_spec(spec_path)
_, program = load_workflow_subject(invocation.program)
graph = lower_program(program)
report = run_analysis(graph)
print(
    json.dumps(
        {
            "graph": ir_graph_to_json(graph),
            "analysis": {
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
            },
        }
    )
)
""".strip()


_EXTERNAL_SPEC_METADATA_SCRIPT = """
import json
import sys
from pathlib import Path

from mentalmodel.invocation import load_workflow_subject, read_verify_invocation_spec
from mentalmodel.ir.lowering import lower_program

spec_path = Path(sys.argv[1])
invocation = read_verify_invocation_spec(spec_path)
_, program = load_workflow_subject(invocation.program)
graph = lower_program(program)
print(
    json.dumps(
        {
            "graph_id": graph.graph_id,
            "invocation_name": invocation.invocation_name,
        }
    )
)
""".strip()


_EXTERNAL_VERIFY_SCRIPT = """
import json
import sys
from pathlib import Path

from mentalmodel.invocation import (
    load_runtime_environment_subject,
    load_workflow_subject,
    read_verify_invocation_spec,
)
from mentalmodel.testing import run_verification

spec_path = Path(sys.argv[1])
runs_dir_arg = sys.argv[2]
runs_dir = None if runs_dir_arg == "-" else Path(runs_dir_arg)
invocation = read_verify_invocation_spec(spec_path)
module, program = load_workflow_subject(invocation.program)
environment = None
if invocation.environment is not None:
    _, environment = load_runtime_environment_subject(invocation.environment)
report = run_verification(
    program,
    module=module,
    runs_dir=runs_dir or invocation.runs_dir,
    environment=environment,
    invocation_name=invocation.invocation_name,
)
print(json.dumps(report.as_dict()))
""".strip()
