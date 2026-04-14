from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import webbrowser
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table

from mentalmodel.analysis import run_analysis
from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.workflow import Workflow
from mentalmodel.docs import render_markdown_artifacts, render_mermaid
from mentalmodel.doctor import DoctorStatus, build_doctor_report
from mentalmodel.environment import RuntimeEnvironment
from mentalmodel.errors import EntrypointLoadError, MentalModelError
from mentalmodel.examples.agent_tool_use import (
    DEFAULT_OUTPUT_DIRNAME as AGENT_TOOL_USE_OUTPUT_DIRNAME,
)
from mentalmodel.examples.agent_tool_use import (
    expected_artifact_names as expected_agent_tool_use_artifact_names,
)
from mentalmodel.examples.agent_tool_use import (
    generate_demo_artifacts as generate_agent_tool_use_artifacts,
)
from mentalmodel.examples.agent_tool_use.demo import build_program as build_agent_tool_use_demo
from mentalmodel.examples.async_rl import (
    DEFAULT_OUTPUT_DIRNAME as ASYNC_RL_OUTPUT_DIRNAME,
)
from mentalmodel.examples.async_rl import (
    expected_artifact_names,
    generate_demo_artifacts,
)
from mentalmodel.examples.async_rl.demo import build_program as build_async_rl_demo
from mentalmodel.examples.autoresearch_sorting.demo import (
    build_program as build_autoresearch_sorting_demo,
)
from mentalmodel.integrations.autoresearch import write_autoresearch_bundle
from mentalmodel.invocation import (
    InvocationFactorySpec,
    VerifyInvocationSpec,
    load_json_object,
    load_runtime_environment_subject,
    load_workflow_subject,
    read_verify_invocation_spec,
)
from mentalmodel.ir.lowering import lower_program
from mentalmodel.observability import load_tracing_config, write_otel_demo
from mentalmodel.observability.live import AsyncLiveExporter, LiveIngestionConfig
from mentalmodel.observability.telemetry import TelemetryResourceContext
from mentalmodel.remote.backend import (
    RemoteBackendConfig,
    RemoteCompletedRunSink,
    RemoteRunStore,
)
from mentalmodel.remote.bootstrap import write_remote_demo
from mentalmodel.remote.contracts import CatalogSource, ProjectRegistration
from mentalmodel.remote.doctor import build_remote_mode_doctor_report
from mentalmodel.remote.project_config import (
    MentalModelProjectConfig,
    discover_project_config_path,
    load_discovered_project_config,
    load_project_config,
)
from mentalmodel.remote.projects import (
    fetch_remote_project_status,
    link_project_to_server,
    publish_catalog_to_server,
)
from mentalmodel.remote.sinks import CompletedRunPublishResult
from mentalmodel.remote.sync import (
    RemoteServiceCompletedRunSink,
    sync_runs_for_project,
    sync_runs_to_server,
)
from mentalmodel.remote.workspace import (
    ProjectRunTarget,
    build_project_run_target,
    find_project_registration,
    find_project_registration_for_path,
    load_workspace_config,
)
from mentalmodel.runtime.context import generate_run_id
from mentalmodel.runtime.replay import build_replay_report, build_run_diff
from mentalmodel.runtime.runs import (
    apply_run_repairs,
    list_run_summaries,
    load_run_node_inputs,
    load_run_node_output,
    load_run_node_trace,
    load_run_payload,
    load_run_records,
    plan_run_repairs,
    resolve_run_summary,
)
from mentalmodel.skills import build_install_plan, install_skills
from mentalmodel.testing import execute_program, run_verification
from mentalmodel.ui.api import create_dashboard_app
from mentalmodel.ui.catalog import DashboardCatalogEntry, load_dashboard_catalog_subject
from mentalmodel.ui.workspace import load_project_catalog_subject, workspace_project_catalogs

DEFAULT_VERIFY_ENTRYPOINT = "mentalmodel.examples.async_rl.demo:build_program"


def load_entrypoint_subject(raw: str) -> tuple[object, Workflow[NamedPrimitive]]:
    module, workflow = load_workflow_subject(InvocationFactorySpec(entrypoint=raw))
    return module, workflow


def load_entrypoint(raw: str) -> Workflow[NamedPrimitive]:
    _, workflow = load_entrypoint_subject(raw)
    return workflow


def load_graph(entrypoint: str) -> Workflow[NamedPrimitive]:
    """Load the workflow entrypoint for CLI commands."""

    return load_entrypoint(entrypoint)


def resolve_verify_invocation(
    *,
    entrypoint: str | None,
    params_json: str | None,
    params_file: Path | None,
    environment_entrypoint: str | None,
    environment_params_json: str | None,
    environment_params_file: Path | None,
    invocation_name: str | None,
    runs_dir: Path | None,
    spec_path: Path | None,
) -> VerifyInvocationSpec:
    base_spec = (
        read_verify_invocation_spec(spec_path)
        if spec_path is not None
        else VerifyInvocationSpec(program=InvocationFactorySpec(DEFAULT_VERIFY_ENTRYPOINT))
    )
    program_entrypoint = entrypoint or base_spec.program.entrypoint
    program_params = (
        load_json_object(
            raw_json=params_json,
            file_path=params_file,
            subject="verification parameters",
        )
        if params_json is not None or params_file is not None
        else dict(base_spec.program.params)
    )
    environment_base = base_spec.environment
    environment_resolved_entrypoint = (
        environment_entrypoint
        if environment_entrypoint is not None
        else (None if environment_base is None else environment_base.entrypoint)
    )
    environment_params = (
        load_json_object(
            raw_json=environment_params_json,
            file_path=environment_params_file,
            subject="environment parameters",
        )
        if environment_params_json is not None or environment_params_file is not None
        else (
            {}
            if environment_base is None
            else dict(environment_base.params)
        )
    )
    if environment_resolved_entrypoint is None and environment_params:
        raise EntrypointLoadError(
            "Environment parameters require --environment-entrypoint or an [environment] spec."
        )
    environment_spec = (
        None
        if environment_resolved_entrypoint is None
        else InvocationFactorySpec(
            entrypoint=environment_resolved_entrypoint,
            params=environment_params or {},
        )
    )
    return VerifyInvocationSpec(
        program=InvocationFactorySpec(
            entrypoint=program_entrypoint,
            params=program_params or {},
        ),
        environment=environment_spec,
        invocation_name=invocation_name or base_spec.invocation_name,
        runs_dir=runs_dir or base_spec.runs_dir,
    )


def run_check(entrypoint: str, *, json_output: bool = False) -> int:
    program = load_graph(entrypoint)
    graph = lower_program(program)
    report = run_analysis(graph)

    if json_output:
        print(
            json.dumps(
                {
                    "graph_id": graph.graph_id,
                    "node_count": len(graph.nodes),
                    "edge_count": len(graph.edges),
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
                indent=2,
                sort_keys=True,
            )
        )
    else:
        console = Console()
        summary = Table(title="mentalmodel check summary")
        summary.add_column("Graph")
        summary.add_column("Nodes", justify="right")
        summary.add_column("Edges", justify="right")
        summary.add_column("Errors", justify="right")
        summary.add_column("Warnings", justify="right")
        summary.add_row(
            graph.graph_id,
            str(len(graph.nodes)),
            str(len(graph.edges)),
            str(report.error_count),
            str(report.warning_count),
        )
        console.print(summary)
        if report.findings:
            findings = Table(title="Findings")
            findings.add_column("Severity")
            findings.add_column("Code")
            findings.add_column("Node")
            findings.add_column("Message")
            for finding in report.findings:
                findings.add_row(
                    finding.severity,
                    finding.code,
                    finding.node_id or "",
                    finding.message,
                )
            console.print(findings)
        else:
            console.print("[green]No findings.[/green]")
    return 1 if report.has_errors else 0


def run_graph(
    entrypoint: str,
    *,
    output: Path | None = None,
) -> int:
    """Render Mermaid graph output for one workflow entrypoint."""

    graph = lower_program(load_graph(entrypoint))
    mermaid = render_mermaid(graph)
    if output is not None:
        output.write_text(mermaid + "\n", encoding="utf-8")
        Console().print(f"[green]wrote[/green] {output}")
        return 0
    Console().print(Panel.fit(mermaid, title=f"{graph.graph_id} Mermaid"))
    return 0


def run_docs(
    entrypoint: str,
    *,
    output_dir: Path | None = None,
    stdout: bool = False,
) -> int:
    """Render markdown documentation artifacts for one workflow entrypoint."""

    graph = lower_program(load_graph(entrypoint))
    report = run_analysis(graph)
    artifacts = render_markdown_artifacts(graph, findings=report.findings)
    console = Console()

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for name, content in artifacts.as_mapping().items():
            target = output_dir / name
            target.write_text(content + "\n", encoding="utf-8")
        console.print(f"[green]wrote[/green] {output_dir}")

    if stdout or output_dir is None:
        for name, content in artifacts.as_mapping().items():
            console.print(Panel(Markdown(content), title=name))
    return 0


def run_ui(
    *,
    runs_dir: Path | None = None,
    workspace_config: Path | None = None,
    host: str = "127.0.0.1",
    port: int = 8765,
    frontend_dist: Path | None = None,
    frontend_dev_url: str | None = None,
    catalog_entrypoint: str | None = None,
    open_browser: bool = False,
    remote_database_url: str | None = None,
    remote_object_store_bucket: str | None = None,
    remote_object_store_endpoint: str | None = None,
    remote_object_store_region: str | None = None,
    remote_object_store_access_key: str | None = None,
    remote_object_store_secret_key: str | None = None,
    remote_object_store_secure: bool | None = None,
    remote_cache_dir: Path | None = None,
) -> int:
    """Launch the dashboard API and static frontend host."""

    import uvicorn

    catalog_entries: tuple[DashboardCatalogEntry, ...] | None = None
    project_catalogs = None
    if workspace_config is not None:
        workspace = load_workspace_config(workspace_config)
        project_catalogs = workspace_project_catalogs(workspace)
        catalog_entries = ()
    if catalog_entrypoint is not None:
        try:
            _, catalog_entries = load_dashboard_catalog_subject(catalog_entrypoint)
        except EntrypointLoadError:
            _, project_catalog = load_project_catalog_subject(catalog_entrypoint)
            existing = tuple(project_catalogs or ())
            project_catalogs = existing + (project_catalog,)
    dist_dir = frontend_dist
    if dist_dir is None and frontend_dev_url is None:
        repo_root = Path(__file__).resolve().parents[2]
        candidate = repo_root / "apps" / "dashboard" / "dist"
        dist_dir = candidate if candidate.exists() else None
    remote_backend_config = _resolve_remote_backend_config(
        database_url=remote_database_url,
        object_store_bucket=remote_object_store_bucket,
        object_store_endpoint=remote_object_store_endpoint,
        object_store_region=remote_object_store_region,
        object_store_access_key=remote_object_store_access_key,
        object_store_secret_key=remote_object_store_secret_key,
        object_store_secure=remote_object_store_secure,
        cache_dir=remote_cache_dir,
    )
    app = create_dashboard_app(
        runs_dir=runs_dir,
        frontend_dist=dist_dir,
        catalog_entries=catalog_entries,
        project_catalogs=project_catalogs,
        remote_backend_config=remote_backend_config,
    )
    url = f"http://{host}:{port}"
    if open_browser:
        webbrowser.open(frontend_dev_url or url)
    uvicorn.run(app, host=host, port=port)
    return 0


def _resolve_remote_backend_config(
    *,
    database_url: str | None,
    object_store_bucket: str | None,
    object_store_endpoint: str | None,
    object_store_region: str | None,
    object_store_access_key: str | None,
    object_store_secret_key: str | None,
    object_store_secure: bool | None,
    cache_dir: Path | None,
    env_values: dict[str, str] | None = None,
) -> RemoteBackendConfig | None:
    if database_url is None and object_store_bucket is None:
        return (
            RemoteBackendConfig.from_mapping(env_values)
            if env_values is not None
            else RemoteBackendConfig.from_env()
        )
    if database_url is None or object_store_bucket is None:
        raise MentalModelError(
            "Both --remote-database-url and --remote-object-store-bucket are required together."
        )
    env_config = (
        RemoteBackendConfig.from_mapping(env_values)
        if env_values is not None
        else RemoteBackendConfig.from_env()
    )
    return RemoteBackendConfig(
        database_url=database_url,
        object_store_bucket=object_store_bucket,
        object_store_endpoint=object_store_endpoint or (
            None if env_config is None else env_config.object_store_endpoint
        ),
        object_store_region=object_store_region or (
            None if env_config is None else env_config.object_store_region
        ),
        object_store_access_key=object_store_access_key or (
            None if env_config is None else env_config.object_store_access_key
        ),
        object_store_secret_key=object_store_secret_key or (
            None if env_config is None else env_config.object_store_secret_key
        ),
        object_store_secure=(
            object_store_secure
            if object_store_secure is not None
            else (True if env_config is None else env_config.object_store_secure)
        ),
        cache_dir=cache_dir or (None if env_config is None else env_config.cache_dir),
    )


def run_verify(
    entrypoint: str | None,
    *,
    json_output: bool = False,
    runs_dir: Path | None = None,
    params_json: str | None = None,
    params_file: Path | None = None,
    environment_entrypoint: str | None = None,
    environment_params_json: str | None = None,
    environment_params_file: Path | None = None,
    invocation_name: str | None = None,
    spec_path: Path | None = None,
    workspace_config: Path | None = None,
    project_id: str | None = None,
    remote_database_url: str | None = None,
    remote_object_store_bucket: str | None = None,
    remote_object_store_endpoint: str | None = None,
    remote_object_store_region: str | None = None,
    remote_object_store_access_key: str | None = None,
    remote_object_store_secret_key: str | None = None,
    remote_object_store_secure: bool | None = None,
    remote_cache_dir: Path | None = None,
    live_otlp_endpoint: str | None = None,
    live_outbox_dir: Path | None = None,
    live_max_outbox_bytes: int = 64 * 1024 * 1024,
    live_max_batch_events: int = 256,
    live_max_batch_bytes: int = 512 * 1024,
    live_flush_interval_ms: int = 1_000,
    live_shutdown_flush_timeout_ms: int = 5_000,
    require_live_delivery: bool = False,
    remote_run_store: RemoteRunStore | None = None,
) -> int:
    """Run analysis, runtime verification, and property checks."""

    invocation = resolve_verify_invocation(
        entrypoint=entrypoint,
        params_json=params_json,
        params_file=params_file,
        environment_entrypoint=environment_entrypoint,
        environment_params_json=environment_params_json,
        environment_params_file=environment_params_file,
        invocation_name=invocation_name,
        runs_dir=runs_dir,
        spec_path=spec_path,
    )
    project = _resolve_verify_project_registration(
        workspace_config=workspace_config,
        project_id=project_id,
        spec_path=spec_path,
    )
    linked_project_config = _resolve_linked_project_config(spec_path=spec_path)
    if (
        project is not None
        and linked_project_config is not None
        and project.project_id != linked_project_config.project_id
    ):
        raise MentalModelError(
            "Workspace project registration and repo-linked mentalmodel.toml disagree "
            f"about project identity ({project.project_id!r} vs "
            f"{linked_project_config.project_id!r})."
        )
    configured_remote_run_store = remote_run_store
    if configured_remote_run_store is None and linked_project_config is None:
        remote_backend_config = _resolve_remote_backend_config(
            database_url=remote_database_url,
            object_store_bucket=remote_object_store_bucket,
            object_store_endpoint=remote_object_store_endpoint,
            object_store_region=remote_object_store_region,
            object_store_access_key=remote_object_store_access_key,
            object_store_secret_key=remote_object_store_secret_key,
            object_store_secure=remote_object_store_secure,
            cache_dir=remote_cache_dir,
        )
        configured_remote_run_store = (
            None
            if remote_backend_config is None
            else RemoteRunStore.from_config(remote_backend_config)
        )
    fallback_runs_dir = invocation.runs_dir
    if fallback_runs_dir is None and linked_project_config is not None:
        fallback_runs_dir = linked_project_config.default_runs_dir
    if fallback_runs_dir is None and configured_remote_run_store is not None:
        fallback_runs_dir = configured_remote_run_store.cache_dir
    run_target = build_project_run_target(
        project=project,
        fallback_runs_dir=fallback_runs_dir,
        catalog_source=CatalogSource.SPEC_PATH if spec_path is not None else None,
    )
    if project is None and linked_project_config is not None:
        run_target = _run_target_with_linked_project_metadata(
            run_target,
            linked_project_config,
        )
    completed_run_sink = _resolve_completed_run_sink(
        linked_project_config=linked_project_config,
        configured_remote_run_store=configured_remote_run_store,
        run_target=run_target,
    )
    live_ingestion = _resolve_live_ingestion_config(
        live_otlp_endpoint=live_otlp_endpoint,
        live_outbox_dir=live_outbox_dir,
        live_max_outbox_bytes=live_max_outbox_bytes,
        live_max_batch_events=live_max_batch_events,
        live_max_batch_bytes=live_max_batch_bytes,
        live_flush_interval_ms=live_flush_interval_ms,
        live_shutdown_flush_timeout_ms=live_shutdown_flush_timeout_ms,
        require_live_delivery=require_live_delivery,
        runs_dir=run_target.runs_dir,
        spec_path=spec_path,
    )
    live_run_id = generate_run_id()
    if _is_external_project_registration(project):
        if live_ingestion is not None:
            raise MentalModelError(
                "Live OTLP export is not supported through the external project verify path."
            )
        payload = _run_external_verify(
            invocation=invocation,
            project=cast(ProjectRegistration, project),
            run_target=run_target,
            completed_run_sink=completed_run_sink,
        )
    else:
        module, program = load_workflow_subject(invocation.program)
        environment = None
        if invocation.environment is not None:
            _, environment = load_runtime_environment_subject(invocation.environment)
        live_execution_sink = _resolve_live_execution_sink(
            live_ingestion=live_ingestion,
            run_target=run_target,
            invocation_name=invocation.invocation_name,
            run_id=live_run_id,
            environment=environment,
        )
        report = run_verification(
            program,
            module=module,
            runs_dir=run_target.runs_dir,
            environment=environment,
            invocation_name=invocation.invocation_name,
            completed_run_sink=completed_run_sink,
            live_execution_sink=live_execution_sink,
            run_id=live_run_id,
        )
        payload = report.as_dict()

    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if _verify_payload_success(payload) else 1

    console = Console()
    summary = Table(title="mentalmodel verify summary")
    summary.add_column("Graph")
    summary.add_column("Static Errors", justify="right")
    summary.add_column("Warnings", justify="right")
    summary.add_column("Runtime", justify="right")
    summary.add_column("Live", justify="right")
    summary.add_column("Upload", justify="right")
    summary.add_column("Warning Invariants", justify="right")
    summary.add_column("Property Checks", justify="right")
    analysis_payload = cast(dict[str, object], payload["analysis"])
    runtime_payload = cast(dict[str, object], payload["runtime"])
    property_checks_payload = cast(list[dict[str, object]], payload["property_checks"])
    warning_invariant_failures = cast(
        list[dict[str, object]],
        runtime_payload.get("warning_invariant_failures", []),
    )
    error_invariant_failures = cast(
        list[dict[str, object]],
        runtime_payload.get("error_invariant_failures", []),
    )
    completed_run_upload = cast(
        dict[str, object] | None,
        runtime_payload.get("completed_run_upload"),
    )
    live_execution_delivery = cast(
        dict[str, object] | None,
        runtime_payload.get("live_execution_delivery"),
    )
    live_status = (
        ""
        if live_execution_delivery is None
        else ("pass" if live_execution_delivery.get("success") is True else "fail")
    )
    upload_status = (
        ""
        if completed_run_upload is None
        else ("pass" if completed_run_upload.get("success") is True else "fail")
    )
    summary.add_row(
        cast(str, payload["graph_id"]),
        str(cast(int, analysis_payload["error_count"])),
        str(cast(int, analysis_payload["warning_count"])),
        "pass" if runtime_payload["success"] is True else "fail",
        live_status,
        upload_status,
        str(len(warning_invariant_failures)),
        str(len(property_checks_payload)),
    )
    console.print(summary)

    runtime_table = Table(title="Runtime Verification")
    runtime_table.add_column("Success")
    runtime_table.add_column("Records", justify="right")
    runtime_table.add_column("Outputs", justify="right")
    runtime_table.add_column("State Entries", justify="right")
    runtime_table.add_column("Invocation")
    runtime_table.add_column("Warning Invariants", justify="right")
    runtime_table.add_column("Run Artifacts")
    runtime_table.add_column("Live")
    runtime_table.add_column("Upload")
    runtime_table.add_column("Error")
    runtime_table.add_row(
        "yes" if runtime_payload["success"] is True else "no",
        str(cast(int, runtime_payload["record_count"])),
        str(cast(int, runtime_payload["output_count"])),
        str(cast(int, runtime_payload["state_count"])),
        cast(str | None, runtime_payload.get("invocation_name")) or "",
        str(len(warning_invariant_failures)),
        cast(str | None, runtime_payload.get("run_artifacts_dir")) or "",
        live_status or "",
        upload_status or "",
        cast(str | None, runtime_payload.get("error")) or "",
    )
    console.print(runtime_table)

    if live_execution_delivery is not None:
        live_table = Table(title="Live Execution Delivery")
        live_table.add_column("Transport")
        live_table.add_column("Mode")
        live_table.add_column("Run")
        live_table.add_column("Accepted")
        live_table.add_column("Exported")
        live_table.add_column("Outbox")
        live_table.add_column("Retry", justify="right")
        live_table.add_column("Ack Lag", justify="right")
        live_table.add_column("State")
        live_table.add_column("Error")
        live_table.add_row(
            cast(str, live_execution_delivery["transport"]),
            cast(str, live_execution_delivery["delivery_mode"]),
            "/".join(
                (
                    cast(str, live_execution_delivery["graph_id"]),
                    cast(str, live_execution_delivery["run_id"]),
                )
            ),
            "/".join(
                (
                    str(cast(int, live_execution_delivery["accepted_log_count"])),
                    str(cast(int, live_execution_delivery["accepted_span_count"])),
                    str(cast(int, live_execution_delivery["accepted_metric_count"])),
                )
            ),
            "/".join(
                (
                    str(cast(int, live_execution_delivery["exported_log_count"])),
                    str(cast(int, live_execution_delivery["exported_span_count"])),
                    str(cast(int, live_execution_delivery["exported_metric_count"])),
                )
            ),
            "/".join(
                (
                    str(cast(int, live_execution_delivery["outbox_depth"])),
                    str(cast(int, live_execution_delivery["outbox_bytes"])),
                )
            ),
            str(cast(int, live_execution_delivery["retry_count"])),
            str(cast(int | None, live_execution_delivery.get("ack_lag_ms")) or ""),
            ",".join(
                state
                for state, enabled in (
                    ("degraded", live_execution_delivery.get("degraded") is True),
                    ("failed-open", live_execution_delivery.get("failed_open") is True),
                    ("closed", live_execution_delivery.get("accepting_events") is not True),
                )
                if enabled
            )
            or "ok",
            cast(str | None, live_execution_delivery.get("last_error")) or "",
        )
        console.print(live_table)

    if completed_run_upload is not None:
        upload_table = Table(title="Completed Run Upload")
        upload_table.add_column("Transport")
        upload_table.add_column("Project")
        upload_table.add_column("Run")
        upload_table.add_column("Uploaded At")
        upload_table.add_column("Remote Location")
        upload_table.add_column("Error")
        upload_table.add_row(
            cast(str, completed_run_upload["transport"]),
            cast(str | None, completed_run_upload.get("project_id")) or "",
            "/".join(
                (
                    cast(str, completed_run_upload["graph_id"]),
                    cast(str, completed_run_upload["run_id"]),
                )
            ),
            str(cast(int | None, completed_run_upload.get("uploaded_at_ms")) or ""),
            cast(str | None, completed_run_upload.get("remote_run_dir")) or "",
            cast(str | None, completed_run_upload.get("error")) or "",
        )
        console.print(upload_table)

    invariant_failures = warning_invariant_failures + error_invariant_failures
    if invariant_failures:
        invariant_table = Table(title="Invariant Outcomes")
        invariant_table.add_column("Node")
        invariant_table.add_column("Severity")
        invariant_table.add_column("Fatal")
        for failure in invariant_failures:
            severity = cast(str, failure["severity"])
            invariant_table.add_row(
                cast(str, failure["node_id"]),
                severity,
                "yes" if severity != "warning" else "no",
            )
        console.print(invariant_table)

    if property_checks_payload:
        checks = Table(title="Property Checks")
        checks.add_column("Name")
        checks.add_column("Hypothesis")
        checks.add_column("Success")
        checks.add_column("Error")
        for result in property_checks_payload:
            checks.add_row(
                cast(str, result["name"]),
                "yes" if result["hypothesis_backed"] is True else "no",
                "yes" if result["success"] is True else "no",
                cast(str | None, result.get("error")) or "",
            )
        console.print(checks)
    else:
        console.print("[yellow]No property checks discovered.[/yellow]")

    return 0 if _verify_payload_success(payload) else 1


def _resolve_verify_project_registration(
    *,
    workspace_config: Path | None,
    project_id: str | None,
    spec_path: Path | None,
) -> ProjectRegistration | None:
    if project_id is not None and workspace_config is None:
        raise MentalModelError("--project-id requires --workspace-config.")
    if workspace_config is None:
        return None
    workspace = load_workspace_config(workspace_config)
    if project_id is not None:
        return find_project_registration(workspace, project_id)
    if spec_path is None:
        return None
    return find_project_registration_for_path(workspace.projects, spec_path)


def _resolve_linked_project_config(
    *,
    spec_path: Path | None,
) -> MentalModelProjectConfig | None:
    start = None if spec_path is None else spec_path.parent
    config_path = discover_project_config_path(start)
    if config_path is None:
        return None
    return load_project_config(config_path)


def _resolve_completed_run_sink(
    *,
    linked_project_config: MentalModelProjectConfig | None,
    configured_remote_run_store: RemoteRunStore | None,
    run_target: ProjectRunTarget,
) -> RemoteCompletedRunSink | RemoteServiceCompletedRunSink | None:
    if linked_project_config is not None:
        return RemoteServiceCompletedRunSink(
            linked_project_config,
            project_id=run_target.project_id,
            project_label=run_target.project_label,
            environment_name=run_target.environment_name,
            catalog_entry_id=run_target.catalog_entry_id,
            catalog_source=run_target.catalog_source,
        )
    if configured_remote_run_store is None:
        return None
    return RemoteCompletedRunSink(
        configured_remote_run_store,
        project_id=run_target.project_id,
        project_label=run_target.project_label,
        environment_name=run_target.environment_name,
        catalog_entry_id=run_target.catalog_entry_id,
        catalog_source=run_target.catalog_source,
    )


def _resolve_live_execution_sink(
    *,
    live_ingestion: LiveIngestionConfig | None,
    run_target: ProjectRunTarget,
    invocation_name: str | None,
    run_id: str,
    environment: RuntimeEnvironment | None,
) -> AsyncLiveExporter | None:
    if live_ingestion is None:
        return None
    runtime_profile_names: tuple[str, ...] = ()
    runtime_default_profile_name: str | None = None
    if environment is not None:
        runtime_profile_names = environment.profile_names()
        runtime_default_profile_name = environment.default_profile_name
    tracing_config = load_tracing_config()
    return AsyncLiveExporter(
        config=live_ingestion,
        run_id=run_id,
        invocation_name=invocation_name,
        resource_context=TelemetryResourceContext(
            project_id=run_target.project_id,
            project_label=run_target.project_label,
            environment_name=run_target.environment_name,
            catalog_entry_id=run_target.catalog_entry_id,
            catalog_source=(
                None if run_target.catalog_source is None else run_target.catalog_source.value
            ),
            service_name=tracing_config.service_name,
            service_namespace=tracing_config.service_namespace,
            service_version=tracing_config.service_version,
        ),
        runtime_default_profile_name=runtime_default_profile_name,
        runtime_profile_names=runtime_profile_names,
        tracing_config=tracing_config,
    )


def _resolve_live_ingestion_config(
    *,
    live_otlp_endpoint: str | None,
    live_outbox_dir: Path | None,
    live_max_outbox_bytes: int,
    live_max_batch_events: int,
    live_max_batch_bytes: int,
    live_flush_interval_ms: int,
    live_shutdown_flush_timeout_ms: int,
    require_live_delivery: bool,
    runs_dir: Path | None,
    spec_path: Path | None,
) -> LiveIngestionConfig | None:
    if live_otlp_endpoint is None:
        return None
    outbox_dir = live_outbox_dir
    if outbox_dir is None:
        if runs_dir is not None:
            outbox_dir = runs_dir.expanduser().resolve().parent / ".live-outbox"
        elif spec_path is not None:
            outbox_dir = spec_path.expanduser().resolve().parent / ".live-outbox"
        else:
            outbox_dir = Path.cwd() / ".live-outbox"
    return LiveIngestionConfig(
        otlp_endpoint=live_otlp_endpoint,
        outbox_dir=outbox_dir,
        max_outbox_bytes=live_max_outbox_bytes,
        max_batch_events=live_max_batch_events,
        max_batch_bytes=live_max_batch_bytes,
        flush_interval_ms=live_flush_interval_ms,
        shutdown_flush_timeout_ms=live_shutdown_flush_timeout_ms,
        require_live_delivery=require_live_delivery,
    )


def _run_target_with_linked_project_metadata(
    run_target: ProjectRunTarget,
    linked_project_config: MentalModelProjectConfig,
) -> ProjectRunTarget:
    return ProjectRunTarget(
        runs_dir=run_target.runs_dir,
        project_id=linked_project_config.project_id,
        project_label=linked_project_config.label,
        environment_name=linked_project_config.default_environment,
        catalog_entry_id=run_target.catalog_entry_id,
        catalog_source=run_target.catalog_source,
    )


def _verify_payload_success(payload: dict[str, object]) -> bool:
    if payload.get("success") is not True:
        return False
    runtime_payload = cast(dict[str, object], payload["runtime"])
    live_execution_delivery = runtime_payload.get("live_execution_delivery")
    if isinstance(live_execution_delivery, dict):
        if (
            live_execution_delivery.get("required") is True
            and live_execution_delivery.get("success") is not True
        ):
            return False
    completed_run_upload = runtime_payload.get("completed_run_upload")
    if not isinstance(completed_run_upload, dict):
        return True
    return completed_run_upload.get("success") is True


def _is_external_project_registration(project: ProjectRegistration | None) -> bool:
    if project is None:
        return False
    current_root = Path(__file__).resolve().parents[2]
    return project.root_dir.expanduser().resolve() != current_root.resolve()


def _run_external_verify(
    *,
    invocation: VerifyInvocationSpec,
    project: ProjectRegistration,
    run_target: ProjectRunTarget,
    completed_run_sink: RemoteCompletedRunSink | RemoteServiceCompletedRunSink | None,
) -> dict[str, object]:
    runs_dir = run_target.runs_dir or invocation.runs_dir
    command = [
        "uv",
        "run",
        "--directory",
        str(project.root_dir),
        "python",
        "-c",
        _EXTERNAL_VERIFY_INVOCATION_SCRIPT,
        json.dumps(_verify_invocation_payload(invocation)),
        "-" if runs_dir is None else str(runs_dir.expanduser().resolve()),
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
        raise MentalModelError(message)
    decoded = json.loads(completed.stdout)
    if not isinstance(decoded, dict):
        raise MentalModelError("External verify helper must return a JSON object.")
    if completed_run_sink is not None:
        runtime_payload = decoded.get("runtime")
        if isinstance(runtime_payload, dict):
            run_artifacts_dir = runtime_payload.get("run_artifacts_dir")
            if isinstance(run_artifacts_dir, str) and run_artifacts_dir:
                run_dir = Path(run_artifacts_dir)
                upload_result: dict[str, object] | CompletedRunPublishResult
                try:
                    upload_result = completed_run_sink.publish_run_dir(run_dir)
                except Exception as exc:
                    existing_upload = runtime_payload.get("completed_run_upload")
                    if isinstance(existing_upload, dict):
                        upload_result = existing_upload
                    else:
                        upload_result = {
                            "transport": type(completed_run_sink).__name__,
                            "success": False,
                            "graph_id": run_dir.parent.name,
                            "run_id": run_dir.name,
                            "project_id": run_target.project_id,
                            "server_url": None,
                            "remote_run_dir": None,
                            "uploaded_at_ms": None,
                            "error": str(exc),
                        }
                        if isinstance(completed_run_sink, RemoteServiceCompletedRunSink):
                            upload_result["transport"] = "service-api"
                            upload_result["server_url"] = completed_run_sink.server_url
                if isinstance(upload_result, dict):
                    runtime_payload["completed_run_upload"] = upload_result
                else:
                    runtime_payload["completed_run_upload"] = upload_result.as_dict()
    return cast(dict[str, object], decoded)


def _verify_invocation_payload(invocation: VerifyInvocationSpec) -> dict[str, object]:
    return {
        "program": {
            "entrypoint": invocation.program.entrypoint,
            "params": dict(invocation.program.params),
        },
        "environment": (
            None
            if invocation.environment is None
            else {
                "entrypoint": invocation.environment.entrypoint,
                "params": dict(invocation.environment.params),
            }
        ),
        "invocation_name": invocation.invocation_name,
    }


def run_replay(
    *,
    runs_dir: Path | None = None,
    graph_id: str,
    run_id: str | None = None,
    invocation_name: str | None = None,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
    json_output: bool = False,
) -> int:
    """Replay one persisted run as a semantic timeline."""

    report = build_replay_report(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    if json_output:
        print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
        return 0

    console = Console()
    summary = Table(title="mentalmodel replay")
    summary.add_column("Field")
    summary.add_column("Value")
    for field, value in (
        ("Graph", report.summary.graph_id),
        ("Run", report.summary.run_id),
        ("Schema", str(report.summary.schema_version)),
        ("Invocation", report.summary.invocation_name or ""),
        ("Success", "yes" if report.summary.success else "no"),
        (
            "Verification",
            "yes"
            if report.verification_success is True
            else "no" if report.verification_success is False else "unknown",
        ),
        ("Frames", str(len(report.frame_ids))),
        ("Events", str(len(report.events))),
        ("Nodes", str(len(report.node_summaries))),
        ("Runtime Error", report.runtime_error or ""),
    ):
        summary.add_row(field, value)
    console.print(summary)

    events = Table(title="Replay Events")
    events.add_column("Seq", justify="right")
    events.add_column("Frame")
    events.add_column("Node")
    events.add_column("Event")
    events.add_column("Timestamp", justify="right")
    events.add_column("Payload")
    for event in report.events:
        events.add_row(
            str(event.sequence),
            event.frame_id,
            event.node_id,
            event.event_type,
            str(event.timestamp_ms),
            json.dumps(event.payload, sort_keys=True),
        )
    console.print(events)

    nodes = Table(title="Replay Node Summary")
    nodes.add_column("Node")
    nodes.add_column("Frame")
    nodes.add_column("Iteration", justify="right")
    nodes.add_column("Events", justify="right")
    nodes.add_column("First Seq", justify="right")
    nodes.add_column("Last Seq", justify="right")
    nodes.add_column("Last Event")
    nodes.add_column("Invariant")
    for node_summary in report.node_summaries:
        nodes.add_row(
            node_summary.node_id,
            node_summary.frame_id,
            str(node_summary.iteration_index or ""),
            str(node_summary.event_count),
            str(node_summary.first_sequence or ""),
            str(node_summary.last_sequence or ""),
            node_summary.last_event_type or "",
            node_summary.invariant_status or "",
        )
    console.print(nodes)
    return 0


def run_otel_show_config(*, json_output: bool = False) -> int:
    """Show the resolved tracing configuration for the current process."""

    config = load_tracing_config()
    payload = {
        "service_name": config.service_name,
        "service_namespace": config.service_namespace,
        "service_version": config.service_version,
        "mode": config.mode.value,
        "otlp_endpoint": config.otlp_endpoint,
        "otlp_headers": config.otlp_headers,
        "otlp_insecure": config.otlp_insecure,
        "mirror_to_disk": config.mirror_to_disk,
        "capture_local_spans": config.capture_local_spans,
        "external_sink_configured": config.external_sink_configured,
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    table = Table(title="mentalmodel otel config")
    table.add_column("Field")
    table.add_column("Value")
    for field, value in payload.items():
        rendered = (
            json.dumps(value, sort_keys=True)
            if isinstance(value, dict)
            else str(value)
        )
        table.add_row(field, rendered)
    Console().print(table)
    return 0


def run_otel_write_demo(
    *,
    stack: str,
    output_dir: Path,
    json_output: bool = False,
) -> int:
    """Write one self-hosted OpenTelemetry demo stack to disk."""

    written = write_otel_demo(output_dir=output_dir, stack=stack)
    payload = {
        "stack": stack,
        "output_dir": str(output_dir),
        "files": [str(path) for path in written],
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    table = Table(title="mentalmodel otel demo")
    table.add_column("Stack")
    table.add_column("File")
    for path in written:
        table.add_row(stack, str(path))
    Console().print(table)
    Console().print(f"[green]wrote[/green] {output_dir}")
    return 0


def run_remote_sync(
    *,
    server_url: str | None = None,
    config: Path | None = None,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    project_id: str | None = None,
    project_label: str | None = None,
    environment_name: str | None = None,
    catalog_entry_id: str | None = None,
    catalog_source: str | None = None,
    json_output: bool = False,
) -> int:
    """Sync local run bundles to the remote ingest API."""

    project_config = None
    if config is not None or server_url is None:
        project_config = (
            load_project_config(config)
            if config is not None
            else load_discovered_project_config()
        )
    resolved_runs_dir = runs_dir
    if resolved_runs_dir is None and project_config is not None:
        resolved_runs_dir = project_config.default_runs_dir
    resolved_catalog_source = (
        None if catalog_source is None else CatalogSource(catalog_source)
    )
    receipts = (
        sync_runs_to_server(
            server_url=cast(str, server_url),
            runs_dir=resolved_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            invocation_name=invocation_name,
            project_id=project_id,
            project_label=project_label,
            environment_name=environment_name,
            catalog_entry_id=catalog_entry_id,
            catalog_source=resolved_catalog_source,
        )
        if project_config is None
        else sync_runs_for_project(
            config=project_config,
            runs_dir=resolved_runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            invocation_name=invocation_name,
            project_id=project_id,
            project_label=project_label,
            environment_name=environment_name,
            catalog_entry_id=catalog_entry_id,
            catalog_source=resolved_catalog_source,
        )
    )
    resolved_server_url = server_url if project_config is None else project_config.server_url
    payload = {
        "server_url": resolved_server_url,
        "count": len(receipts),
        "runs": [receipt.as_dict() for receipt, _ in receipts],
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    table = Table(title="mentalmodel remote sync")
    table.add_column("Graph")
    table.add_column("Run")
    table.add_column("Project")
    table.add_column("Uploaded", justify="right")
    for receipt, _attempt_count in receipts:
        table.add_row(
            receipt.graph_id,
            receipt.run_id,
            receipt.project_id or "",
            str(receipt.uploaded_at_ms),
        )
    Console().print(table)
    return 0


def run_remote_link(
    *,
    config: Path | None = None,
    json_output: bool = False,
) -> int:
    """Link one repo-owned mentalmodel project to a remote service."""

    project_config = (
        load_project_config(config)
        if config is not None
        else load_discovered_project_config()
    )
    project = link_project_to_server(project_config)
    payload = {
        "config_path": str(project_config.config_path),
        "repo_root": str(project_config.repo_root),
        "server_url": project_config.server_url,
        "project": project.as_dict(include_catalog_snapshot=True),
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    table = Table(title="mentalmodel remote link")
    table.add_column("Project")
    table.add_column("Server")
    table.add_column("Catalog")
    table.add_column("Version")
    table.add_row(
        project.project_id,
        project_config.server_url,
        (
            f"published ({project.catalog_entry_count} entries)"
            if project.catalog_published
            else "not published"
        ),
        str(project.catalog_version or ""),
    )
    Console().print(table)
    return 0


def run_remote_status(
    *,
    config: Path | None = None,
    json_output: bool = False,
) -> int:
    """Read remote project link status for one repo-owned mentalmodel project."""

    project_config = (
        load_project_config(config)
        if config is not None
        else load_discovered_project_config()
    )
    project = fetch_remote_project_status(project_config)
    payload = {
        "config_path": str(project_config.config_path),
        "repo_root": str(project_config.repo_root),
        "server_url": project_config.server_url,
        "project": project.as_dict(include_catalog_snapshot=False),
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    table = Table(title="mentalmodel remote status")
    table.add_column("Project")
    table.add_column("Server")
    table.add_column("Linked")
    table.add_column("Updated")
    table.add_column("Catalog")
    table.add_column("Version")
    table.add_column("Last Upload")
    table.add_row(
        project.project_id,
        project_config.server_url,
        str(project.linked_at_ms),
        str(project.updated_at_ms),
        (
            f"published ({project.catalog_entry_count} entries)"
            if project.catalog_published
            else "not published"
        ),
        str(project.catalog_version or ""),
        (
            ""
            if project.last_completed_run_upload_at_ms is None
            else (
                f"{project.last_completed_run_graph_id}/"
                f"{project.last_completed_run_id} @ "
                f"{project.last_completed_run_upload_at_ms}"
            )
        ),
    )
    Console().print(table)
    return 0


def run_remote_publish_catalog(
    *,
    config: Path | None = None,
    json_output: bool = False,
) -> int:
    """Publish the current repo-owned dashboard catalog snapshot to the remote service."""

    project_config = (
        load_project_config(config)
        if config is not None
        else load_discovered_project_config()
    )
    project = publish_catalog_to_server(project_config)
    payload = {
        "config_path": str(project_config.config_path),
        "repo_root": str(project_config.repo_root),
        "server_url": project_config.server_url,
        "project": project.as_dict(include_catalog_snapshot=True),
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    table = Table(title="mentalmodel remote publish-catalog")
    table.add_column("Project")
    table.add_column("Server")
    table.add_column("Catalog")
    table.add_column("Version")
    table.add_row(
        project.project_id,
        project_config.server_url,
        (
            f"published ({project.catalog_entry_count} entries)"
            if project.catalog_published
            else "not published"
        ),
        str(project.catalog_version or ""),
    )
    Console().print(table)
    return 0


def run_remote_write_demo(
    *,
    output_dir: Path,
    profile: str,
    workspace_id: str | None = None,
    workspace_label: str | None = None,
    mentalmodel_root: Path | None = None,
    pangramanizer_root: Path | None = None,
    json_output: bool = False,
) -> int:
    """Write a local remote-demo directory with workspace config and helper assets."""

    written = write_remote_demo(
        output_dir=output_dir,
        profile=profile,
        workspace_id=workspace_id or "mentalmodel-local",
        workspace_label=workspace_label or "Mentalmodel Local Stack",
        mentalmodel_root=mentalmodel_root,
        pangramanizer_root=pangramanizer_root,
    )
    payload = {
        "profile": profile,
        "output_dir": str(output_dir.expanduser().resolve()),
        "files": [str(path) for path in written],
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    table = Table(title="mentalmodel remote write-demo")
    table.add_column("Profile")
    table.add_column("File")
    for path in written:
        table.add_row(profile, str(path))
    Console().print(table)
    Console().print(f"[green]wrote[/green] {output_dir.expanduser().resolve()}")
    return 0


def run_remote_doctor(
    *,
    config: Path | None = None,
    workspace_config: Path | None = None,
    runs_dir: Path | None = None,
    json_output: bool = False,
) -> int:
    """Validate hosted repo-linked mode or the generated local stack mode."""

    result = build_remote_mode_doctor_report(
        config=config,
        workspace_config=workspace_config,
        runs_dir=runs_dir,
    )
    if json_output:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
        return 0 if result.success else 1

    console = Console()
    summary = Table(title=f"mentalmodel remote doctor ({result.mode.value})")
    summary.add_column("Check")
    summary.add_column("Status")
    summary.add_column("Message")
    for check in result.report.checks:
        status_style = {
            DoctorStatus.PASS: "[green]pass[/green]",
            DoctorStatus.WARN: "[yellow]warn[/yellow]",
            DoctorStatus.FAIL: "[red]fail[/red]",
            DoctorStatus.SKIP: "[cyan]skip[/cyan]",
        }[check.status]
        summary.add_row(check.name, status_style, check.message)
    console.print(summary)
    return 0 if result.success else 1


def run_remote_up(
    *,
    output_dir: Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = False,
    frontend_dist: Path | None = None,
    frontend_dev_url: str | None = None,
) -> int:
    """Start the generated remote backend services and launch the dashboard."""

    resolved_output = output_dir.expanduser().resolve()
    workspace_config = resolved_output / "workspace.toml"
    compose_path = resolved_output / "docker-compose.remote-minimal.yml"
    if not workspace_config.exists() or not compose_path.exists():
        raise MentalModelError(
            "Remote demo assets are missing. "
            "Run `mentalmodel remote write-demo --output-dir ...` first."
        )
    completed = subprocess.run(
        ["docker", "compose", "-f", str(compose_path), "up", "-d"],
        cwd=resolved_output,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or (
            f"docker compose failed with exit code {completed.returncode}."
        )
        raise MentalModelError(message)
    env = _load_env_file(resolved_output / "mentalmodel.remote.env")
    return run_ui(
        runs_dir=Path(env["MENTALMODEL_REMOTE_RUNS_DIR"]),
        workspace_config=workspace_config,
        host=host,
        port=port,
        frontend_dist=frontend_dist,
        frontend_dev_url=frontend_dev_url,
        open_browser=open_browser,
        remote_database_url=env.get("MENTALMODEL_REMOTE_DATABASE_URL"),
        remote_object_store_bucket=env.get("MENTALMODEL_REMOTE_OBJECT_STORE_BUCKET"),
        remote_object_store_endpoint=env.get("MENTALMODEL_REMOTE_OBJECT_STORE_ENDPOINT"),
        remote_object_store_region=env.get("MENTALMODEL_REMOTE_OBJECT_STORE_REGION"),
        remote_object_store_access_key=env.get("MENTALMODEL_REMOTE_OBJECT_STORE_ACCESS_KEY"),
        remote_object_store_secret_key=env.get("MENTALMODEL_REMOTE_OBJECT_STORE_SECRET_KEY"),
        remote_object_store_secure=_parse_bool_env(
            env.get("MENTALMODEL_REMOTE_OBJECT_STORE_SECURE")
        ),
        remote_cache_dir=(
            None
            if env.get("MENTALMODEL_REMOTE_CACHE_DIR") is None
            else Path(env["MENTALMODEL_REMOTE_CACHE_DIR"])
        ),
    )


def run_remote_down(
    *,
    output_dir: Path,
) -> int:
    """Stop the generated remote backend services."""

    resolved_output = output_dir.expanduser().resolve()
    compose_path = resolved_output / "docker-compose.remote-minimal.yml"
    if not compose_path.exists():
        raise MentalModelError(
            "Remote demo docker-compose file is missing. "
            "Run `mentalmodel remote write-demo --output-dir ...` first."
        )
    completed = subprocess.run(
        ["docker", "compose", "-f", str(compose_path), "down"],
        cwd=resolved_output,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or (
            f"docker compose failed with exit code {completed.returncode}."
        )
        raise MentalModelError(message)
    Console().print(f"[green]stopped[/green] {compose_path}")
    return 0


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise MentalModelError(f"Expected env file at {path}.")
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            continue
        env[key] = value
    return env


def _parse_bool_env(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise MentalModelError(f"Expected boolean env value, got {value!r}.")


def run_install_skills_command(
    agent: str,
    *,
    target_dir: Path | None = None,
    dry_run: bool = False,
) -> int:
    """Install packaged skill templates for one supported agent."""

    plan = build_install_plan(agent, target_dir=target_dir)
    if not dry_run:
        plan = install_skills(agent, target_dir=target_dir, dry_run=False)

    console = Console()
    title = "mentalmodel install-skills dry run" if dry_run else "mentalmodel install-skills"
    table = Table(title=title)
    table.add_column("Agent")
    table.add_column("Target")
    table.add_column("File")
    for file in plan.files:
        table.add_row(plan.agent, str(file.path.parent), str(file.path))
    console.print(table)
    return 0


def run_doctor(
    *,
    agent: str,
    target_dir: Path | None = None,
    runs_dir: Path | None = None,
    entrypoint: str | None = None,
    json_output: bool = False,
) -> int:
    """Run lightweight setup and debugging preflight checks."""

    report = build_doctor_report(
        agent=agent,
        target_dir=target_dir,
        runs_dir=runs_dir,
        entrypoint=entrypoint,
    )
    if json_output:
        print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
        return 0 if report.success else 1

    console = Console()
    summary = Table(title="mentalmodel doctor")
    summary.add_column("Check")
    summary.add_column("Status")
    summary.add_column("Message")
    for check in report.checks:
        status_style = {
            DoctorStatus.PASS: "[green]pass[/green]",
            DoctorStatus.WARN: "[yellow]warn[/yellow]",
            DoctorStatus.FAIL: "[red]fail[/red]",
            DoctorStatus.SKIP: "[cyan]skip[/cyan]",
        }[check.status]
        summary.add_row(check.name, status_style, check.message)
    console.print(summary)

    details = Table(title="Doctor Details")
    details.add_column("Check")
    details.add_column("Details")
    for check in report.checks:
        if not check.details:
            continue
        details.add_row(check.name, json.dumps(check.details, sort_keys=True))
    if details.row_count > 0:
        console.print(details)
    return 0 if report.success else 1


def run_demo_command(
    name: str,
    *,
    write_artifacts: bool = False,
    output_dir: Path | None = None,
    runs_dir: Path | None = None,
    json_output: bool = False,
) -> int:
    """Run or materialize one packaged reference demo."""

    if name == "async-rl":
        return _run_packaged_demo(
            name=name,
            build_program=build_async_rl_demo,
            module_name="mentalmodel.examples.async_rl.demo",
            default_output_dirname=ASYNC_RL_OUTPUT_DIRNAME,
            artifact_names=expected_artifact_names(),
            generate_artifacts=generate_demo_artifacts,
            write_artifacts=write_artifacts,
            output_dir=output_dir,
            runs_dir=runs_dir,
            json_output=json_output,
        )
    if name == "agent-tool-use":
        return _run_packaged_demo(
            name=name,
            build_program=build_agent_tool_use_demo,
            module_name="mentalmodel.examples.agent_tool_use.demo",
            default_output_dirname=AGENT_TOOL_USE_OUTPUT_DIRNAME,
            artifact_names=expected_agent_tool_use_artifact_names(),
            generate_artifacts=generate_agent_tool_use_artifacts,
            write_artifacts=write_artifacts,
            output_dir=output_dir,
            runs_dir=runs_dir,
            json_output=json_output,
        )
    if name == "autoresearch-sorting":
        return _run_autoresearch_sorting_demo(
            write_artifacts=write_artifacts,
            output_dir=output_dir,
            runs_dir=runs_dir,
            json_output=json_output,
        )
    raise EntrypointLoadError(
        f"Unknown demo {name!r}. Expected 'async-rl', 'agent-tool-use', or "
        "'autoresearch-sorting'."
    )


def _run_packaged_demo(
    *,
    name: str,
    build_program: Callable[[], Workflow[NamedPrimitive]],
    module_name: str,
    default_output_dirname: str,
    artifact_names: tuple[str, ...],
    generate_artifacts: Callable[[], dict[str, str]],
    write_artifacts: bool,
    output_dir: Path | None,
    runs_dir: Path | None,
    json_output: bool,
) -> int:
    graph = lower_program(build_program())
    report = run_analysis(graph)
    demo_module = importlib.import_module(module_name)
    verification = run_verification(
        build_program(),
        module=demo_module,
        runs_dir=runs_dir,
    )
    artifact_dir = output_dir or (Path.cwd() / default_output_dirname)

    if write_artifacts:
        generated = generate_artifacts()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        for artifact_name, content in generated.items():
            (artifact_dir / artifact_name).write_text(content + "\n", encoding="utf-8")

    if json_output:
        payload = {
            "demo": name,
            "graph_id": graph.graph_id,
            "node_count": len(graph.nodes),
            "edge_count": len(graph.edges),
            "artifacts": list(artifact_names),
            "artifact_dir": str(artifact_dir),
            "wrote_artifacts": write_artifacts,
            "run_artifacts_dir": verification.runtime.run_artifacts_dir,
            "verification": verification.as_dict(),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if verification.success and not report.has_errors else 1

    console = Console()
    summary = Table(title="mentalmodel demo summary")
    summary.add_column("Demo")
    summary.add_column("Graph")
    summary.add_column("Nodes", justify="right")
    summary.add_column("Edges", justify="right")
    summary.add_column("Runtime", justify="right")
    summary.add_column("Property Checks", justify="right")
    summary.add_column("Run Artifacts")
    summary.add_row(
        name,
        graph.graph_id,
        str(len(graph.nodes)),
        str(len(graph.edges)),
        "pass" if verification.runtime.success else "fail",
        str(len(verification.property_checks)),
        verification.runtime.run_artifacts_dir or "",
    )
    console.print(summary)

    artifacts = Table(title="Demo Artifacts")
    artifacts.add_column("Artifact")
    artifacts.add_column("Target Directory")
    for artifact_name in artifact_names:
        artifacts.add_row(artifact_name, str(artifact_dir))
    console.print(artifacts)

    if write_artifacts:
        console.print(f"[green]wrote[/green] {artifact_dir}")
    else:
        console.print(
            "[yellow]Artifacts not written. Use --write-artifacts to materialize them.[/yellow]"
        )
    return 0 if verification.success and not report.has_errors else 1


def _run_autoresearch_sorting_demo(
    *,
    write_artifacts: bool,
    output_dir: Path | None,
    runs_dir: Path | None,
    json_output: bool,
) -> int:
    program = build_autoresearch_sorting_demo()
    verification = run_verification(program, runs_dir=runs_dir)
    execution = execute_program(program)
    output = cast(dict[str, object], execution.outputs["autoresearch_sorting"])
    artifact_dir = output_dir or (Path.cwd() / "mentalmodel-demo-autoresearch-sorting")
    bundle = write_autoresearch_bundle(artifact_dir) if write_artifacts else None
    payload = {
        "demo": "autoresearch-sorting",
        "objective_name": output["objective_name"],
        "graph_id": execution.graph.graph_id,
        "best_candidate": output["best_candidate"],
        "best_score": output["best_score"],
        "metric_name": output["metric_name"],
        "results": output["candidate_results"],
        "run_artifacts_dir": verification.runtime.run_artifacts_dir,
        "verification": verification.as_dict(),
        "wrote_artifacts": write_artifacts,
        "artifact_dir": str(artifact_dir),
        "bundle_files": (
            []
            if bundle is None
            else [
                str(bundle.program_path),
                str(bundle.objective_path),
                str(bundle.candidates_path),
            ]
        ),
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    console = Console()
    summary = Table(title="mentalmodel demo summary")
    summary.add_column("Demo")
    summary.add_column("Objective")
    summary.add_column("Best Candidate")
    summary.add_column("Score", justify="right")
    summary.add_column("Metric")
    summary.add_row(
        "autoresearch-sorting",
        str(output["objective_name"]),
        str(output["best_candidate"]),
        str(output["best_score"]),
        str(output["metric_name"]),
    )
    console.print(summary)

    results = Table(title="Autoresearch Candidate Results")
    results.add_column("Candidate")
    results.add_column("Success")
    results.add_column("Verification")
    results.add_column("Score", justify="right")
    candidate_results = cast(list[dict[str, object]], output["candidate_results"])
    for result in candidate_results:
        results.add_row(
            str(result["candidate_label"]),
            "yes" if bool(result["success"]) else "no",
            "yes" if bool(result["verification_success"]) else "no",
            str(result["score"]),
        )
    console.print(results)

    if bundle is not None:
        console.print(f"[green]wrote[/green] {artifact_dir}")
    else:
        console.print(
            "[yellow]Bundle not written. Use --write-artifacts to materialize program.md and "
            "objective metadata.[/yellow]"
        )
    return 0


def run_runs_list(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    invocation_name: str | None = None,
    limit: int = 20,
    json_output: bool = False,
) -> int:
    """List persisted run bundles."""

    summaries = list_run_summaries(
        runs_dir=runs_dir,
        graph_id=graph_id,
        invocation_name=invocation_name,
    )[: max(1, limit)]
    if json_output:
        print(
            json.dumps(
                [
                    {
                        "schema_version": summary.schema_version,
                        "graph_id": summary.graph_id,
                        "run_id": summary.run_id,
                        "created_at_ms": summary.created_at_ms,
                    "success": summary.success,
                    "record_count": summary.record_count,
                    "output_count": summary.output_count,
                        "state_count": summary.state_count,
                        "trace_mode": summary.trace_mode,
                        "trace_mirror_to_disk": summary.trace_mirror_to_disk,
                        "invocation_name": summary.invocation_name,
                        "runtime_default_profile_name": summary.runtime_default_profile_name,
                        "runtime_profile_names": list(summary.runtime_profile_names),
                        "run_dir": str(summary.run_dir),
                    }
                    for summary in summaries
                ],
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    table = Table(title="mentalmodel runs")
    table.add_column("Graph")
    table.add_column("Run")
    table.add_column("Success")
    table.add_column("Records", justify="right")
    table.add_column("Outputs", justify="right")
    table.add_column("State", justify="right")
    table.add_column("Trace")
    table.add_column("Invocation")
    table.add_column("Profiles")
    table.add_column("Path")
    for summary in summaries:
        table.add_row(
            summary.graph_id,
            summary.run_id,
            "yes" if summary.success else "no",
            str(summary.record_count),
            str(summary.output_count),
            str(summary.state_count),
            summary.trace_mode,
            summary.invocation_name or "",
            ", ".join(summary.runtime_profile_names) or "none",
            str(summary.run_dir),
        )
    if not summaries:
        Console().print("[yellow]No runs found.[/yellow]")
        return 0
    Console().print(table)
    return 0


def run_runs_show(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    json_output: bool = False,
) -> int:
    """Show one persisted run bundle and its files."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    verification = _optional_run_payload(summary.run_dir / "verification.json")
    payload = {
        "graph_id": summary.graph_id,
        "schema_version": summary.schema_version,
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
        "invocation_name": summary.invocation_name,
        "runtime_default_profile_name": summary.runtime_default_profile_name,
        "runtime_profile_names": list(summary.runtime_profile_names),
        "run_dir": str(summary.run_dir),
        "files": {
            "summary": str(summary.run_dir / "summary.json"),
            "verification": str(summary.run_dir / "verification.json"),
            "records": str(summary.run_dir / "records.jsonl"),
            "outputs": str(summary.run_dir / "outputs.json"),
            "state": str(summary.run_dir / "state.json"),
            "spans": str(summary.run_dir / "otel-spans.jsonl"),
        },
        "verification_success": _verification_success(verification),
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    console = Console()
    summary_table = Table(title="mentalmodel run")
    summary_table.add_column("Field")
    summary_table.add_column("Value")
    for field, value in (
        ("Graph", summary.graph_id),
        ("Schema", str(summary.schema_version)),
        ("Run", summary.run_id),
        ("Success", "yes" if summary.success else "no"),
        ("Created", str(summary.created_at_ms)),
        ("Records", str(summary.record_count)),
        ("Outputs", str(summary.output_count)),
        ("State", str(summary.state_count)),
        ("Trace Mode", summary.trace_mode),
        ("Trace Endpoint", summary.trace_otlp_endpoint or ""),
        ("Trace Sink", "configured" if summary.trace_sink_configured else "disk fallback"),
        ("Mirror To Disk", "yes" if summary.trace_mirror_to_disk else "no"),
        ("Invocation", summary.invocation_name or ""),
        ("Default Profile", summary.runtime_default_profile_name or ""),
        ("Profiles", ", ".join(summary.runtime_profile_names) or "none"),
        ("Run Dir", str(summary.run_dir)),
    ):
        summary_table.add_row(field, value)
    console.print(summary_table)

    files_table = Table(title="Run Files")
    files_table.add_column("Name")
    files_table.add_column("Path")
    for label, filename in (
        ("summary", "summary.json"),
        ("verification", "verification.json"),
        ("records", "records.jsonl"),
        ("outputs", "outputs.json"),
        ("state", "state.json"),
        ("spans", "otel-spans.jsonl"),
    ):
        files_table.add_row(label, str(summary.run_dir / filename))
    console.print(files_table)
    return 0


def run_runs_latest(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    invocation_name: str | None = None,
    json_output: bool = False,
) -> int:
    """Resolve and show the newest matching run."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=None,
        invocation_name=invocation_name,
    )
    payload = {
        "graph_id": summary.graph_id,
        "schema_version": summary.schema_version,
        "run_id": summary.run_id,
        "created_at_ms": summary.created_at_ms,
        "success": summary.success,
        "run_dir": str(summary.run_dir),
        "record_count": summary.record_count,
        "output_count": summary.output_count,
        "state_count": summary.state_count,
        "trace_mode": summary.trace_mode,
        "trace_mirror_to_disk": summary.trace_mirror_to_disk,
        "invocation_name": summary.invocation_name,
        "runtime_default_profile_name": summary.runtime_default_profile_name,
        "runtime_profile_names": list(summary.runtime_profile_names),
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    table = Table(title="mentalmodel latest run")
    table.add_column("Field")
    table.add_column("Value")
    for field, value in (
        ("Graph", summary.graph_id),
        ("Run", summary.run_id),
        ("Success", "yes" if summary.success else "no"),
        ("Created", str(summary.created_at_ms)),
        ("Trace Mode", summary.trace_mode),
        ("Invocation", summary.invocation_name or ""),
        ("Default Profile", summary.runtime_default_profile_name or ""),
        ("Profiles", ", ".join(summary.runtime_profile_names) or "none"),
        ("Run Dir", str(summary.run_dir)),
    ):
        table.add_row(field, value)
    Console().print(table)
    return 0


def run_runs_repair(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    dry_run: bool = False,
    json_output: bool = False,
) -> int:
    """Plan or apply deterministic repairs for run bundle summaries."""

    plan = plan_run_repairs(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    if not dry_run:
        plan = apply_run_repairs(plan)

    if json_output:
        print(
            json.dumps(
                {
                    "root_dir": str(plan.root_dir),
                    "dry_run": dry_run,
                    "action_count": len(plan.actions),
                    "actions": [
                        {
                            "graph_id": action.graph_id,
                            "run_id": action.run_id,
                            "run_dir": str(action.run_dir),
                            "from_schema_version": action.from_schema_version,
                            "to_schema_version": action.to_schema_version,
                            "updates": action.updates,
                        }
                        for action in plan.actions
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    table = Table(
        title="mentalmodel runs repair dry run"
        if dry_run
        else "mentalmodel runs repair"
    )
    table.add_column("Graph")
    table.add_column("Run")
    table.add_column("From")
    table.add_column("To")
    table.add_column("Updates")
    for action in plan.actions:
        table.add_row(
            action.graph_id,
            action.run_id,
            str(action.from_schema_version),
            str(action.to_schema_version),
            ", ".join(sorted(action.updates.keys())),
        )
    console = Console()
    if plan.actions:
        console.print(table)
    else:
        console.print("[green]No repairs needed.[/green]")
    return 0


def run_runs_diff(
    *,
    runs_dir: Path | None = None,
    graph_id: str,
    run_a: str,
    run_b: str,
    node_id: str | None = None,
    invariant: str | None = None,
    json_output: bool = False,
) -> int:
    """Compare two persisted run bundles from the same graph."""

    diff = build_run_diff(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_a=run_a,
        run_b=run_b,
        node_id=node_id,
        invariant=invariant,
    )
    if json_output:
        print(json.dumps(diff.as_dict(), indent=2, sort_keys=True))
        return 0

    console = Console()
    summary = Table(title="mentalmodel runs diff")
    summary.add_column("Field")
    summary.add_column("Value")
    for field, value in (
        ("Graph", diff.graph_id),
        ("Run A", diff.run_a.run_id),
        ("Run B", diff.run_b.run_id),
        ("Differs", "yes" if diff.differs else "no"),
        ("State Equal", "yes" if diff.state_equal else "no"),
        (
            "Verification A",
            "yes"
            if diff.verification_success_run_a is True
            else "no" if diff.verification_success_run_a is False else "unknown",
        ),
        (
            "Verification B",
            "yes"
            if diff.verification_success_run_b is True
            else "no" if diff.verification_success_run_b is False else "unknown",
        ),
    ):
        summary.add_row(field, value)
    console.print(summary)

    node_table = Table(title="Node Diffs")
    node_table.add_column("Node")
    node_table.add_column("Frame")
    node_table.add_column("Differs")
    node_table.add_column("Events")
    node_table.add_column("Inputs")
    node_table.add_column("Outputs")
    node_table.add_column("Missing")
    for node_diff in diff.node_diffs:
        missing = []
        if node_diff.missing_in_run_a:
            missing.append("A")
        if node_diff.missing_in_run_b:
            missing.append("B")
        node_table.add_row(
            node_diff.node_id,
            node_diff.frame_id,
            "yes" if node_diff.differs else "no",
            "same" if node_diff.events_equal else "different",
            _comparison_status(node_diff.inputs_equal),
            _comparison_status(node_diff.outputs_equal),
            ",".join(missing),
        )
    console.print(node_table)

    if diff.invariant_diffs:
        invariant_table = Table(title="Invariant Diffs")
        invariant_table.add_column("Node")
        invariant_table.add_column("Frame")
        invariant_table.add_column("Outcome A")
        invariant_table.add_column("Outcome B")
        invariant_table.add_column("Severity A")
        invariant_table.add_column("Severity B")
        invariant_table.add_column("Differs")
        for invariant_diff in diff.invariant_diffs:
            invariant_table.add_row(
                invariant_diff.node_id,
                invariant_diff.frame_id,
                _invariant_outcome_label(invariant_diff.outcome_run_a),
                _invariant_outcome_label(invariant_diff.outcome_run_b),
                invariant_diff.severity_run_a or "",
                invariant_diff.severity_run_b or "",
                "yes" if invariant_diff.differs else "no",
            )
        console.print(invariant_table)

    if not diff.state_equal:
        console.print(
            Panel.fit(
                json.dumps(
                    {
                        "state_run_a": diff.as_dict()["state_run_a"],
                        "state_run_b": diff.as_dict()["state_run_b"],
                    },
                    indent=2,
                    sort_keys=True,
                ),
                title="State Diff",
            )
        )
    return 0


def run_runs_inputs(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    node_id: str,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
    json_output: bool = False,
) -> int:
    """Show the resolved input payload for one node."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    payload = load_run_node_inputs(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        invocation_name=summary.invocation_name,
        node_id=node_id,
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    output = {
        "graph_id": summary.graph_id,
        "run_id": summary.run_id,
        "invocation_name": summary.invocation_name,
        "node_id": node_id,
        "frame_id": frame_id or "root",
        "loop_node_id": loop_node_id,
        "iteration_index": iteration_index,
        "inputs": payload,
    }
    if json_output:
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    Console().print(Panel.fit(json.dumps(output, indent=2, sort_keys=True), title="run inputs"))
    return 0


def run_runs_outputs(
    *,
    runs_dir: Path | None = None,
    graph_id: str | None = None,
    run_id: str | None = None,
    invocation_name: str | None = None,
    node_id: str,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
    json_output: bool = False,
) -> int:
    """Show the output payload for one node."""

    summary = resolve_run_summary(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
    )
    payload = load_run_node_output(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        invocation_name=summary.invocation_name,
        node_id=node_id,
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    output = {
        "graph_id": summary.graph_id,
        "run_id": summary.run_id,
        "invocation_name": summary.invocation_name,
        "node_id": node_id,
        "frame_id": frame_id or "root",
        "loop_node_id": loop_node_id,
        "iteration_index": iteration_index,
        "output": payload,
    }
    if json_output:
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    Console().print(Panel.fit(json.dumps(output, indent=2, sort_keys=True), title="run output"))
    return 0


def run_runs_trace(
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
    json_output: bool = False,
) -> int:
    """Show semantic trace data for one node."""

    trace = load_run_node_trace(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        invocation_name=invocation_name,
        node_id=node_id,
        event_type=event_type,
        frame_id=frame_id,
        loop_node_id=loop_node_id,
        iteration_index=iteration_index,
    )
    payload = {
        "graph_id": trace.summary.graph_id,
        "run_id": trace.summary.run_id,
        "invocation_name": trace.summary.invocation_name,
        "node_id": trace.node_id,
        "frame_id": frame_id,
        "loop_node_id": loop_node_id,
        "iteration_index": iteration_index,
        "records": list(trace.records),
        "spans": list(trace.spans),
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    console = Console()
    record_table = Table(title=f"mentalmodel trace {trace.summary.run_id} {trace.node_id}")
    record_table.add_column("Seq", justify="right")
    record_table.add_column("Frame")
    record_table.add_column("Event")
    record_table.add_column("Timestamp", justify="right")
    record_table.add_column("Payload")
    for record in trace.records:
        record_table.add_row(
            str(record.get("sequence", "")),
            str(record.get("frame_id", "")),
            str(record.get("event_type", "")),
            str(record.get("timestamp_ms", "")),
            json.dumps(record.get("payload", {}), sort_keys=True),
        )
    if trace.records:
        console.print(record_table)
    else:
        console.print("[yellow]No semantic records found for node.[/yellow]")

    if trace.spans:
        span_table = Table(title="Matching Spans")
        span_table.add_column("Name")
        span_table.add_column("Frame")
        span_table.add_column("Duration (ns)", justify="right")
        span_table.add_column("Error")
        for span in trace.spans:
            span_table.add_row(
                str(span.get("name", "")),
                str(span.get("frame_id", "")),
                str(span.get("duration_ns", "")),
                str(span.get("error_type", "") or ""),
            )
        console.print(span_table)
    return 0


def run_runs_records(
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
    limit: int = 50,
    json_output: bool = False,
) -> int:
    """Show semantic execution records for one persisted run."""

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
    limited = records[-max(1, limit) :]
    if json_output:
        print(json.dumps(list(limited), indent=2, sort_keys=True))
        return 0

    table = Table(title=f"mentalmodel records {summary.run_id}")
    table.add_column("Seq", justify="right")
    table.add_column("Node")
    table.add_column("Frame")
    table.add_column("Event")
    table.add_column("Timestamp", justify="right")
    table.add_column("Payload")
    for record in limited:
        table.add_row(
            str(record.get("sequence", "")),
            str(record.get("node_id", "")),
            str(record.get("frame_id", "")),
            str(record.get("event_type", "")),
            str(record.get("timestamp_ms", "")),
            json.dumps(record.get("payload", {}), sort_keys=True),
        )
    if not limited:
        Console().print("[yellow]No matching records found.[/yellow]")
        return 0
    Console().print(table)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mentalmodel",
        description="CLI scaffold for the mentalmodel package.",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("init", help="Initialize a mentalmodel project scaffold.")
    check = subparsers.add_parser("check", help="Run structural and semantic checks.")
    check.add_argument(
        "--entrypoint",
        default="mentalmodel.examples.async_rl.demo:build_program",
        help="Program entrypoint in `module:function` format.",
    )
    check.add_argument("--json", action="store_true", help="Emit JSON output.")
    graph = subparsers.add_parser("graph", help="Render graph artifacts from IR.")
    graph.add_argument(
        "--entrypoint",
        default="mentalmodel.examples.async_rl.demo:build_program",
        help="Program entrypoint in `module:function` format.",
    )
    graph.add_argument("--output", type=Path, help="Optional path to write Mermaid output.")
    docs = subparsers.add_parser("docs", help="Generate documentation from IR.")
    docs.add_argument(
        "--entrypoint",
        default="mentalmodel.examples.async_rl.demo:build_program",
        help="Program entrypoint in `module:function` format.",
    )
    docs.add_argument("--output-dir", type=Path, help="Directory to write markdown artifacts.")
    docs.add_argument("--stdout", action="store_true", help="Also render docs to stdout.")
    verify = subparsers.add_parser("verify", help="Run invariants and verification helpers.")
    verify.add_argument(
        "--entrypoint",
        default=None,
        help="Program entrypoint in `module:function` format.",
    )
    verify.add_argument(
        "--runs-dir",
        type=Path,
        help="Optional root directory for persisted run artifacts. Defaults to ./.runs.",
    )
    verify.add_argument(
        "--params-json",
        help="Optional JSON object passed as keyword arguments to a callable entrypoint.",
    )
    verify.add_argument(
        "--params-file",
        type=Path,
        help="Path to a JSON file passed as keyword arguments to a callable entrypoint.",
    )
    verify.add_argument(
        "--environment-entrypoint",
        help="Optional runtime environment entrypoint in `module:function` format.",
    )
    verify.add_argument(
        "--environment-params-json",
        help=(
            "Optional JSON object passed as keyword arguments to a callable "
            "environment entrypoint."
        ),
    )
    verify.add_argument(
        "--environment-params-file",
        type=Path,
        help=(
            "Path to a JSON file passed as keyword arguments to a callable "
            "environment entrypoint."
        ),
    )
    verify.add_argument(
        "--invocation-name",
        help="Optional run-level label persisted in .runs and emitted telemetry.",
    )
    verify.add_argument(
        "--spec",
        type=Path,
        help="Optional TOML verify spec describing program, environment, and runtime invocation.",
    )
    verify.add_argument(
        "--workspace-config",
        type=Path,
        help=(
            "Local stack mode only. Use a generated workspace.toml to route runs "
            "in one shared local dashboard stack."
        ),
    )
    verify.add_argument("--project-id")
    verify.add_argument("--remote-database-url")
    verify.add_argument("--remote-object-store-bucket")
    verify.add_argument("--remote-object-store-endpoint")
    verify.add_argument("--remote-object-store-region")
    verify.add_argument("--remote-object-store-access-key")
    verify.add_argument("--remote-object-store-secret-key")
    verify.add_argument(
        "--remote-object-store-secure",
        dest="remote_object_store_secure",
        action="store_true",
        default=None,
    )
    verify.add_argument(
        "--no-remote-object-store-secure",
        dest="remote_object_store_secure",
        action="store_false",
    )
    verify.add_argument("--remote-cache-dir", type=Path)
    verify.add_argument(
        "--live-otlp-endpoint",
        help="Optional OTLP collector endpoint for durable async live telemetry export.",
    )
    verify.add_argument(
        "--live-outbox-dir",
        type=Path,
        help="Optional directory for the durable live telemetry outbox.",
    )
    verify.add_argument(
        "--live-max-outbox-bytes",
        type=int,
        default=64 * 1024 * 1024,
        help="Hard cap for local live telemetry backlog before policy applies.",
    )
    verify.add_argument(
        "--live-max-batch-events",
        type=int,
        default=256,
        help="Maximum number of live telemetry envelopes per OTLP batch.",
    )
    verify.add_argument(
        "--live-max-batch-bytes",
        type=int,
        default=512 * 1024,
        help="Maximum serialized bytes per OTLP batch.",
    )
    verify.add_argument(
        "--live-flush-interval-ms",
        type=int,
        default=1_000,
        help="Maximum delay before the live exporter drains the outbox.",
    )
    verify.add_argument(
        "--live-shutdown-flush-timeout-ms",
        type=int,
        default=5_000,
        help="How long verify waits for the live exporter to drain on shutdown.",
    )
    verify.add_argument(
        "--require-live-delivery",
        action="store_true",
        help="Fail the run if the live outbox hits its hard capacity cap.",
    )
    verify.add_argument("--json", action="store_true", help="Emit JSON output.")
    replay = subparsers.add_parser("replay", help="Replay a recorded execution.")
    replay.add_argument("--runs-dir", type=Path)
    replay.add_argument("--graph-id", required=True)
    replay.add_argument("--run-id")
    replay.add_argument("--invocation-name")
    replay.add_argument("--frame-id")
    replay.add_argument("--loop-node-id")
    replay.add_argument("--iteration-index", type=int)
    replay.add_argument("--json", action="store_true", help="Emit JSON output.")

    ui = subparsers.add_parser("ui", help="Launch the hosted dashboard UI.")
    ui.add_argument("--runs-dir", type=Path)
    ui.add_argument(
        "--workspace-config",
        type=Path,
        help="Local stack mode only. Load projects from a generated workspace.toml.",
    )
    ui.add_argument("--host", default="127.0.0.1")
    ui.add_argument("--port", type=int, default=8765)
    ui.add_argument("--frontend-dist", type=Path)
    ui.add_argument(
        "--frontend-dev-url",
        help=(
            "Optional frontend dev-server URL. When set, the UI backend serves only "
            "the API and opens this URL instead of the built frontend shell."
        ),
    )
    ui.add_argument(
        "--catalog-entrypoint",
        help=(
            "Local mode only. Optional dashboard catalog provider in `module:attribute` "
            "format for repo-imported development catalogs."
        ),
    )
    ui.add_argument("--remote-database-url")
    ui.add_argument("--remote-object-store-bucket")
    ui.add_argument("--remote-object-store-endpoint")
    ui.add_argument("--remote-object-store-region")
    ui.add_argument("--remote-object-store-access-key")
    ui.add_argument("--remote-object-store-secret-key")
    ui.add_argument(
        "--remote-object-store-secure",
        dest="remote_object_store_secure",
        action="store_true",
        default=None,
    )
    ui.add_argument(
        "--no-remote-object-store-secure",
        dest="remote_object_store_secure",
        action="store_false",
    )
    ui.add_argument("--remote-cache-dir", type=Path)
    ui.add_argument("--open-browser", action="store_true")

    otel = subparsers.add_parser("otel", help="Inspect or materialize OTEL configuration.")
    otel_subparsers = otel.add_subparsers(dest="otel_command")
    otel_subparsers.required = True

    otel_show = otel_subparsers.add_parser("show-config", help="Show resolved tracing config.")
    otel_show.add_argument("--json", action="store_true", help="Emit JSON output.")

    otel_demo = otel_subparsers.add_parser(
        "write-demo",
        help="Write a self-hosted OTEL demo stack.",
    )
    otel_demo.add_argument("--stack", choices=["lgtm", "jaeger"], default="lgtm")
    otel_demo.add_argument("--output-dir", type=Path, required=True)
    otel_demo.add_argument("--json", action="store_true", help="Emit JSON output.")

    remote = subparsers.add_parser("remote", help="Sync runs to a remote ingest API.")
    remote_subparsers = remote.add_subparsers(dest="remote_command")
    remote_subparsers.required = True

    remote_link = remote_subparsers.add_parser(
        "link",
        help="Link the current repo-owned mentalmodel project to a remote service.",
    )
    remote_link.add_argument("--config", type=Path)
    remote_link.add_argument("--json", action="store_true", help="Emit JSON output.")

    remote_status = remote_subparsers.add_parser(
        "status",
        help="Read remote link status for the current repo-owned project.",
    )
    remote_status.add_argument("--config", type=Path)
    remote_status.add_argument("--json", action="store_true", help="Emit JSON output.")

    remote_publish_catalog = remote_subparsers.add_parser(
        "publish-catalog",
        help="Publish the current repo-owned dashboard catalog snapshot to the remote service.",
    )
    remote_publish_catalog.add_argument("--config", type=Path)
    remote_publish_catalog.add_argument(
        "--json", action="store_true", help="Emit JSON output."
    )

    remote_sync = remote_subparsers.add_parser(
        "sync",
        help="Sync persisted local runs to the remote ingest API.",
    )
    remote_sync.add_argument("--server-url")
    remote_sync.add_argument("--config", type=Path)
    remote_sync.add_argument("--runs-dir", type=Path)
    remote_sync.add_argument("--graph-id")
    remote_sync.add_argument("--run-id")
    remote_sync.add_argument("--invocation-name")
    remote_sync.add_argument("--project-id")
    remote_sync.add_argument("--project-label")
    remote_sync.add_argument("--environment-name")
    remote_sync.add_argument("--catalog-entry-id")
    remote_sync.add_argument(
        "--catalog-source",
        choices=[source.value for source in CatalogSource],
    )
    remote_sync.add_argument("--json", action="store_true", help="Emit JSON output.")

    remote_write_demo = remote_subparsers.add_parser(
        "write-demo",
        help="Write a localhost remote-runs demo directory.",
    )
    remote_write_demo.add_argument("--profile", choices=["minimal"], default="minimal")
    remote_write_demo.add_argument("--output-dir", type=Path, required=True)
    remote_write_demo.add_argument("--workspace-id")
    remote_write_demo.add_argument("--workspace-label")
    remote_write_demo.add_argument("--mentalmodel-root", type=Path)
    remote_write_demo.add_argument("--pangramanizer-root", type=Path)
    remote_write_demo.add_argument("--json", action="store_true", help="Emit JSON output.")

    remote_doctor = remote_subparsers.add_parser(
        "doctor",
        help="Validate hosted repo-linked mode or the generated local stack.",
    )
    remote_doctor.add_argument("--config", type=Path)
    remote_doctor.add_argument("--workspace-config", type=Path)
    remote_doctor.add_argument("--runs-dir", type=Path)
    remote_doctor.add_argument("--json", action="store_true", help="Emit JSON output.")

    remote_up = remote_subparsers.add_parser(
        "up",
        help="Start the generated remote backend services and launch the dashboard.",
    )
    remote_up.add_argument("--output-dir", type=Path, required=True)
    remote_up.add_argument("--host", default="127.0.0.1")
    remote_up.add_argument("--port", type=int, default=8765)
    remote_up.add_argument("--frontend-dist", type=Path)
    remote_up.add_argument("--frontend-dev-url")
    remote_up.add_argument("--open-browser", action="store_true")

    remote_down = remote_subparsers.add_parser(
        "down",
        help="Stop the generated remote backend services.",
    )
    remote_down.add_argument("--output-dir", type=Path, required=True)

    runs = subparsers.add_parser("runs", help="Inspect persisted run artifacts.")
    runs_subparsers = runs.add_subparsers(dest="runs_command")
    runs_subparsers.required = True

    runs_list = runs_subparsers.add_parser("list", help="List recent run bundles.")
    runs_list.add_argument("--runs-dir", type=Path)
    runs_list.add_argument("--graph-id")
    runs_list.add_argument("--invocation-name")
    runs_list.add_argument("--limit", type=int, default=20)
    runs_list.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_show = runs_subparsers.add_parser("show", help="Show one run bundle.")
    runs_show.add_argument("--runs-dir", type=Path)
    runs_show.add_argument("--graph-id")
    runs_show.add_argument("--run-id")
    runs_show.add_argument("--invocation-name")
    runs_show.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_latest = runs_subparsers.add_parser("latest", help="Resolve the newest matching run.")
    runs_latest.add_argument("--runs-dir", type=Path)
    runs_latest.add_argument("--graph-id")
    runs_latest.add_argument("--invocation-name")
    runs_latest.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_inputs = runs_subparsers.add_parser("inputs", help="Show one node input payload.")
    runs_inputs.add_argument("--runs-dir", type=Path)
    runs_inputs.add_argument("--graph-id")
    runs_inputs.add_argument("--run-id")
    runs_inputs.add_argument("--invocation-name")
    runs_inputs.add_argument("--node-id", required=True)
    runs_inputs.add_argument("--frame-id")
    runs_inputs.add_argument("--loop-node-id")
    runs_inputs.add_argument("--iteration-index", type=int)
    runs_inputs.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_outputs = runs_subparsers.add_parser("outputs", help="Show one node output payload.")
    runs_outputs.add_argument("--runs-dir", type=Path)
    runs_outputs.add_argument("--graph-id")
    runs_outputs.add_argument("--run-id")
    runs_outputs.add_argument("--invocation-name")
    runs_outputs.add_argument("--node-id", required=True)
    runs_outputs.add_argument("--frame-id")
    runs_outputs.add_argument("--loop-node-id")
    runs_outputs.add_argument("--iteration-index", type=int)
    runs_outputs.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_trace = runs_subparsers.add_parser("trace", help="Show semantic trace data for one node.")
    runs_trace.add_argument("--runs-dir", type=Path)
    runs_trace.add_argument("--graph-id")
    runs_trace.add_argument("--run-id")
    runs_trace.add_argument("--invocation-name")
    runs_trace.add_argument("--node-id", required=True)
    runs_trace.add_argument("--event-type")
    runs_trace.add_argument("--frame-id")
    runs_trace.add_argument("--loop-node-id")
    runs_trace.add_argument("--iteration-index", type=int)
    runs_trace.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_records = runs_subparsers.add_parser("records", help="Show run records.")
    runs_records.add_argument("--runs-dir", type=Path)
    runs_records.add_argument("--graph-id")
    runs_records.add_argument("--run-id")
    runs_records.add_argument("--invocation-name")
    runs_records.add_argument("--node-id")
    runs_records.add_argument("--event-type")
    runs_records.add_argument("--frame-id")
    runs_records.add_argument("--loop-node-id")
    runs_records.add_argument("--iteration-index", type=int)
    runs_records.add_argument("--limit", type=int, default=50)
    runs_records.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_repair = runs_subparsers.add_parser(
        "repair",
        help="Repair legacy run bundle summaries.",
    )
    runs_repair.add_argument("--runs-dir", type=Path)
    runs_repair.add_argument("--graph-id")
    runs_repair.add_argument("--run-id")
    runs_repair.add_argument("--dry-run", action="store_true")
    runs_repair.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_diff = runs_subparsers.add_parser(
        "diff",
        help="Compare two persisted run bundles.",
    )
    runs_diff.add_argument("--runs-dir", type=Path)
    runs_diff.add_argument("--graph-id", required=True)
    runs_diff.add_argument("--run-a", required=True)
    runs_diff.add_argument("--run-b", required=True)
    runs_diff.add_argument("--node-id")
    runs_diff.add_argument("--invariant")
    runs_diff.add_argument("--json", action="store_true", help="Emit JSON output.")

    demo = subparsers.add_parser("demo", help="Run or inspect a reference demo.")
    demo.add_argument(
        "name",
        nargs="?",
        default="async-rl",
        choices=["async-rl", "agent-tool-use", "autoresearch-sorting"],
    )
    demo.add_argument("--write-artifacts", action="store_true")
    demo.add_argument("--output-dir", type=Path)
    demo.add_argument("--runs-dir", type=Path)
    demo.add_argument("--json", action="store_true", help="Emit JSON output.")

    doctor = subparsers.add_parser("doctor", help="Run agent/debugging preflight checks.")
    doctor.add_argument("--agent", default="codex")
    doctor.add_argument("--target-dir", type=Path)
    doctor.add_argument("--runs-dir", type=Path)
    doctor.add_argument("--entrypoint")
    doctor.add_argument("--json", action="store_true", help="Emit JSON output.")

    install_skills = subparsers.add_parser(
        "install-skills",
        help="Install packaged agent skills.",
    )
    install_skills.add_argument("--agent", default="codex")
    install_skills.add_argument("--target-dir", type=Path)
    install_skills.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 0
    try:
        if args.command == "check":
            return run_check(args.entrypoint, json_output=args.json)
        if args.command == "graph":
            return run_graph(args.entrypoint, output=args.output)
        if args.command == "docs":
            return run_docs(args.entrypoint, output_dir=args.output_dir, stdout=args.stdout)
        if args.command == "verify":
            return run_verify(
                args.entrypoint,
                json_output=args.json,
                runs_dir=args.runs_dir,
                params_json=args.params_json,
                params_file=args.params_file,
                environment_entrypoint=args.environment_entrypoint,
                environment_params_json=args.environment_params_json,
                environment_params_file=args.environment_params_file,
                invocation_name=args.invocation_name,
                spec_path=args.spec,
                workspace_config=args.workspace_config,
                project_id=args.project_id,
                remote_database_url=args.remote_database_url,
                remote_object_store_bucket=args.remote_object_store_bucket,
                remote_object_store_endpoint=args.remote_object_store_endpoint,
                remote_object_store_region=args.remote_object_store_region,
                remote_object_store_access_key=args.remote_object_store_access_key,
                remote_object_store_secret_key=args.remote_object_store_secret_key,
                remote_object_store_secure=args.remote_object_store_secure,
                remote_cache_dir=args.remote_cache_dir,
                live_otlp_endpoint=args.live_otlp_endpoint,
                live_outbox_dir=args.live_outbox_dir,
                live_max_outbox_bytes=args.live_max_outbox_bytes,
                live_max_batch_events=args.live_max_batch_events,
                live_max_batch_bytes=args.live_max_batch_bytes,
                live_flush_interval_ms=args.live_flush_interval_ms,
                live_shutdown_flush_timeout_ms=args.live_shutdown_flush_timeout_ms,
                require_live_delivery=args.require_live_delivery,
            )
        if args.command == "replay":
            return run_replay(
                runs_dir=args.runs_dir,
                graph_id=args.graph_id,
                run_id=args.run_id,
                invocation_name=args.invocation_name,
                frame_id=args.frame_id,
                loop_node_id=args.loop_node_id,
                iteration_index=args.iteration_index,
                json_output=args.json,
            )
        if args.command == "ui":
            return run_ui(
                runs_dir=args.runs_dir,
                workspace_config=args.workspace_config,
                host=args.host,
                port=args.port,
                frontend_dist=args.frontend_dist,
                frontend_dev_url=args.frontend_dev_url,
                catalog_entrypoint=args.catalog_entrypoint,
                open_browser=args.open_browser,
                remote_database_url=args.remote_database_url,
                remote_object_store_bucket=args.remote_object_store_bucket,
                remote_object_store_endpoint=args.remote_object_store_endpoint,
                remote_object_store_region=args.remote_object_store_region,
                remote_object_store_access_key=args.remote_object_store_access_key,
                remote_object_store_secret_key=args.remote_object_store_secret_key,
                remote_object_store_secure=args.remote_object_store_secure,
                remote_cache_dir=args.remote_cache_dir,
            )
        if args.command == "otel":
            if args.otel_command == "show-config":
                return run_otel_show_config(json_output=args.json)
            if args.otel_command == "write-demo":
                return run_otel_write_demo(
                    stack=args.stack,
                    output_dir=args.output_dir,
                    json_output=args.json,
                )
        if args.command == "remote":
            if args.remote_command == "link":
                return run_remote_link(
                    config=args.config,
                    json_output=args.json,
                )
            if args.remote_command == "status":
                return run_remote_status(
                    config=args.config,
                    json_output=args.json,
                )
            if args.remote_command == "publish-catalog":
                return run_remote_publish_catalog(
                    config=args.config,
                    json_output=args.json,
                )
            if args.remote_command == "sync":
                return run_remote_sync(
                    server_url=args.server_url,
                    config=args.config,
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    invocation_name=args.invocation_name,
                    project_id=args.project_id,
                    project_label=args.project_label,
                    environment_name=args.environment_name,
                    catalog_entry_id=args.catalog_entry_id,
                    catalog_source=args.catalog_source,
                    json_output=args.json,
                )
            if args.remote_command == "write-demo":
                return run_remote_write_demo(
                    output_dir=args.output_dir,
                    profile=args.profile,
                    workspace_id=args.workspace_id,
                    workspace_label=args.workspace_label,
                    mentalmodel_root=args.mentalmodel_root,
                    pangramanizer_root=args.pangramanizer_root,
                    json_output=args.json,
                )
            if args.remote_command == "doctor":
                return run_remote_doctor(
                    config=args.config,
                    workspace_config=args.workspace_config,
                    runs_dir=args.runs_dir,
                    json_output=args.json,
                )
            if args.remote_command == "up":
                return run_remote_up(
                    output_dir=args.output_dir,
                    host=args.host,
                    port=args.port,
                    frontend_dist=args.frontend_dist,
                    frontend_dev_url=args.frontend_dev_url,
                    open_browser=args.open_browser,
                )
            if args.remote_command == "down":
                return run_remote_down(output_dir=args.output_dir)
        if args.command == "install-skills":
            return run_install_skills_command(
                args.agent,
                target_dir=args.target_dir,
                dry_run=args.dry_run,
            )
        if args.command == "runs":
            if args.runs_command == "list":
                return run_runs_list(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    invocation_name=args.invocation_name,
                    limit=args.limit,
                    json_output=args.json,
                )
            if args.runs_command == "show":
                return run_runs_show(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    invocation_name=args.invocation_name,
                    json_output=args.json,
                )
            if args.runs_command == "latest":
                return run_runs_latest(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    invocation_name=args.invocation_name,
                    json_output=args.json,
                )
            if args.runs_command == "inputs":
                return run_runs_inputs(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    invocation_name=args.invocation_name,
                    node_id=args.node_id,
                    frame_id=args.frame_id,
                    loop_node_id=args.loop_node_id,
                    iteration_index=args.iteration_index,
                    json_output=args.json,
                )
            if args.runs_command == "outputs":
                return run_runs_outputs(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    invocation_name=args.invocation_name,
                    node_id=args.node_id,
                    frame_id=args.frame_id,
                    loop_node_id=args.loop_node_id,
                    iteration_index=args.iteration_index,
                    json_output=args.json,
                )
            if args.runs_command == "trace":
                return run_runs_trace(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    invocation_name=args.invocation_name,
                    node_id=args.node_id,
                    event_type=args.event_type,
                    frame_id=args.frame_id,
                    loop_node_id=args.loop_node_id,
                    iteration_index=args.iteration_index,
                    json_output=args.json,
                )
            if args.runs_command == "records":
                return run_runs_records(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    invocation_name=args.invocation_name,
                    node_id=args.node_id,
                    event_type=args.event_type,
                    frame_id=args.frame_id,
                    loop_node_id=args.loop_node_id,
                    iteration_index=args.iteration_index,
                    limit=args.limit,
                    json_output=args.json,
                )
            if args.runs_command == "repair":
                return run_runs_repair(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    dry_run=args.dry_run,
                    json_output=args.json,
                )
            if args.runs_command == "diff":
                return run_runs_diff(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_a=args.run_a,
                    run_b=args.run_b,
                    node_id=args.node_id,
                    invariant=args.invariant,
                    json_output=args.json,
                )
        if args.command == "demo":
            return run_demo_command(
                args.name,
                write_artifacts=args.write_artifacts,
                output_dir=args.output_dir,
                runs_dir=args.runs_dir,
                json_output=args.json,
            )
        if args.command == "doctor":
            return run_doctor(
                agent=args.agent,
                target_dir=args.target_dir,
                runs_dir=args.runs_dir,
                entrypoint=args.entrypoint,
                json_output=args.json,
            )
        print(f"mentalmodel scaffold command selected: {args.command}")
        return 0
    except MentalModelError as exc:
        print(f"mentalmodel error: {exc}")
        return 1


def _optional_run_payload(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    raw = load_run_payload(
        runs_dir=path.parents[2],
        graph_id=path.parent.parent.name,
        run_id=path.parent.name,
        filename=path.name,
    )
    return dict(raw)


def _verification_success(payload: dict[str, object] | None) -> bool | None:
    if payload is None:
        return None
    success = payload.get("success")
    return success if isinstance(success, bool) else None


def _comparison_status(value: bool | None) -> str:
    if value is True:
        return "same"
    if value is False:
        return "different"
    return "n/a"


def _invariant_outcome_label(value: bool | None) -> str:
    if value is True:
        return "pass"
    if value is False:
        return "fail"
    return "unknown"


_EXTERNAL_VERIFY_INVOCATION_SCRIPT = """
import json
import sys
from pathlib import Path

from mentalmodel.invocation import InvocationFactorySpec, VerifyInvocationSpec
from mentalmodel.invocation import load_runtime_environment_subject, load_workflow_subject
from mentalmodel.testing import run_verification

payload = json.loads(sys.argv[1])
runs_dir_arg = sys.argv[2]
runs_dir = None if runs_dir_arg == "-" else Path(runs_dir_arg)
program_payload = payload["program"]
environment_payload = payload.get("environment")
invocation = VerifyInvocationSpec(
    program=InvocationFactorySpec(
        entrypoint=program_payload["entrypoint"],
        params=program_payload.get("params", {}),
    ),
    environment=(
        None
        if environment_payload is None
        else InvocationFactorySpec(
            entrypoint=environment_payload["entrypoint"],
            params=environment_payload.get("params", {}),
        )
    ),
    invocation_name=payload.get("invocation_name"),
    runs_dir=None,
)
module, program = load_workflow_subject(invocation.program)
environment = None
if invocation.environment is not None:
    _, environment = load_runtime_environment_subject(invocation.environment)
report = run_verification(
    program,
    module=module,
    runs_dir=runs_dir,
    environment=environment,
    invocation_name=invocation.invocation_name,
)
print(json.dumps(report.as_dict()))
""".strip()


if __name__ == "__main__":
    raise SystemExit(main())
