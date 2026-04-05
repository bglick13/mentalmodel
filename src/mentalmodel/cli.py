from __future__ import annotations

import argparse
import importlib
import json
from collections.abc import Callable, Sequence
from pathlib import Path
from types import ModuleType
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
from mentalmodel.ir.lowering import lower_program
from mentalmodel.ir.schemas import EntryPointSpec
from mentalmodel.observability import load_tracing_config, write_otel_demo
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


def parse_entrypoint(raw: str) -> EntryPointSpec:
    if ":" not in raw:
        raise EntrypointLoadError(
            "Entrypoint must be in the format 'module.submodule:function_name'."
        )
    module_name, attribute_name = raw.split(":", 1)
    if not module_name or not attribute_name:
        raise EntrypointLoadError("Entrypoint must include both a module and an attribute name.")
    return EntryPointSpec(module_name=module_name, attribute_name=attribute_name)


def load_entrypoint_subject(raw: str) -> tuple[ModuleType, Workflow[NamedPrimitive]]:
    spec = parse_entrypoint(raw)
    try:
        module = importlib.import_module(spec.module_name)
    except Exception as exc:  # pragma: no cover - exercised by CLI path.
        raise EntrypointLoadError(f"Failed to import module {spec.module_name!r}: {exc}") from exc
    try:
        attribute = getattr(module, spec.attribute_name)
    except AttributeError as exc:
        raise EntrypointLoadError(
            f"Module {spec.module_name!r} does not define {spec.attribute_name!r}."
        ) from exc
    loaded = attribute() if callable(attribute) else attribute
    if not isinstance(loaded, Workflow):
        raise EntrypointLoadError(
            f"Entrypoint {raw!r} must resolve to a Workflow, got {type(loaded).__name__}."
        )
    return module, cast(Workflow[NamedPrimitive], loaded)


def load_entrypoint(raw: str) -> Workflow[NamedPrimitive]:
    _, workflow = load_entrypoint_subject(raw)
    return workflow


def load_graph(entrypoint: str) -> Workflow[NamedPrimitive]:
    """Load the workflow entrypoint for CLI commands."""

    return load_entrypoint(entrypoint)


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


def run_verify(
    entrypoint: str,
    *,
    json_output: bool = False,
    runs_dir: Path | None = None,
) -> int:
    """Run analysis, runtime verification, and property checks."""

    module, program = load_entrypoint_subject(entrypoint)
    report = run_verification(program, module=module, runs_dir=runs_dir)

    if json_output:
        print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
        return 0 if report.success else 1

    console = Console()
    summary = Table(title="mentalmodel verify summary")
    summary.add_column("Graph")
    summary.add_column("Static Errors", justify="right")
    summary.add_column("Warnings", justify="right")
    summary.add_column("Runtime", justify="right")
    summary.add_column("Property Checks", justify="right")
    summary.add_row(
        report.analysis.graph.graph_id,
        str(report.analysis.error_count),
        str(report.analysis.warning_count),
        "pass" if report.runtime.success else "fail",
        str(len(report.property_checks)),
    )
    console.print(summary)

    runtime_table = Table(title="Runtime Verification")
    runtime_table.add_column("Success")
    runtime_table.add_column("Records", justify="right")
    runtime_table.add_column("Outputs", justify="right")
    runtime_table.add_column("State Entries", justify="right")
    runtime_table.add_column("Run Artifacts")
    runtime_table.add_column("Error")
    runtime_table.add_row(
        "yes" if report.runtime.success else "no",
        str(report.runtime.record_count),
        str(report.runtime.output_count),
        str(report.runtime.state_count),
        report.runtime.run_artifacts_dir or "",
        report.runtime.error or "",
    )
    console.print(runtime_table)

    if report.property_checks:
        checks = Table(title="Property Checks")
        checks.add_column("Name")
        checks.add_column("Hypothesis")
        checks.add_column("Success")
        checks.add_column("Error")
        for result in report.property_checks:
            checks.add_row(
                result.name,
                "yes" if result.hypothesis_backed else "no",
                "yes" if result.success else "no",
                result.error or "",
            )
        console.print(checks)
    else:
        console.print("[yellow]No property checks discovered.[/yellow]")

    return 0 if report.success else 1


def run_replay(
    *,
    runs_dir: Path | None = None,
    graph_id: str,
    run_id: str | None = None,
    json_output: bool = False,
) -> int:
    """Replay one persisted run as a semantic timeline."""

    report = build_replay_report(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
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
        ("Success", "yes" if report.summary.success else "no"),
        (
            "Verification",
            "yes"
            if report.verification_success is True
            else "no" if report.verification_success is False else "unknown",
        ),
        ("Events", str(len(report.events))),
        ("Nodes", str(len(report.node_summaries))),
        ("Runtime Error", report.runtime_error or ""),
    ):
        summary.add_row(field, value)
    console.print(summary)

    events = Table(title="Replay Events")
    events.add_column("Seq", justify="right")
    events.add_column("Node")
    events.add_column("Event")
    events.add_column("Timestamp", justify="right")
    events.add_column("Payload")
    for event in report.events:
        events.add_row(
            str(event.sequence),
            event.node_id,
            event.event_type,
            str(event.timestamp_ms),
            json.dumps(event.payload, sort_keys=True),
        )
    console.print(events)

    nodes = Table(title="Replay Node Summary")
    nodes.add_column("Node")
    nodes.add_column("Events", justify="right")
    nodes.add_column("First Seq", justify="right")
    nodes.add_column("Last Seq", justify="right")
    nodes.add_column("Last Event")
    nodes.add_column("Invariant")
    for node_summary in report.node_summaries:
        nodes.add_row(
            node_summary.node_id,
            str(node_summary.event_count),
            str(node_summary.first_sequence or ""),
            str(node_summary.last_sequence or ""),
            node_summary.last_event_type or "",
            (
                "pass"
                if node_summary.invariant_passed is True
                else "fail" if node_summary.invariant_passed is False else ""
            ),
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
    limit: int = 20,
    json_output: bool = False,
) -> int:
    """List persisted run bundles."""

    summaries = list_run_summaries(runs_dir=runs_dir, graph_id=graph_id)[: max(1, limit)]
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
    json_output: bool = False,
) -> int:
    """Show one persisted run bundle and its files."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
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
    json_output: bool = False,
) -> int:
    """Resolve and show the newest matching run."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=None)
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
        invariant_table.add_column("Outcome A")
        invariant_table.add_column("Outcome B")
        invariant_table.add_column("Differs")
        for invariant_diff in diff.invariant_diffs:
            invariant_table.add_row(
                invariant_diff.node_id,
                _invariant_outcome_label(invariant_diff.outcome_run_a),
                _invariant_outcome_label(invariant_diff.outcome_run_b),
                "yes" if invariant_diff.differs else "no",
            )
        console.print(invariant_table)

    if not diff.state_equal:
        console.print(
            Panel.fit(
                json.dumps(
                    {
                        "state_run_a": diff.state_run_a,
                        "state_run_b": diff.state_run_b,
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
    node_id: str,
    json_output: bool = False,
) -> int:
    """Show the resolved input payload for one node."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    payload = load_run_node_inputs(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        node_id=node_id,
    )
    output = {
        "graph_id": summary.graph_id,
        "run_id": summary.run_id,
        "node_id": node_id,
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
    node_id: str,
    json_output: bool = False,
) -> int:
    """Show the output payload for one node."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    payload = load_run_node_output(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        node_id=node_id,
    )
    output = {
        "graph_id": summary.graph_id,
        "run_id": summary.run_id,
        "node_id": node_id,
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
    node_id: str,
    event_type: str | None = None,
    json_output: bool = False,
) -> int:
    """Show semantic trace data for one node."""

    trace = load_run_node_trace(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        node_id=node_id,
        event_type=event_type,
    )
    payload = {
        "graph_id": trace.summary.graph_id,
        "run_id": trace.summary.run_id,
        "node_id": trace.node_id,
        "records": list(trace.records),
        "spans": list(trace.spans),
    }
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    console = Console()
    record_table = Table(title=f"mentalmodel trace {trace.summary.run_id} {trace.node_id}")
    record_table.add_column("Seq", justify="right")
    record_table.add_column("Event")
    record_table.add_column("Timestamp", justify="right")
    record_table.add_column("Payload")
    for record in trace.records:
        record_table.add_row(
            str(record.get("sequence", "")),
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
        span_table.add_column("Duration (ns)", justify="right")
        span_table.add_column("Error")
        for span in trace.spans:
            span_table.add_row(
                str(span.get("name", "")),
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
    node_id: str | None = None,
    event_type: str | None = None,
    limit: int = 50,
    json_output: bool = False,
) -> int:
    """Show semantic execution records for one persisted run."""

    summary = resolve_run_summary(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    records = load_run_records(
        runs_dir=runs_dir,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        node_id=node_id,
        event_type=event_type,
    )
    limited = records[-max(1, limit) :]
    if json_output:
        print(json.dumps(list(limited), indent=2, sort_keys=True))
        return 0

    table = Table(title=f"mentalmodel records {summary.run_id}")
    table.add_column("Seq", justify="right")
    table.add_column("Node")
    table.add_column("Event")
    table.add_column("Timestamp", justify="right")
    table.add_column("Payload")
    for record in limited:
        table.add_row(
            str(record.get("sequence", "")),
            str(record.get("node_id", "")),
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
        default="mentalmodel.examples.async_rl.demo:build_program",
        help="Program entrypoint in `module:function` format.",
    )
    verify.add_argument(
        "--runs-dir",
        type=Path,
        help="Optional root directory for persisted run artifacts. Defaults to ./.runs.",
    )
    verify.add_argument("--json", action="store_true", help="Emit JSON output.")
    replay = subparsers.add_parser("replay", help="Replay a recorded execution.")
    replay.add_argument("--runs-dir", type=Path)
    replay.add_argument("--graph-id", required=True)
    replay.add_argument("--run-id")
    replay.add_argument("--json", action="store_true", help="Emit JSON output.")

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

    runs = subparsers.add_parser("runs", help="Inspect persisted run artifacts.")
    runs_subparsers = runs.add_subparsers(dest="runs_command")
    runs_subparsers.required = True

    runs_list = runs_subparsers.add_parser("list", help="List recent run bundles.")
    runs_list.add_argument("--runs-dir", type=Path)
    runs_list.add_argument("--graph-id")
    runs_list.add_argument("--limit", type=int, default=20)
    runs_list.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_show = runs_subparsers.add_parser("show", help="Show one run bundle.")
    runs_show.add_argument("--runs-dir", type=Path)
    runs_show.add_argument("--graph-id")
    runs_show.add_argument("--run-id")
    runs_show.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_latest = runs_subparsers.add_parser("latest", help="Resolve the newest matching run.")
    runs_latest.add_argument("--runs-dir", type=Path)
    runs_latest.add_argument("--graph-id")
    runs_latest.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_inputs = runs_subparsers.add_parser("inputs", help="Show one node input payload.")
    runs_inputs.add_argument("--runs-dir", type=Path)
    runs_inputs.add_argument("--graph-id")
    runs_inputs.add_argument("--run-id")
    runs_inputs.add_argument("--node-id", required=True)
    runs_inputs.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_outputs = runs_subparsers.add_parser("outputs", help="Show one node output payload.")
    runs_outputs.add_argument("--runs-dir", type=Path)
    runs_outputs.add_argument("--graph-id")
    runs_outputs.add_argument("--run-id")
    runs_outputs.add_argument("--node-id", required=True)
    runs_outputs.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_trace = runs_subparsers.add_parser("trace", help="Show semantic trace data for one node.")
    runs_trace.add_argument("--runs-dir", type=Path)
    runs_trace.add_argument("--graph-id")
    runs_trace.add_argument("--run-id")
    runs_trace.add_argument("--node-id", required=True)
    runs_trace.add_argument("--event-type")
    runs_trace.add_argument("--json", action="store_true", help="Emit JSON output.")

    runs_records = runs_subparsers.add_parser("records", help="Show run records.")
    runs_records.add_argument("--runs-dir", type=Path)
    runs_records.add_argument("--graph-id")
    runs_records.add_argument("--run-id")
    runs_records.add_argument("--node-id")
    runs_records.add_argument("--event-type")
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
            )
        if args.command == "replay":
            return run_replay(
                runs_dir=args.runs_dir,
                graph_id=args.graph_id,
                run_id=args.run_id,
                json_output=args.json,
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
                    limit=args.limit,
                    json_output=args.json,
                )
            if args.runs_command == "show":
                return run_runs_show(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    json_output=args.json,
                )
            if args.runs_command == "latest":
                return run_runs_latest(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    json_output=args.json,
                )
            if args.runs_command == "inputs":
                return run_runs_inputs(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    node_id=args.node_id,
                    json_output=args.json,
                )
            if args.runs_command == "outputs":
                return run_runs_outputs(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    node_id=args.node_id,
                    json_output=args.json,
                )
            if args.runs_command == "trace":
                return run_runs_trace(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    node_id=args.node_id,
                    event_type=args.event_type,
                    json_output=args.json,
                )
            if args.runs_command == "records":
                return run_runs_records(
                    runs_dir=args.runs_dir,
                    graph_id=args.graph_id,
                    run_id=args.run_id,
                    node_id=args.node_id,
                    event_type=args.event_type,
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


if __name__ == "__main__":
    raise SystemExit(main())
