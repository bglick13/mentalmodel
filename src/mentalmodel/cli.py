from __future__ import annotations

import argparse
import importlib
import json
from collections.abc import Sequence
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
from mentalmodel.errors import EntrypointLoadError, MentalModelError
from mentalmodel.ir.lowering import lower_program
from mentalmodel.ir.schemas import EntryPointSpec


def parse_entrypoint(raw: str) -> EntryPointSpec:
    if ":" not in raw:
        raise EntrypointLoadError(
            "Entrypoint must be in the format 'module.submodule:function_name'."
        )
    module_name, attribute_name = raw.split(":", 1)
    if not module_name or not attribute_name:
        raise EntrypointLoadError("Entrypoint must include both a module and an attribute name.")
    return EntryPointSpec(module_name=module_name, attribute_name=attribute_name)


def load_entrypoint(raw: str) -> Workflow[NamedPrimitive]:
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
    return cast(Workflow[NamedPrimitive], loaded)


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
    subparsers.add_parser("verify", help="Run invariants and verification helpers.")
    subparsers.add_parser("replay", help="Replay a recorded execution.")

    demo = subparsers.add_parser("demo", help="Run or inspect a reference demo.")
    demo.add_argument("name", nargs="?", default="async-rl")

    install_skills = subparsers.add_parser(
        "install-skills",
        help="Install packaged agent skills.",
    )
    install_skills.add_argument("--agent", default="codex")
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
        print(f"mentalmodel scaffold command selected: {args.command}")
        return 0
    except MentalModelError as exc:
        print(f"mentalmodel error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
