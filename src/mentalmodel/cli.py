from __future__ import annotations

import argparse
import importlib
import json
from collections.abc import Sequence
from typing import cast

from mentalmodel.analysis import run_graph_checks, run_semantic_checks
from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.workflow import Workflow
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


def run_check(entrypoint: str, *, json_output: bool = False) -> int:
    program = load_entrypoint(entrypoint)
    graph = lower_program(program)
    findings = [*run_graph_checks(graph), *run_semantic_checks(graph)]
    error_count = sum(1 for finding in findings if finding.severity == "error")

    if json_output:
        print(
            json.dumps(
                {
                    "graph_id": graph.graph_id,
                    "node_count": len(graph.nodes),
                    "edge_count": len(graph.edges),
                    "findings": [
                        {
                            "code": finding.code,
                            "severity": finding.severity,
                            "message": finding.message,
                            "node_id": finding.node_id,
                        }
                        for finding in findings
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(
            f"check summary: graph={graph.graph_id} nodes={len(graph.nodes)} "
            f"edges={len(graph.edges)} findings={len(findings)}"
        )
        for finding in findings:
            print(finding.render())
    return 1 if error_count > 0 else 0


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
    subparsers.add_parser("graph", help="Render graph artifacts from IR.")
    subparsers.add_parser("docs", help="Generate documentation from IR.")
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
        print(f"mentalmodel scaffold command selected: {args.command}")
        return 0
    except MentalModelError as exc:
        print(f"mentalmodel error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
