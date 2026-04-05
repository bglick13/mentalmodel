from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mentalmodel.examples.autoresearch_sorting.demo import SORT_CANDIDATES, build_objective
from mentalmodel.observability.export import write_json
from mentalmodel.optimization import ObjectiveSignal


@dataclass(slots=True, frozen=True)
class AutoresearchBundle:
    """Materialized files for an autoresearch-style objective bundle."""

    output_dir: Path
    program_path: Path
    objective_path: Path
    candidates_path: Path


def build_sorting_demo_bundle() -> tuple[str, dict[str, object], dict[str, object]]:
    objective = build_objective()
    return (
        render_program_markdown(objective.signal),
        {
            "name": objective.name,
            "signal": objective_signal_payload(objective.signal),
        },
        {
            "candidates": list(SORT_CANDIDATES),
            "notes": [
                "Each candidate corresponds to one sorting workflow variant.",
                "Use `uv run mentalmodel demo autoresearch-sorting --json` "
                "to evaluate them locally.",
                "Treat invariant failure or missing metrics as an unsuccessful experiment.",
            ],
        },
    )


def write_autoresearch_bundle(output_dir: Path) -> AutoresearchBundle:
    output_dir.mkdir(parents=True, exist_ok=True)
    program_path = output_dir / "program.md"
    objective_path = output_dir / "objective.json"
    candidates_path = output_dir / "candidates.json"
    program_markdown, objective_payload, candidates_payload = build_sorting_demo_bundle()
    program_path.write_text(program_markdown, encoding="utf-8")
    write_json(objective_path, objective_payload)
    write_json(candidates_path, candidates_payload)
    return AutoresearchBundle(
        output_dir=output_dir,
        program_path=program_path,
        objective_path=objective_path,
        candidates_path=candidates_path,
    )


def objective_signal_payload(signal: ObjectiveSignal) -> dict[str, object]:
    return {
        "metric_name": signal.metric_name,
        "direction": signal.direction.value,
        "aggregation": signal.aggregation.value,
        "success_threshold": signal.success_threshold,
    }


def render_program_markdown(signal: ObjectiveSignal) -> str:
    return "\n".join(
        [
            "# mentalmodel autoresearch sorting demo",
            "",
            "This bundle demonstrates how to run an autoresearch-style search over a",
            "deterministic `mentalmodel` workflow with a verifiable metric signal.",
            "",
            "## Objective",
            "",
            f"- Metric signal: `{signal.metric_name}`",
            f"- Direction: `{signal.direction.value}`",
            f"- Aggregation: `{signal.aggregation.value}`",
            "- Hard correctness constraint: the runtime invariant must pass",
            "",
            "## Candidate workflow variants",
            "",
            *(f"- `{candidate}`" for candidate in SORT_CANDIDATES),
            "",
            "## Local evaluation loop",
            "",
            "1. Run `uv run mentalmodel demo autoresearch-sorting --json`.",
            "2. Discard any candidate whose invariant fails or whose metric signal is absent.",
            "3. Among successful candidates, prefer the one with the lowest comparison count.",
            "",
            "## Mapping to upstream autoresearch",
            "",
            "The upstream `karpathy/autoresearch` project uses a `program.md` file to",
            "define the agent research loop. This bundle gives the same kind of",
            "machine-readable objective contract for a bounded, deterministic search",
            "space over `mentalmodel` workflows.",
        ]
    ) + "\n"
