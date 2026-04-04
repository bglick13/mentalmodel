from __future__ import annotations

from pathlib import Path

from mentalmodel.analysis import run_analysis
from mentalmodel.docs import MarkdownArtifacts, render_markdown_artifacts, render_mermaid
from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.ir.lowering import lower_program

MERMAID_ARTIFACT_NAME = "expected_mermaid.txt"
DEFAULT_OUTPUT_DIRNAME = "mentalmodel-demo-async-rl"


def package_dir() -> Path:
    """Return the package directory containing the checked-in demo artifacts."""

    return Path(__file__).resolve().parent


def generate_demo_artifacts() -> dict[str, str]:
    """Generate the full async RL demo artifact set from shared services."""

    graph = lower_program(build_program())
    report = run_analysis(graph)
    markdown = render_markdown_artifacts(graph, findings=report.findings)
    return {
        MERMAID_ARTIFACT_NAME: render_mermaid(graph),
        **markdown.as_mapping(),
    }


def generate_demo_markdown() -> MarkdownArtifacts:
    """Generate the markdown artifacts for the async RL demo."""

    graph = lower_program(build_program())
    report = run_analysis(graph)
    return render_markdown_artifacts(graph, findings=report.findings)


def read_expected_demo_artifacts() -> dict[str, str]:
    """Read the checked-in expected artifact files for the demo."""

    base_dir = package_dir()
    return {
        name: (base_dir / name).read_text(encoding="utf-8").rstrip("\n")
        for name in expected_artifact_names()
    }


def write_demo_artifacts(output_dir: Path) -> tuple[Path, ...]:
    """Write the current generated demo artifacts to the target directory."""

    artifacts = generate_demo_artifacts()
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for name, content in artifacts.items():
        target = output_dir / name
        target.write_text(content + "\n", encoding="utf-8")
        written.append(target)
    return tuple(sorted(written))


def expected_artifact_names() -> tuple[str, ...]:
    """Return the stable file names for the checked-in demo artifacts."""

    return (
        MERMAID_ARTIFACT_NAME,
        "invariants.md",
        "node-inventory.md",
        "runtime-contexts.md",
        "topology.md",
    )
