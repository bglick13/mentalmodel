from __future__ import annotations

from pathlib import Path

from mentalmodel.errors import MentalModelError


def template_dir() -> Path:
    """Return the directory containing packaged observability demo templates."""

    return Path(__file__).resolve().parent / "templates"


def write_otel_demo(
    *,
    output_dir: Path,
    stack: str,
) -> tuple[Path, ...]:
    """Write one packaged self-hosted OpenTelemetry demo stack."""

    if stack not in {"lgtm", "jaeger"}:
        raise MentalModelError(
            f"Unknown OTEL demo stack {stack!r}. Expected 'lgtm' or 'jaeger'."
        )
    filenames = (
        (
            "docker-compose.otel-lgtm.yml",
            "mentalmodel.otel.env",
            "OTEL-DEMO.md",
        )
        if stack == "lgtm"
        else (
            "docker-compose.otel-jaeger.yml",
            "mentalmodel.otel.jaeger.env",
            "OTEL-DEMO.md",
        )
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for filename in filenames:
        source = template_dir() / filename
        target = output_dir / filename
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        written.append(target)
    return tuple(written)
