from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mentalmodel.errors import SkillInstallError

SUPPORTED_AGENTS = ("claude", "codex")


@dataclass(slots=True, frozen=True)
class SkillInstallFile:
    """One file to be written during skill installation."""

    path: Path
    content: str


@dataclass(slots=True, frozen=True)
class SkillInstallPlan:
    """Resolved install plan for a packaged agent skill."""

    agent: str
    target_dir: Path
    files: tuple[SkillInstallFile, ...]


def build_install_plan(agent: str, *, target_dir: Path | None = None) -> SkillInstallPlan:
    """Resolve the installation plan for one supported agent."""

    normalized_agent = agent.lower()
    if normalized_agent not in SUPPORTED_AGENTS:
        raise SkillInstallError(
            f"Unsupported agent {agent!r}. Expected one of: {', '.join(SUPPORTED_AGENTS)}."
        )

    resolved_dir = target_dir or default_target_dir(normalized_agent)
    template_path = template_path_for_agent(normalized_agent)
    content = template_path.read_text(encoding="utf-8")
    files = (SkillInstallFile(path=resolved_dir / "SKILL.md", content=content),)
    return SkillInstallPlan(
        agent=normalized_agent,
        target_dir=resolved_dir,
        files=files,
    )


def install_skills(
    agent: str,
    *,
    target_dir: Path | None = None,
    dry_run: bool = False,
) -> SkillInstallPlan:
    """Install packaged skill files for one agent."""

    plan = build_install_plan(agent, target_dir=target_dir)
    if dry_run:
        return plan

    plan.target_dir.mkdir(parents=True, exist_ok=True)
    for file in plan.files:
        file.path.write_text(file.content, encoding="utf-8")
    return plan


def default_target_dir(agent: str) -> Path:
    """Return the conventional install directory for one supported agent."""

    home = Path.home()
    if agent == "codex":
        return home / ".codex" / "skills" / "mentalmodel"
    if agent == "claude":
        return home / ".claude" / "skills" / "mentalmodel"
    raise SkillInstallError(f"Unsupported agent {agent!r}.")


def template_path_for_agent(agent: str) -> Path:
    """Return the packaged template path for one supported agent."""

    templates_dir = Path(__file__).with_suffix("").parent / "templates"
    if agent == "codex":
        return templates_dir / "codex_skill.md"
    if agent == "claude":
        return templates_dir / "claude_skill.md"
    raise SkillInstallError(f"Unsupported agent {agent!r}.")
