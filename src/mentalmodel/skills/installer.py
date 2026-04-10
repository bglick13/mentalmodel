from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mentalmodel.errors import SkillInstallError

SUPPORTED_AGENTS = ("claude", "codex")
SUPPORTED_SKILLS = (
    "mentalmodel-base",
    "mentalmodel-plugin-authoring",
    "mentalmodel-invariants-testing",
    "mentalmodel-debugging",
    "mentalmodel-dashboard-authoring",
)


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
    files = tuple(
        SkillInstallFile(
            path=resolved_dir / skill_name / "SKILL.md",
            content=template_path_for_agent(normalized_agent, skill_name).read_text(
                encoding="utf-8"
            ),
        )
        for skill_name in SUPPORTED_SKILLS
    )
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
        file.path.parent.mkdir(parents=True, exist_ok=True)
        file.path.write_text(file.content, encoding="utf-8")
    return plan


def default_target_dir(agent: str) -> Path:
    """Return the conventional install directory for one supported agent."""

    home = Path.home()
    if agent == "codex":
        return home / ".codex" / "skills"
    if agent == "claude":
        return home / ".claude" / "skills"
    raise SkillInstallError(f"Unsupported agent {agent!r}.")


def template_path_for_agent(agent: str, skill_name: str) -> Path:
    """Return the packaged template path for one supported agent and skill."""

    templates_dir = Path(__file__).with_suffix("").parent / "templates"
    if skill_name not in SUPPORTED_SKILLS:
        raise SkillInstallError(
            f"Unsupported skill {skill_name!r}. Expected one of: {', '.join(SUPPORTED_SKILLS)}."
        )
    filename = _template_filename(agent, skill_name)
    return templates_dir / filename


def _template_filename(agent: str, skill_name: str) -> str:
    if agent not in SUPPORTED_AGENTS:
        raise SkillInstallError(f"Unsupported agent {agent!r}.")
    suffix = skill_name.removeprefix("mentalmodel-")
    return f"{agent}-{suffix}.md"
