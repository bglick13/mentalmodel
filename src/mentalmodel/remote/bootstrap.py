from __future__ import annotations

import json
from pathlib import Path

from mentalmodel.doctor import DoctorCheck, DoctorReport, DoctorStatus
from mentalmodel.observability import write_otel_demo
from mentalmodel.remote.contracts import ProjectRegistration, WorkspaceConfig
from mentalmodel.remote.workspace import load_workspace_config, write_workspace_config
from mentalmodel.ui.workspace import resolve_project_catalog, workspace_project_catalogs


def write_remote_demo(
    *,
    output_dir: Path,
    profile: str = "minimal",
    workspace_id: str = "mentalmodel-local",
    workspace_label: str = "Mentalmodel Local Stack",
    mentalmodel_root: Path | None = None,
    pangramanizer_root: Path | None = None,
) -> tuple[Path, ...]:
    """Write a local remote-demo directory with workspace config and helper scripts."""

    if profile != "minimal":
        raise ValueError("Only the 'minimal' remote demo profile is supported today.")
    resolved_output = output_dir.expanduser().resolve()
    resolved_output.mkdir(parents=True, exist_ok=True)

    workspace = WorkspaceConfig(
        workspace_id=workspace_id,
        label=workspace_label,
        description="Generated localhost stack for the remote runs MVP.",
        projects=_default_projects(
            mentalmodel_root=mentalmodel_root,
            pangramanizer_root=pangramanizer_root,
        ),
    )
    workspace_path = write_workspace_config(resolved_output / "workspace.toml", workspace)
    env_path = resolved_output / "mentalmodel.remote.env"
    env_path.write_text(
        _remote_env(
            workspace_path=workspace_path,
            repo_root=_repo_root(mentalmodel_root),
        ),
        encoding="utf-8",
    )

    dashboard_script = resolved_output / "run-dashboard.sh"
    dashboard_script.write_text(
        _dashboard_script(
            workspace_path=workspace_path,
            runs_dir=resolved_output / "data",
            repo_root=_repo_root(mentalmodel_root),
        ),
        encoding="utf-8",
    )
    dashboard_script.chmod(0o755)

    sync_script = resolved_output / "sync-local-runs.sh"
    sync_script.write_text(
        _sync_script(
            server_url="http://127.0.0.1:8765",
            repo_root=_repo_root(mentalmodel_root),
        ),
        encoding="utf-8",
    )
    sync_script.chmod(0o755)

    otel_paths = write_otel_demo(output_dir=resolved_output / "otel", stack="lgtm")
    readme_path = resolved_output / "REMOTE-DEMO.md"
    readme_path.write_text(
        _remote_demo_readme(
            workspace=workspace,
            workspace_path=workspace_path,
            output_dir=resolved_output,
        ),
        encoding="utf-8",
    )
    return (
        workspace_path,
        env_path,
        dashboard_script,
        sync_script,
        readme_path,
        *otel_paths,
    )


def build_remote_doctor_report(
    *,
    workspace_config: Path,
    runs_dir: Path | None = None,
) -> DoctorReport:
    """Validate a local remote workspace and provider-backed project registry."""

    workspace_path = workspace_config.expanduser().resolve()
    resolved_runs_dir = (runs_dir or workspace_path.parent / "data").expanduser().resolve()
    checks = (
        _check_workspace_file(workspace_path),
        _check_project_catalogs(workspace_path),
        _check_runs_dir(resolved_runs_dir),
        _check_demo_assets(workspace_path.parent),
    )
    return DoctorReport(checks=checks)


def _default_projects(
    *,
    mentalmodel_root: Path | None,
    pangramanizer_root: Path | None,
) -> tuple[ProjectRegistration, ...]:
    repo_root = _repo_root(mentalmodel_root)
    projects = [
        ProjectRegistration(
            project_id="mentalmodel-examples",
            label="Mentalmodel Examples",
            root_dir=repo_root,
            catalog_provider="mentalmodel.ui.catalog:default_dashboard_catalog",
            description="Built-in mentalmodel dashboard examples and fixtures.",
            tags=("builtin", "examples"),
            default_environment="localhost",
        )
    ]
    candidate_pangram_root = (
        pangramanizer_root.expanduser().resolve()
        if pangramanizer_root is not None
        else repo_root.parent / "pangramanizer"
    )
    if candidate_pangram_root.exists():
        pangram_project = ProjectRegistration(
            project_id="pangramanizer-training",
            label="Pangramanizer Training",
            root_dir=candidate_pangram_root,
            catalog_provider=(
                "pangramanizer.mentalmodel_training.verification.spec_catalog:"
                "verification_spec_catalog"
            ),
            description="Real and fixture Pangramanizer verification specs.",
            tags=("external", "training"),
            default_environment="localhost",
        )
        projects.append(_project_with_resolution_status(pangram_project))
    return tuple(projects)


def _check_workspace_file(workspace_path: Path) -> DoctorCheck:
    try:
        workspace = load_workspace_config(workspace_path)
    except Exception as exc:
        return DoctorCheck(
            name="workspace",
            status=DoctorStatus.FAIL,
            message=str(exc),
            details={"workspace_config": str(workspace_path)},
        )
    return DoctorCheck(
        name="workspace",
        status=DoctorStatus.PASS,
        message="Workspace config loads successfully.",
        details={
            "workspace_config": str(workspace_path),
            "workspace_id": workspace.workspace_id,
            "project_count": len(workspace.projects),
        },
    )


def _check_project_catalogs(workspace_path: Path) -> DoctorCheck:
    try:
        workspace = load_workspace_config(workspace_path)
        catalogs = workspace_project_catalogs(workspace)
    except Exception as exc:
        return DoctorCheck(
            name="projects",
            status=DoctorStatus.FAIL,
            message=str(exc),
            details={"workspace_config": str(workspace_path)},
        )
    return DoctorCheck(
        name="projects",
        status=DoctorStatus.PASS,
        message="Project catalogs resolve successfully.",
        details={
            "workspace_config": str(workspace_path),
            "projects": [
                {
                    "project_id": catalog.project.project_id,
                    "catalog_entry_count": len(catalog.entries),
                }
                for catalog in catalogs
            ],
        },
    )


def _check_runs_dir(runs_dir: Path) -> DoctorCheck:
    runs_dir.parent.mkdir(parents=True, exist_ok=True)
    if runs_dir.exists() and not runs_dir.is_dir():
        return DoctorCheck(
            name="runs_dir",
            status=DoctorStatus.FAIL,
            message="Configured runs directory exists but is not a directory.",
            details={"runs_dir": str(runs_dir)},
        )
    runs_dir.mkdir(parents=True, exist_ok=True)
    probe = runs_dir / ".doctor-write-test"
    try:
        probe.write_text("ok\n", encoding="utf-8")
        probe.unlink()
    except Exception as exc:
        return DoctorCheck(
            name="runs_dir",
            status=DoctorStatus.FAIL,
            message=f"Runs directory is not writable: {exc}",
            details={"runs_dir": str(runs_dir)},
        )
    return DoctorCheck(
        name="runs_dir",
        status=DoctorStatus.PASS,
        message="Runs directory is writable.",
        details={"runs_dir": str(runs_dir)},
    )


def _check_demo_assets(output_dir: Path) -> DoctorCheck:
    expected = (
        output_dir / "workspace.toml",
        output_dir / "run-dashboard.sh",
        output_dir / "sync-local-runs.sh",
        output_dir / "REMOTE-DEMO.md",
        output_dir / "otel" / "docker-compose.otel-lgtm.yml",
    )
    missing = [str(path) for path in expected if not path.exists()]
    if missing:
        return DoctorCheck(
            name="demo_assets",
            status=DoctorStatus.WARN,
            message="Remote demo directory is missing one or more helper assets.",
            details={"missing_paths": missing},
        )
    return DoctorCheck(
        name="demo_assets",
        status=DoctorStatus.PASS,
        message="Remote demo assets are present.",
        details={"output_dir": str(output_dir)},
    )


def _remote_env(*, workspace_path: Path, repo_root: Path) -> str:
    output_dir = workspace_path.parent
    return "\n".join(
        (
            f"MENTALMODEL_REMOTE_SERVER_URL=http://127.0.0.1:8765",
            f"MENTALMODEL_REMOTE_RUNS_DIR={output_dir / 'data'}",
            f"MENTALMODEL_REMOTE_WORKSPACE_CONFIG={workspace_path}",
            f"MENTALMODEL_REMOTE_REPO_ROOT={repo_root}",
            "",
        )
    )


def _dashboard_script(*, workspace_path: Path, runs_dir: Path, repo_root: Path) -> str:
    return "\n".join(
        (
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
            'RUNS_DIR="${MENTALMODEL_REMOTE_RUNS_DIR:-$SCRIPT_DIR/data}"',
            'WORKSPACE_CONFIG="${MENTALMODEL_REMOTE_WORKSPACE_CONFIG:-$SCRIPT_DIR/workspace.toml}"',
            f'REPO_ROOT="${{MENTALMODEL_REMOTE_REPO_ROOT:-{json.dumps(str(repo_root))}}}"',
            'uv run --directory "$REPO_ROOT" mentalmodel ui --host 127.0.0.1 --port 8765 '
            '--runs-dir "$RUNS_DIR" --workspace-config "$WORKSPACE_CONFIG" --open-browser "$@"',
            "",
        )
    )


def _sync_script(*, server_url: str, repo_root: Path) -> str:
    return "\n".join(
        (
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'LOCAL_RUNS_DIR="${1:-$PWD/.runs}"',
            f'REPO_ROOT="${{MENTALMODEL_REMOTE_REPO_ROOT:-{json.dumps(str(repo_root))}}}"',
            f'uv run --directory "$REPO_ROOT" mentalmodel remote sync --server-url "{server_url}" --runs-dir "$LOCAL_RUNS_DIR" "${{@:2}}"',
            "",
        )
    )


def _remote_demo_readme(
    *,
    workspace: WorkspaceConfig,
    workspace_path: Path,
    output_dir: Path,
) -> str:
    project_lines = "\n".join(
        f"- `{project.project_id}` via `{project.catalog_provider}`"
        for project in workspace.projects
    )
    return f"""# mentalmodel Remote Demo

This directory bootstraps the current remote-runs MVP for one local stack.

## What is here

- `workspace.toml`: project registry for the shared dashboard stack
- `run-dashboard.sh`: launches `mentalmodel ui` against the generated workspace
- `sync-local-runs.sh`: uploads local `.runs` bundles into the shared stack
- `otel/`: optional LGTM trace demo generated from `mentalmodel otel write-demo`

Registered projects:

{project_lines}

## Start the stack

1. Optional traces UI:

```bash
cd {output_dir / "otel"}
docker compose -f docker-compose.otel-lgtm.yml up -d
```

2. Start the shared dashboard/API:

```bash
cd {output_dir}
./run-dashboard.sh
```

That launches:

```bash
uv run mentalmodel ui --runs-dir {output_dir / "data"} --workspace-config {workspace_path}
```

3. Materialize and sync runs from any registered project:

```bash
uv run mentalmodel verify --entrypoint mentalmodel.examples.async_rl.demo:build_program
./sync-local-runs.sh /path/to/local/runs
```

If Pangramanizer is registered, its canonical verification catalog will also be
available from the same dashboard, provided the repo exists at the configured
`root_dir`.
"""


def _repo_root(mentalmodel_root: Path | None) -> Path:
    return (
        mentalmodel_root.expanduser().resolve()
        if mentalmodel_root is not None
        else Path(__file__).resolve().parents[3]
    )


def _project_with_resolution_status(project: ProjectRegistration) -> ProjectRegistration:
    try:
        resolve_project_catalog(project)
    except Exception as exc:
        reason = f"Disabled by generated demo because the provider did not resolve: {exc}"
        return ProjectRegistration(
            project_id=project.project_id,
            label=project.label,
            root_dir=project.root_dir,
            catalog_provider=project.catalog_provider,
            runs_dir=project.runs_dir,
            description=reason,
            tags=project.tags,
            default_environment=project.default_environment,
            enabled=False,
        )
    return project
