from __future__ import annotations

import json
from pathlib import Path

from mentalmodel.doctor import DoctorCheck, DoctorReport, DoctorStatus
from mentalmodel.remote.contracts import ProjectRegistration, WorkspaceConfig
from mentalmodel.remote.workspace import load_workspace_config, write_workspace_config
from mentalmodel.ui.workspace import workspace_project_catalogs


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
            shared_runs_dir=resolved_output / "data",
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

    compose_path = resolved_output / "docker-compose.remote-minimal.yml"
    compose_path.write_text(
        _remote_compose(repo_root=_repo_root(mentalmodel_root)),
        encoding="utf-8",
    )

    collector_config_path = resolved_output / "otel-collector.remote.yml"
    collector_config_path.write_text(_collector_config(), encoding="utf-8")

    start_script = resolved_output / "start-stack.sh"
    start_script.write_text(
        _start_stack_script(),
        encoding="utf-8",
    )
    start_script.chmod(0o755)

    stop_script = resolved_output / "stop-stack.sh"
    stop_script.write_text(
        _stop_stack_script(),
        encoding="utf-8",
    )
    stop_script.chmod(0o755)

    sync_script = resolved_output / "sync-local-runs.sh"
    sync_script.write_text(
        _sync_script(
            server_url="http://127.0.0.1:8765",
            repo_root=_repo_root(mentalmodel_root),
        ),
        encoding="utf-8",
    )
    sync_script.chmod(0o755)

    live_verify_script = resolved_output / "verify-live.sh"
    live_verify_script.write_text(
        _verify_live_script(repo_root=_repo_root(mentalmodel_root)),
        encoding="utf-8",
    )
    live_verify_script.chmod(0o755)

    consume_script = resolved_output / "run-telemetry-consumer.sh"
    consume_script.write_text(
        _telemetry_consumer_script(repo_root=_repo_root(mentalmodel_root)),
        encoding="utf-8",
    )
    consume_script.chmod(0o755)

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
        compose_path,
        collector_config_path,
        dashboard_script,
        start_script,
        stop_script,
        sync_script,
        live_verify_script,
        consume_script,
        readme_path,
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
        _check_project_output_routes(workspace_path),
        _check_runs_dir(resolved_runs_dir),
        _check_demo_assets(workspace_path.parent),
    )
    return DoctorReport(checks=checks)


def _default_projects(
    *,
    shared_runs_dir: Path,
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
            runs_dir=shared_runs_dir,
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
            runs_dir=shared_runs_dir,
            description="Real and fixture Pangramanizer verification specs.",
            tags=("external", "training"),
            default_environment="localhost",
        )
        projects.append(pangram_project)
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


def _check_project_output_routes(workspace_path: Path) -> DoctorCheck:
    try:
        workspace = load_workspace_config(workspace_path)
    except Exception as exc:
        return DoctorCheck(
            name="project_routes",
            status=DoctorStatus.FAIL,
            message=str(exc),
            details={"workspace_config": str(workspace_path)},
        )
    missing = [
        project.project_id
        for project in workspace.projects
        if project.enabled and project.runs_dir is None
    ]
    if missing:
        return DoctorCheck(
            name="project_routes",
            status=DoctorStatus.FAIL,
            message=(
                "Enabled projects must declare runs_dir for shared-stack launches."
            ),
            details={
                "workspace_config": str(workspace_path),
                "missing_runs_dir_projects": missing,
            },
        )
    return DoctorCheck(
        name="project_routes",
        status=DoctorStatus.PASS,
        message="Enabled projects declare explicit output routing.",
        details={
            "workspace_config": str(workspace_path),
            "projects": [
                {
                    "project_id": project.project_id,
                    "runs_dir": str(project.runs_dir),
                }
                for project in workspace.projects
                if project.enabled
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
        output_dir / "docker-compose.remote-minimal.yml",
        output_dir / "otel-collector.remote.yml",
        output_dir / "run-dashboard.sh",
        output_dir / "start-stack.sh",
        output_dir / "stop-stack.sh",
        output_dir / "sync-local-runs.sh",
        output_dir / "verify-live.sh",
        output_dir / "REMOTE-DEMO.md",
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
            "MENTALMODEL_REMOTE_SERVER_URL=http://127.0.0.1:8765",
            f"MENTALMODEL_REMOTE_RUNS_DIR={output_dir / 'data'}",
            f"MENTALMODEL_REMOTE_CACHE_DIR={output_dir / 'data'}",
            f"MENTALMODEL_REMOTE_WORKSPACE_CONFIG={workspace_path}",
            f"MENTALMODEL_REMOTE_REPO_ROOT={repo_root}",
            "MENTALMODEL_REMOTE_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/mentalmodel",
            "MENTALMODEL_REMOTE_OBJECT_STORE_BUCKET=mentalmodel-runs",
            "MENTALMODEL_REMOTE_OBJECT_STORE_ENDPOINT=http://127.0.0.1:9000",
            "MENTALMODEL_REMOTE_OBJECT_STORE_REGION=us-east-1",
            "MENTALMODEL_REMOTE_OBJECT_STORE_ACCESS_KEY=minio",
            "MENTALMODEL_REMOTE_OBJECT_STORE_SECRET_KEY=miniosecret",
            "MENTALMODEL_REMOTE_OBJECT_STORE_SECURE=false",
            "MENTALMODEL_REMOTE_LIVE_OTLP_ENDPOINT=http://127.0.0.1:4318",
            f"MENTALMODEL_REMOTE_LIVE_OUTBOX_DIR={output_dir / 'live-outbox'}",
            "MENTALMODEL_REMOTE_KAFKA_BROKERS=127.0.0.1:19092",
            "MENTALMODEL_REMOTE_CLICKHOUSE_ENDPOINT=http://127.0.0.1:8123",
            "",
        )
    )


def _dashboard_script(*, workspace_path: Path, runs_dir: Path, repo_root: Path) -> str:
    return "\n".join(
        (
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
            'if [[ -f "$SCRIPT_DIR/mentalmodel.remote.env" ]]; then',
            "  set -a",
            '  source "$SCRIPT_DIR/mentalmodel.remote.env"',
            "  set +a",
            "fi",
            'RUNS_DIR="${MENTALMODEL_REMOTE_RUNS_DIR:-$SCRIPT_DIR/data}"',
            'WORKSPACE_CONFIG="${MENTALMODEL_REMOTE_WORKSPACE_CONFIG:-$SCRIPT_DIR/workspace.toml}"',
            f'REPO_ROOT="${{MENTALMODEL_REMOTE_REPO_ROOT:-{json.dumps(str(repo_root))}}}"',
            'uv run --directory "$REPO_ROOT" mentalmodel ui --host 127.0.0.1 --port 8765 '
            '--runs-dir "$RUNS_DIR" --workspace-config "$WORKSPACE_CONFIG" --open-browser "$@"',
            "",
        )
    )


def _remote_compose(*, repo_root: Path) -> str:
    return f"""services:
  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    environment:
      POSTGRES_DB: mentalmodel
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d mentalmodel"]
      interval: 5s
      timeout: 5s
      retries: 20
    volumes:
      - postgres-data:/var/lib/postgresql/data

  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v25.1.4
    restart: unless-stopped
    command:
      - redpanda
      - start
      - --overprovisioned
      - --smp
      - "1"
      - --memory
      - "1G"
      - --reserve-memory
      - "0M"
      - --check=false
      - --node-id
      - "0"
      - --kafka-addr
      - internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr
      - internal://redpanda:9092,external://127.0.0.1:19092
      - --pandaproxy-addr
      - internal://0.0.0.0:8082
      - --advertise-pandaproxy-addr
      - internal://redpanda:8082
    ports:
      - "19092:19092"
      - "9644:9644"
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy: true'"]
      interval: 10s
      timeout: 10s
      retries: 20
    volumes:
      - redpanda-data:/var/lib/redpanda/data

  clickhouse:
    image: clickhouse/clickhouse-server:25.3
    restart: unless-stopped
    environment:
      CLICKHOUSE_DB: mentalmodel
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
    ports:
      - "8123:8123"
      - "9002:9000"
    healthcheck:
      test: ["CMD-SHELL", "clickhouse-client --query 'select 1'"]
      interval: 10s
      timeout: 10s
      retries: 20
    volumes:
      - clickhouse-data:/var/lib/clickhouse

  minio:
    image: minio/minio:RELEASE.2025-02-28T09-55-16Z
    restart: unless-stopped
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: miniosecret
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data

  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.125.0
    restart: unless-stopped
    command:
      - "--config=/etc/otelcol-contrib/config.yaml"
    depends_on:
      redpanda:
        condition: service_healthy
    ports:
      - "4318:4318"
      - "13133:13133"
    volumes:
      - ./otel-collector.remote.yml:/etc/otelcol-contrib/config.yaml:ro
      - otelcol-data:/var/lib/otelcol

  telemetry-consumer:
    image: ghcr.io/astral-sh/uv:python3.11-bookworm
    restart: unless-stopped
    depends_on:
      redpanda:
        condition: service_healthy
      clickhouse:
        condition: service_healthy
    working_dir: /workspace/repo
    env_file:
      - ./mentalmodel.remote.env
    environment:
      MENTALMODEL_REMOTE_KAFKA_BROKERS: redpanda:9092
      MENTALMODEL_REMOTE_CLICKHOUSE_ENDPOINT: http://clickhouse:8123
    command:
      - uv
      - run
      - --directory
      - /workspace/repo
      - mentalmodel
      - remote
      - consume-telemetry
    volumes:
      - {json.dumps(str(repo_root))}:/workspace/repo:ro

volumes:
  postgres-data:
  redpanda-data:
  clickhouse-data:
  minio-data:
  otelcol-data:
"""


def _collector_config() -> str:
    return """extensions:
  health_check:
    endpoint: 0.0.0.0:13133
  file_storage:
    directory: /var/lib/otelcol/file_storage

receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 512
    spike_limit_mib: 128
  batch:
    send_batch_size: 256
    send_batch_max_size: 512
    timeout: 1s

exporters:
  debug:
    verbosity: basic
  kafka/logs:
    brokers: ["redpanda:9092"]
    topic: mentalmodel.telemetry.logs
    encoding: otlp_proto
    sending_queue:
      enabled: true
      queue_size: 2048
      storage: file_storage
    retry_on_failure:
      enabled: true
  kafka/traces:
    brokers: ["redpanda:9092"]
    topic: mentalmodel.telemetry.traces
    encoding: otlp_proto
    sending_queue:
      enabled: true
      queue_size: 2048
      storage: file_storage
    retry_on_failure:
      enabled: true
  kafka/metrics:
    brokers: ["redpanda:9092"]
    topic: mentalmodel.telemetry.metrics
    encoding: otlp_proto
    sending_queue:
      enabled: true
      queue_size: 2048
      storage: file_storage
    retry_on_failure:
      enabled: true

service:
  extensions: [health_check, file_storage]
  telemetry:
    logs:
      level: info
  pipelines:
    logs:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [kafka/logs, debug]
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [kafka/traces, debug]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [kafka/metrics, debug]
"""


def _start_stack_script() -> str:
    return "\n".join(
        (
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
            'docker compose -f "$SCRIPT_DIR/docker-compose.remote-minimal.yml" up -d',
            'exec "$SCRIPT_DIR/run-dashboard.sh" "$@"',
            "",
        )
    )


def _stop_stack_script() -> str:
    return "\n".join(
        (
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
            'docker compose -f "$SCRIPT_DIR/docker-compose.remote-minimal.yml" down "$@"',
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
            'uv run --directory "$REPO_ROOT" mentalmodel remote sync '
            f'--server-url "{server_url}" --runs-dir "$LOCAL_RUNS_DIR" "${{@:2}}"',
            "",
        )
    )


def _verify_live_script(*, repo_root: Path) -> str:
    return "\n".join(
        (
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
            'if [[ -f "$SCRIPT_DIR/mentalmodel.remote.env" ]]; then',
            "  set -a",
            '  source "$SCRIPT_DIR/mentalmodel.remote.env"',
            "  set +a",
            "fi",
            'REPO_ROOT="${MENTALMODEL_REMOTE_REPO_ROOT:-'
            + json.dumps(str(repo_root))
            + '}"',
            'LIVE_ENDPOINT="${MENTALMODEL_REMOTE_LIVE_OTLP_ENDPOINT:-http://127.0.0.1:4318}"',
            'OUTBOX_DIR="${MENTALMODEL_REMOTE_LIVE_OUTBOX_DIR:-$SCRIPT_DIR/live-outbox}"',
            'uv run --directory "$REPO_ROOT" mentalmodel verify '
            '--live-otlp-endpoint "$LIVE_ENDPOINT" '
            '--live-outbox-dir "$OUTBOX_DIR" "$@"',
            "",
        )
    )


def _telemetry_consumer_script(*, repo_root: Path) -> str:
    return "\n".join(
        (
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
            'if [[ -f "$SCRIPT_DIR/mentalmodel.remote.env" ]]; then',
            "  set -a",
            '  source "$SCRIPT_DIR/mentalmodel.remote.env"',
            "  set +a",
            "fi",
            'REPO_ROOT="${MENTALMODEL_REMOTE_REPO_ROOT:-'
            + json.dumps(str(repo_root))
            + '}"',
            'uv run --directory "$REPO_ROOT" mentalmodel remote consume-telemetry "$@"',
            "",
        )
    )


def _remote_demo_readme(
    *,
    workspace: WorkspaceConfig,
    workspace_path: Path,
    output_dir: Path,
) -> str:
    review_fixture_path = (
        Path(__file__).resolve().parents[1]
        / "examples"
        / "review_workflow"
        / "review_workflow_fixture.toml"
    )
    project_lines = "\n".join(
        f"- `{project.project_id}` via `{project.catalog_provider}`"
        for project in workspace.projects
    )
    return f"""# mentalmodel Remote Demo

This directory bootstraps the current local development stack for the
collector-first remote ingestion path.

## What is here

- `docker-compose.remote-minimal.yml`: local Postgres, MinIO, Redpanda,
  ClickHouse, and OpenTelemetry Collector
- `otel-collector.remote.yml`: collector config for OTLP receive, batching,
  retry, and Kafka routing
- `workspace.toml`: project registry for the shared local dashboard stack
- `mentalmodel.remote.env`: wired backend credentials and dashboard config
- `run-dashboard.sh`: launches `mentalmodel ui` against the generated local workspace
- `start-stack.sh`: starts the backend services and then launches the dashboard
- `stop-stack.sh`: stops the backend services
- `sync-local-runs.sh`: uploads local `.runs` bundles into the shared stack
- `verify-live.sh`: runs `mentalmodel verify` against the local OTLP collector
- `run-telemetry-consumer.sh`: manually runs the ClickHouse indexer against the Kafka topics

Registered projects:

{project_lines}

## Start the stack

1. Start the remote backend services and dashboard:

```bash
cd {output_dir}
./start-stack.sh
```

That launches the backend services from:

```bash
docker compose -f {output_dir / "docker-compose.remote-minimal.yml"} up -d
```

and then starts the shared dashboard/API with:

```bash
uv run mentalmodel ui --runs-dir {output_dir / "data"} --workspace-config {workspace_path}
```

2. Run a live-managed verify against the collector:

```bash
cd {output_dir}
./verify-live.sh --entrypoint mentalmodel.examples.async_rl.demo:build_program
```

The stack now includes the ClickHouse indexer consumer, so OTLP live telemetry
lands in the same hosted query model used by uploaded completed runs.

3. Materialize and sync completed runs from any registered project:

```bash
uv run mentalmodel verify \
  --spec {review_fixture_path} \
  --runs-dir {output_dir / "demo-local-runs"}
./sync-local-runs.sh {output_dir / "demo-local-runs"}
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
