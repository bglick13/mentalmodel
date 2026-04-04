# mentalmodel

`mentalmodel` is a Python package skeleton for building programs whose topology,
causality, state transitions, effects, and invariants are mechanically
recoverable by humans and AI coding agents.

The package is intended to provide:

- A small set of semantic primitives such as `Workflow`, `Actor`, `Effect`,
  `Invariant`, `Parallel`, `Join`, and `Ref`
- A canonical intermediate representation (IR) that all primitives lower into
- An async runtime with opinionated OpenTelemetry export
- Static analysis, generated docs, and generated diagrams from the same IR
- A CLI for scaffolding, validation, graph/doc generation, and skill
  installation for coding agents
- An extension model so domain-specific primitives such as `RuntimeContext` can
  be added without changing the base package

Primary design boundaries:

- The core package is a programming model, not a domain-specific RL library
- OpenTelemetry is an export/backend layer, not the source of truth
- Domain-specific behavior should be implemented as extensions that lower into
  the core IR

Verification:

- Python module syntax can be validated with
  `python3 -m compileall /Users/ben/repos/mentalmodel/src`

See [PLAN.md](/Users/ben/repos/mentalmodel/PLAN.md) for the concrete package
plan, module layout, core interfaces, CLI surface, and a minimal async RL demo.

## Tooling

The project uses a single `pyproject.toml` as the source of truth for Python
tooling.

Development setup:

```bash
uv sync
```

Common commands:

```bash
uv run ruff check --fix .
uv run ruff format .
uv run mypy
uv run pytest
uv run mentalmodel check --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel graph --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel docs --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel verify --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel runs list
uv run mentalmodel runs latest --graph-id async_rl_demo
uv run mentalmodel runs show --graph-id async_rl_demo
uv run mentalmodel runs inputs --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs outputs --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs trace --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs records --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs repair --dry-run
uv run mentalmodel install-skills --agent codex --dry-run
uv run mentalmodel demo async-rl
uv run mentalmodel demo async-rl --write-artifacts --output-dir /tmp/mentalmodel-demo
```

`mentalmodel verify` writes a per-run debugging bundle under
`.runs/<graph_id>/<run_id>/` by default. The most useful files are:

- `verification.json`
- `summary.json`
- `records.jsonl`
- `outputs.json`
- `state.json`
- `otel-spans.jsonl` when no external OpenTelemetry sink is configured

Milestone 8 run-inspection commands:

- `runs latest` resolves the newest matching run bundle
- `runs inputs` shows one node's persisted bound input payload
- `runs outputs` shows one node's persisted output
- `runs trace` shows one node's semantic execution trace and matching spans

Run bundle versioning:

- new `summary.json` files include `schema_version`
- older bundles remain readable through compatibility loading
- `runs repair` backfills legacy `summary.json` files to the current schema

Installed skills currently include:

- `mentalmodel-base`
- `mentalmodel-plugin-authoring`
- `mentalmodel-invariants-testing`
- `mentalmodel-debugging`

Import sorting is handled by Ruff's `I` rules rather than a separate `isort`
configuration.

Optional pre-commit setup:

```bash
uv run pre-commit install
```
