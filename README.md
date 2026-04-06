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
- Reusable verification helpers for common graph/runtime assertions
- A lightweight objective/search layer for bounded optimization over verifiable
  metric signals

Primary design boundaries:

- The core package is a programming model, not a domain-specific RL library
- OpenTelemetry is an export/backend layer, not the source of truth
- Domain-specific behavior should be implemented as extensions that lower into
  the core IR
- Metrics are projections of semantic runtime behavior and stable node outputs,
  not handwritten calls scattered through handlers

Verification:

- Python module syntax can be validated with
  `python3 -m compileall /Users/ben/repos/mentalmodel/src`

See [PLAN.md](/Users/ben/repos/mentalmodel/PLAN.md) for the concrete package
plan, module layout, core interfaces, CLI surface, and a minimal async RL demo.

## Docs

The Mintlify docs source now lives in this repository.

- Mintlify config: [docs.json](/Users/ben/repos/mentalmodel/docs.json)
- Site entry pages: [index.mdx](/Users/ben/repos/mentalmodel/index.mdx),
  [introduction.mdx](/Users/ben/repos/mentalmodel/introduction.mdx),
  [quickstart.mdx](/Users/ben/repos/mentalmodel/quickstart.mdx),
  [installation.mdx](/Users/ben/repos/mentalmodel/installation.mdx)
- Existing operational docs and recipes remain under
  [docs/](/Users/ben/repos/mentalmodel/docs)

This repo should be treated as the source of truth for both package code and
docs. The temporary generated docs repo should not be the long-term editing
surface.

Local docs development:

```bash
npx mintlify dev
```

Run that from the repository root so Mintlify picks up [docs.json](/Users/ben/repos/mentalmodel/docs.json).

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
uv run mentalmodel replay --graph-id async_rl_demo
uv run mentalmodel otel show-config
uv run mentalmodel otel write-demo --stack lgtm --output-dir /tmp/mentalmodel-otel
uv run mentalmodel runs list
uv run mentalmodel runs latest --graph-id async_rl_demo
uv run mentalmodel runs show --graph-id async_rl_demo
uv run mentalmodel runs inputs --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs outputs --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs trace --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs records --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs diff --graph-id async_rl_demo --run-a <run_a> --run-b <run_b>
uv run mentalmodel runs repair --dry-run
uv run mentalmodel doctor --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel install-skills --agent codex --dry-run
uv run mentalmodel demo async-rl
uv run mentalmodel demo async-rl --write-artifacts --output-dir /tmp/mentalmodel-demo
uv run mentalmodel demo agent-tool-use
uv run mentalmodel demo agent-tool-use --write-artifacts --output-dir /tmp/mentalmodel-agent-demo
uv run mentalmodel demo autoresearch-sorting
uv run mentalmodel demo autoresearch-sorting --write-artifacts --output-dir /tmp/mentalmodel-autoresearch
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
- `replay` reconstructs the full semantic event timeline for one run
- `runs diff` compares two persisted run bundles, including invariant outcomes
  and node-level payload changes

Run bundle versioning:

- new `summary.json` files include `schema_version`
- older bundles remain readable through compatibility loading
- `runs repair` backfills legacy `summary.json` files to the current schema

Doctor:

- `mentalmodel doctor` is the preflight command for agent and debugging setup
- it checks packaged skill installation, optional entrypoint resolution,
  `.runs` availability, tracing config resolution, and packaged template files
- it supports `--json` for agent-facing automation

OpenTelemetry setup:

- `mentalmodel` now has explicit tracing config resolution rather than env
  sniffing scattered through the runtime
- use `uv run mentalmodel otel show-config` to inspect the fully resolved mode
- use `uv run mentalmodel otel write-demo --stack lgtm --output-dir ...` to
  materialize a self-hosted OTEL demo stack
- see [otel-self-hosted.md](/Users/ben/repos/mentalmodel/docs/otel-self-hosted.md)
  for a quick local demo path
- for OTLP HTTP traces, the endpoint should resolve to `/v1/traces`

Metric emission:

- `mentalmodel` now emits a small built-in operational metric set:
  `mentalmodel.run.started`, `mentalmodel.run.completed`,
  `mentalmodel.node.executions`, `mentalmodel.node.duration_ms`, and
  `mentalmodel.invariant.failures`
- `Actor(...)` and `Effect(...)` accept `metrics=[...]` so authored programs can
  expose domain metrics without bypassing the semantic runtime
- use `infer_output_metrics(...)` for safe flat numeric summaries such as
  `{"sample_count": 8, "updated_policy_version": 4}`
- use `extract_output_metrics(...)` with a typed extractor for richer or
  aggregated output-derived metrics such as reward means/counts from dynamic
  per-sample maps
- automatic inference intentionally rejects high-cardinality or per-example
  mappings; those should be aggregated first through a custom extractor
- metrics export is active only when an external observability sink is
  configured; `.runs` remains the canonical local debugging surface

Reference demos:

- `async-rl` remains the main runtime/actor/concurrency demo
- `agent-tool-use` is the second serious reference example and shows:
  multiple runtime contexts, multiple tool effects, a join, a structured
  answer invariant, and output-derived answer metrics
- `autoresearch-sorting` demonstrates the objective/search layer on a bounded,
  deterministic workflow, authors search through a real executable
  `AutoResearch` plugin node, and can also materialize an `autoresearch`-style
  `program.md` bundle

Optimization and autoresearch:

- use `mentalmodel.optimization` for bounded candidate search over workflows
  that expose a stable metric signal
- use the `AutoResearch` plugin primitive when you want that bounded search to
  be part of the authored workflow and runtime execution model
- runtime-executable plugins now extend the execution plan itself, not just IR
  lowering and docs
- current autoresearch support is dependency-free and bundle-oriented: it
  generates `program.md`, `objective.json`, and candidate metadata rather than
  importing upstream autoresearch internals
- no additional demo dependencies were added to the core package for Milestone
  15; if a future demo requires heavy third-party packages, it should be added
  through uv optional groups or extras instead of the default install

Verification helper APIs:

- `assert_aligned_key_sets(...)` for fanout/join key alignment
- `assert_causal_order(...)` for sampled-versus-current or cursor-versus-head checks
- `assert_monotonic_non_decreasing(...)` for version/counter progress
- `collect_runtime_boundary_observations(...)` and
  `assert_runtime_boundary_crossings(...)` for runtime-context boundary checks

Installed skills currently include:

- `mentalmodel-base`
- `mentalmodel-plugin-authoring`
- `mentalmodel-invariants-testing`
- `mentalmodel-debugging`

Debug recipe docs:

- [structure-debugging.md](/Users/ben/repos/mentalmodel/docs/recipes/structure-debugging.md)
- [invariant-debugging.md](/Users/ben/repos/mentalmodel/docs/recipes/invariant-debugging.md)
- [runtime-failure-debugging.md](/Users/ben/repos/mentalmodel/docs/recipes/runtime-failure-debugging.md)
- [run-comparison.md](/Users/ben/repos/mentalmodel/docs/recipes/run-comparison.md)

Import sorting is handled by Ruff's `I` rules rather than a separate `isort`
configuration.

Optional pre-commit setup:

```bash
uv run pre-commit install
```
