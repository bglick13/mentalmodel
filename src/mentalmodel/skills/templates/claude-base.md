---
name: mentalmodel-base
description: Use when authoring, refactoring, reviewing, or extending code that should follow the mentalmodel programming model. Covers core primitives, IR-first design, core CLI commands, and a small reference workflow shape.
---

# mentalmodel Base

Use this skill when working inside a project that uses `mentalmodel` as its
authoring model.

## Core primitives

- `Workflow`
- `Actor`
- `Effect`
- `Invariant`
- `Parallel`
- `Join`
- `Ref`
- `Block` / `Use`
- `StepLoop`
- `RuntimeEnvironment` / `RuntimeProfile`
- `ResourceKey`
- `RuntimeContext`

## Rules

- Prefer semantic primitives over ad hoc orchestration.
- Route all impure work through `Effect`.
- Keep dependencies explicit with `Ref`.
- Prefer primitive-local `metrics=[...]` over custom instrumentation inside
  handlers when you need output-derived metrics.
- Use `infer_output_metrics(...)` for flat bounded numeric summaries.
- Use `project_metric_map(...)` or `project_flat_metric_map(...)` for stable
  named fields from richer provider metric maps.
- Use `extract_output_metrics(...)` for typed aggregation when projection alone
  is not enough.
- Use dashboard metric groups for numeric charts and snapshot rails.
- Use dashboard custom views for row-based operator tables over stable reporting
  outputs.
- Preserve strong typing and generics.
- Run `mentalmodel doctor` when setup, entrypoints, installed skills, or
  tracing look suspicious.
- Run `mentalmodel check`, `docs`, and `verify` after meaningful changes.
- Inspect `.runs/<graph_id>/<run_id>/` after `mentalmodel verify` when debugging
  runtime behavior.

## Useful commands

```bash
uv run mentalmodel demo async-rl
uv run mentalmodel check --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel verify --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel verify --entrypoint mypkg.workflows:build_program --params-file verification/smoke.json
uv run mentalmodel replay --graph-id async_rl_demo
uv run mentalmodel otel show-config
uv run mentalmodel otel write-demo --stack lgtm --output-dir /tmp/mentalmodel-otel
uv run mentalmodel runs list
uv run mentalmodel runs latest --graph-id async_rl_demo
uv run mentalmodel runs show --graph-id async_rl_demo
uv run mentalmodel runs inputs --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs outputs --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs trace --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs diff --graph-id async_rl_demo --run-a <run_a> --run-b <run_b>
uv run mentalmodel runs repair --dry-run
uv run mentalmodel doctor --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel demo agent-tool-use
uv run mentalmodel demo autoresearch-sorting
uv run mentalmodel verify --spec src/mentalmodel/examples/review_workflow/review_workflow_fixture.toml
uv run mentalmodel ui
```

`mentalmodel verify` writes a run bundle to `.runs` by default. The most useful
files are `verification.json`, `records.jsonl`, `outputs.json`, and
`otel-spans.jsonl`. `replay` reconstructs the semantic event timeline for one
run, and `runs diff` compares two bundles when behavior changes across runs.
Use `--params-json` or `--params-file` when the verify entrypoint is a workflow
factory that needs explicit invocation parameters.
Use `--environment-entrypoint` when runtime binding is intentionally separate
from workflow construction, and prefer `--spec` when a project has a durable
TOML invocation spec.
Use `--invocation-name` to keep smoke, shadow, and production-like runs
separable in `.runs` and telemetry.
Use `otel show-config` when OTLP export looks wrong, and `otel write-demo` to
materialize a self-hosted tracing demo quickly.
Metrics are derived from runtime semantics and stable node outputs, not a
separate handwritten instrumentation path.
`agent-tool-use` is the second serious reference example, and
`review_workflow` is the serious reference path for `Block`, `StepLoop`,
runtime environments, and spec-driven verification.
`mentalmodel ui` is the hosted-style dashboard surface over the same `.runs`,
replay, and invocation services used by the CLI.
`review_workflow` can be verified directly with its packaged TOML specs.
`autoresearch-sorting` shows the bounded objective/search layer with an
autoresearch-style bundle output.
`summary.json` is versioned with `schema_version`, and `runs repair` can
normalize older bundles.

Recipe docs live under `docs/recipes/`:

- `block-reuse.md`
- `custom-view-authoring.md`
- `structure-debugging.md`
- `invariant-debugging.md`
- `loop-debugging.md`
- `runtime-profile-selection.md`
- `resource-injection.md`
- `parameterized-verification.md`
- `runtime-failure-debugging.md`
- `run-comparison.md`
