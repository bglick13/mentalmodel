---
name: mentalmodel-base
description: Use when authoring, refactoring, reviewing, or extending code that should follow the mentalmodel programming model. Covers core primitives, IR-first design, core CLI commands, and a small reference workflow shape.
---

# mentalmodel Base

Use this skill when working inside a project that uses `mentalmodel` as its
authoring model.

## Core primitives

- `Workflow`: top-level program boundary
- `Actor`: stateful semantic node
- `Effect`: explicit impure boundary
- `Invariant`: runtime or property-level constraint
- `Parallel`: structural fanout container
- `Join`: explicit merge point
- `Ref`: non-tree dependency edge
- `Block` / `Use`: reusable namespaced composition
- `StepLoop`: sequential multi-step execution over a reusable block body
- `RuntimeEnvironment` / `RuntimeProfile`: typed runtime binding
- `ResourceKey`: typed shared resource declaration
- `RuntimeContext`: reference extension primitive for runtime grouping

## Source of truth

- Authored programs lower into canonical IR.
- Analysis, docs, diagrams, verification, and demo artifacts must be derived
  from IR or execution records, not handwritten parallel representations.
- Plugin metadata and provenance must stay explicit in node metadata.

## Authoring rules

- Prefer semantic primitives over ad hoc orchestration.
- Route all impure work through `Effect`.
- Keep dependencies explicit with `Ref`.
- When a remote capability is shared across workflows, expose it as a reusable
  `Effect` plus typed resource/service boundary instead of hiding it inside a
  study-local service implementation.
- If an effect owns batching, retries, multi-phase orchestration, or other
  meaningful control flow, lift that structure into the workflow with
  `Block`, `Use`, `StepLoop`, additional effects, or joins instead of hiding it
  inside one handler.
- If a loop performs meaningful sub-steps such as sampling, scoring, or
  materialization, model those as distinct child nodes so spans, records, and
  loop history expose them directly.
- Do not retain raw high-cardinality loop outputs in history unless operators
  truly need the full payload. Add a lightweight summary node for dashboard
  tables and retain that in loop history instead.
- Prefer loop work items that already carry the context a step needs over
  relying on closure-like outer references for operationally significant state.
- Treat container children as structure, not data flow.
- Add or update invariants when adding joins, state transitions, or policy
  boundaries.
- Prefer primitive-local `metrics=[...]` over ad hoc observability code inside
  handlers when you need output-derived metrics.
- Use `infer_output_metrics(...)` only for flat, bounded numeric summaries.
- Use `project_metric_map(...)` or `project_flat_metric_map(...)` when you need
  to preserve a rich provider metric map but export a stable named subset.
- Use `extract_output_metrics(...)` with a typed extractor when the output shape
  needs real aggregation before it is safe to emit as metrics.
- Use dashboard metric groups for numeric charts and snapshot rails.
- Use dashboard custom views for row-based operator tables over stable reporting
  outputs.
- Preserve strong typing and generics. Do not introduce `Any` to move faster.

## Useful commands

```bash
uv run mentalmodel check --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel graph --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel docs --entrypoint mentalmodel.examples.async_rl.demo:build_program
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
uv run mentalmodel runs records --graph-id async_rl_demo --node-id staleness_invariant
uv run mentalmodel runs diff --graph-id async_rl_demo --run-a <run_a> --run-b <run_b>
uv run mentalmodel runs repair --dry-run
uv run mentalmodel doctor --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel demo async-rl
uv run mentalmodel demo agent-tool-use
uv run mentalmodel demo autoresearch-sorting
uv run mentalmodel verify --spec src/mentalmodel/examples/review_workflow/review_workflow_fixture.toml
uv run mentalmodel ui
```

## Run artifacts

- `mentalmodel verify` persists a per-run bundle under `.runs/<graph_id>/<run_id>/`
  by default.
- use `--params-json` or `--params-file` when the entrypoint is a workflow
  factory that needs explicit invocation parameters
- use `--environment-entrypoint` plus environment params when runtime binding is
  intentionally separate from workflow construction
- use `--spec` when a project has a durable TOML invocation spec for workflow +
  environment + run metadata
- use `--invocation-name` to distinguish smoke, shadow, and production-like
  runs even when graph ids are shared
- Treat `.runs` as the primary debugging surface for runtime behavior.
- Useful files include `summary.json`, `verification.json`, `records.jsonl`,
  `outputs.json`, `state.json`, and `otel-spans.jsonl`.
- Semantic records are the first thing to inspect; OTel span files are a
  fallback mirror when no external sink is configured.
- Metrics are projections of semantic runtime behavior and stable node outputs,
  not a second handwritten instrumentation path.
- `runs inputs` reads the persisted `node.inputs_resolved` event payload for a
  node, so it shows exactly what the runtime bound before handler execution.
- `replay` is the fastest way to reconstruct the full semantic lifecycle of a
  run without manually joining records and summary metadata.
- `runs diff` is the fastest way to compare two persisted runs when a bug only
  appears after a configuration or code change.
- `otel show-config` is the first command to run when you suspect OTLP export
  is misconfigured.
- `otel write-demo` materializes a self-hosted OTEL demo stack without making
  you hand-author Docker or env files.
- `summary.json` is versioned with `schema_version`; use `runs repair` to
  normalize older bundles when needed.
- `agent-tool-use` is the serious second reference workflow for verifying that
  the programming model is not RL-shaped by accident.
- `review_workflow` is the serious reference path for `Block`, `StepLoop`,
  runtime environments, and spec-driven verification.
- `mentalmodel ui` is the hosted-style dashboard surface over the same `.runs`,
  replay, and invocation services used by the CLI.
- `autoresearch-sorting` shows the bounded objective/search layer and writes an
  autoresearch-style `program.md` bundle when run with `--write-artifacts`.

## Hello world shape

```python
from mentalmodel import Actor, Effect, Invariant, Ref, Workflow

program = Workflow(
    name="hello_world",
    children=[
        Actor("source", handler=SourceHandler()),
        Effect("transform", handler=TransformEffect(), inputs=[Ref("source")]),
        Invariant("shape_ok", checker=ShapeChecker(), inputs=[Ref("transform")]),
    ],
)
```

## Working loop

1. Run `mentalmodel doctor` when setup, entrypoints, skills, or tracing look suspicious.
   Treat topology warnings as an observability smell, not just a style note.
2. Lower and inspect structure with `mentalmodel check`.
3. Render artifacts with `mentalmodel graph` and `mentalmodel docs`.
4. Run `mentalmodel verify` after meaningful changes.
5. Inspect the newest `.runs/...` bundle when debugging runtime behavior.
6. If the program is a reference demo, refresh and compare golden artifacts.

## Debug recipe docs

- `docs/recipes/structure-debugging.md`
- `docs/recipes/block-reuse.md`
- `docs/recipes/custom-view-authoring.md`
- `docs/recipes/invariant-debugging.md`
- `docs/recipes/loop-debugging.md`
- `docs/recipes/runtime-profile-selection.md`
- `docs/recipes/resource-injection.md`
- `docs/recipes/parameterized-verification.md`
- `docs/recipes/runtime-failure-debugging.md`
- `docs/recipes/run-comparison.md`
