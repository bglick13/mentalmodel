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
- Treat container children as structure, not data flow.
- Add or update invariants when adding joins, state transitions, or policy
  boundaries.
- Preserve strong typing and generics. Do not introduce `Any` to move faster.

## Useful commands

```bash
uv run mentalmodel check --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel graph --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel docs --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel verify --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel demo async-rl
```

## Run artifacts

- `mentalmodel verify` persists a per-run bundle under `.runs/<graph_id>/<run_id>/`
  by default.
- Treat `.runs` as the primary debugging surface for runtime behavior.
- Useful files include `summary.json`, `verification.json`, `records.jsonl`,
  `outputs.json`, `state.json`, and `otel-spans.jsonl`.
- Semantic records are the first thing to inspect; OTel span files are a
  fallback mirror when no external sink is configured.

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

1. Lower and inspect structure with `mentalmodel check`.
2. Render artifacts with `mentalmodel graph` and `mentalmodel docs`.
3. Run `mentalmodel verify` after meaningful changes.
4. Inspect the newest `.runs/...` bundle when debugging runtime behavior.
5. If the program is a reference demo, refresh and compare golden artifacts.
