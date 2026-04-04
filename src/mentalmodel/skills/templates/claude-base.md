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
- `RuntimeContext`

## Rules

- Prefer semantic primitives over ad hoc orchestration.
- Route all impure work through `Effect`.
- Keep dependencies explicit with `Ref`.
- Preserve strong typing and generics.
- Run `mentalmodel check`, `docs`, and `verify` after meaningful changes.
- Inspect `.runs/<graph_id>/<run_id>/` after `mentalmodel verify` when debugging
  runtime behavior.

## Useful commands

```bash
uv run mentalmodel demo async-rl
uv run mentalmodel check --entrypoint mentalmodel.examples.async_rl.demo:build_program
uv run mentalmodel verify --entrypoint mentalmodel.examples.async_rl.demo:build_program
```

`mentalmodel verify` writes a run bundle to `.runs` by default. The most useful
files are `verification.json`, `records.jsonl`, `outputs.json`, and
`otel-spans.jsonl`.
