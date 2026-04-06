# Parameterized Verification

Use this recipe when a real workflow is built by a factory with arguments, or
when runtime binding is intentionally supplied by a separate environment
factory.

## Goal

Answer:

- How should this workflow be invoked reproducibly from the CLI?
- Should I use direct flags or a TOML spec?
- How do I distinguish this run from other runs in `.runs` and telemetry?

## Recommended flow

### Direct flags

```bash
uv run mentalmodel verify \
  --entrypoint mypkg.workflow:build_program \
  --params-json '{"mode":"real_smoke"}' \
  --environment-entrypoint mypkg.runtime:build_environment \
  --invocation-name real_smoke
```

### TOML spec

```bash
uv run mentalmodel verify --spec verification/smoke.toml
```

Example:

```toml
[program]
entrypoint = "mypkg.workflow:build_program"

[program.params]
mode = "real_smoke"

[environment]
entrypoint = "mypkg.runtime:build_environment"

[runtime]
invocation_name = "real_smoke"
runs_dir = ".runs"
```

## What to look for after the run

```bash
uv run mentalmodel runs list --invocation-name real_smoke
uv run mentalmodel runs show --graph-id <graph_id> --invocation-name real_smoke
```

Confirm:

- the invocation succeeded
- `summary.json` preserved the intended `invocation_name`
- the right runtime profiles were available

## Rule

Use direct flags for ad hoc work. Use TOML specs for durable, shared workflow
invocations.
