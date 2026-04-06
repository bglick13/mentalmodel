# Runtime Profile Selection

Use this recipe when a workflow should run against different resource sets and
you need to prove which runtime profile actually executed which nodes.

## Goal

Answer:

- Which runtime profile executed this node?
- Was the intended profile selected by `RuntimeContext`, `BlockDefaults`, or the
  environment default?
- Are smoke, fixture, and production-like runs cleanly distinguishable?

## Recommended flow

1. Verify with an explicit invocation label.

```bash
uv run mentalmodel verify \
  --entrypoint <module:function> \
  --environment-entrypoint <module:function> \
  --invocation-name real_smoke
```

2. Inspect the run summary.

```bash
uv run mentalmodel runs show --graph-id <graph_id>
```

3. Inspect a node trace.

```bash
uv run mentalmodel runs trace --graph-id <graph_id> --node-id <node_id>
```

## What to look for

- `summary.json` fields:
  - `invocation_name`
  - `runtime_default_profile_name`
  - `runtime_profile_names`
- `node.started` payload:
  - `runtime_profile`
- span attributes:
  - `mentalmodel.runtime.profile`
  - `mentalmodel.invocation.name`

## Rule

Use `invocation_name` for top-level run categories like `real_smoke` or
`training_prod`. Use `runtime_profile` for per-node resource binding.
