# Run Comparison

Use this recipe when a bug only appears after a code/config change or when two
runs disagree and you need to localize the drift quickly.

## Goal

Answer:

- Which nodes changed behavior between the two runs?
- Did an invariant outcome change?
- Did output payloads or state transitions drift?

## Recommended flow

1. Identify the two run ids.

```bash
uv run mentalmodel runs list --graph-id <graph_id>
```

2. Compare the runs directly.

```bash
uv run mentalmodel runs diff --graph-id <graph_id> --run-a <run_a> --run-b <run_b>
```

3. Narrow to one suspicious node if needed.

```bash
uv run mentalmodel runs diff --graph-id <graph_id> --run-a <run_a> --run-b <run_b> --node-id <node_id>
```

4. Narrow to one invariant if needed.

```bash
uv run mentalmodel runs diff --graph-id <graph_id> --run-a <run_a> --run-b <run_b> --invariant <invariant_node>
```

5. Inspect the diverged node payloads directly.

```bash
uv run mentalmodel runs inputs --graph-id <graph_id> --run-id <run_a> --node-id <node_id>
uv run mentalmodel runs inputs --graph-id <graph_id> --run-id <run_b> --node-id <node_id>
```

## What to look for

- invariant pass/fail drift
- different node output payloads
- changed execution event sequences
- state transition differences
- runtime-context or dependency changes that explain the behavioral drift

## Rule

Use `runs diff` to localize the regression, then drop back into the invariant
or runtime-failure recipe for the specific node that changed.
