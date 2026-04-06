# Loop Debugging

Use this recipe when a `StepLoop` behaves unexpectedly, a carried state drifts,
or a loop body looks correct in source but wrong in `.runs`.

## Goal

Answer:

- Which iteration is wrong?
- What did that iteration receive as `LoopItemRef(...)` and `LoopStateRef(...)`?
- Did the carried state evolve the way the authored model says it should?

## Recommended flow

1. Verify the workflow and persist a run bundle.

```bash
uv run mentalmodel verify --entrypoint <module:function>
```

2. Replay the whole loop first.

```bash
uv run mentalmodel replay --graph-id <graph_id> --loop-node-id <loop_node_id>
```

3. Narrow to one iteration.

```bash
uv run mentalmodel replay --graph-id <graph_id> --loop-node-id <loop_node_id> --iteration-index 1
```

4. Inspect the body node inputs and outputs for that iteration.

```bash
uv run mentalmodel runs inputs \
  --graph-id <graph_id> \
  --node-id <loop_node_id>.<use_name>.<node_id> \
  --iteration-index 1

uv run mentalmodel runs outputs \
  --graph-id <graph_id> \
  --node-id <loop_node_id>.<use_name>.<node_id> \
  --iteration-index 1
```

## What to look for

- `frame_id` such as `steps[1]`
- `loop_node_id`
- the bound loop item payload
- the bound carry-state payload
- whether `node.inputs_resolved` matches the authored `Use(..., bind=...)`
- whether `final_outputs` and `history_outputs` match the `LoopSummary(...)`

## Rule

Start with the whole loop, then narrow to one iteration. Do not guess from the
final loop summary when the bug is iteration-local.
