# Invariant Debugging

Use this recipe when an invariant fails, looks vacuous, or appears to be
checking the wrong values.

## Goal

Answer:

- What exact payload did the invariant receive?
- Where were those values produced?
- Is the invariant checking a real semantic truth or just a derived summary?

## Recommended flow

1. Verify the program and persist a run bundle.

```bash
uv run mentalmodel verify --entrypoint <module:function>
```

2. Resolve the newest run.

```bash
uv run mentalmodel runs latest --graph-id <graph_id>
```

3. Inspect the invariant inputs.

```bash
uv run mentalmodel runs inputs --graph-id <graph_id> --node-id <invariant_node>
```

4. Inspect the invariant output and event trace.

```bash
uv run mentalmodel runs outputs --graph-id <graph_id> --node-id <invariant_node>
uv run mentalmodel runs trace --graph-id <graph_id> --node-id <invariant_node>
```

5. Inspect the producer node that fed the invariant.

```bash
uv run mentalmodel runs outputs --graph-id <graph_id> --node-id <producer_node>
```

## What to ask

- Could the compared values ever diverge in a realistic bug?
- Do the values come from truly independent sources?
- Is the invariant using the real source values rather than a post-processed
  summary?
- Would a property check catch the same class of failure better?

## Common strengthening moves

- replace length-only checks with key-set equality
- compare sampled versus current state directly
- use `assert_causal_order(...)` for version/lag relationships
- add a property check when several parameter combinations can expose the bug
