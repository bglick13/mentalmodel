# Block Reuse

Use this recipe when a workflow fragment should be instantiated more than once
and the authored code is starting to accumulate manual prefixes or stringly
`Ref(...)` plumbing.

## Goal

Answer:

- Should this fragment become a `Block`?
- What are the logical block inputs and outputs?
- Are downstream nodes consuming `Use.output_ref(...)` instead of prefixed ids?

## Recommended flow

1. Inspect the authored shape and generated docs.

```bash
uv run mentalmodel check --entrypoint <module:function>
uv run mentalmodel docs --entrypoint <module:function>
```

2. Define the reusable fragment as a `Block`.

- move repeated children into one block definition
- replace external input refs with `BlockRef(...)`
- declare `BlockInput(...)` and `BlockOutput(...)`

3. Instantiate the block with `Use(...)`.

```python
review_step = Use(
    "review_step",
    block=review_step_block,
    bind={"ticket": Ref("incoming_ticket")},
)
```

4. Consume logical outputs instead of prefixed node ids.

```python
Join(
    "queue_join",
    inputs=[review_step.output_ref("review_audit")],
)
```

## What to look for in generated docs

- `use` nodes are present and named clearly
- bind edges make the outer inputs obvious
- the block internals stay prefix-agnostic in authored code
- downstream nodes read through declared outputs instead of `Ref("prefix.inner")`

## Rule

If application code is spelling internal prefixed ids manually, the block
boundary is not being used cleanly enough.
