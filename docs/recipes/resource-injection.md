# Resource Injection

Use this recipe when several nodes need the same shared helper object, client,
or model and you want that dependency to stay typed, explicit, and runtime
swappable.

## Goal

Answer:

- Should this dependency be a `ResourceKey[...]`?
- Which block or node should declare the resource requirement?
- Is the resource bound at runtime instead of hidden inside closures?

## Recommended flow

1. Define a typed resource key.

```python
reward_model = ResourceKey("reward_model", CompositeRewardModel)
```

2. Declare the requirement on the node or block.

```python
Effect(
    "reward_ticket",
    handler=RewardTicket(),
    resources=(reward_model,),
)
```

or once on a block:

```python
BlockDefaults(resources=(reward_model,))
```

3. Bind the resource in a `RuntimeEnvironment`.

```python
RuntimeProfile(
    name="fixture",
    resources={reward_model: CompositeRewardModel(...)},
)
```

4. Resolve it inside the handler.

```python
model = ctx.resources.require(reward_model)
```

## Debugging flow

If runtime execution fails:

```bash
uv run mentalmodel verify --entrypoint <module:function>
uv run mentalmodel runs trace --graph-id <graph_id> --node-id <node_id>
```

Look for:

- missing runtime profile selection
- missing resource in the active profile
- a resource declared on the node but not on the shared block defaults you meant
  to use

## Rule

Prefer `ResourceKey[...]` and `RuntimeEnvironment` over application-owned
closures or hidden module-level globals.
