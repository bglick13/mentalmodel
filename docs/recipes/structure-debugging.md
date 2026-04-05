# Structure Debugging

Use this recipe when a workflow shape looks wrong in generated docs or when a
node seems to be missing, duplicated, or connected incorrectly.

## Goal

Answer:

- Did the authored workflow lower into the IR you expected?
- Are plugin provenance and runtime-context boundaries correct?
- Are data edges and containment edges being generated correctly?

## Recommended flow

1. Run doctor first if the repo or skill setup looks suspicious.

```bash
uv run mentalmodel doctor --entrypoint <module:function>
```

2. Check the lowered graph and findings.

```bash
uv run mentalmodel check --entrypoint <module:function>
```

3. Render the graph.

```bash
uv run mentalmodel graph --entrypoint <module:function>
```

4. Render the docs.

```bash
uv run mentalmodel docs --entrypoint <module:function>
```

## What to inspect

- `topology.md`
  Use this to confirm the authored program lowered into the expected node/edge
  structure.
- `node-inventory.md`
  Use this to confirm node kind, provenance, runtime context, and dependencies.
- `runtime-contexts.md`
  Use this to confirm runtime grouping and boundary ownership.
- `invariants.md`
  Use this to confirm important invariants are attached at the right nodes.

## Common failure patterns

- a plugin primitive lowered, but provenance is missing or wrong
- a `Ref(...)` edge was not declared, so a required data edge is absent
- a containment structure exists, but no runtime/data edge connects the nodes
- an extension primitive was not registered, so lowering failed or shape drifted

## When to stop

Once the IR and docs are correct, switch to runtime debugging. Do not keep
debugging a runtime symptom if the structure is already wrong in generated docs.
