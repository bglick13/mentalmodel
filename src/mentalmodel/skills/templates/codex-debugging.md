---
name: mentalmodel-debugging
description: Use when debugging a mentalmodel workflow or generated artifact. Covers IR inspection, verification flow, runtime tracing/records, and how to follow a bug from docs back to authored primitives.
---

# mentalmodel Debugging

Use this skill when investigating a bug in a workflow, runtime execution, or
generated artifact.

## Debugging order

1. `mentalmodel check`
2. `mentalmodel docs`
3. `mentalmodel doctor`
4. `mentalmodel verify`
   Use `--params-json` or `--params-file` when the workflow entrypoint is parameterized.
   Use `--environment-entrypoint` when the runtime environment is built separately.
   Prefer `--spec` when the project already has a TOML invocation spec.
5. `mentalmodel runs latest --graph-id <graph_id>`
6. `mentalmodel otel show-config` if tracing/export looks suspicious
7. `mentalmodel replay --graph-id <graph_id>`
8. `mentalmodel runs inputs --graph-id <graph_id> --node-id <node_id>`
9. `mentalmodel runs outputs --graph-id <graph_id> --node-id <node_id>`
10. `mentalmodel runs trace --graph-id <graph_id> --node-id <node_id>`
11. `mentalmodel runs diff --graph-id <graph_id> --run-a <run_a> --run-b <run_b>` when you need change-focused debugging
12. If bundle metadata looks stale, run `mentalmodel runs repair --dry-run`
13. Inspect authored workflow and handler implementations

When loops are involved, add:

- `--loop-node-id <loop_node_id>`
- `--iteration-index <n>`
- `--frame-id <loop_node_id>[<n>]`

## What to inspect

- topology bugs: `graph` and `topology.md`
- boundary/provenance bugs: `node-inventory.md` and runtime-context docs
- invariant bugs: `invariants.md`, then checker inputs and producer nodes
- runtime failures: `verification.json`, `summary.json`, `records.jsonl`, and
  `outputs.json`
- OTel detail: `otel-spans.jsonl` when no external sink is configured
- `runs inputs` shows the exact bound input payload persisted by
  `node.inputs_resolved`
- `replay` shows the full semantic event timeline for one run
- `runs diff` compares two persisted runs and highlights invariant and payload
  drift
- `otel show-config` reveals the exact resolved tracing mode and endpoint
- `otel write-demo --stack lgtm --output-dir ...` gives you a local OTEL UI
  path quickly when you need to demo traces externally
- `summary.json` now carries `schema_version`; old bundles can be normalized
  with `runs repair`

## Runtime notes

- every executable node emits semantic records
- effects emit `effect.invoked` and `effect.completed`
- invariants emit `invariant.checked`
- stateful actors emit `state.read` and `state.transition`
- OTel is the export layer; semantic records are the primary debugging source
- `mentalmodel verify` writes the run bundle to `.runs` by default

## Investigation pattern

When a generated doc “looks wrong”:

1. confirm the doc is generated, not stale
2. find the corresponding node in the authored workflow
3. inspect the inputs to that node
4. inspect the producers of those inputs
5. use `replay` to understand the full run lifecycle
6. use `runs inputs`, `runs outputs`, and `runs trace` for the node in question
7. use `runs diff` if the regression is relative to an earlier run
8. cross-check the runtime bundle in `.runs`
9. decide whether the bug is in authoring, lowering, runtime execution, or docs

## Recipe docs

- `docs/recipes/structure-debugging.md`
- `docs/recipes/block-reuse.md`
- `docs/recipes/custom-view-authoring.md`
- `docs/recipes/invariant-debugging.md`
- `docs/recipes/loop-debugging.md`
- `docs/recipes/runtime-profile-selection.md`
- `docs/recipes/resource-injection.md`
- `docs/recipes/parameterized-verification.md`
- `docs/recipes/runtime-failure-debugging.md`
- `docs/recipes/run-comparison.md`
