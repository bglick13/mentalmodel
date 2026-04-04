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
3. `mentalmodel verify`
4. Inspect the newest `.runs/<graph_id>/<run_id>/` bundle
5. Inspect authored workflow and handler implementations

## What to inspect

- topology bugs: `graph` and `topology.md`
- boundary/provenance bugs: `node-inventory.md` and runtime-context docs
- invariant bugs: `invariants.md`, then checker inputs and producer nodes
- runtime failures: `verification.json`, `summary.json`, `records.jsonl`, and
  `outputs.json`
- OTel detail: `otel-spans.jsonl` when no external sink is configured

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
5. cross-check the runtime bundle in `.runs`
6. decide whether the bug is in authoring, lowering, runtime execution, or docs
