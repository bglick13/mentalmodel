---
name: mentalmodel-invariants-testing
description: Use when adding or debugging invariants, property checks, or verification flows in mentalmodel. Covers runtime invariants, Hypothesis-backed property checks, and what makes a useful verification target.
---

# mentalmodel Invariants And Testing

Use this skill when working on invariants, verification, or property checks.

## Good invariants

- Check semantic truths, not implementation trivia.
- Prefer invariants at joins, state transitions, and policy boundaries.
- Make the compared values explicit in the input payload.
- A useful invariant should be falsifiable by a realistic bug.

## Smells

- invariant compares two fields produced by the same step and can never diverge
- invariant reads a derived summary instead of the true source values
- property check only validates lengths when key-set equality matters
- docs list an invariant, but the invariant cannot fail under any realistic run

## Property checks

- Use `@property_check` for deterministic checks.
- Use `@hypothesis_property_check` when varying inputs exposes structural bugs.
- Keep generated input domains small and meaningful.
- Property checks should exercise the actual workflow, not mock around it.
- Prefer reusable helpers before writing bespoke assertions:
  - `assert_aligned_key_sets(...)`
  - `assert_causal_order(...)`
  - `assert_monotonic_non_decreasing(...)`
  - `collect_runtime_boundary_observations(...)` with
    `assert_runtime_boundary_crossings(...)`

## Verification workflow

```bash
uv run mentalmodel verify --entrypoint <module:function>
```

Verification writes a runtime bundle to `.runs/<graph_id>/<run_id>/`. When an
invariant fails or looks suspicious, inspect:

- `verification.json` for the combined report
- `records.jsonl` for `invariant.checked` and upstream node events
- `outputs.json` for the actual join payload seen by the invariant
- `otel-spans.jsonl` only after the semantic records are understood

When investigating a suspected invariant bug:

1. Read generated `invariants.md`.
2. Inspect the invariant inputs in the authored program.
3. Trace where those values are produced.
4. Inspect the matching `.runs/.../outputs.json` and `records.jsonl`.
5. Ask whether the invariant could actually fail.
6. Strengthen the invariant or property check if it is too weak.

## Helper selection

- fanout/join map consistency: use `assert_aligned_key_sets(...)`
- version, cursor, epoch, or offset progress: use
  `assert_monotonic_non_decreasing(...)`
- observed-versus-current causal relationships: use `assert_causal_order(...)`
- runtime boundary policy checks: derive observations from the lowered graph with
  `collect_runtime_boundary_observations(...)`, then validate them with
  `assert_runtime_boundary_crossings(...)`
