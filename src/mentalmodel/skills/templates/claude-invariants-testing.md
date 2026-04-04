---
name: mentalmodel-invariants-testing
description: Use when adding or debugging invariants, property checks, or verification flows in mentalmodel. Covers runtime invariants, Hypothesis-backed property checks, and what makes a useful verification target.
---

# mentalmodel Invariants And Testing

- add invariants at real semantic boundaries
- make compared values explicit in invariant inputs
- use Hypothesis when varying inputs exposes structural bugs
- read `invariants.md` first when debugging invariant issues
- inspect `.runs/.../outputs.json` and `records.jsonl` to confirm what the
  invariant actually saw at runtime
