# Pangramanizer Migration Learnings

This document records concrete learnings from the Pangramanizer port as we
continue to exercise `mentalmodel` on a larger, service-heavy real workflow.

It is intentionally lightweight. The goal is to preserve signals while they are
fresh, not to prematurely lock in solutions.

When a learning looks durable, we can later promote it into:

- a plan item
- a docs/skills update
- a dedicated design note
- a concrete package change

## Current Learnings

### 1. Runtime environments need strong CLI ergonomics to be useful

`RuntimeEnvironment` was the right abstraction, but it did not feel complete
until CLI verification could bind environments cleanly and durably.

Practical takeaway:

- workflow factories and environment factories should remain separate concepts
- the CLI and invocation-spec layer are part of the runtime-environment product
  surface, not optional polish

### 2. Run-level invocation naming matters separately from runtime profiles

A single run can involve multiple runtime profiles, but operators still need a
top-level way to distinguish "what kind of run this is" across `.runs`,
telemetry, replay, and UI surfaces.

Practical takeaway:

- `invocation_name` is operationally important, not just cosmetic metadata

### 3. Reusable blocks are a real improvement for serious workflows

The Pangramanizer port got materially better once the training step shape became
reusable structure instead of a flat top-level list of workflow fragments.

Practical takeaway:

- `Block` / `Use` is not only about DRYness
- it materially improves authored legibility and makes future `StepLoop`
  adoption cleaner

### 4. Plugin primitives should be block-safe by default

The Pangramanizer runtime-executable plugins originally assumed `list[Ref]`
inputs. The Block/Use migration exposed that as too narrow immediately.

Practical takeaway:

- plugin authoring should assume reusable composition from the start
- `InputRef` is often the more honest input surface than raw `Ref`

### 5. Small structural variations across reused blocks still need a clean story

Reusing the rollout block for evaluation was much better with `Use(...)`, but
the eval path still needed one compile-time variation from training: a
different sampling service budget.

Practical takeaway:

- reusable composition solved the main namespacing problem
- there is still useful design space around “same semantic block, slightly
  different bound configuration”

### 6. Correct namespacing improves structure but raises path-management pressure

Once a program is honestly structured with `Use(...)`, node ids become more
correct and more verbose. That is the right tradeoff, but it pushes more weight
onto path ergonomics in tests, CLI usage, replay, and UI.

Practical takeaway:

- namespacing should stay honest
- the surrounding inspection surfaces need to make namespaced graphs easy to
  navigate

### 7. Logical input names should align with semantic contracts

Reducer and effect contracts stayed cleaner when block logical inputs were named
after the semantic payload they represent, instead of introducing extra alias
layers like `rollout` when the contract is really `rollout_join`.

Practical takeaway:

- block boundaries are easier to reason about when their logical input/output
  names preserve the existing semantic contract language

### 8. Native loops force temporal state to become explicit in a good way

Promoting the Pangramanizer training flow to `StepLoop` exposed that several
existing actors were really carrying temporal state across steps:

- batch cursor progression
- policy version evolution
- step lifecycle snapshots
- best-checkpoint registry state

Those concerns had to move into explicit loop carry and loop-local reducers.

Practical takeaway:

- `StepLoop` is valuable partly because it flushes hidden temporal state out of
  stateful actors
- real multi-step programs get cleaner when cross-iteration state is authored as
  loop carry instead of incidental actor state

### 9. Loop bootstrap state still needs a clean environment-driven story

`LoopCarry.initial` is intentionally a literal authored value. That is a good
default for determinism, but real applications often need the first iteration
to incorporate runtime/environment bootstrap state such as initial policy
versions or seeded batch streams.

Practical takeaway:

- the current model is workable, but larger applications will sometimes need to
  thread environment bootstrap values through loop items in slightly awkward
  ways
- this looks like a genuine framework design area rather than a Pangramanizer
  quirk

### 10. Frame-aware inspection is not optional once loops are real

Once Pangramanizer moved to `StepLoop`, many previously convenient root-output
lookups became ambiguous. Tests and verification helpers needed explicit
frame-aware inspection helpers to stay honest.

Practical takeaway:

- framed outputs and framed replay are the right substrate
- applications will still want higher-level helper utilities once looped runs
  become normal
- “which frame?” becomes an operator question the package and UI should help
  answer naturally

### 11. Honest namespacing needs dotted-path friendliness throughout the stack

The Block/Use migration already made namespacing more explicit. The later
StepLoop migration compounded that with loop-owner and frame prefixes. Local
input-resolution helpers had to learn dotted logical path matching to stay
usable.

Practical takeaway:

- reusable composition plus loops makes dotted logical paths first-class
- helper layers that only understand flat names or suffix conventions will
  eventually become friction points

### 12. Parity tooling needs a first-class notion of semantic vs volatile outputs

The Phase 6 shadow work eventually converged, but only after the parity harness
started treating some outputs as inherently volatile instead of pretending every
tracked number should match exactly across replayed or differently scheduled
runs.

The main examples were:

- wall-clock timing metrics
- run-unique identity/cycle counters
- a very small set of optimizer diagnostics with bounded numeric noise

Practical takeaway:

- run comparison surfaces should distinguish semantic mismatches from volatile
  operational differences
- timing and identity metrics are still useful, but they belong in a separate
  comparison bucket from reward, control-plane, and core optimizer semantics

## How To Use This Doc

Add a new item when:

- the migration reveals a repeated friction point
- a design choice in `mentalmodel` turns out to be unusually good or unusually
  costly
- a local Pangramanizer workaround feels like it is pointing at a reusable
  framework lesson

Do not add speculative ideas that have not shown up in the migration yet.
