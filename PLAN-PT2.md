# mentalmodel Plan Part 2

This document is the follow-on implementation plan after the initial
`PLAN.md` milestones. It focuses on turning the current foundation into a more
serious debugging, verification, replay, and agent-facing system.

The goal of Part 2 is not to add isolated features. The goal is to make the
runtime artifacts, verification flows, observability story, and agent workflow
feel like one coherent system.

This plan assumes the same engineering standard as [PLAN.md](/Users/ben/repos/mentalmodel/PLAN.md):

- no shortcut implementations that weaken the intended architecture
- no `Any` or unnecessary type erasure
- every milestone must include solid tests for success and failure cases
- verification must stay green before moving to the next milestone

## Priority Order

Recommended implementation order:

1. Inspectable runs
2. Explicit resolved-input recording
3. Run schema versioning
4. Replay and run diffing
5. Verification helpers beyond the demo
6. First-class OpenTelemetry configuration
7. Second serious reference example
8. Tighter agent workflow packaging

This order matters. Items 1 through 4 strengthen the runtime/debugging substrate
that later milestones will depend on.

## Working Principles

- `.runs` is a first-class product surface, not a debug byproduct
- semantic records remain the primary source of truth for debugging
- OpenTelemetry is an export/interoperability layer, not the core model
- all new CLI inspection paths should have both human-readable and JSON outputs
- new features should improve both human debugging and agent debugging

## Milestone 8: Make `.runs` More Inspectable

### Goal

Reduce the amount of manual artifact joining required to understand one run.
Humans and agents should be able to answer common runtime questions directly
from the CLI without opening multiple files by hand.

### Why

The current run bundle is useful, but it still expects the user to mentally join:

- summary metadata
- semantic event streams
- outputs
- state snapshots
- OTel fallback data

That is too much friction for the central debugging surface of the package.

### Deliverables

Add ergonomic run inspection commands:

- `mentalmodel runs latest --graph-id ...`
- `mentalmodel runs inputs --graph-id ... --node-id ...`
- `mentalmodel runs outputs --graph-id ... --node-id ...`
- `mentalmodel runs trace --graph-id ... --node-id ...`

Potential future aliases can be added later, but these four are the target
surface for this milestone.

### Detailed behavior

`runs latest`

- resolve the newest run for one graph
- print the resolved run id and run directory
- support `--json`
- optionally include the same summary shape as `runs show`

`runs inputs`

- resolve one run and one node
- show the concrete resolved input payload seen by that node
- support `--json`
- if no direct input artifact exists yet, this command should not guess by
  walking upstream outputs silently; that gap should be closed by Milestone 9

`runs outputs`

- resolve one run and one node
- show the concrete output produced by that node
- support `--json`

`runs trace`

- show the semantic lifecycle for one node in one run
- prefer semantic records over raw OTel spans
- include span info only when useful and available
- support filtering and `--json`

### Scope boundary

This milestone is about inspection ergonomics, not replay semantics.

Do not:

- implement full run diffing here
- implement full replay here
- let commands silently reconstruct data from unreliable heuristics

### Likely file ownership

- [cli.py](/Users/ben/repos/mentalmodel/src/mentalmodel/cli.py)
- [runs.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/runs.py)
- new helper module if needed:
  [inspect.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/inspect.py)
- [README.md](/Users/ben/repos/mentalmodel/README.md)
- skill templates under
  [/Users/ben/repos/mentalmodel/src/mentalmodel/skills/templates](/Users/ben/repos/mentalmodel/src/mentalmodel/skills/templates)

### Acceptance criteria

- all new commands work for the async RL demo
- all commands support stable JSON output
- there are tests for latest-run resolution and missing-node / missing-run errors
- the commands are useful against previously written `.runs` bundles

## Milestone 9: Record Resolved Node Inputs Explicitly

### Goal

Persist what each node actually saw after input binding, instead of forcing
debuggers to infer it indirectly from upstream outputs.

### Why

This closes one of the biggest remaining debugging gaps. It should become easy
to answer:

- what exact payload hit this invariant?
- what state did this actor read?
- what effect inputs were invoked?
- what concrete join payload was reduced?

### Deliverables

Add a new semantic event, probably `node.inputs_resolved`, with a JSON-safe
payload representing the bound inputs for one executable node.

The design should support:

- actors
- effects
- joins
- invariants

### Detailed behavior

Requirements:

- record the bound input payload after dependency binding and before handler execution
- keep payloads JSON-safe and deterministic
- preserve useful structure, not just shallow summaries
- avoid duplicating large payloads unnecessarily if a future compaction strategy
  is introduced

Open design question to resolve during implementation:

- whether `node.inputs_resolved` should contain full payloads or a compact
  summary plus separately materialized per-node input artifacts

My recommendation:

- use semantic records for compact but still meaningful payloads
- materialize full node input/output artifacts in the `.runs` bundle for CLI inspection

### Follow-on CLI impact

This milestone is what makes `runs inputs` and richer `runs outputs` truly
valuable. Milestone 8 may land first with a partial UX, but this milestone
completes the inspectability story.

### Scope boundary

Do not cram replay diffing into this milestone.

The output should focus on correctness and inspectability first.

### Likely file ownership

- [plan.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/plan.py)
- [executor.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/executor.py)
- [events.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/events.py)
- [runs.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/runs.py)
- [export.py](/Users/ben/repos/mentalmodel/src/mentalmodel/observability/export.py)

### Acceptance criteria

- each executable node type records resolved inputs
- `runs inputs` works directly from persisted run data
- tests cover representative actor/effect/join/invariant inputs
- tests cover payload serialization for nested typed structures

## Milestone 10: Add Run Schema Versioning

### Goal

Make `.runs` a durable, evolvable on-disk interface instead of an accidental
format that breaks whenever fields change.

### Why

We already hit one compatibility issue around `created_at_ms`. That will happen
again unless the on-disk schema becomes intentional.

### Deliverables

- add `schema_version` to `summary.json`
- define a compatibility strategy for legacy bundles
- add `mentalmodel runs repair`

### Detailed behavior

`schema_version`

- must be present on all newly written runs
- should live in `summary.json`
- should be treated as the version of the run bundle schema, not the package version

Compatibility layer

- run loaders must support a defined set of older bundle shapes
- compatibility logic should be centralized in one place, not scattered across commands
- older bundles should remain readable even before repair

`runs repair`

- scan one `.runs` root or one graph subtree
- detect legacy bundles
- backfill missing fields when safe
- avoid rewriting already-valid bundles unnecessarily
- support `--dry-run`
- support `--json`

### Scope boundary

Do not build a migration framework for every future artifact format yet. Keep
the mechanism simple and focused on the run bundle surface.

### Likely file ownership

- [runs.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/runs.py)
- [cli.py](/Users/ben/repos/mentalmodel/src/mentalmodel/cli.py)
- possibly new schema helper:
  [schema.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/schema.py)

### Acceptance criteria

- new runs always include `schema_version`
- old runs still load
- `runs repair --dry-run` reports necessary changes
- `runs repair` updates legacy bundles deterministically
- tests include at least one legacy run fixture

## Milestone 11: Improve Replayability

### Goal

Turn replay from a placeholder into a meaningful debugging and analysis tool.

### Why

Replay and run diffing are some of the highest-leverage features for a system
whose purpose is mental-model retention.

### Deliverables

At minimum:

- replay semantic records
- diff two runs
- show where outputs or state diverged
- support investigating “why did invariant X fail in run A but not run B?”

### Detailed behavior

Phase 1: semantic replay

- reconstruct the node execution timeline from persisted records
- render a readable replay summary
- support `--json`

Phase 2: run diff

- compare two run bundles
- highlight differences in:
  - node execution order
  - semantic event presence
  - invariant pass/fail outcomes
  - node outputs
  - final state

Phase 3: targeted explanation

- support node-focused and invariant-focused diffing
- make it easy to answer why one specific node behaved differently

### Recommended CLI shape

- `mentalmodel replay --graph-id ... --run-id ...`
- `mentalmodel runs diff --graph-id ... --run-a ... --run-b ...`
- possibly `mentalmodel runs diff --node-id ...`

### Scope boundary

This does not need deterministic re-execution from records yet. Focus on
playback and comparison first.

### Likely file ownership

- [replay.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/replay.py)
- [runs.py](/Users/ben/repos/mentalmodel/src/mentalmodel/runtime/runs.py)
- [cli.py](/Users/ben/repos/mentalmodel/src/mentalmodel/cli.py)

### Acceptance criteria

- replay works on the async RL demo run bundles
- diffing two runs produces stable output
- invariant divergence is visible in diffs
- tests cover both matching and divergent runs

## Milestone 12: Strengthen Verification Beyond The Demo

### Goal

Turn the good verification patterns currently embedded in the demo into reusable
framework-level helpers.

### Why

Users should not need to reinvent the same useful checks every time. Strong
verification should be easy to author in the mentalmodel style.

### Deliverables

Add helper APIs for:

- key-set alignment checks
- monotonic state transition checks
- causal consistency checks
- runtime-context boundary checks

### Detailed behavior

Examples:

Key-set alignment

- useful for fanout/join pipelines
- assert that all downstream score maps, traces, or annotations align to the
  same upstream sample ids

Monotonic state transitions

- useful for version counters, cursors, offsets, epochs, step counters

Causal consistency

- useful for checking relationships like sampled policy version versus current
  policy version

Runtime-context boundaries

- useful for checking that data/effects cross boundaries only through declared
  nodes or bridges

### API direction

Do not hard-code RL-specific helpers.

Prefer a small library of generic helpers in something like:

- [verification_helpers.py](/Users/ben/repos/mentalmodel/src/mentalmodel/testing/verification_helpers.py)

Those helpers should compose cleanly with:

- runtime invariants
- deterministic property checks
- Hypothesis-backed property checks

### Scope boundary

Do not turn this into a large assertion DSL. Keep the helper surface small and
composable.

### Acceptance criteria

- at least two helpers are adopted by the async RL demo
- helpers have direct unit tests
- docs and skill templates explain when to use them

## Milestone 13: Make OTel Configuration First-Class

### Goal

Replace the current “best effort” OTel setup with an explicit configuration and
deployment story.

### Why

The package now has:

- semantic records on disk
- OTel span fallback on disk

That is a good start, but not a full observability story.

### Deliverables

- env/config-driven exporter selection
- documented OTLP setup
- clear runtime rule for disk fallback versus external export
- optional `mentalmodel runs export-otel`
- explicit notes on self-hosted OTel UIs
- a path to domain-specific visualizations built from the DSL/runtime data

### Concrete decisions to make

- configuration source: env vars only, config file, or both
- sink precedence rules
- exporter defaults
- whether spans should be mirrored to disk even when an external sink exists

### Open-source / self-hosting angle

This milestone should explicitly evaluate and document a self-hosted path.

Examples worth considering:

- Jaeger
- Grafana Tempo + Grafana
- SigNoz

The output does not need to implement full deployment automation yet, but it
should produce a clear recommended integration path.

### Hosted-service angle

Longer term, the package may want:

- a hosted telemetry/debugging service
- domain-specific visualization views powered by the semantic runtime model

This milestone should document the architectural implications of that future so
we do not paint ourselves into a corner.

### Acceptance criteria

- tracing configuration is explicit and documented
- runtime behavior is deterministic with respect to sink selection
- tests cover disk fallback and configured external-sink behavior
- README or dedicated docs explain the supported setups

## Milestone 14: Scope OTel Metrics And Logs Deliberately

### Goal

Reserve explicit design space for metrics and logs so tracing does not become a
catch-all observability implementation by accident.

The design should prioritize:

- ergonomics
- extensibility

### Why

The package now has strong semantic records and improving trace export, but it
does not yet have a deliberate metrics model or log-export model. Those should
be designed as first-class interfaces, not bolted on as incidental side
effects of trace wiring.

### Deliverables

- decide what the package-level metrics surface should actually measure
- decide whether OTel logs should mirror semantic records, provide a reduced
  operational stream, or stay out of scope
- document the relationship between:
  - semantic records
  - OTel traces
  - future OTel metrics
  - future OTel logs
- identify the smallest credible first metric set if metrics are added later
- decide how node outputs can become metrics ergonomically without relying on
  brittle implicit inference

### Design priorities

#### 1. Ergonomics

Users should be able to materialize useful runtime metrics without learning the
OpenTelemetry SDK or hand-writing exporter-specific code.

The guiding principle is:

- most of the metrics a user would normally log to W&B for a system like
  `pangramanizer` should be easy to express in `mentalmodel`

That should work through one of two paths:

- safe automatic inference for clearly metric-shaped outputs
- a lightweight public API for custom output-derived metrics

#### 2. Extensibility

The metrics/logs design should not hard-code RL-specific concepts into the
framework, but it must make it easy for RL-style systems to expose domain
metrics intentionally.

Future plugins and domain packages should be able to add signal definitions
without patching runtime internals.

### Concrete questions to answer

- which counters and histograms are stable enough to export?
- should metrics be derived from semantic records, runtime state, or both?
- should logs mirror semantic records or only carry operational/error detail?
- which signals belong in `.runs` versus external backends?
- what correlation model should tie traces, metrics, logs, and semantic records
  together?
- which node output shapes are safe enough for automatic metric inference?
- what is the smallest public API for attaching custom metrics to actors/effects
  without making users think in raw OTEL primitives?

### Output-derived metrics

This milestone should explicitly scope two paths for metrics derived from node
outputs.

#### A. Safe automatic inference

Automatic inference is good for ergonomics, but only when the framework can
identify clearly metric-shaped outputs without guesswork.

Candidate safe cases:

- flat mappings of `str -> int | float`
- typed outputs explicitly marked as metric outputs
- small, bounded numeric summary objects

Guardrails:

- no automatic export of dynamic per-example or per-id maps by default
- no automatic export of high-cardinality dimensions
- no automatic inference when key names or structure are likely to grow without
  bound

For example:

- a summary output like `{"mean_reward": 0.8, "mean_kl": 0.12}` is a good
  automatic metric candidate
- a map like `{"prompt-1:0": 0.8, "prompt-1:1": 0.7, ...}` is not

#### B. Lightweight custom metric adapters

Many useful domain metrics will not be safe to infer automatically, but they
should still be easy to expose.

The framework should provide a small public API so a primitive can attach
metric extraction logic to its output without dropping into OTEL SDK details.

This should likely look like one of:

- primitive-local metric specifications
- output metric extractors attached to actors/effects

The design goal is:

- custom metrics should be local, typed, explicit, and lightweight

The `pangramanizer` litmus test is:

- reward means
- KL summaries
- rollout counts
- policy lag
- step counters
- other W&B-style training summaries

should all be easy to express either through safe automatic inference or a
small attached metric extractor API.

### Recommended first metric surface

The initial built-in metrics should stay small and operationally useful.

Candidate defaults:

- `mentalmodel.run.started`
- `mentalmodel.run.completed`
- `mentalmodel.node.executions`
- `mentalmodel.node.duration`
- `mentalmodel.invariant.failures`

Then output-derived metrics can layer on top through the scoped inference /
extractor design above.

### Recommended log surface

Logs should not become a full mirror of semantic records.

The likely direction is:

- export reduced operational log events
- focus on failures, invariant violations, misconfiguration, and run-level
  summaries
- keep detailed semantic debugging in `.runs` and traces

### Scope boundary

This milestone is intentionally a placeholder for now.

Do not implement ad hoc metrics or logs before this scoping work is completed.

### Acceptance criteria

- the roadmap explicitly reserves this work
- future observability milestones can reference this milestone instead of
  inventing metrics/logs scope ad hoc
- the roadmap explicitly captures the ergonomics/extensibility priorities
- the plan includes both safe metric inference and lightweight custom
  metric-adapter paths

## Milestone 15: Add A Second Serious Reference Example

### Goal

Prove that the programming model is not implicitly shaped around async RL, and
introduce a typed objective/search substrate that can drive bounded
optimization over workflows exposing verifiable metric signals.

### Why

One demo is not enough to validate that the primitives are general-purpose.

### Scope

This milestone is intentionally split into three slices built on one shared
substrate:

- `15A`: a serious second reference example
- `15B`: an autoresearch-style demo built on the same objective model
- `15.5`: runtime-executable plugin infrastructure
- `15.6`: `AutoResearch` as a real executable plugin primitive

#### 15A: Agent tool-use workflow

The second serious reference example should be an `agent_tool_use` workflow
that stresses:

- multiple runtime contexts
- multiple effects
- joins
- invariants
- verification helpers
- useful `.runs` inspection

This is the recommended second example because it is meaningfully different
from async RL while still exercising the same runtime, docs, verification, and
CLI surfaces.

#### 15B: Autoresearch-style bounded optimization demo

Add a lightweight optimization substrate that can evaluate bounded candidate
sets over workflows exposing stable metric signals. Then build an
`autoresearch`-style sorting demo on top of it.

The first objective should be intentionally simple:

- sorting arrays
- exact correctness invariant
- scalar optimization metric like comparison count

This is not a full autonomous code-mutation system. It is a deterministic proof
that `mentalmodel` workflows can expose signals suitable for agent-driven search
and autoresearch-style loops.

#### 15.5: Runtime-executable plugin infrastructure

Current plugins prove authored extension primitives can participate in:

- lowering
- provenance
- analysis/docs/diagrams

That is useful, but incomplete. For long-term extensibility, plugins also need
to be able to contribute real runtime behavior.

This slice adds the generic substrate required for that:

- extend the plugin contract beyond lowering-only behavior
- allow the plan compiler to ask a plugin for an executable compiled node
- execute plugin-owned nodes through the same runtime path as core nodes
- preserve the same guarantees around:
  - semantic records
  - spans
  - metrics
  - `.runs` persistence
  - replay/diff compatibility

This slice should not be `AutoResearch`-specific. It should be a general
runtime-extension capability.

#### 15.6: `AutoResearch` as an executable plugin primitive

Once runtime-executable plugins exist, `AutoResearch` should become an idiomatic
DSL primitive instead of a helper that lives beside the workflow runtime.

The intended authored shape is:

- optimization/search intent is explicit in the source
- docs and diagrams can show the search boundary directly
- agents can see that one node owns bounded candidate evaluation

Important architectural constraint:

- do not delete the core `optimization` substrate
- keep `optimization` as the reusable typed objective/evaluation layer
- implement `AutoResearch` on top of that substrate as a plugin-authored,
  runtime-executable primitive

This gives us:

- a strong plugin-story proof point
- a better DSL surface
- reuse of the typed evaluation logic already exercised by the objective layer

### Deliverables

- one new reference example under `src/mentalmodel/examples/agent_tool_use/`
- generated artifacts checked in for that example
- at least one non-trivial property check
- explicit use of one or more verification helpers
- a typed optimization layer under `src/mentalmodel/optimization/`
- an `autoresearch` integration/bundle layer under
  `src/mentalmodel/integrations/autoresearch/`
- a bounded sorting objective demo under
  `src/mentalmodel/examples/autoresearch_sorting/`
- runtime-executable plugin infrastructure in the plan/compiler/runtime path
- an executable `AutoResearch` primitive implemented as a plugin on top of the
  optimization substrate
- CLI demo paths for both:
  - `mentalmodel demo agent-tool-use`
  - `mentalmodel demo autoresearch-sorting`

### Acceptance criteria

- `agent_tool_use` is meaningfully different from the RL demo
- it exercises the same core primitives without special-casing
- docs and CLI flows work unchanged against it
- the optimization layer is typed, deterministic, and tested
- the sorting objective exposes a real metric signal and invariant
- plugins can own runtime execution, not just lowering
- `AutoResearch` executes inside the workflow runtime through the plugin system
- `autoresearch-sorting` is authored with the `AutoResearch` primitive, while
  the core `optimization` module remains the shared evaluation substrate
- `autoresearch-sorting` can both evaluate candidates locally and materialize an
  autoresearch-style `program.md` bundle
- no heavy demo-only dependencies are added to the core package install

## Milestone 16: Package The Agent Workflow More Tightly

### Goal

Make the agent experience feel like a first-class workflow instead of a loose
collection of templates and commands.

### Why

The skills are useful, but they still lean too much on human memory. The CLI and
skills should reinforce each other more directly.

### Deliverables

- expand the base skill beyond listing primitives
- explain what each primitive does, when to use it, and common patterns
- ensure every important inspection command has machine-optimized JSON output
- add one “debug recipe” document per command family
- evaluate `mentalmodel inspect` as a long-term namespace
- evaluate `mentalmodel doctor` for common setup and project issues

### Concrete expansions

Base skill should include:

- primitive responsibilities
- common composition patterns
- common anti-patterns
- expected verification loop
- `.runs` inspection flow

Debug recipe docs should cover:

- structure debugging
- invariant debugging
- runtime failure debugging
- run comparison workflow

`mentalmodel doctor` could eventually check:

- missing skill installation
- broken entrypoints
- absent `.runs` bundles
- tracing misconfiguration
- package-data / artifact mismatches

### Scope boundary

Do not overbuild an autonomous agent subsystem here. The target is a tighter
interface between:

- packaged skills
- CLI inspection commands
- project documentation

### Acceptance criteria

- skill templates are materially richer and less skeletal
- JSON outputs exist for all inspection/debugging commands
- at least one new command or doc explicitly improves agent debugging flow


## Needs to be scoped into milestones
- Docs (mintlify) pages. Comprehensive tutorial starting with hello world program and building from there introducing each core primitive
- Step mode - I should be able to run my program step by step and inspect node states/inputs/outputs without needing any extra manual breakpoints, debugger setup, etc
- Bespoke UI based on mentalmodel outputs + otel. Most important features: graph representation w/ drill down, output streaming during a run, invariant debugging

## Cross-Cutting Requirements

These apply to every Part 2 milestone.

### 1. Test requirements

Every milestone must include:

- direct unit tests for new helpers or loaders
- CLI tests for new commands where applicable
- backward-compatibility tests for persisted data when relevant
- failure-path tests for missing runs, missing nodes, malformed data, or bad config

### 2. Documentation requirements

Every milestone should update:

- [README.md](/Users/ben/repos/mentalmodel/README.md) when user-facing commands change
- skill templates when agent workflows change
- example docs when the async RL demo or future demos change materially

### 3. Artifact requirements

Whenever runtime artifacts change shape, update:

- run bundle writers
- run bundle readers
- compatibility logic
- tests for both new and older artifact shapes

### 4. JSON output requirement

All new inspection/debugging commands must support `--json`.

This is not optional. The package is explicitly for humans and AI coding agents.

## Immediate Implementation Recommendation

Start with:

1. Milestone 8: inspectable runs
2. Milestone 9: explicit resolved-input recording
3. Milestone 10: run schema versioning

That gives the package a much stronger debugging and artifact model before
moving into replay, richer verification, and broader observability integration.

## Definition Of Done For Part 2 Work

A Part 2 milestone is complete only when:

- the architecture is clean and durable
- the runtime and CLI behavior are well specified
- tests cover the important paths and regressions
- the docs and skills reflect the new workflow
- the feature is usable by both a human and an agent without hidden manual steps
