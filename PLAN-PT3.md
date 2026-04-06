# mentalmodel Plan Part 3

This document scopes the next major source-improvement round for `mentalmodel`.
It is driven by real port pressure from the Pangramanizer training-loop
migration, not by demo convenience.

Part 3 focuses on the next set of durable gaps:

- native multi-step workflow support
- reusable namespaced block composition
- runtime-environment and shared-resource ergonomics
- provider metric-map projection ergonomics
- parameterized verification entrypoints
- skills and agent-workflow improvements for real programs

The goal is not to patch Pangramanizer-specific pain locally. The goal is to
add durable, extensible, ergonomic primitives to `mentalmodel` so real programs
can stay legible without accumulating application-owned scaffolding.

This plan inherits the same standards from [PLAN.md](/Users/ben/repos/mentalmodel/PLAN.md)
and [PLAN-PT2.md](/Users/ben/repos/mentalmodel/PLAN-PT2.md):

- no shortcut implementations that weaken the intended architecture
- no `Any` or unnecessary type erasure
- every phase includes solid success-path and failure-path testing
- verification must stay green before moving on

## Source Inputs

This round is motivated by concrete feedback captured during the
Pangramanizer port in:

- [mentalmodel-ergonomics-feedback.md](/Users/ben/repos/pangramanizer/docs/mentalmodel-ergonomics-feedback.md)

The most important findings from that document are:

1. reusable subworkflow composition still needs too much manual prefix plumbing
2. runtime construction is entirely application-side
3. real programs need clearer block-level runtime defaults
4. shared semantic resources need a first-class injection pattern
5. provider-native metric maps need a first-class typed projection path
6. verification entrypoints need to support parameters for real workflows
7. native loop / multi-step support is currently missing

## Design Goals

### 1. Real-program legibility

Top-level program structure should read like the architecture of the system, not
like a wiring script.

### 2. Reusable composition without stringly plumbing

Blocks should be reusable without manually rewriting node ids and handler input
keys.

### 3. Temporal semantics must be first-class

If a workflow has iterations, loop-carried state, cadence gates, or per-step
causality, those semantics must be represented natively in the authored model,
runtime records, `.runs`, replay, and observability outputs.

### 4. Runtime binding should be explicit but ergonomic

Programs should remain authored against semantic primitives while still being
easy to bind to fixtures, real providers, and mixed environments.

### 5. Extensibility should remain central

The changes in this plan should strengthen the core/plugin/runtime boundaries,
not create Pangramanizer-only branches.

### 6. Observability should be sufficient as the primary run-tracking surface

If metrics, logs, traces, `.runs`, replay, and debugging surfaces are designed
correctly, a user should not feel compelled to rely on a separate experiment
tracker such as W&B just to understand or compare training runs.

This does not mean external tracking tools are forbidden. It means
`mentalmodel` should aim to make them optional rather than necessary.

## Settled API Directions

This section captures the intended public shapes before implementation.

## 1. Reusable Block Composition

### Problem

Today a reusable workflow fragment is awkward to instantiate repeatedly because:

- child node ids need manual prefixing
- internal refs need manual rebinding
- handlers and reducers end up aware of prefixed names
- the top-level program surface gets noisy quickly

### Direction

Add a first-class reusable block model:

- `Block`
- `Use`
- `BlockInput`
- `BlockOutput`
- `BlockDefaults`

### Public shape

```python
@dataclass(slots=True)
class Block(Generic[ChildT]):
    name: str
    inputs: Mapping[str, BlockInput[object]]
    outputs: Mapping[str, BlockOutput[object]]
    children: Sequence[NamedPrimitive]
    defaults: BlockDefaults | None = None
    description: str | None = None
```

```python
@dataclass(slots=True, frozen=True)
class BlockInput(Generic[InputT]):
    required: bool = True
```

```python
@dataclass(slots=True, frozen=True)
class BlockOutput(Generic[OutputT]):
    source_node_id: str
```

```python
@dataclass(slots=True)
class Use:
    name: str
    block: Block[NamedPrimitive]
    bind: Mapping[str, Ref | "LoopItemRef" | "LoopStateRef"]
    defaults: BlockDefaults | None = None
```

```python
@dataclass(slots=True, frozen=True)
class BlockDefaults:
    runtime_context: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)
    resources: Sequence["ResourceKey[object]"] = field(default_factory=tuple)
```

### Behavior

`Block`

- declares a reusable semantic fragment with a typed interface
- can be lowered many times
- remains prefix-agnostic internally

`Use`

- instantiates a block under a namespace
- rewrites internal node ids and refs automatically
- binds logical block inputs to outer refs or loop-local refs
- can apply local defaults without mutating the block definition

### Why this shape

This solves nested rollout/eval reuse directly, and it creates the right
composition substrate for `StepLoop` bodies.

### Non-goals for the first slice

- no general templating system
- no dynamic node creation outside explicit `Use`
- no implicit magic input matching beyond declared block inputs

## 2. Native Multi-Step Support

### Problem

`mentalmodel` currently executes one DAG to completion. It does not natively
represent:

- repeated iterations
- loop-carried state
- per-iteration causality
- multi-step replay and run inspection

This is now a real expressibility gap, not a hypothetical future enhancement.

### Direction

Add a sequential loop primitive specialized for real workflows:

- `StepLoop`
- `LoopCarry`
- `LoopItemRef`
- `LoopStateRef`
- `LoopSummary`

Do not start with a generic Python-like `while`.

### Public shape

```python
@dataclass(slots=True)
class StepLoop:
    name: str
    body: Use
    for_each: Ref
    carry: LoopCarry[object] | None = None
    summary: LoopSummary | None = None
    max_iterations: int | None = None
    description: str | None = None
```

```python
@dataclass(slots=True, frozen=True)
class LoopCarry(Generic[StateT]):
    state_name: str
    initial: StateT
    next_state_output: str
```

```python
@dataclass(slots=True, frozen=True)
class LoopSummary:
    final_outputs: Sequence[str] = field(default_factory=tuple)
    history_outputs: Sequence[str] = field(default_factory=tuple)
```

```python
@dataclass(slots=True, frozen=True)
class LoopItemRef:
    logical_name: str = "item"

@dataclass(slots=True, frozen=True)
class LoopStateRef:
    logical_name: str
```

### Example

```python
training_step = Block(
    "training_step",
    inputs={
        "batch": BlockInput[PromptBatch](),
        "trainer_state": BlockInput[TrainerState](),
    },
    outputs={
        "next_state": BlockOutput[TrainerState]("policy_commit"),
        "tracking": BlockOutput[StepMetricProjection]("tracking_projection"),
    },
    children=(...),
)

program = Workflow(
    "trainer",
    children=(
        StepLoop(
            "steps",
            for_each=Ref("prompt_batches"),
            body=Use(
                "step",
                block=training_step,
                bind={
                    "batch": LoopItemRef(),
                    "trainer_state": LoopStateRef("trainer_state"),
                },
            ),
            carry=LoopCarry(
                state_name="trainer_state",
                initial=initial_trainer_state,
                next_state_output="next_state",
            ),
            summary=LoopSummary(
                final_outputs=("tracking",),
                history_outputs=("tracking",),
            ),
        ),
    ),
)
```

### Runtime semantics

The first slice is intentionally narrow:

- sequential iteration only
- deterministic iteration order
- one loop input item per iteration
- no `break` / `continue`
- no parallel loop-body execution

### Required runtime substrate changes

Loop support is not just one primitive. It requires frame-aware runtime state.

Add:

- execution-frame identity
- iteration index in records and spans
- frame-aware `.runs` inspection
- replay support scoped by iteration

Execution records should gain stable frame metadata, for example:

- `frame_path`
- `iteration_index`
- `loop_node_id`

This must flow into:

- semantic records
- `.runs` input/output inspection
- replay
- diffing
- OTEL span attributes

### Why `StepLoop` instead of a generic `Loop`

Because real orchestration loops care about:

- a stream of step inputs
- explicit loop-carried state
- per-step summaries
- deterministic replay/debugging

That maps much better to training loops, agent episodes, ETL batches, and
document-processing pipelines than a generic `while`.

### Non-goals for the first slice

- nested parallel loops
- arbitrary mutation of loop state outside `LoopCarry`
- implicit access to prior iteration outputs without explicit summary/carry

## 3. Runtime Environment and Shared Resources

### Problem

Today real runtime assembly is entirely application-side. That creates repeated
local patterns for:

- fixture vs real mode selection
- resource bundles
- provider adapters
- shared domain helpers

### Direction

Introduce a first-class runtime environment model:

- `RuntimeEnvironment`
- `RuntimeProfile`
- `ResourceKey`

This should provide a clean line between:

- authored workflow structure
- runtime profiles and resources

### Public shape

```python
@dataclass(slots=True, frozen=True)
class ResourceKey(Generic[ResourceT]):
    name: str
    type_: type[ResourceT]
```

```python
@dataclass(slots=True, frozen=True)
class RuntimeProfile:
    name: str
    resources: Mapping[ResourceKey[object], object]
    metadata: Mapping[str, str] = field(default_factory=dict)
```

```python
@dataclass(slots=True, frozen=True)
class RuntimeEnvironment:
    profiles: Mapping[str, RuntimeProfile]
```

### Consumption model

Primitives should be able to declare needed resources explicitly:

```python
reward_model = ResourceKey("reward_model", CompositeRewardModel)

Effect(
    "reward_pangram",
    handler=PangramRewardHandler(),
    inputs=[Ref("reward_defense")],
    resources=(reward_model,),
)
```

Handlers should then resolve resources through a typed context surface:

```python
model = ctx.resources.require(reward_model)
```

### Why this shape

- resources remain explicit and typed
- runtime binding stays outside authored primitives
- shared semantic helpers become a first-class concept instead of bespoke app
  scaffolding

### Interaction with `BlockDefaults`

`BlockDefaults.resources` and `BlockDefaults.runtime_context` should let a block
declare what profile/resources are expected by default, without repeating the
same runtime-scoping wrapper around every child.

### Non-goals for the first slice

- dependency-injection container magic
- lifecycle-managed resources for every possible backend
- automatic constructor injection into handlers

## 4. Block-Level Runtime Defaults and Boundary Policies

### Problem

Large workflows repeat runtime-context wrappers and boundary metadata too often.

### Direction

Use `BlockDefaults` as the primary authored surface for block-level runtime
policy, instead of proliferating ad hoc `RuntimeContext` wrappers.

### Design rule

- `RuntimeContext` remains useful for standalone grouping
- `BlockDefaults.runtime_context` becomes the preferred pattern for reusable
  blocks

This keeps top-level workflows shorter and makes block reuse less noisy.

### Future extension point

This design should leave room for:

- service-budget defaults
- capability policies
- network / sandbox policies

without requiring a new wrapper primitive for each one.

## 5. Provider Metric-Map Projection Helpers

### Problem

Real providers often return one rich `dict[str, float]`. The package currently
supports output-derived metrics, but the ergonomic path for “keep the full map
and safely project summary plus exported metrics” is still too ad hoc.

### Direction

Add a first-class metric-map projection helper layer:

- `MetricMapProjection`
- `MetricFieldProjection`
- `project_metric_map(...)`
- integration with `OutputMetricSpec`

### Public shape

```python
@dataclass(slots=True, frozen=True)
class MetricFieldProjection:
    source_key: str
    metric_name: str
    unit: str | None = None
```

```python
@dataclass(slots=True, frozen=True)
class MetricMapProjection:
    raw_prefix: str | None = None
    fields: Sequence[MetricFieldProjection] = field(default_factory=tuple)
    include_raw: bool = False
    allow_keys: Sequence[str] = field(default_factory=tuple)
```

```python
def project_metric_map(
    *,
    metric_map: Mapping[str, float],
    projection: MetricMapProjection,
    context: MetricContext,
) -> Sequence[MetricObservation]: ...
```

### Behavior

- preserve the full provider-native metric map in the typed output model
- expose a stable exported subset intentionally
- optionally export the raw map under a controlled prefix when the cardinality is
  acceptable

### Why this shape

It keeps parity/debugging surfaces rich without forcing every effect author to
hand-roll the same extractor logic.

## 6. Parameterized Verification Invocation

### Problem

`mentalmodel verify` currently assumes zero-argument entrypoints. That works for
demos, but real workflows need parameters such as:

- config path
- runtime mode
- environment file
- graph suffix / run label

### Direction

Extend CLI invocation to support explicit parameter passing without inventing
stringly one-off patterns.

### Public shape

CLI:

```bash
mentalmodel verify \
  --entrypoint pkg.module:build_program \
  --params-file verification/smoke.json
```

or:

```bash
mentalmodel verify \
  --entrypoint pkg.module:build_program \
  --params-json '{"config_path":"configs/e2e_smoke.json","mode":"real_smoke"}'
```

Loader behavior:

- if the entrypoint resolves to a zero-arg callable, current behavior remains
- if parameters are provided, they are passed as keyword arguments
- invalid parameter shapes fail fast with a clear CLI error

### Optional follow-on

If needed later, introduce a dedicated `ProgramFactory` protocol. Do not require
that in the first slice.

## 7. Skills and Agent-Workflow Improvements

### Problem

The current skills are strong on demos and debugging, but they do not yet teach
the patterns needed for real, multi-file, provider-backed programs.

### Direction

Update the installed skills to cover:

- `Block` and `Use` authoring
- `StepLoop` / multi-step workflow authoring
- resource and runtime-profile patterns
- provider protocol design guidance
- metric-map preservation and projection guidance
- parameterized verification invocation

### Specific improvements

`mentalmodel-base`

- prefer blocks over giant inline workflows
- prefer `Use` over manual prefix helpers
- use `StepLoop` for repeated step semantics, not app-side orchestration

`mentalmodel-plugin-authoring`

- clarify when a capability belongs in core vs plugin surface
- document plugin expectations for frame-aware runtime semantics

`mentalmodel-invariants-testing`

- add guidance for per-iteration invariants and loop-summary checks

`mentalmodel-debugging`

- add iteration-aware `.runs` inspection flow
- teach frame-scoped replay and diffing
- document parameterized `verify`

## Recommended Implementation Order

The changes above should not land as one giant slice. The right order is:

1. documentation product and tutorial track
2. reusable blocks and namespaced composition
3. loop/frame runtime substrate
4. `StepLoop`
5. runtime environment and typed resource injection
6. block-level runtime defaults on top of the new composition layer
7. metric-map projection helpers
8. parameterized verification invocation
9. skills/docs/recipes refresh

This order matters because:

- docs should evolve with the package shape instead of trailing it
- `StepLoop` should build on reusable `Block`/`Use`
- runtime resources should be shaped after loop/frame execution is clear
- verification and docs should reflect the actual new runtime surface

## Phased Implementation Plan

## Phase 16.5: Documentation Product and Tutorial Track

### Goal

Turn the package docs into a real learning surface for both humans and agents,
starting from a minimal hello-world workflow and building upward through the
current core primitives and debugging model.

### Why

The package has outgrown README fragments. Documentation needs to become a
first-class product surface before the next wave of primitives lands, so new
API work has a place to plug into immediately.

### Deliverables

- a Mintlify docs site or equivalent docs product under version control
- a tutorial ladder that starts from:
  - hello world
  - inputs/refs
  - effects and actors
  - joins and invariants
  - current debugging and `.runs` flows
  - plugins and runtime contexts
- one focused page per currently shipped primitive
- one page for plugin authoring
- one page for observability, `.runs`, replay, and OTEL
- a clear “current package model” page that future milestones can extend

### Likely file ownership

- `docs-site/` or equivalent Mintlify directory
- `README.md`
- `docs/recipes/*`
- skill templates under `src/mentalmodel/skills/templates`

### Acceptance criteria

- a new user can follow the tutorial track from hello world to a structured
  workflow without reading source code first
- the docs accurately reflect the current package surface before Phase 17 work
  begins
- all examples in the docs are runnable and verified
- the docs establish a stable location and pattern for subsequent milestone
  updates
- major CLI and tutorial pages include realistic output examples plus guidance
  on how to interpret that output and what command to run next

## Phase 17: Reusable Blocks and Namespaced Instantiation

### Goal

Make reusable workflow fragments first-class and remove manual prefix plumbing.

### Deliverables

- `Block`
- `Use`
- `BlockInput`
- `BlockOutput`
- `BlockDefaults`
- IR lowering for namespaced block instantiation
- doc rendering that surfaces block instances and logical inputs/outputs

### Likely file ownership

- `src/mentalmodel/core/block.py`
- `src/mentalmodel/core/use.py`
- `src/mentalmodel/core/__init__.py`
- `src/mentalmodel/ir/lowering.py`
- `src/mentalmodel/docs/*`
- `src/mentalmodel/analysis/*`

### Acceptance criteria

- a reused block can be instantiated twice without manual id rewriting
- docs and Mermaid remain legible
- the async RL or agent-tool-use examples can be partially refactored to use
  `Block`/`Use`
- tests cover input rebinding, duplicate instantiation, and invalid bindings
- official docs and tutorials are updated to explain `Block`, `Use`, block
  inputs/outputs, and reuse patterns

## Phase 18: Frame-Aware Runtime Substrate

### Goal

Prepare runtime, `.runs`, replay, and tracing for iterative execution.

### Deliverables

- execution-frame identity model
- frame-aware record schema
- frame-aware `.runs` input/output addressing
- replay filters by frame / iteration
- span attributes for loop/iteration metadata

### Likely file ownership

- `src/mentalmodel/ir/records.py`
- `src/mentalmodel/runtime/context.py`
- `src/mentalmodel/runtime/events.py`
- `src/mentalmodel/runtime/runs.py`
- `src/mentalmodel/runtime/replay.py`
- `src/mentalmodel/observability/tracing.py`
- `src/mentalmodel/cli.py`

### Acceptance criteria

- runtime can distinguish repeated execution of the same logical node across
  iterations
- `runs inputs`, `runs outputs`, and `runs trace` can scope by iteration/frame
- replay remains deterministic and testable
- official docs are updated for frame-aware inspection and replay behavior

## Phase 19: Native `StepLoop`

### Goal

Add native sequential multi-step execution.

### Deliverables

- `StepLoop`
- `LoopCarry`
- `LoopItemRef`
- `LoopStateRef`
- `LoopSummary`
- lowering/compilation/runtime execution support
- one serious loop-based reference example

### Likely file ownership

- `src/mentalmodel/core/loop.py`
- `src/mentalmodel/core/__init__.py`
- `src/mentalmodel/ir/lowering.py`
- `src/mentalmodel/runtime/plan.py`
- `src/mentalmodel/runtime/executor.py`
- `src/mentalmodel/docs/*`
- `src/mentalmodel/examples/*`

### Acceptance criteria

- a multi-step example runs with explicit carried state
- `.runs` and replay show iteration-scoped execution clearly
- loop summary outputs are inspectable
- failure paths include iteration context
- official docs and tutorials are updated for `StepLoop`, loop carry, and
  iteration debugging

## Phase 20: Runtime Environment and Shared Resources

### Goal

Make real runtime binding a first-class, typed package concept.

### Deliverables

- `ResourceKey`
- `RuntimeProfile`
- `RuntimeEnvironment`
- context resource access helpers
- authored primitive support for declared resource requirements

### Likely file ownership

- `src/mentalmodel/runtime/environment.py`
- `src/mentalmodel/core/interfaces.py`
- `src/mentalmodel/runtime/context.py`
- `src/mentalmodel/cli.py`
- `src/mentalmodel/testing/harness.py`

### Acceptance criteria

- handlers can request declared resources through typed keys
- programs can run against at least two profiles, for example fixture vs real
- docs and debugging outputs surface active runtime profile metadata
- official docs are updated for runtime environments, profiles, and typed
  resources

## Phase 21: Metric-Map Projection Helpers

### Goal

Keep provider-native metric maps rich while making exported metrics ergonomic and
safe.

### Deliverables

- `MetricMapProjection`
- `MetricFieldProjection`
- helper functions integrated with `OutputMetricSpec`
- documentation and example updates

### Likely file ownership

- `src/mentalmodel/observability/metrics.py`
- `src/mentalmodel/core/__init__.py`
- `src/mentalmodel/examples/*`
- `tests/test_metrics.py`

### Acceptance criteria

- full metric maps remain available in typed outputs
- stable projected metrics can be emitted with a few lines of code
- tests cover raw-map preservation, stable-field projection, and cardinality
  guardrails
- official docs are updated for metric-map projection and the broader
  observability story

## Phase 22: Parameterized Verification Invocation

### Goal

Make `mentalmodel verify` usable for real workflows without wrapper-entrypoint
boilerplate.

### Deliverables

- `--params-json`
- `--params-file`
- invocation validation
- iteration-aware verification docs where relevant

### Likely file ownership

- `src/mentalmodel/cli.py`
- `src/mentalmodel/testing/harness.py`
- `tests/test_cli.py`
- skill templates and docs

### Acceptance criteria

- a real workflow can be verified with CLI-provided parameters
- invalid parameter payloads fail fast and clearly
- zero-argument demo entrypoints still work unchanged
- official docs are updated for parameterized verification flows

## Phase 23: Skills, Recipes, and Reference Examples

### Goal

Make the new surfaces teachable to both humans and coding agents.

### Deliverables

- updated skill templates
- new recipes for:
  - block reuse
  - loop debugging
  - runtime-profile selection
  - resource injection
  - parameterized verification
- at least one refactored example that exercises `Block` + `StepLoop`

### Acceptance criteria

- installed skills reflect the new primitives and runtime model
- docs explain both authored shape and debugging flow
- there is a serious non-demo reference path that proves the ergonomics claims

## Expected Pangramanizer Impact

If this plan lands well, the Pangramanizer port should be able to replace:

- manual rollout/eval prefix plumbing with `Block` + `Use`
- one-step orchestration with `StepLoop`
- bespoke runtime bundles with `RuntimeEnvironment`
- ad hoc shared-helper threading with typed `ResourceKey`s
- custom optimizer-metric extractors with `MetricMapProjection`
- dedicated zero-arg smoke entrypoint modules with parameterized `verify`

That is the standard for success: the package changes should delete real local
scaffolding from Pangramanizer, not just add more features to `mentalmodel`.

## Decision Log

### Core decisions

- Native multi-step support will be implemented as a core primitive, not as
  app-side orchestration.
- The first multi-step primitive will be `StepLoop`, not a generic `while`.
- Reusable composition will be solved with `Block` + `Use`, not manual
  namespace helpers.
- Shared runtime resources will use typed keys and explicit runtime
  environments, not implicit dependency injection.
- Verification will gain parameter passing before adding any bigger factory
  abstraction.
- Skills and docs are part of the implementation, not follow-up polish.

### Anti-goals

- no shortcut “just run the workflow N times” parity harness and call it native
  loop support
- no stringly block reuse API
- no hidden global resource registry
- no magical verification parameter conventions without typed validation
- no implementation that improves Pangramanizer at the cost of weakening
  generality

## Signals from the Generated Docs Pass

The initial Mintlify-generated docs assumed a package that was somewhat more
capable than the current implementation. Much of that overreach was generic doc
generator noise, but some of it is useful signal for the roadmap.

These signals should influence future package shape:

### 1. The package wants a stronger multi-step story

The generated docs naturally reached for a fuller workflow model than the
current one-step DAG runtime provides. That reinforces native multi-step support
as a core gap, not just an application-level annoyance.

### 2. Reusable composition wants to be more first-class

The generated documentation read more naturally when describing reusable
sub-workflows than the current package supports ergonomically. That reinforces
`Block` + `Use` as the right direction.

### 3. Interactive debugging is a real product need

The docs tended to assume a more interactive inspection/debugging experience
than the package currently has. That is useful signal in favor of the planned
step-mode milestone rather than a reason to dismiss the gap.

### 4. Richer graph and machine-readable inspection will matter

Some generated descriptions assumed broader inspection surfaces than the current
CLI offers. We should not add features blindly because a doc generator guessed
them, but it is useful signal that:

- richer IR inspection
- richer graph inspection
- clearer structured CLI outputs

will likely be high leverage once blocks and loops land.

### 5. Static analysis breadth is a future growth area

Some generated claims about checks were wrong today, but the categories they
reached for were reasonable. Once the core model grows to include blocks and
loops, the analyzer surface should expand too.

### Non-signal examples

The following generated assumptions are not strong product signals by
themselves:

- vague “SDK” framing
- unsupported claims with no clear runtime or IR backing
- broad promises that sound good but do not map cleanly to a package surface

### Practical conclusion

When generated docs repeatedly assume a capability that fits the package’s
direction and real user pain, we should treat that as useful product signal.
When they assume a capability that does not map to a clear runtime/IR/design
surface, we should treat it as generator noise.

## Follow-On Product Milestones

These milestones depend on the core/runtime improvements above. They are added
here now so they are tracked explicitly rather than remaining informal future
ideas.

## Phase 25: Step Mode and Interactive Execution Inspection

### Goal

Provide a first-class paused execution mode so users can run a program
incrementally and inspect inputs, outputs, state, and invariant behavior
without external debuggers or ad hoc breakpoints.

### Why

Real workflows become much easier to understand when the runtime can stop after:

- one node
- one ready-set decision
- one iteration
- one invariant failure

This should be a native package capability, not a debugger workaround.

### Deliverables

- a step-capable execution mode in the runtime
- a CLI surface such as:
  - `mentalmodel step --entrypoint ...`
  - or `mentalmodel run --step`
- step controls:
  - next node
  - next ready-set resolution
  - next iteration
  - continue to failure / completion
- inspection surfaces for:
  - current ready nodes
  - resolved inputs
  - current state store
  - latest output
  - loop/frame position
- step-mode artifact persistence compatible with `.runs`

### Design constraints

- step mode must execute the same compiled/runtime core as normal runs
- it must not rely on monkey-patching handlers or external debugger hooks
- it must remain compatible with traces, metrics, semantic records, and replay

### Likely file ownership

- `src/mentalmodel/runtime/executor.py`
- `src/mentalmodel/runtime/stepper.py`
- `src/mentalmodel/runtime/context.py`
- `src/mentalmodel/cli.py`
- `src/mentalmodel/runtime/runs.py`
- `tests/test_executor.py`
- `tests/test_cli.py`

### Acceptance criteria

- a user can pause and advance a workflow node-by-node from the CLI
- state/inputs/outputs are inspectable at each pause point
- step mode works for both single-step workflows and `StepLoop` workflows
- invariant failures can be inspected in-place without re-running from scratch

## Phase 26: Bespoke UI for Graphs, Runs, and Invariants

### Goal

Build a dedicated UI on top of `mentalmodel` outputs and OTEL data that makes
real workflows explorable without dropping to raw JSON, Markdown, or external
trace tools.

### Why

The CLI and `.runs` surface should remain first-class, but they are not enough
for the best human debugging experience once workflows become large, iterative,
and highly observable.

### Initial feature priorities

1. graph representation with drill-down
2. live output / event streaming during a run
3. invariant debugging

### Deliverables

- a UI app that reads the same shared run/replay services as the CLI
- graph explorer:
  - topology view
  - block instance drill-down
  - loop/iteration drill-down
  - runtime-context overlays
- live run view:
  - semantic event stream
  - output updates
  - current ready/running nodes
  - trace correlation
- invariant debugging view:
  - failing invariant inputs
  - related upstream outputs
  - nearby records/spans
  - iteration/frame position when applicable
- run comparison and replay views if the scope remains manageable

### Architecture constraints

- the UI must consume the same core services as the CLI wherever possible
- do not build UI-only data models that diverge from `.runs` / replay / runtime
  records
- the UI should remain usable against local `.runs` even without an external
  OTEL backend

### Likely file ownership

- a new UI package or app directory in the repo
- shared service layer extracted as needed from:
  - `src/mentalmodel/runtime/runs.py`
  - `src/mentalmodel/runtime/replay.py`
  - `src/mentalmodel/analysis/*`
- UI-focused docs and recipes

### Acceptance criteria

- users can inspect a run graphically without opening raw artifact files
- invariant failures are easier to diagnose in the UI than via raw CLI output
- the UI works on top of existing `.runs` data and improves when OTEL is
  configured
- at least one serious workflow can be demonstrated end-to-end in the UI
