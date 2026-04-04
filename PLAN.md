# mentalmodel Plan

## Goal

`mentalmodel` is a package for authoring programs in a way that preserves a
strong mental model for both humans and AI coding agents.

The package should make the following mechanically recoverable:

- Static topology: which semantic nodes exist and how they depend on each other
- Dynamic execution: what actually ran, in what order, with what causality
- State transitions: what changed, where, and under which input
- Side effects: what impure operations happened and from which boundary
- Invariants: what properties were expected and whether they held
- Runtime boundaries: where code is allowed to run and with what capabilities

This plan assumes a layered design:

1. Semantic core
2. Runtime and observability substrate
3. Tooling and CLI
4. Extension/plugin API
5. Agent-facing conventions and skill installation

## Engineering Standard

Milestones are not considered complete if they rely on shortcuts that weaken the
intended architecture.

Examples of disallowed milestone shortcuts:

- `Any` where a real generic or protocol contract should exist
- Erasing types at public boundaries to speed implementation
- Skipping the compiled/runtime abstraction layer and executing directly from
  authoring objects when a stable intermediate boundary is expected
- Marking a feature complete while key docs, diagrams, analysis metadata, or
  verification hooks are still bypassed by the implementation

The package should favor durable abstractions over fast local wins. If a
milestone reveals that an abstraction boundary is missing, the milestone should
expand to include that boundary rather than shipping a shortcut.

Milestones are also not considered complete unless they include solid,
targeted verification for the behavior they introduce.

Minimum acceptance standard for each milestone:

- add automated tests for success paths and meaningful failure paths
- verify structural contracts, runtime semantics, and extension boundaries where
  relevant
- run the project verification toolchain before moving to the next milestone
- do not defer missing tests or validation gaps to later milestones

Implementation is not complete until the code, tests, and verification path all
agree on the intended behavior.

## Repo Skeleton

```text
mentalmodel/
  README.md
  PLAN.md
  pyproject.toml
  src/
    mentalmodel/
      __init__.py
      cli.py
      version.py
      errors.py
      py.typed
      core/
        __init__.py
        interfaces.py
        models.py
        refs.py
        workflow.py
        actor.py
        effect.py
        invariants.py
        composition.py
      ir/
        __init__.py
        graph.py
        lowering.py
        records.py
        schemas.py
      runtime/
        __init__.py
        plan.py
        executor.py
        context.py
        events.py
        recorder.py
        replay.py
      observability/
        __init__.py
        tracing.py
        metrics.py
        export.py
      analysis/
        __init__.py
        graph_checks.py
        semantic_checks.py
        findings.py
      docs/
        __init__.py
        mermaid.py
        markdown.py
        inventory.py
      plugins/
        __init__.py
        base.py
        registry.py
        runtime_context.py
      testing/
        __init__.py
        harness.py
        invariants.py
      skills/
        __init__.py
        installer.py
        templates/
          codex_skill.md
          claude_skill.md
      examples/
        __init__.py
        async_rl/
          __init__.py
          demo.py
          graph.md
          expected_mermaid.txt
  tests/
    test_lowering.py
    test_executor.py
    test_analysis.py
    test_cli.py
    test_async_rl_demo.py
```

## Package Layers

## 1. Semantic Core

The semantic core defines the authoring model. Users and coding agents should
author against these interfaces instead of building ad hoc orchestrators.

Core responsibilities:

- Declare program structure
- Model ownership and dependencies
- Define state and effect boundaries
- Attach invariants
- Lower all authoring constructs into a canonical IR

Planned base primitives:

- `Workflow`
- `Actor`
- `Effect`
- `Invariant`
- `Parallel`
- `Join`
- `Ref`

Important rule:

Children in the authoring syntax mean containment or local sequencing.
Non-tree dependencies must be explicit via `Ref`.

## 2. IR

The IR is the center of the system. Every primitive, including extensions,
must lower into the same graph representation.

IR responsibilities:

- Canonical graph model for nodes, edges, ports, and metadata
- Stable schema for docs, diagrams, runtime scheduling, and analysis
- Machine-readable semantic metadata
- Versioned execution record schema

Nothing should bypass the IR:

- Mermaid diagrams are rendered from IR
- Markdown docs are rendered from IR
- Static analysis targets IR
- Runtime execution plans are compiled from IR
- Agent skills should reason over IR projections where possible

## 3. Runtime

The runtime executes compiled execution plans derived from IR graphs and records
semantic execution events.

Runtime responsibilities:

- Compile IR into typed execution-plan nodes before execution
- Async execution
- Dependency-aware scheduling
- Context propagation
- Causality recording
- State transition logging
- Effect invocation wrapping
- OTel export
- Replay support

The runtime should remain deterministic where practical:

- Stable node ids
- Stable edge ids
- Stable compiled execution-plan metadata
- Deterministic traversal order when multiple nodes become ready
- Optional deterministic clock and id providers for tests

## 4. Tooling and CLI

The CLI should make the package useful immediately after install.

Primary commands:

- `mentalmodel init`
- `mentalmodel check`
- `mentalmodel graph`
- `mentalmodel docs`
- `mentalmodel demo async-rl`
- `mentalmodel install-skills`

Secondary commands:

- `mentalmodel inspect ir`
- `mentalmodel inspect records`
- `mentalmodel replay`
- `mentalmodel verify`

## 5. Extension API

New primitives should be implemented as plugins that compile into the core IR.
The extension system is a first-class design requirement, not an afterthought.

Examples of extension primitives:

- `RuntimeContext`
- `CheckpointPolicy`
- `RetryPolicy`
- `HumanApproval`
- `SandboxBoundary`
- `LLMCall`
- RL-specific nodes such as `RolloutCollector` or `LearnerStep`

## 6. Agent Support

The package should ship with conventions and installable coding-agent skills.

The skills should teach agents to:

- Prefer semantic primitives over ad hoc orchestration
- Route all side effects through `Effect`
- Attach invariants to joins and state transitions
- Keep program structure explicit via `Ref`
- Run `mentalmodel check` and `mentalmodel docs` after meaningful changes
- Avoid bypassing runtime boundaries without an explicit primitive

## Core Interfaces

The interfaces below are design targets, not final code.

### Program Authoring Protocols

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, Mapping, Protocol, Sequence, TypeVar

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
StateT = TypeVar("StateT")


class LowersToIR(Protocol):
    def lower(self, ctx: "LoweringContext") -> "IRFragment":
        ...


class RuntimeExecutable(Protocol):
    async def run(self, ctx: "ExecutionContext") -> "ExecutionValue":
        ...
```

### Base Semantic Nodes

```python
@dataclass(slots=True)
class Ref:
    target: str
    port: str | None = None


@dataclass(slots=True)
class Workflow(LowersToIR):
    name: str
    children: Sequence[LowersToIR]
    description: str | None = None


@dataclass(slots=True)
class Actor(Generic[InputT, OutputT, StateT], LowersToIR):
    name: str
    handler: "ActorHandler[InputT, OutputT, StateT]"
    inputs: Sequence[Ref] = field(default_factory=tuple)
    state: "StateStore[StateT] | None" = None
    invariants: Sequence["Invariant"] = field(default_factory=tuple)
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class Effect(Generic[InputT, OutputT], LowersToIR):
    name: str
    handler: "EffectHandler[InputT, OutputT]"
    inputs: Sequence[Ref] = field(default_factory=tuple)
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class Invariant(LowersToIR):
    name: str
    inputs: Sequence[Ref]
    checker: "InvariantChecker"
    severity: str = "error"


@dataclass(slots=True)
class Parallel(LowersToIR):
    name: str
    children: Sequence[LowersToIR]


@dataclass(slots=True)
class Join(LowersToIR):
    name: str
    inputs: Sequence[Ref]
    reducer: "JoinReducer | None" = None
```

### Handlers and Contracts

```python
class ActorHandler(Protocol[InputT, OutputT, StateT]):
    async def handle(
        self,
        data: InputT,
        state: StateT | None,
        ctx: "ExecutionContext",
    ) -> "ActorResult[OutputT, StateT]":
        ...


class EffectHandler(Protocol[InputT, OutputT]):
    async def invoke(
        self,
        data: InputT,
        ctx: "ExecutionContext",
    ) -> OutputT:
        ...


class InvariantChecker(Protocol):
    async def check(
        self,
        resolved_inputs: Mapping[str, Any],
        ctx: "ExecutionContext",
    ) -> "InvariantResult":
        ...
```

### Result Models

```python
@dataclass(slots=True)
class ActorResult(Generic[OutputT, StateT]):
    output: OutputT
    next_state: StateT | None = None
    observations: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InvariantResult:
    passed: bool
    details: Mapping[str, Any] = field(default_factory=dict)
```

## Canonical IR

The IR must be stable and expressive enough to support new primitives.

```python
@dataclass(slots=True, frozen=True)
class IRNode:
    node_id: str
    kind: str
    label: str
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ("default",)
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class IREdge:
    edge_id: str
    source_node_id: str
    source_port: str
    target_node_id: str
    target_port: str
    kind: str = "data"


@dataclass(slots=True, frozen=True)
class IRGraph:
    graph_id: str
    nodes: tuple[IRNode, ...]
    edges: tuple[IREdge, ...]
    metadata: Mapping[str, str] = field(default_factory=dict)
```

The minimum node metadata expected by downstream tooling:

- `kind`
- `semantic_type`
- `runtime_context`
- `stateful`
- `effectful`
- `strict`
- `doc_group`
- `plugin_origin`

## Runtime and Records

The runtime should record semantic execution, not only generic spans.

### Execution Context

```python
@dataclass(slots=True)
class ExecutionContext:
    run_id: str
    trace_id: str
    parent_span_id: str | None
    graph: IRGraph
    recorder: "ExecutionRecorder"
    clock: "Clock"
    otel: "TracingAdapter"
    capabilities: Mapping[str, Any] = field(default_factory=dict)
```

### Execution Records

```python
@dataclass(slots=True, frozen=True)
class ExecutionRecord:
    record_id: str
    run_id: str
    node_id: str
    event_type: str
    timestamp_ms: int
    payload: Mapping[str, Any]
```

Expected event types:

- `node.started`
- `node.succeeded`
- `node.failed`
- `state.read`
- `state.transition`
- `effect.invoked`
- `effect.completed`
- `invariant.checked`
- `join.resolved`

## Opinionated OTel

OTel should be wired into the runtime by default.

Rules:

- Every node execution creates a span
- Span attributes must include semantic node metadata
- Fanout and join relationships should use links where parent-child nesting is
  not semantically correct
- State transitions and effect invocations should also emit structured events
  into the package's own recorder

Recommended span attribute set:

- `mentalmodel.node.id`
- `mentalmodel.node.kind`
- `mentalmodel.node.label`
- `mentalmodel.workflow.id`
- `mentalmodel.runtime.context`
- `mentalmodel.stateful`
- `mentalmodel.effectful`

## Invariants and Hypothesis

`Invariant` should be a base semantic primitive with a simple runtime contract.
Hypothesis should power optional stronger verification paths.

Two intended modes:

1. Runtime invariant
   Checked against actual execution values inside a run

2. Property invariant
   Checked in tests against generated inputs and, where applicable, state-machine
   transitions

Planned verification helpers:

- `RuntimeInvariant`
- `PropertyInvariant`
- `StateMachineInvariant`

`mentalmodel verify` should run both plain runtime checks and any registered
Hypothesis-powered checks.

## Static Analysis

Static analysis should operate on IR projections plus optional source metadata.

### Structural checks

- duplicate node ids
- unresolved refs
- orphaned nodes
- illegal cycles
- missing join after parallel fanout where strict mode requires a join
- undeclared effect usage

### Semantic checks

- actor performs effects without an explicit `Effect` wrapper
- stateful actor has no invariant
- runtime-context crossing without explicit bridge primitive
- workflow has effectful edges but no docs projection
- plugin lowered nodes with missing metadata

### Output format

Static analysis should emit machine-readable findings:

```python
@dataclass(slots=True, frozen=True)
class Finding:
    code: str
    severity: str
    message: str
    node_id: str | None = None
    path: str | None = None
```

## Plugin and Extension API

The extension system should make new primitives easy to add without changing the
core runtime model.

### Plugin contract

```python
class PrimitivePlugin(Protocol):
    kind: str

    def lower(self, spec: Any, ctx: "LoweringContext") -> "IRFragment":
        ...

    def analyze(self, spec: Any, ctx: "AnalysisContext") -> list[Finding]:
        ...

    def docs(self, spec: Any) -> "DocFragment | None":
        ...
```

### Registry responsibilities

- register core and third-party primitives
- resolve lowering handlers
- resolve docs/diagram projections
- attach plugin provenance metadata

### Example extension: `RuntimeContext`

This should ship as a reference plugin, not as a special-case part of the
runtime.

Conceptual usage:

```python
RuntimeContext(
    "remote_sampler",
    runtime="sandbox",
    children=[
        Effect("sample_remote", handler=RemoteSampler(...)),
    ],
)
```

Expected lowering behavior:

- annotate descendant nodes with `runtime_context=sandbox`
- enforce context-crossing checks
- group nodes in generated docs/diagrams
- expose scheduling hints to the runtime

## CLI Plan

The CLI is a required part of the product.

### `mentalmodel init`

Purpose:

- bootstrap a new project using the package

Behavior:

- create a `mentalmodel.toml`
- create `docs/mentalmodel/`
- optionally create `src/<project>/workflows.py`
- optionally add example `examples/async_rl_demo.py`

### `mentalmodel check`

Purpose:

- run structural and semantic analysis

Behavior:

- load project entrypoint
- lower authoring model into IR
- run analyzers
- print findings
- optionally emit JSON

Example:

```bash
mentalmodel check --entrypoint examples.async_rl.demo:build_program
```

### `mentalmodel graph`

Purpose:

- render topology artifacts from IR

Behavior:

- output Mermaid
- optionally output JSON IR
- optionally output Graphviz later

Examples:

```bash
mentalmodel graph --entrypoint examples.async_rl.demo:build_program --format mermaid
mentalmodel graph --entrypoint examples.async_rl.demo:build_program --format json
```

### `mentalmodel docs`

Purpose:

- generate markdown docs from IR

Behavior:

- write workflow inventory
- write node inventory
- write invariant catalog
- write runtime-context map

Example:

```bash
mentalmodel docs --entrypoint examples.async_rl.demo:build_program --output docs/mentalmodel
```

### `mentalmodel replay`

Purpose:

- replay a recorded execution if deterministic inputs are available

### `mentalmodel verify`

Purpose:

- run runtime checks and Hypothesis-backed invariants

### `mentalmodel demo async-rl`

Purpose:

- execute or print the reference async RL demo

### `mentalmodel install-skills`

Purpose:

- install packaged skills for coding agents

Behavior:

- detect supported agent skill directories
- copy templated skill files
- print installed paths

Flags:

- `--agent codex`
- `--agent claude`
- `--dry-run`

Example:

```bash
mentalmodel install-skills --agent codex
```

## Minimal Async RL Demo

The demo should show the package as a programming model, not an RL framework.
It should use fake in-memory components but preserve async RL structure:

- prompt batch source
- policy sampling
- reward fanout
- optional KL prefetch
- rollout join
- learner update
- sampler refresh
- invariant checks

### Intended semantic graph

```text
batch_source -> sample_policy
sample_policy -> pangram_reward
sample_policy -> quality_reward
sample_policy -> kl_prefetch
sample_policy -> rollout_join
pangram_reward -> rollout_join
quality_reward -> rollout_join
kl_prefetch -> rollout_join
rollout_join -> staleness_invariant
rollout_join -> learner_update
learner_update -> refresh_sampler
```

### Intended authoring shape

```python
from mentalmodel.core import Actor, Effect, Invariant, Join, Parallel, Ref, Workflow
from mentalmodel.plugins.runtime_context import RuntimeContext


def build_program() -> Workflow:
    return Workflow(
        name="async_rl_demo",
        children=[
            RuntimeContext(
                name="local_control_plane",
                runtime="local",
                children=[
                    Actor("batch_source", handler=BatchSource()),
                    RuntimeContext(
                        name="remote_sampling",
                        runtime="sandbox",
                        children=[
                            Effect(
                                "sample_policy",
                                handler=PolicySampler(group_size=4),
                                inputs=[Ref("batch_source")],
                            ),
                        ],
                    ),
                    Parallel(
                        name="reward_fanout",
                        children=[
                            Effect(
                                "pangram_reward",
                                handler=PangramScorer(),
                                inputs=[Ref("sample_policy")],
                            ),
                            Effect(
                                "quality_reward",
                                handler=QualityScorer(),
                                inputs=[Ref("sample_policy")],
                            ),
                            Effect(
                                "kl_prefetch",
                                handler=KLPrefetch(),
                                inputs=[Ref("sample_policy")],
                            ),
                        ],
                    ),
                    Join(
                        "rollout_join",
                        inputs=[
                            Ref("sample_policy"),
                            Ref("pangram_reward"),
                            Ref("quality_reward"),
                            Ref("kl_prefetch"),
                        ],
                    ),
                    Invariant(
                        "staleness_invariant",
                        inputs=[Ref("rollout_join")],
                        checker=PolicyStalenessChecker(max_off_policy_steps=0),
                    ),
                    Actor(
                        "learner_update",
                        handler=LearnerUpdate(),
                        inputs=[Ref("rollout_join")],
                    ),
                    Effect(
                        "refresh_sampler",
                        handler=RefreshSampler(),
                        inputs=[Ref("learner_update")],
                    ),
                ],
            )
        ],
    )
```

### Intended runtime behavior

The demo does not need to talk to real remote services. It should use simple
async handlers:

- `BatchSource` returns a small batch of prompt-like records
- `PolicySampler` produces fake completions and a policy version
- reward scorers operate concurrently with `asyncio.sleep()` to demonstrate
  fanout and join
- `KLPrefetch` returns fake reference-logprob metadata
- `LearnerUpdate` increments policy version and records a state transition
- `RefreshSampler` records a side-effect completion event
- `PolicyStalenessChecker` validates that the sampled and current policy
  versions match

### Expected generated docs for the demo

The demo should be able to generate:

- topology overview
- per-node inventory
- runtime-context grouping
- invariant catalog
- Mermaid diagram

## Documentation Outputs

Generated docs should be derived from IR, not handwritten.

Suggested markdown outputs:

- `docs/mentalmodel/topology.md`
- `docs/mentalmodel/invariants.md`
- `docs/mentalmodel/runtime-contexts.md`
- `docs/mentalmodel/node-inventory.md`

## Milestones

### Milestone 1: IR-first scaffold

- create package structure
- define core interfaces
- define IR models
- define lowering contract
- define CLI skeleton
- add direct tests for lowering, graph validity, and CLI entrypoint loading
- verify milestone with lint, typecheck, and test runs before continuing

### Milestone 2: Base primitives and runtime

- implement `Workflow`, `Actor`, `Effect`, `Invariant`, `Parallel`, `Join`, `Ref`
- implement lowering to IR
- implement async executor
- implement execution recorder
- implement OTel adapter
- add end-to-end runtime tests, failure-path tests, and execution-record tests
- verify milestone with lint, typecheck, and test runs before continuing

### Milestone 3: Analysis and docs

- implement structural analyzers
- implement semantic analyzers
- implement Mermaid and markdown renderers
- implement `mentalmodel check`, `graph`, and `docs`
- add direct analyzer and artifact-generation tests, including failure cases
- verify milestone with lint, typecheck, and test runs before continuing

### Milestone 4: Extension model

- implement plugin registry
- implement `RuntimeContext` as the reference plugin
- stamp plugin provenance into IR during lowering
- validate plugin provenance in IR and docs
- update the async RL demo so `RuntimeContext` is the canonical proof that the
  plugin boundary works end to end
- add plugin registration, plugin failure, and extension-lowering tests
- verify milestone with lint, typecheck, and test runs before continuing

### Milestone 5: Verification and skills

- implement runtime invariant checks
- add Hypothesis-backed verification hooks
- implement `mentalmodel verify`
- implement `mentalmodel install-skills`
- add verification-command tests and deterministic property-check coverage
- verify milestone with lint, typecheck, and test runs before continuing

### Milestone 6: Demo and reference workflow

- implement minimal async RL demo
- add expected docs artifacts
- add CLI path for generating demo outputs
- add demo semantic tests for output structure, invariants, and generated
  artifacts
- verify milestone with lint, typecheck, and test runs before continuing

### Milestone 7: Flesh out skills
- Base skill should be much more detailed: include all core primitives, best practices, hello world program, useful CLI commands, and all core concepts
- Plugin specific skill: detailed instructions and examples for authoring new plugins
- Invariants/testing specific skill: best practices for authoring useful invariants/property checks
- Debugging specific skill: detailed overview of otel implementation, how to run and debug a program. Should facilitate auto-research style self improvement

## Immediate Next Build Steps

The next implementation pass should focus on:

1. `pyproject.toml` with a console script entrypoint
2. IR dataclasses
3. base semantic dataclasses
4. plugin registry
5. `mentalmodel check` that lowers and validates a program
6. reference `RuntimeContext` plugin
7. async RL demo with fake handlers

This should be enough to validate the package thesis before adding richer DSL
syntax or more domain-specific primitives.
