# Runtime Environment CLI Binding

## Problem

Phase 20 introduced `RuntimeEnvironment`, `RuntimeProfile`, and typed shared
resources. That made runtime binding a first-class package concept, but the CLI
still only knows how to load and verify a `Workflow`.

Today this creates an awkward gap:

- `mentalmodel check`, `graph`, and `docs` work with a plain workflow entrypoint
- `mentalmodel verify` can now pass workflow factory parameters
- but `mentalmodel verify` still cannot bind a separate `RuntimeEnvironment`

That is why the runtime-environment demo still needs:

```python
run_verification(
    build_program(),
    environment=build_environment(),
)
```

instead of a clean CLI invocation.

This is a real product gap, not a demo-specific quirk.

## Design Goal

Make CLI invocation able to describe:

1. how to build the workflow
2. how to build the runtime environment
3. what parameters should be passed to each

without making config files the source of truth for Python object graphs.

The CLI should be able to bind real provider-backed workflows and real runtime
environments cleanly, but typed Python factories should remain the canonical
construction boundary.

## Non-Goals

- no YAML/TOML serialization of `RuntimeEnvironment` object graphs
- no hidden global environment registry
- no requirement that every workflow bundle runtime construction into
  `build_program()`
- no one-off flags that solve only the runtime-environment demo

## Source of Truth

The source of truth remains:

- Python workflow factories returning `Workflow[...]`
- Python environment factories returning `RuntimeEnvironment`

Config files should only:

- point at those factories
- provide JSON/TOML parameters
- choose named invocation profiles

They should not attempt to define provider clients, tokenizers, reward models,
or other shared resources directly.

## Recommended Shape

Introduce a first-class invocation model for CLI-driven verification.

### Core types

```python
@dataclass(slots=True, frozen=True)
class InvocationFactorySpec:
    entrypoint: str
    params: Mapping[str, object] = field(default_factory=dict)
```

```python
@dataclass(slots=True, frozen=True)
class VerifyInvocationSpec:
    program: InvocationFactorySpec
    environment: InvocationFactorySpec | None = None
    invocation_name: str | None = None
    runs_dir: Path | None = None
```

### CLI shape

Short-term direct flags:

```bash
mentalmodel verify \
  --entrypoint mypkg.workflow:build_program \
  --params-file verification/smoke.program.json \
  --environment-entrypoint mypkg.runtime:build_environment \
  --environment-params-file verification/smoke.environment.json
```

Longer-term config-driven shape:

```bash
mentalmodel verify --spec verification/smoke.toml
```

with a TOML file like:

```toml
[program]
entrypoint = "mypkg.workflow:build_program"
params_file = "verification/smoke.program.json"

[environment]
entrypoint = "mypkg.runtime:build_environment"
params_file = "verification/smoke.environment.json"

[runtime]
invocation_name = "real_smoke"
runs_dir = ".runs"
```

This `invocation_name` should become the stable run-level label for:

- `.runs` summaries
- replay metadata
- OTEL resource/span attributes
- emitted metric attributes where a run-level label is appropriate

That is distinct from per-node `runtime_profile`.

## Why TOML Over YAML

If we add a config-file layer, TOML is the better default:

- Python has `tomllib` in the standard library
- it avoids a new YAML dependency
- the format is good for small structured invocation files
- it is less permissive and less ambiguous than YAML

YAML support could be added later if there is real demand, but TOML should be
the primary shape.

## Factory Semantics

The CLI should support:

- zero-arg workflow factories
- parameterized workflow factories
- zero-arg environment factories
- parameterized environment factories

Validation rules:

- workflow entrypoint must resolve to a `Workflow` or a callable returning one
- environment entrypoint must resolve to a `RuntimeEnvironment` or a callable
  returning one
- workflow params and environment params must each decode to JSON objects
- signature mismatches should fail before runtime execution begins

## Why Not Put Environment Construction Inside `build_program()`

That would make the demo look nicer, but it would weaken the design:

- it collapses authored workflow shape and runtime binding into one function
- it makes runtime-profile selection harder to swap independently
- it encourages apps to hide real provider/runtime setup inside workflow
  factories

Phase 20 was valuable precisely because it separated those layers cleanly. The
CLI should learn that model rather than forcing authors back into wrapper
boilerplate.

## Run Distinction and Telemetry

The package already has some runtime-profile observability:

- node execution metrics include `runtime_profile`
- `node.started` payloads include `runtime_profile`
- run summaries include `runtime_default_profile_name` and
  `runtime_profile_names`
- traces carry runtime-profile metadata at the node/span level

That is useful, but it is not enough for the top-level operational distinction:

- smoke verification runs
- real training runs
- fixture runs
- shadow-parity runs

Those need a run-level identifier, not just per-node profile metadata.

### Recommended addition

Introduce a first-class invocation label/profile:

```python
@dataclass(slots=True, frozen=True)
class VerifyInvocationSpec:
    program: InvocationFactorySpec
    environment: InvocationFactorySpec | None = None
    invocation_name: str | None = None
    runs_dir: Path | None = None
```

Suggested semantics:

- `invocation_name` identifies the overall execution mode, for example:
  - `fixture_demo`
  - `real_smoke`
  - `shadow_verify1`
  - `training_prod`
- `runtime_profile` continues to identify the active per-node bound runtime
  profile

### Why both are needed

- `invocation_name` answers: “what kind of run was this?”
- `runtime_profile` answers: “which runtime profile executed this node?”

You need both when a single run can touch multiple profiles, but still belongs
to one top-level operational category.

### Observability propagation

`invocation_name` should be propagated to:

- `summary.json`
- verification report metadata
- replay report metadata
- built-in metric attributes
- trace resource/span attributes
- CLI run listing and filtering

### Acceptance criteria

- users can distinguish smoke vs real vs shadow runs directly from `runs list`
- metrics can be filtered by invocation name without inferring from graph ids
- traces can be grouped by invocation name in OTEL backends
- `.runs` bundles preserve the invocation label as a stable top-level field

## Recommended Implementation Order

### Phase A: Direct CLI Environment Factory Support

Add:

- `--environment-entrypoint`
- `--environment-params-json`
- `--environment-params-file`

Acceptance criteria:

- the runtime-environment demo can be verified without a Python snippet
- a real provider-backed app can supply separate workflow and environment
  factories
- invalid environment entrypoints or params fail fast and clearly

### Phase B: Shared Invocation Loader

Refactor the CLI loader so workflow factories and environment factories both use
the same typed invocation-path helpers.

Acceptance criteria:

- no duplicated loader logic between workflow and environment binding
- signature validation remains deterministic
- tests cover both callable and direct-object paths

### Phase C: TOML Invocation Spec

Add:

- `mentalmodel verify --spec verification/smoke.toml`

Acceptance criteria:

- the spec can define both workflow and environment factories
- the spec can define `invocation_name`
- the spec format is documented and example-backed
- direct CLI flags still work and can override spec fields if needed

### Important note

This phase should be treated as part of the intended ergonomic solution, not
optional polish. The direct flag form is useful and should ship first, but the
TOML spec is what makes the model feel coherent for real applications.

## Expected User Impact

This should eliminate the current awkwardness for the runtime-environment demo
and for real apps like Pangramanizer:

- no more Python one-liners just to bind the environment
- real runtime profiles become easy to select from the CLI
- smoke, shadow, and training runs become easy to distinguish in `.runs` and
  external telemetry
- workflow factories stay clean and authored-shape-focused
- runtime construction remains typed and explicit in Python

## Relation to PLAN-PT3

This is a deliberate detour from the current PT3 order. It should land before
or alongside later PT3 work because Phase 20 is substantially less useful
without a corresponding CLI binding model.
