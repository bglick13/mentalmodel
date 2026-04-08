# ADR: Remote Runs Data Plane Phase 0 Contracts

Date: 2026-04-08
Status: Accepted for Phase 0

## Context

`mentalmodel` already has two strong observability surfaces:

- semantic run bundles under `.runs/<graph_id>/<run_id>/`
- OpenTelemetry trace export through the existing tracing configuration

The next step is to support both self-hosted and managed remote inspection
without rewriting the runtime or forking the dashboard contract.

Two practical constraints shape this decision:

1. The local `.runs` model is already the product-level debugging surface.
2. Pangramanizer is a real downstream consumer with a concrete need for one
   shared stack that can host both built-in mentalmodel demos and external
   project catalogs.

## Decision

### 1. Keep semantic runs and OTel as separate product contracts

We will preserve a dual-stream model:

- **semantic run data**
  - canonical run manifest
  - semantic execution records
  - outputs, state, verification payloads, graph artifacts
- **OTel trace export**
  - spans and trace metadata
  - cross-service observability integration

Semantic records are not treated as “just another OTLP signal.” They remain a
first-class runtime ledger with their own contract.

### 2. Define a canonical run manifest

Remote storage will not be designed around ad hoc SQL rows or object naming.
Instead, both local bundles and remote persistence will map onto the same
conceptual model:

- `RunManifest`
- `ArtifactDescriptor`
- `RunTraceSummary`

This keeps one source of truth for:

- run identity
- graph identity
- invocation/runtime metadata
- schema versions
- artifact presence and locations
- trace export summary

### 3. Preserve dashboard read compatibility first

The first remote-compatible surface is the historical run-inspection API:

- `/api/runs`
- `/api/runs/{graph_id}/{run_id}/overview`
- `/graph`
- `/records`
- `/spans`
- `/replay`
- `/nodes/{node_id}`

The local execution/catalog launch APIs remain separate concerns.

### 4. Make project/workspace registration explicit

One running stack must be able to host multiple projects. Phase 0 formalizes:

- `ProjectRegistration`
- `ProjectCatalog`
- `ProjectCatalogProvider`
- `WorkspaceConfig`

This gives the shared stack a stable way to register:

- built-in mentalmodel examples
- Pangramanizer training verification specs
- future downstream project catalogs

Provider-based registration is the primary path. Directory scanning is a
fallback, not the canonical contract.

### 5. Prefer a minimal remote path before streaming

The first remote implementation after Phase 0 should be:

- Postgres for run index/control metadata
- object storage for artifacts
- HTTP bundle upload for completed runs

Kafka, ClickHouse, and record streaming remain future phases for scale and live
analytics.

### 6. Treat Pangramanizer as the first serious acceptance target

Pangramanizer is not a hypothetical future user. It already has:

- durable verification specs
- stable invocation naming
- a canonical verification spec catalog
- active operator-workflow needs around metrics/tracker normalization and UI

The remote runs plan is successful only if one shared stack can present both:

- mentalmodel built-in examples
- Pangramanizer operator-facing verification flows

## Consequences

### Positive

- The contract stays aligned with today’s `.runs` surface.
- Self-hosted and managed paths can share the same conceptual model.
- The dashboard can become project-aware without depending on raw filesystem
  paths.
- Pangramanizer gets a clear path to a shared operator stack.

### Negative

- This phase adds types and docs without immediate user-visible infrastructure.
- Existing `DashboardCatalogEntry` usage remains the practical runtime surface
  until workspace/project registration is implemented.
- Some fields such as `project_id` and `catalog_entry_id` will be optional
  locally before they become fully indexed remotely.

## Alternatives considered

### Kafka-first remote ingestion

Rejected for the first remote slice. It forces ordering, projection, replay,
and blob coordination problems too early.

### Treat semantic records as OTLP logs

Rejected for now. It would blur the boundary between mentalmodel-specific
semantic debugging and generic observability export before the record contract
is fully stabilized.

### Continue relying on ad hoc `spec_path` registration

Rejected as the only model. It is fine for one-off local exploration, but it is
not a scalable answer to one stack hosting multiple repos and operator-facing
catalogs.

## Phase 0 deliverables

- ADR documenting the contracts and boundaries
- typed internal contracts in `mentalmodel.remote`
- tests for manifest, artifact, and workspace/project validation

## Follow-on work

- Phase 1: sink boundaries and local-first internal abstractions
- Phase 2: minimal remote bundle upload
- Phase 3: self-hosted multi-project workspace UX
- Phase 4+: streaming, analytics, and managed hardening
