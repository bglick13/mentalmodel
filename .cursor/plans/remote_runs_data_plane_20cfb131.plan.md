---
name: Remote runs data plane
overview: Evolve from local `.runs` bundles toward an optional remote data plane that supports both self-hosted and managed deployments without changing the runtime's semantic artifact model or the dashboard's run-inspection contract. Phase the work so the first remote path is simple (bundle upload + object storage + Postgres), then add streaming and high-volume analytics only when the product surface requires them.
todos:
  - id: adr-contracts
    content: "Add docs/ADR covering the dual telemetry split, the canonical run manifest, API compatibility tiers, and versioning rules"
    status: pending
  - id: canonical-manifest
    content: "Define canonical RunManifest and ArtifactDescriptor types that map to both local `.runs` bundles and remote storage"
    status: pending
  - id: sink-boundaries
    content: "Design explicit runtime seams for completed-run artifact upload, per-record streaming, and trace export without coupling domain APIs to transport details"
    status: pending
  - id: remote-upload-api
    content: "Sketch minimal remote ingest API for bundle upload, idempotency, sealing, and backfill sync"
    status: pending
  - id: dashboard-storage-abstraction
    content: "Split DashboardService storage concerns from API shaping so local-file and remote-backed read paths can share response contracts"
    status: pending
  - id: self-hosted-devx
    content: "Specify self-hosted localhost story: generated compose templates, CLI commands, demo profiles, and migration from today's OTEL demo flow"
    status: pending
  - id: analytics-rollout
    content: "Define when Postgres-only scans are acceptable, when pre-aggregation is needed, and when ClickHouse becomes mandatory"
    status: pending
isProject: false
---

# Remote runs and observability sinks (revised phased plan)

## Executive pass

The current plan has the right instinct, but it jumps too quickly from local
files to Kafka/ClickHouse. The missing piece is a **simple remote bundle path**
that preserves the existing `.runs` model and gives you a usable self-hosted
story before you commit to the full streaming stack.

The key change in this revision is:

1. Keep **run bundles** as the product-level source of truth.
2. Add a **canonical run manifest** that can be stored locally or remotely.
3. Make the **first remote write path** an idempotent bundle upload to
   **Postgres + object storage**, not Kafka-first.
4. Treat **streaming records + ClickHouse** as an optional later phase for live
   analytics and scale, not a prerequisite for remote inspection.
5. Make the self-hosted path a first-class product surface with a clear local
   stack workflow, not a side note.

## Where things stand today

- **Run bundles** are written under
  [`default_runs_dir`](src/mentalmodel/runtime/runs.py) as
  `{graph_id}/{run_id}/` with `summary.json`, `graph.json`, `records.jsonl`,
  `outputs.json`, `state.json`, optional `otel-spans.jsonl`, and optional
  `verification.json`.
  [`write_run_artifacts`](src/mentalmodel/runtime/runs.py) is the single write
  path for persisted execution.
- **Semantic records** are already distinct from traces.
  [`ExecutionRecord`](src/mentalmodel/ir/records.py) serializes through
  [`execution_record_to_json`](src/mentalmodel/observability/export.py).
  That stream is a product-specific runtime ledger, not generic OTLP.
- **Tracing** already has a clean split between OTLP export and local disk
  mirroring through [`TracingConfig`](src/mentalmodel/observability/config.py),
  `trace_*` summary fields, and `mentalmodel otel` CLI commands.
- **Dashboard reads** are already mostly API-contract-based rather than
  filesystem-based. The React app in
  [`apps/dashboard/src/lib/api.ts`](apps/dashboard/src/lib/api.ts) calls REST
  endpoints, while the current backend implementation in
  [`DashboardService`](src/mentalmodel/ui/service.py) happens to read local
  files.

That means the durable product contracts already exist in practice:

- run inspection JSON responses
- run bundle artifact shapes
- semantic record rows
- trace export configuration

## What was overlooked in the first draft

### 1. The first remote path should not require Kafka

You need a remote persistence story before you need a remote streaming story.
Today the runtime naturally produces a **completed run bundle**. That maps much
more directly to:

- upload metadata to **Postgres**
- upload large artifacts to **object storage**
- keep existing local `.runs` behavior during rollout

If you skip that step and go straight to Kafka, you force the first deployable
version to solve:

- record ordering
- consumer materialization
- partial run lifecycle
- blob pointer coordination
- replay reconstruction
- local developer stack complexity

all at once.

### 2. The dashboard API is not one monolith

The current API surface actually falls into three groups:

| Group | Endpoints | Notes |
|------|-----------|-------|
| Historical run inspection | `/api/runs`, `/overview`, `/graph`, `/records`, `/spans`, `/replay`, `/nodes/{node}` | These should be the first remote-compatible surface. |
| Analytics | `/api/analytics/timeseries` | Can start as low-scale scans or pre-aggregation, then move to ClickHouse. |
| Local execution/control | `/api/catalog*`, `/api/executions*` | These are local-control-plane concerns, not required for the first remote data plane. |

The original plan implicitly treated all of `DashboardService` as one thing. It
should instead separate **read-model storage** from **local execution launch**.

### 3. There is no explicit canonical run manifest yet

Right now `summary.json` is the closest thing to a manifest, but remote storage
needs a slightly richer canonical model:

- run identity and status
- graph identity
- invocation/runtime metadata
- schema versions
- artifact presence
- blob descriptors
- sizes/checksums/content types

Without that, Postgres rows, object keys, local files, and API responses will
start drifting into parallel representations.

### 4. Self-hosting needs a real product story

The repo already has a good pattern for traces-only OTEL demos:

- packaged templates
- `mentalmodel otel write-demo`
- generated compose/env files outside the repo root

The remote data plane should follow the same pattern instead of assuming users
will hand-author infrastructure or that a checked-in repo-root
`docker-compose.yaml` is enough.

### 5. Retention, replay cost, and backfill were missing

Remote storage introduces operational concerns that local `.runs` mostly hides:

- how old runs are retained or pruned
- whether replay scans raw `records.jsonl` blobs or precomputed projections
- how existing local bundles are backfilled into remote storage
- how re-upload/idempotency works when the same run is synced twice

These do not all need to be built in the first slice, but they need to be in
the plan.

### 6. Project discovery and setup were underspecified

The current UI model is still centered on:

- built-in example catalog entries
- `--catalog-entrypoint`
- ad hoc `spec_path` registration

That is not a sufficient answer to “run one local stack and inspect multiple
repos.” The plan needs an explicit **workspace/project registry** so a single
stack can host both the built-in mentalmodel demos and external projects like
Pangramanizer without path-by-path manual setup.

### 7. The Pangramanizer operator story was left implicit

Pangramanizer already has real adoption signals:

- durable verification specs
- stable invocation names
- a canonical verification spec catalog in
  `pangramanizer.mentalmodel_training.verification.spec_catalog`
- an explicitly blocked next push around metrics/tracker normalization,
  dashboard usability, and operator workflow review

That makes Pangramanizer the best concrete non-demo acceptance target for this
plan. The data-plane work should explicitly unblock that operator workflow, not
just hope it benefits later.

## API shaping checkpoint

- Actors:
  - Runtime writes local artifacts, emits records, and exports traces.
  - Remote ingest API accepts completed bundles first, streaming later.
  - Dashboard API reads a storage-backed run model.
  - Optional analytics pipeline projects high-volume records/spans for fast time
    queries.
- Source of truth:
  - The canonical source of truth is the **run manifest + named artifacts**.
  - Local `.runs` and remote PG/object storage are two storage backends for the
    same conceptual run.
  - OTLP spans remain a parallel observability export, not the source of truth
    for semantic replay.
- New core types / interfaces:
  - `RunManifest`
  - `ArtifactDescriptor`
  - `RunArtifactStore`
  - `CompletedRunSink`
  - `ExecutionRecordSink`
  - `ProjectRegistration`
  - `ProjectCatalogProvider`
  - `WorkspaceRegistry`
  - `DashboardRunRepository`
- Public entrypoints:
  - Runtime config for local-only, dual-write, and remote upload.
  - Remote ingest API for run bundle upload and backfill sync.
  - Dashboard backend wired to either local-file or remote storage.
  - Project/workspace registration for multi-repo discovery.
  - Self-hosted CLI for generating local stack assets and validating config.
- Validation / failure model:
  - Local bundle write remains the default safe path.
  - Remote upload is idempotent and non-fatal by default during migration.
  - Bundle sealing/finalization is explicit so partial uploads do not masquerade
    as completed runs.
- Execution flow:
  - First remote slice uploads completed bundles.
  - Later slices add live record streaming and scale-oriented analytics.
- Tradeoffs:
  - PG + object storage is slower for large analytics, but it is much simpler
    and enough for early remote inspection.
  - Kafka + ClickHouse is the right scale path, but it should enter only after
    the product contract and self-hosting UX are solid.
- Open questions:
  - Whether the first managed service includes multi-tenancy and auth, or ships
    single-tenant first with future tenant columns reserved.
  - Whether remote analytics MVP is scan-based or ingest-time pre-aggregated.

## Canonical model

### Canonical source of truth

Define the remote/local system around one conceptual entity:

- **RunManifest**
  - `run_id`
  - `graph_id`
  - `created_at_ms`
  - `completed_at_ms`
  - `status`
  - `success`
  - `invocation_name`
  - `runtime_default_profile_name`
  - `runtime_profile_names`
  - `run_schema_version`
  - `record_schema_version`
  - `trace_summary`
  - `artifact_descriptors`

- **ArtifactDescriptor**
  - logical name: `summary`, `graph`, `records`, `outputs`, `state`,
    `verification`, `spans`
  - content type
  - byte size
  - checksum
  - storage URI or object key
  - optional compression metadata

This does **not** mean changing the local `.runs` surface immediately. It means
making future local and remote writes derive from the same internal model.

### Why this matters

It avoids four parallel sources of truth:

- local `summary.json`
- remote Postgres `runs` row
- object storage path conventions
- dashboard response payload assembly

The UI and CLI should keep speaking in terms of runs, records, replay, and
artifacts. Storage-specific details stay behind repository/store boundaries.

## Project and workspace model

### Problem to solve

One local stack should be able to host multiple logical projects at once, for
example:

- built-in mentalmodel examples
- Pangramanizer's `mentalmodel_training` verification specs

without forcing the operator to manually register one `spec_path` at a time or
run separate dashboard instances.

### Recommended model

Introduce three explicit concepts:

- **Workspace**
  - one running local or remote mentalmodel stack
  - owns infrastructure config and project registrations
- **Project**
  - one logical source of workflows/specs/runs, usually tied to one repo or one
    application
- **Catalog provider**
  - a stable code-defined provider that returns project metadata and launchable
    catalog entries

This gives the stack a real answer to:

- which projects are registered?
- which specs belong to which project?
- how should runs be grouped in the UI?
- which defaults, pinned nodes, and metric groups should operators see?

### Project identity

The canonical run model should grow first-class project identity rather than
trying to infer it from path layout later.

Recommended run-level fields:

- `project_id`
- `project_label`
- `environment_name`
- `catalog_entry_id` when launched from a known catalog entry
- `catalog_source` such as `builtin`, `module-provider`, or `path-scan`

These can start as optional local metadata, but they should become indexed
fields in the remote read model.

### Discovery mechanism

Prefer provider-based registration over directory crawling as the primary
contract.

Primary path:

- register one Python provider entrypoint per project
- provider returns project metadata and catalog entries

Fallback path:

- scan a configured directory for verify TOML specs and synthesize minimal
  catalog entries

Provider-first is the right default because Pangramanizer already has a
canonical spec catalog module and because providers can attach stable labels,
categories, pinned nodes, and metric groups.

### Proposed interfaces

- `ProjectRegistration`
  - `project_id`
  - `label`
  - `root_dir`
  - `catalog_provider`
  - optional `runs_dir`
  - optional tags/default environment
- `ProjectCatalogProvider`
  - returns project metadata plus `DashboardCatalogEntry`-like items
- `WorkspaceRegistry`
  - loads registered projects for one running stack
  - resolves catalog entries across projects

### Local single-stack UX

The desired localhost workflow should be:

1. Start one local mentalmodel stack.
2. Register the mentalmodel examples project.
3. Register the Pangramanizer training project.
4. Browse both projects from one dashboard.
5. Run specs locally or inspect previously synced runs remotely.

That should not require manual per-spec path entry.

### CLI surface

Recommended additions:

- `mentalmodel projects add --id mentalmodel-examples --provider mentalmodel.ui.projects.examples:project`
- `mentalmodel projects add --id pangramanizer-training --provider pangramanizer.mentalmodel_training.verification.ui_catalog:project`
- `mentalmodel projects list`
- `mentalmodel projects inspect --id pangramanizer-training`

The generated remote demo directory can include a small `workspace.toml` or
`projects.toml` describing registered projects and providers.

### UI implications

The dashboard should gain project-aware navigation:

- project switcher/filter
- per-project catalog sections
- stable run grouping by `project_id`, `graph_id`, and `invocation_name`
- project-scoped defaults for pinned nodes and metric groups

This directly addresses the blocked “combined Phase E + F + 7” work: dashboard
usability improves materially when the UI understands projects and operator
entrypoints instead of flattening everything into one run pool.

## Recommended deployment profiles

### Profile A: local-only default

This remains the default developer and OSS path:

- local `.runs`
- optional local OTLP export
- no remote dependencies

### Profile B: self-hosted minimal remote

This should be the first real remote product path:

- mentalmodel remote API
- Postgres
- object storage, preferably S3-compatible via MinIO locally
- workspace/project registry

Characteristics:

- uploads completed run bundles over HTTP
- supports dashboard inspection against remote runs
- supports backfill from existing `.runs`
- supports multiple registered projects from one stack
- no Kafka, no ClickHouse, no collector required

This is the right first self-hosted story because it is understandable,
operable, and cheap to run on localhost or a single VM.

### Profile C: self-hosted full data plane

Add only when needed:

- OTel Collector
- Kafka-compatible broker, likely Redpanda for localhost ergonomics
- ClickHouse
- optional background consumers/projectors

Characteristics:

- live record streaming
- faster multi-run analytics
- span analytics at volume
- more operational complexity

### Profile D: managed service

Use the same contracts as Profiles B/C, but hosted:

- managed auth and API keys
- hosted Postgres/object store/broker/analytics
- tenant/project isolation
- retention and lifecycle policies

The managed path should reuse the same runtime sink configuration and manifest
model rather than inventing a separate managed-only write path.

## Storage mapping

### Postgres

Use Postgres for:

- run manifest / index
- org/project/environment metadata
- API keys and auth metadata
- artifact descriptors
- sync state and upload status
- optional low-scale aggregate tables

Candidate tables:

- `runs`
- `run_artifacts`
- `projects`
- `environments`
- `api_keys`
- `run_ingest_attempts`

### Object storage

Use object storage for:

- `graph.json`
- `records.jsonl`
- `outputs.json`
- `state.json`
- `verification.json`
- `otel-spans.jsonl`
- optional compressed archives

Suggested key scheme:

```text
/<tenant_or_default>/<project_or_default>/<graph_id>/<run_id>/<artifact_name>
```

Keep the key scheme opaque to callers. The manifest stores the pointer.

### ClickHouse

Use ClickHouse only when you need:

- multi-run time-range analytics
- fast arbitrary rollups
- high-cardinality facet exploration
- larger-scale span queries

Do not make ClickHouse required for the first remote MVP.

## Sink and repository boundaries

### Runtime-side boundaries

Keep transport at the edges. The runtime should know about these interfaces,
not about Kafka topics or SQL tables:

- `CompletedRunSink`
  - consumes one finalized `RunManifest` plus access to artifact bytes
- `ExecutionRecordSink`
  - consumes individual semantic record envelopes
- `TracingConfig`
  - remains the trace export boundary

Recommended implementations:

- `LocalBundleSink`
- `CompositeCompletedRunSink`
- `NoOpCompletedRunSink`
- `HttpBundleUploadSink`
- later `KafkaExecutionRecordSink`

Important: `write_run_artifacts` is still the natural completion seam for the
first remote slice. Per-record emission is a separate seam and should not be
forced into the same abstraction if it harms clarity.

### Dashboard-side boundaries

Split the current service into:

- `DashboardRunRepository`
  - list runs
  - fetch manifest
  - fetch artifact payloads
  - fetch node-scoped trace data
  - fetch replay inputs
- `DashboardExecutionService`
  - local launch sessions
  - `/api/executions*`
  - `/api/catalog*`

That separation will let one backend remain local-control-plane-friendly while
another is remote-storage-backed.

## API compatibility strategy

### Remote MVP contract

The first remote-compatible read path should cover:

- `GET /api/runs`
- `GET /api/runs/{graph_id}/{run_id}/overview`
- `GET /api/runs/{graph_id}/{run_id}/graph`
- `GET /api/runs/{graph_id}/{run_id}/records`
- `GET /api/runs/{graph_id}/{run_id}/spans`
- `GET /api/runs/{graph_id}/{run_id}/replay`
- `GET /api/runs/{graph_id}/{run_id}/nodes/{node_id}`

### Non-goals for the first remote slice

These should not block the first remote data plane:

- launching verification from the remote dashboard
- remote catalog registration from local file paths
- large-scale arbitrary analytics rollups
- tenant-aware auth UI

### Analytics endpoint guidance

`/api/analytics/timeseries` should be explicitly phased:

- **Remote MVP**: optional or scan-based for modest data sets
- **Intermediate**: ingest-time pre-aggregated tables in Postgres
- **Scale path**: ClickHouse-backed rollups

Do not implicitly promise ClickHouse-grade performance before you actually ship
that path.

## Ingest protocol

The first remote write path should be a small HTTP ingest contract for
**completed bundles**.

Recommended lifecycle:

1. Client declares a run upload with manifest metadata.
2. Server returns artifact upload targets or accepts direct multipart upload.
3. Client uploads named artifacts.
4. Client seals/finalizes the run.
5. Server marks the run indexed and available for reads.

Required semantics:

- idempotent by `run_id`
- safe re-upload/backfill behavior
- explicit partial/uploading/sealed states
- checksum validation
- server-side rejection when required artifacts are missing

This gives you a clean `mentalmodel runs sync` or `mentalmodel remote sync`
story for backfilling local bundles into remote storage.

## Self-hosted story

### Recommendation

Do **not** make a repo-root `docker-compose.yaml` the primary UX.

Instead:

- keep checked-in templates in the package
- generate a user-local stack directory via CLI
- let users start it with Docker Compose from that output directory

This matches the existing `mentalmodel otel write-demo` pattern and works both
for source checkouts and installed package users.

### What to ship

For the remote data plane, add a new command group rather than overloading the
existing traces-only `otel` namespace.

Recommended commands:

- `mentalmodel remote write-demo --profile minimal --output-dir <dir>`
- `mentalmodel remote write-demo --profile full --output-dir <dir>`
- `mentalmodel remote show-config`
- `mentalmodel remote sync --runs-dir <dir> --server <url>`
- `mentalmodel remote doctor`
- `mentalmodel projects add ...`
- `mentalmodel projects list`

Keep `mentalmodel otel write-demo` as the traces-only demo path.

The generated demo should include an obvious way to register both:

- the built-in mentalmodel example catalog
- a second repo like Pangramanizer via provider entrypoint or workspace config

### Localhost profiles

`minimal` profile:

- API
- Postgres
- MinIO
- workspace/project registry config
- generated env/example config

`full` profile:

- API
- Postgres
- MinIO
- Redpanda
- ClickHouse
- OTel Collector
- optional projector/consumer worker

### Why generated templates are better than one checked-in compose file

- works for installed package users, not only repo contributors
- can emit matching `.env` files and docs
- avoids pinning repo-root workflow around one environment shape
- lets you version minimal and full profiles cleanly
- matches the existing observability demo UX already in the repo

## Recommended phase order

### Phase 0: contracts and boundaries

- Write an ADR covering:
  - semantic records vs OTel traces
  - canonical run manifest
  - remote API compatibility tiers
  - storage roles
  - versioning rules
- Add versioning to semantic record envelopes if needed.
- Decide which dashboard endpoints are in-scope for remote MVP.
- Define the workspace/project registry contract and provider API.

Deliverable:

- docs + typed internal contracts, no infrastructure yet

### Phase 1: internal abstractions, still local-first

- Introduce:
  - `RunManifest`
  - `ArtifactDescriptor`
  - `CompletedRunSink`
  - `ExecutionRecordSink`
  - `ProjectRegistration`
  - `ProjectCatalogProvider`
  - composite/no-op/local implementations
- Keep local `.runs` as default.
- Ensure remote sink failures are non-fatal by default.
- Add deterministic tests proving the canonical manifest matches current bundle
  semantics.
- Split catalog/discovery concerns so projects are not encoded as bare
  filesystem paths.

Deliverable:

- local behavior preserved, runtime ready for alternate sinks

### Phase 2: minimal remote bundle upload

- Build remote ingest API for completed runs.
- Store manifests in Postgres and artifacts in object storage.
- Add `HttpBundleUploadSink`.
- Add CLI backfill/sync command for existing `.runs`.
- Add remote-backed repository for historical run inspection endpoints.
- Add indexed `project_id` and project-scoped catalog metadata to the read
  model.

Deliverable:

- first usable self-hosted and managed remote inspection path
- no Kafka/ClickHouse required

### Phase 3: self-hosted developer UX

- Ship generated localhost stack templates for the minimal remote profile.
- Add `mentalmodel remote write-demo`.
- Add `mentalmodel remote doctor` and config inspection.
- Add project registration commands and provider-based discovery.
- Document the one-stack multi-project workflow explicitly.
- Document transition from today's traces-only OTEL demo to remote data-plane
  demos.

Deliverable:

- one-command local stack materialization for the remote MVP plus multi-project
  registration

### Phase 4: optional streaming and scale analytics

- Add per-record streaming sink and broker integration.
- Add projector jobs into ClickHouse.
- Add scale-oriented implementation of `/api/analytics/timeseries`.
- Add optional span query path beyond raw mirrored run artifacts.

Deliverable:

- live analytics and higher-volume observability path

### Phase 4.5: Pangramanizer operator slice

Treat Pangramanizer as the first serious non-demo acceptance target.

Scope:

- register Pangramanizer as a first-class project through a provider-backed
  catalog, not via ad hoc spec-path registration
- make the `real_smoke`, `real_verify1`, and `real_verify3` specs visible and
  launchable from the shared stack
- validate stable grouping/filtering by:
  - `project_id = pangramanizer-training`
  - `graph_id`
  - `invocation_name`
- review metric naming and tracker-normalization needs against the dashboard
  metric grouping model
- confirm operator workflows for:
  - smoke run inspection
  - verify1 tracker inspection
  - verify3 multi-step replay and frame drill-down

Acceptance criteria:

- one local stack can show both mentalmodel examples and Pangramanizer runs
- Pangramanizer does not require a separate dashboard instance
- the dashboard can surface the real-provider verification paths as coherent
  operator journeys rather than raw run lists
- any remaining UI blockers for the combined Phase E + F + 7 push are explicit
  and tracked as product work, not hidden data-plane gaps

### Phase 5: managed-service hardening

- tenant/project isolation
- auth and API keys
- retention and pruning policies
- object lifecycle rules
- retry/dead-letter behavior
- ingestion observability and audit trails

Deliverable:

- operationally credible managed service

## Verification expectations

Every phase should carry a deterministic proof path.

- Phase 1:
  - tests that current `.runs` bundles still match prior contract
  - tests that manifest generation is stable
- Phase 2:
  - end-to-end upload/download integration test against local Postgres + MinIO
  - sync test proving existing `.runs` can be imported idempotently
- Phase 3:
  - CLI test for demo asset generation
  - doctor checks for packaged templates and required config
- Phase 3 project tests:
  - provider-based multi-project registration test
  - dashboard catalog resolution test across mentalmodel and Pangramanizer
- Phase 4:
  - projector correctness tests
  - replay equivalence tests between local bundle and remote read model
- Pangramanizer acceptance:
  - deterministic test or scripted demo that one stack can load both projects
    and resolve Pangramanizer's canonical spec catalog

## Open decisions that actually matter

1. Whether semantic record envelopes add explicit `schema_version` and
   `graph_id` fields at the top level for remote transport, or whether that
   stays derivable from the run manifest.
2. Whether remote analytics MVP scans records blobs on demand or writes
   pre-aggregated Postgres buckets during ingest.
3. Whether the first managed-service release is single-tenant with reserved
   tenant columns, or truly multi-tenant from day one.
4. Whether compressed artifact archives are part of the first upload protocol
   or deferred until large-run pressure makes them necessary.
5. Whether project registration is purely file-config-based, purely
   provider-based, or supports both with provider-first precedence.
6. Whether Pangramanizer's existing verification spec catalog should be adopted
   directly as the first `ProjectCatalogProvider` shape.

## Bottom-line recommendation

The phasing should change from:

- sinks
- remote API
- streaming
- collector hardening

to:

- contracts
- sink boundaries
- minimal remote bundle upload
- self-hosted localhost UX
- multi-project registry and discovery
- optional streaming/analytics
- Pangramanizer operator slice
- managed hardening

That order better matches the current codebase, gives you a practical
self-hosted story early, and keeps Kafka/ClickHouse as optional scale
components instead of forcing them into the first deployable version. It also
turns Pangramanizer into an explicit design target for project management and
operator workflow review instead of leaving that integration implicit.
