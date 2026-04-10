# Mentalmodel Remote Service Productization Plan

## Purpose

This document defines the migration from the current local-workspace and
manual-sync remote model to a more mature product shape closer to services like
Convex, Vercel, or Supabase.

The target experience is:

- a repo declares its `mentalmodel` project identity and remote connection
- the service knows the project and its dashboard contract
- runs show up in the hosted dashboard without manual local project registration
- live and completed run data flow into the same remote product surface
- local workspace registration remains available for development, but it is no
  longer the primary operator path

This plan intentionally focuses on durable product seams, not just the fastest
way to remove one local workflow pain point.

## Problem Statement

The current remote stack is a good localhost MVP, but it still feels like
developer tooling rather than a hosted product.

Current shape:

- local runs are persisted under `.runs/...`
- operators may manually upload completed run bundles with
  `mentalmodel remote sync`
- the dashboard server reads project catalogs from a local `workspace.toml`
- projects must be registered manually in that workspace config
- catalog providers are often Python entrypoints resolved from a local repo path

This creates a few product-shape problems:

1. The server knows projects only because a local operator told it about them.
2. A repo cannot simply "connect to the service" and start producing visible
   runs.
3. In-progress runs are not first-class remote citizens.
4. Hosted deployment is awkward because project catalogs depend on local Python
   imports rather than remote-owned project metadata.
5. The current model is excellent for local development, but it is not the
   right default for a service operators depend on day to day.

## Design Goals

### Primary goals

- Project configuration should be repo-owned.
- Project registration should be service-owned.
- The common path should not require editing a local dashboard workspace file.
- Hosted dashboards should not depend on importing arbitrary local repo Python.
- Live run visibility and completed run inspection should share one coherent
  remote model.
- The dashboard should be strong enough that users do not feel they need W&B
  just to understand training behavior.

### Secondary goals

- Preserve a strong local-development workflow.
- Keep the current `.runs` bundle model valuable as the canonical completed-run
  artifact format.
- Avoid forcing a giant one-shot migration. There should be a staged path from
  the current remote MVP to the hosted product model.

## Non-Goals

- Replacing `.runs` with a remote-only storage model
- Building multi-tenant billing or org-admin features in the first pass
- Full CI/CD integration in the first pass
- Remote server-side execution of repo workflows from the hosted dashboard
- Making every part of project configuration declarative if doing so would
  weaken typing or push too much Python object construction into TOML/YAML

### Explicitly out of scope: hosted dashboard-launched execution

The current "launch verify from dashboard" workflow is mainly a convenience we
built during local prototyping. Supporting that properly in the mature
remote/repo seam model would be a significant product area of its own.

It would likely require:

- project source syncing or deployable project artifacts
- server-side execution workers and sandbox lifecycle management
- revision pinning and environment selection
- secret injection and credential management
- remote filesystem/object staging for specs and outputs

That is not part of this plan.

The current dashboard-launched execution path may continue to exist for local
prototyping, but only if it does not distort the remote service architecture or
delay the clean producer-plane versus service-plane split.

## Product Principles

### 1. Repo owns intent

The repository should define:

- project identity
- how it connects to the `mentalmodel` service
- how its dashboard should look
- how its workflows should be launched by default

### 2. Service owns state

The service should own:

- registered projects
- workspaces / organizations
- remote run manifests and artifact storage
- live execution sessions
- catalog snapshots used by the hosted dashboard

### 3. Python remains the source of truth for rich behavior

We should not force full runtime environments or dashboard semantics into raw
TOML. Python should still construct:

- runtime environments
- workflow factories
- project catalogs

But the hosted service should consume serialized contracts derived from those
Python definitions, not import the repo directly.

## Target Architecture

There are two planes:

### Producer plane

The app repo, such as Pangramanizer.

Responsibilities:

- declare project config
- define catalog/provider metadata
- define workflow specs
- build runtime environments
- emit live execution events
- materialize completed run bundles
- upload live/completed data to the remote backend

### Service plane

The hosted `mentalmodel` service.

Responsibilities:

- authenticate producers
- register and update project metadata
- store catalog snapshots
- ingest live records/spans/metrics/status events
- ingest completed run bundles
- index projects, runs, invocations, and environments
- render the hosted dashboard/API entirely from remote state

## Recommended Repo-Owned Config

Add a repo-root `mentalmodel.toml`.

This file is the service-facing project config, not a replacement for workflow
specs.

Illustrative shape:

```toml
[project]
project_id = "pangramanizer"
label = "Pangramanizer"
description = "Mentalmodel-native training workflows for Pangramanizer."

[remote]
server_url = "https://mentalmodel.example.com"
api_key_env = "MENTALMODEL_API_KEY"
default_environment = "prod"

[catalog]
provider = "pangramanizer.mentalmodel_training.dashboard:catalog"
publish_on_link = true

[runs]
default_runs_dir = ".runs"

[verify]
default_spec = "pangramanizer/mentalmodel_training/verification/real_smoke.toml"
```

### What belongs here

- stable project identity
- remote endpoint and auth env var names
- catalog provider entrypoint
- default runs dir
- default invocation behavior
- default service environment selection

### What should not belong here

- rich Python object graphs
- full runtime environment construction details
- inline catalog metadata copied out of Python by hand
- user-facing toggles for core hosted behavior that should be service defaults,
  such as automatic completed-run upload or live execution streaming

### Service defaults versus repo config

The mature hosted path should assume:

- completed runs upload automatically when remote mode is configured
- live execution streams when the selected producer and backend support it

Those should be product defaults, not early user-facing booleans in
`mentalmodel.toml`.

If we later discover a real need for explicit opt-out controls, we can add them
with clear semantics and operational justification. They should not be part of
the first contract.

## Recommended User Workflow

### One-time or occasional setup

1. Repo declares `mentalmodel.toml`.
2. Operator runs:

```bash
mentalmodel remote link
```

This should:

- read `mentalmodel.toml`
- authenticate against the remote service
- register the project if missing
- update server-side project metadata if changed
- publish the current catalog snapshot

### Day-to-day workflow

Operators and agents should run:

```bash
mentalmodel verify --spec ...
```

If remote mode is configured for the repo:

- live events stream to the remote backend during execution
- completed run bundle uploads automatically at the end
- the hosted dashboard already knows the project and catalog

No manual `projects add` or `remote sync` should be necessary for the common
hosted path.

## New Product Concepts

### 1. Project Link

A project should be linked to the service explicitly.

Proposed command:

```bash
mentalmodel remote link
```

Responsibilities:

- read `mentalmodel.toml`
- resolve project identity
- authenticate
- create or update the remote project record
- publish the catalog snapshot
- optionally validate the default verify spec and runs dir

This command should be idempotent.

### 2. Catalog Snapshot

Hosted dashboards should not import the repo’s Python catalog provider at
request time.

Instead:

- the repo still owns a Python `ProjectCatalog`
- `mentalmodel remote link` serializes that catalog into a stable JSON contract
- the service stores that snapshot
- the hosted dashboard reads the stored contract

This is the key move that makes hosted dashboards independent of local repo
imports.

### 3. Live Execution Ingestion

The hosted service should understand in-progress runs, not just completed
bundles.

The producer should stream:

- execution records
- spans
- status updates
- summary deltas
- optional output snapshots where appropriate

The service should render a live run state from those events.

### 4. Completed Bundle Commit

At the end of a run, the producer should still upload the canonical run bundle.

This should:

- preserve the current `.runs` model
- give the service a durable final artifact set
- allow replay and bundle inspection to stay consistent with the CLI

### 5. Invocation and Environment Identity

The hosted product should distinguish:

- `project_id`
- `graph_id`
- `invocation_name`
- `runtime_profile`
- service environment name, such as `dev`, `staging`, or `prod`

This separation is already directionally present in the current package and
should become more central in the hosted model.

## Data Model Evolution

### Remote project record

The remote backend should store something like:

- `project_id`
- `label`
- `description`
- `workspace_id` / tenant linkage
- `default_environment`
- `catalog_snapshot`
- `catalog_version`
- `source_repo_url` optionally later
- `updated_at_ms`

### Remote run record

The remote backend should already continue storing:

- `project_id`
- `graph_id`
- `run_id`
- `invocation_name`
- environment name
- run status
- timestamps
- artifact descriptors

But it should also support live-session state such as:

- latest event sequence
- live status
- current node statuses
- partial summaries / metrics

### Remote catalog snapshot

The snapshot should include the hosted dashboard contract:

- entries
- metric groups
- pinned nodes
- custom views
- default loop node ids

This should be the same logical schema used by the local UI service, just
serialized and stored remotely.

## CLI Evolution

### Keep current commands

These remain useful:

- `mentalmodel verify`
- `mentalmodel remote sync`
- `mentalmodel projects add`
- `mentalmodel remote up`

### Add new commands

#### `mentalmodel remote link`

Main hosted setup command.

Responsibilities:

- read repo config
- register project remotely
- upload catalog snapshot
- validate remote connectivity

#### `mentalmodel remote status`

Show:

- linked project
- remote server
- last catalog publish time/version
- whether live upload is enabled

#### `mentalmodel remote publish-catalog`

Useful when:

- custom views changed
- pinned nodes changed
- a CI deployment should refresh the service-side dashboard contract

### Deprecation direction

The current local workspace/project registration commands should remain for:

- local development
- multi-repo localhost dashboards
- test environments

But they should become clearly “local/dev stack” workflows rather than the main
hosted product pattern.

## UI/Dashboard Implications

### Hosted dashboard should become remote-first

The hosted UI should read:

- project records from the remote backend
- catalog snapshots from the remote backend
- live sessions from the remote backend
- completed runs from the remote backend

It should not need:

- direct filesystem access to repo roots
- local Python catalog imports
- local workspace config in the common hosted path

### Local dashboard still matters

The current local stack remains valuable for:

- offline or air-gapped use
- local development
- tests
- rapid iteration on custom views before publishing catalog snapshots

## Authentication and Credentials

The producer repo should provide:

- remote server URL
- env var name for API key or token

The actual secret should remain in env or a secret manager, not in repo config.

Later hosted-service features may add:

- org/workspace-scoped tokens
- per-project tokens
- CI-issued short-lived upload tokens

## Live vs Completed Ingestion Model

We should support both.

### Live stream

Best for:

- realtime dashboard visibility
- long-running training workflows
- Datadog-like operator experience

Payload classes:

- records
- spans
- status transitions
- summary checkpoints

### Completed bundle

Best for:

- canonical artifact completeness
- replay
- durable final inspection
- parity with the local CLI/bundle model

The completed bundle should not replace live streaming. It should close the run.

## Migration Plan

The phases below are intended to be the implementation roadmap, not just a
strategy sketch. Each phase should leave behind concrete product behavior,
durable interfaces, and verification artifacts that the next phase can build
on.

### Phase 1: Repo Identity and Remote Link

#### Goal

Establish the repo-owned contract and the service-owned project record.

#### Deliverables

- repo-root `mentalmodel.toml`
- project config loader and validation
- `mentalmodel remote link`
- remote project record schema and write API
- remote status/read API

#### Producer-plane work

- define the first `mentalmodel.toml` contract
- load and validate project identity, remote endpoint, auth env var name, runs
  dir, catalog provider, and default verify spec
- add `mentalmodel remote link`
- add `mentalmodel remote status`

#### Service-plane work

- add persistent remote project records keyed by `project_id`
- store label, description, default environment, and linkage metadata
- add idempotent create/update semantics for project link

#### CLI and UX work

- `mentalmodel remote link` should:
  - discover repo config
  - authenticate
  - create or update the remote project
  - surface clear success/failure messaging
- `mentalmodel remote status` should show:
  - linked project
  - server URL
  - last link/update time
  - whether a catalog snapshot is published

#### Acceptance criteria

- one repo can link itself to a remote service without editing a local
  `workspace.toml`
- rerunning `mentalmodel remote link` is idempotent
- the remote service lists the linked project even before any runs are uploaded
- official docs are updated with the `mentalmodel.toml` contract and link flow

#### Explicitly out of scope

- automatic run upload
- live execution streaming
- hosted dashboard-launched execution

### Phase 2: Remote Catalog Snapshots

#### Goal

Move hosted dashboard semantics off local Python imports and onto a serialized,
remote-owned catalog contract.

#### Deliverables

- serialization contract for `ProjectCatalog`
- catalog snapshot storage in the remote backend
- catalog publish path from `mentalmodel remote link`
- hosted dashboard reads remote catalog snapshots

#### Producer-plane work

- serialize Python-defined catalog/provider output into a stable JSON contract
- validate that custom views, metric groups, pinned nodes, and default loop
  node ids are serializable
- publish the snapshot during `remote link`
- add `mentalmodel remote publish-catalog`

#### Service-plane work

- store current catalog snapshot and version for each project
- support catalog replacement/update without recreating the project
- expose project + catalog snapshot data to the hosted UI/API

#### UI work

- hosted dashboard reads remote project + catalog state first
- remove any hosted-path dependence on local repo filesystem imports
- preserve the existing local workspace import path for dev-only usage

#### Acceptance criteria

- a hosted dashboard can render project entries, pinned nodes, metric groups,
  and custom views without importing the repo locally
- catalog updates can be republished without relinking the whole project
- official docs are updated with the snapshot model and publish workflow

#### Explicitly out of scope

- automatic run upload
- live execution streaming
- executing specs from the hosted dashboard

### Phase 3: Automatic Completed-Run Upload

#### Goal

Make completed runs arrive in the hosted product automatically, without manual
`remote sync`.

#### Deliverables

- producer-side completed-run upload sink
- upload wiring from `mentalmodel verify`
- remote completed-run ingest API
- idempotent bundle upload semantics

#### Producer-plane work

- detect linked remote project configuration automatically
- upload the completed run bundle after local `.runs` materialization succeeds
- preserve the local `.runs` bundle as the canonical completed artifact
- expose enough metadata to correlate local bundle path and remote run record

#### Service-plane work

- ingest completed runs under the linked remote project
- store bundle descriptors, summaries, graph artifacts, and replay inputs
- deduplicate or safely overwrite repeated uploads of the same completed run

#### CLI and UX work

- `mentalmodel verify` should not require extra flags in the common linked path
- `mentalmodel remote sync` remains as a fallback/manual recovery command, not
  the normal operator flow
- `mentalmodel remote status` should expose last successful completed-run upload

#### Acceptance criteria

- a completed run appears in the hosted dashboard without a manual
  `mentalmodel remote sync`
- the same run remains fully inspectable locally through `.runs`
- upload failures do not corrupt the local canonical bundle
- official docs are updated with normal versus recovery upload flows

#### Explicitly out of scope

- live execution visibility before run completion

### Phase 4: Live Execution Streaming

#### Goal

Make long-running workflows visible in the hosted dashboard while they are
still executing.

#### Deliverables

- producer-side live execution sink
- remote live session and event-ingest model
- hosted UI support for live run status, records, and metric updates
- completed-run closeout path that reconciles live and final states

#### Producer-plane work

- stream live execution records, spans, status transitions, and summary deltas
- assign stable correlation between the live session and the eventual completed
  bundle
- degrade gracefully when live upload is unavailable while still preserving
  local execution

#### Service-plane work

- persist live session state keyed by project, graph, invocation, and run id
- support ordered event ingestion and reconnection safety
- materialize current run state for UI queries
- mark live sessions closed when the completed bundle lands

#### UI work

- show in-progress runs before completion
- update logs/records, metrics, graph state, and invariant status live
- transition cleanly from live state to final completed-run inspection

#### Acceptance criteria

- a long-running workflow is visible in the hosted UI before completion
- the hosted UI shows meaningful live progress without waiting for bundle upload
- completed-run inspection remains consistent with the final `.runs` artifact
- official docs are updated with live-versus-completed semantics

#### Explicitly out of scope

- server-side remote execution of workflows

### Phase 5: Local and Hosted Mode Cleanup

#### Goal

Make the product model legible: local workspace mode remains valuable, but the
hosted repo-linked path becomes the primary documented experience.

#### Deliverables

- clearer mode boundaries in docs, doctor, and CLI help
- explicit local/dev versus hosted/service terminology
- compatibility cleanup for `workspace.toml`, `projects add`, and `remote sync`

#### Producer-plane work

- make repo-linked hosted mode the primary path in repo examples
- keep local workspace flows available for offline/dev use

#### Service-plane work

- ensure hosted dashboards can operate without any local workspace registration
- keep local stack commands working for development and tests

#### CLI and docs work

- clarify when to use:
  - `remote link`
  - `remote status`
  - `remote publish-catalog`
  - `remote sync`
  - `projects add`
- add doctor/help surfaces that tell users whether they are in local or hosted
  mode and what is missing

#### Acceptance criteria

- users can clearly choose between:
  - local dev stack
  - hosted remote service
- the primary docs no longer imply that local workspace registration is the
  standard hosted setup path
- official docs are updated with the final local-versus-hosted mode model

### Phase 6: Hosted-Service Hardening and Operator Readiness

#### Goal

Turn the linked remote path into something teams can depend on day to day for
real workflows, not just an architectural proof.

#### Deliverables

- failure-mode handling and retries for link, catalog publish, completed upload,
  and live streaming
- project and run observability for the service itself
- operator-facing guidance for diagnosing remote ingestion issues
- migration guide from the current local-workspace MVP

#### Producer-plane work

- resilient retry/backoff behavior where appropriate
- explicit local buffering/fallback behavior for transient remote outages
- deterministic reporting when remote delivery partially succeeds or fails

#### Service-plane work

- operational visibility into project registration, upload failures, and live
  session health
- durable API contracts and schema versioning guidance
- safe handling of catalog schema evolution

#### Acceptance criteria

- a repo can link once and then use normal `mentalmodel verify --spec ...`
  workflows without repeated service-specific setup
- the hosted dashboard is strong enough that users do not feel they need W&B
  just to understand training behavior
- local development workflows still remain available without compromising the
  hosted model
- official docs are updated with the hardened operator workflow

## Migration Path From Today

We should not break the current remote MVP immediately.

Recommended compatibility path:

1. Keep local workspace config + `projects add`.
2. Add repo-root config and `remote link`.
3. Add server-side project records and catalog snapshots.
4. Teach the hosted UI to prefer remote project records.
5. Keep `remote sync` as a fallback/manual recovery path.
6. Later add automatic upload and live streaming.

This avoids forcing a coordinated one-shot migration across all projects.

## Pangramanizer as the First Serious Consumer

Pangramanizer is a good proving case because it exercises:

- multi-step `StepLoop`
- runtime environments
- grouped metrics
- custom views
- training-oriented operator workflows
- long-running runs where live visibility matters

It should be one of the first repos to adopt:

- `mentalmodel.toml`
- `remote link`
- remote catalog snapshot publish
- automatic completed-run upload
- live execution streaming

## Risks and Design Traps

### 1. Over-declarative config

Do not try to make `mentalmodel.toml` define full runtime environments or
complex Python object construction.

That would weaken typing and make failures harder to reason about.

### 2. Keeping hosted catalogs dependent on local imports

If hosted dashboards still require importing a repo-local Python module at
request time, we have not really crossed the product boundary.

### 3. Treating completed-bundle upload as sufficient

That will never feel as realtime or useful as Datadog/W&B for long-running
training jobs. Live streaming matters.

### 4. Losing the value of `.runs`

The local bundle model is an asset. The hosted service should build on it, not
replace it with an opaque alternative.

## Acceptance Criteria for the Mature Model

We should consider the service pattern mature when all of these are true:

1. A repo can declare its `mentalmodel` project identity and remote config in
   repo-owned config.
2. A one-time explicit link command registers the project remotely.
3. The hosted dashboard lists that project without editing a local workspace
   file.
4. The hosted dashboard uses a remote catalog snapshot, not a local repo import.
5. Completed runs appear remotely without manual `remote sync`.
6. Long-running workflows are visible live during execution.
7. Local workspace-driven dashboards still remain available for development and
   offline use.

## Immediate Next Steps

1. Finalize the initial `mentalmodel.toml` contract for Phase 1.
2. Define the remote project record schema and `remote link` API shape.
3. Define the serialized `ProjectCatalog` snapshot contract for Phase 2.
4. Break out implementation tasks for:
   - producer CLI/config work
   - remote backend schema/API work
   - hosted dashboard remote-project loading
5. Start Phase 1 before adding any more local-workspace conveniences.

This order keeps identity, project registration, and catalog ownership clean
before we layer upload and streaming behavior on top of them.
