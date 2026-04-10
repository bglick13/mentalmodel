# mentalmodel.remote

Remote runs data-plane package for contracts, sink seams, and the first bundle
upload path, plus the first repo-owned project-link seam for the hosted model.

## Purpose

This package defines the typed contracts and the first transport/storage
primitives for remote-compatible run handling:

- canonical run manifests
- named artifact descriptors
- completed-run and execution-record sink interfaces
- project/workspace registration
- repo-owned `mentalmodel.toml` project config loading
- remote project records and catalog snapshot publication
- project-scoped catalog provider shape
- canonical run bundle upload payloads
- a deterministic file-backed remote ingest store
- a Postgres-backed manifest index
- an S3-compatible object-store backend for artifact blobs
- a remote-backed run repository that materializes bundles into a local cache
- a CLI/API sync path for uploading persisted local runs
- workspace TOML load/write helpers for one shared stack
- localhost bootstrap and doctor flows for the remote MVP

The durable Phase 2 backend stores:

- manifest/index rows in Postgres
- artifact bytes in S3-compatible object storage
- a local read cache under `.runs` only as a materialization layer for the
  existing inspection helpers

The older file-backed store remains in-tree as a deterministic fallback for
tests and local transition scenarios.

## Main entrypoints

- `RunManifest`
- `ArtifactDescriptor`
- `RunTraceSummary`
- `ExecutionRecordSink`
- `CompletedRunSink`
- `ProjectRegistration`
- `RemoteProjectLinkRequest`
- `RemoteProjectRecord`
- `ProjectCatalogSnapshot`
- `ProjectCatalog`
- `WorkspaceConfig`
- `MentalModelProjectConfig`
- `RunBundleUpload`
- `RemoteBackendConfig`
- `RemoteProjectStore`
- `RemoteRunStore`
- `FileRemoteRunStore`
- `load_project_config`
- `link_project_to_server`
- `build_run_bundle_upload`
- `sync_runs_to_server`
- `load_workspace_config`
- `write_workspace_config`

## Boundaries

- Keep these types transport-neutral.
- Keep dashboard JSON shapes and storage-specific details at the edges.
- Treat local `.runs` and future remote storage as two backends for the same
  manifest/artifact model.
- Keep the read cache as an implementation detail, not the source of truth.
- Keep the file-backed store deterministic so it can serve as a testable
  fallback alongside the real backend.

## Current flow

1. Local verification persists a run in `.runs/<graph_id>/<run_id>/...`.
2. `build_run_bundle_upload(...)` resolves that run and emits a canonical
   `RunManifest` plus base64-encoded artifact bodies.
3. `POST /api/remote/runs` accepts the upload payload and validates it.
4. `RemoteRunStore.ingest(...)` uploads artifact blobs to S3-compatible object
   storage, stores the indexed manifest/read-model row in Postgres, and
   materializes the bundle into a local cache.
5. The dashboard read APIs inspect the indexed remote run through the same
   historical run surfaces by materializing bundles from the remote backend into
   the cache on demand.
6. `FileRemoteRunStore.ingest(...)` remains available as a deterministic fallback.
7. `mentalmodel remote write-demo` generates a workspace registry plus helper
   scripts for one local multi-project stack.
8. `mentalmodel remote link` reads repo-owned `mentalmodel.toml`, resolves the
   configured catalog provider, and publishes a remote project record plus the
   current catalog snapshot.

## Verification

Run:

```bash
uv run pytest \
  tests/test_remote_bootstrap.py \
  tests/test_remote_contracts.py \
  tests/test_remote_sinks.py \
  tests/test_remote_store.py \
  tests/test_ui_api.py \
  tests/test_ui_workspace.py \
  tests/test_cli.py
```
