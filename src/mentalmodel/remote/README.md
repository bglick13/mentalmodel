# mentalmodel.remote

Remote runs data-plane package for contracts, sink seams, and the first bundle
upload path.

## Purpose

This package defines the typed contracts and the first transport/storage
primitives for remote-compatible run handling:

- canonical run manifests
- named artifact descriptors
- completed-run and execution-record sink interfaces
- project/workspace registration
- project-scoped catalog provider shape
- canonical run bundle upload payloads
- a deterministic file-backed remote ingest store
- a CLI/API sync path for uploading persisted local runs
- workspace TOML load/write helpers for one shared stack
- localhost bootstrap and doctor flows for the remote MVP

The initial remote backend stays deliberately simple: local `.runs` bundles are
repackaged as `RunBundleUpload` payloads and ingested into another `.runs`-style
store plus a `.remote/manifests` index. That keeps the public model stable
before Postgres/object storage adapters arrive.

## Main entrypoints

- `RunManifest`
- `ArtifactDescriptor`
- `RunTraceSummary`
- `ExecutionRecordSink`
- `CompletedRunSink`
- `ProjectRegistration`
- `ProjectCatalog`
- `WorkspaceConfig`
- `RunBundleUpload`
- `FileRemoteRunStore`
- `build_run_bundle_upload`
- `sync_runs_to_server`
- `load_workspace_config`
- `write_workspace_config`

## Boundaries

- Keep these types transport-neutral.
- Keep dashboard JSON shapes and storage-specific details at the edges.
- Treat local `.runs` and future remote storage as two backends for the same
  manifest/artifact model.
- Keep the current file-backed store deterministic so it can serve as a testable
  stand-in for the later Postgres/object-store split.

## Current flow

1. Local verification persists a run in `.runs/<graph_id>/<run_id>/...`.
2. `build_run_bundle_upload(...)` resolves that run and emits a canonical
   `RunManifest` plus base64-encoded artifact bodies.
3. `POST /api/remote/runs` accepts the upload payload and validates it.
4. `FileRemoteRunStore.ingest(...)` writes the artifacts into a remote `.runs`
   tree and stores the manifest under `.remote/manifests/<graph_id>/<run_id>.json`.
5. The existing dashboard read APIs continue to inspect the ingested run through
   the same historical run surfaces.
6. `mentalmodel remote write-demo` generates a workspace registry plus helper
   scripts for one local multi-project stack.

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
