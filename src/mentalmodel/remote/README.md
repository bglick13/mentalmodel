# mentalmodel.remote

Phase 0 contract package for the remote runs data plane.

## Purpose

This package defines the typed contracts that later phases will implement:

- canonical run manifests
- named artifact descriptors
- project/workspace registration
- project-scoped catalog provider shape

It intentionally does **not** implement transport, storage, or runtime sinks
yet. Those belong to later phases once the contract is stable.

## Main entrypoints

- `RunManifest`
- `ArtifactDescriptor`
- `RunTraceSummary`
- `ProjectRegistration`
- `ProjectCatalog`
- `WorkspaceConfig`

## Boundaries

- Keep these types transport-neutral.
- Keep dashboard JSON shapes and storage-specific details at the edges.
- Treat local `.runs` and future remote storage as two backends for the same
  manifest/artifact model.

## Verification

Run:

```bash
uv run pytest tests/test_remote_contracts.py
```
