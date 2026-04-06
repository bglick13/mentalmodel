# mentalmodel dashboard

React/TypeScript frontend for the Phase 26 hosted dashboard proof.

## Purpose

This app is the human-facing UI on top of the same persisted `.runs`, replay,
and verification services that power the CLI. The initial proof target is the
`review_workflow` example:

- launch either runtime environment from the UI
- inspect the lowered graph
- inspect node inputs and outputs
- inspect invariant status and loop frames
- browse the full semantic record stream

## Architecture

- frontend: React + Vite in this directory
- backend API: `mentalmodel.ui.api:create_dashboard_app`
- shared service layer: `src/mentalmodel/ui/service.py`

The frontend should stay thin. It should not invent a separate run model that
diverges from persisted `.runs`, replay reports, or run summaries.

## Local verification

```bash
npm install
npm run typecheck
npm run build
```

Then, from the repository root:

```bash
uv run mentalmodel ui --open-browser
```
