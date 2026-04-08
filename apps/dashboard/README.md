# mentalmodel dashboard

React/TypeScript frontend for the hosted `mentalmodel` dashboard surface.

## Purpose

This app is the human-facing UI on top of the same persisted `.runs`, replay,
and verification services that power the CLI. The dashboard is catalog-driven:
the backend exposes launchable specs plus UI hints, and the frontend renders a
generic observability shell around them.

The built-in proof target is the `review_workflow` example:

- launch either runtime environment from the UI
- inspect the lowered graph
- inspect node inputs and outputs
- inspect invariant status and loop frames
- browse the full semantic record stream
- pivot from grouped metrics into the node and frame that produced them

## Architecture

- frontend: React + Vite in this directory
- backend API: `mentalmodel.ui.api:create_dashboard_app`
- shared service layer: `src/mentalmodel/ui/service.py`

The frontend should stay thin. It should not invent a separate run model that
diverges from persisted `.runs`, replay reports, run summaries, or catalog
metadata.

## Local verification

```bash
npm install
npm run typecheck
npm run build
```

## Local dev mode

Run the Vite frontend and the Python backend together from this directory:

```bash
npm install
npm run dev:stack
```

That starts:

- Vite on `http://127.0.0.1:5173`
- `mentalmodel ui` on `http://127.0.0.1:8765`

The Vite dev server proxies `/api/*` to the backend, so you can iterate on the
dashboard without rebuilding `dist` on every change.

If you want the backend separately:

```bash
npm run dev:backend
```

## Static build path

Then, from the repository root:

```bash
uv run mentalmodel ui --open-browser
```
