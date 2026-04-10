---
name: mentalmodel-dashboard-authoring
description: Use when authoring or refining mentalmodel dashboard catalogs, metric groups, pinned nodes, and custom views. Covers when to choose charts vs tables, how to design stable row sources, and how to prove a dashboard surface against live and persisted runs.
---

# mentalmodel Dashboard Authoring

Use this skill when a project is shaping a dashboard surface on top of
`mentalmodel`.

## What this skill is for

- dashboard catalogs
- grouped metric rails and iteration charts
- pinned drill-down nodes
- custom views over reporting outputs

## Decision rules

Use metric groups when:

- the signal is numeric
- operators care about trends by iteration
- a chart or latest-value rail is the right surface

Use pinned nodes when:

- a node is a stable debugging entrypoint
- operators should jump directly to its inputs, outputs, or trace

Use custom views when:

- each row matters independently
- text or mixed columns matter
- operators need tables like sample quality, eval history, or checkpoint history

## Preferred dashboard shape

1. expose stable reporting nodes from the workflow
2. derive grouped metrics from those stable outputs
3. define custom views over stable row lists
4. keep raw records and node drill-down as the fallback, not the primary surface

## Custom-view rules

- prefer `row_source.kind="node_output_items"`
- prefer `DashboardValueSelector(kind="row_item", ...)`
- use `node_output` only when another stable reporting node is the real source
- avoid `record_payload` unless the record contract is intentionally stable

## Verification loop

1. run the workflow with `mentalmodel verify --spec ...`
2. open the dashboard with `mentalmodel ui`
3. confirm grouped metrics render
4. confirm custom views render on the completed run
5. for live workflows, confirm the same view updates before final bundle handoff

## Reference docs

- `guides/dashboard-ui.mdx`
- `guides/custom-views.mdx`
- `docs/recipes/custom-view-authoring.md`
