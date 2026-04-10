# Custom View Authoring

Use this recipe when a project needs a dashboard table that is more useful than
raw records or a numeric metric rail.

## Goal

Answer:

- Which stable node should back this table?
- Should the table read row items directly or pull related values from another
  node?
- Will the same view still make sense while the run is live?

## Recommended flow

1. Normalize reporting data first.

Prefer a dedicated reporting node that emits one stable list of row items.

2. Add the custom view to the catalog.

Use:

- `row_source.kind="node_output_items"`
- `DashboardValueSelector(kind="row_item", ...)` for most columns

3. Verify the run and inspect it in the dashboard.

```bash
uv run mentalmodel verify --spec path/to/spec.toml
uv run mentalmodel ui --catalog-entrypoint mypkg.dashboard:catalog
```

4. For live workflows, confirm the view updates while the run is still active.

## Rules

- prefer `row_item` selectors over `record_payload`
- use stable reporting nodes instead of scraping deep internal outputs
- keep row items JSON-compatible
- choose metric groups for numeric trend signals and custom views for row-based
  operator inspection
