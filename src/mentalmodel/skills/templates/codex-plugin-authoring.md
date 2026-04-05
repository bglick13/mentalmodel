---
name: mentalmodel-plugin-authoring
description: Use when adding or modifying mentalmodel plugins. Covers plugin registry expectations, provenance rules, lowering patterns, and how to prove a plugin end to end through IR, docs, and tests.
---

# mentalmodel Plugin Authoring

Use this skill when creating or changing a `mentalmodel` extension primitive.

## Plugin contract

- Implement `kind`, `origin`, and `version`.
- `supports()` must be precise and non-overlapping with other plugins.
- `lower()` must emit canonical IR only.
- Do not bypass `LoweringContext`; provenance must be stamped by lowering.
- If the plugin owns runtime behavior, compile it through the executable plugin
  path instead of bypassing the execution plan.

## Reference model

Use `RuntimeContextPlugin` as the reference shape:

- plugin primitive owns authored extension data
- plugin lowers to IR nodes and containment edges
- nested core nodes inherit structural metadata
- docs and analysis consume provenance from metadata, not runtime objects

## Required proof points

- registry can resolve the plugin
- lowered nodes include `plugin_kind` and `plugin_origin`
- docs surface plugin provenance
- missing plugin registration fails lowering
- provenance regressions fail analysis
- executable plugins emit normal runtime records, spans, metrics, and `.runs`
  artifacts through the shared runtime path

## Workflow

1. Add or update the plugin class and primitive type.
2. Lower the extension through `LoweringContext`.
3. Add direct lowering tests.
4. Add analysis/doc tests proving provenance is visible.
5. Run:

```bash
uv run mentalmodel check --entrypoint <module:function>
uv run mentalmodel docs --entrypoint <module:function>
uv run pytest
```
