---
name: mentalmodel-plugin-authoring
description: Use when adding or modifying mentalmodel plugins. Covers plugin registry expectations, provenance rules, lowering patterns, and how to prove a plugin end to end through IR, docs, and tests.
---

# mentalmodel Plugin Authoring

- implement `kind`, `origin`, `version`
- lower only into canonical IR
- keep provenance explicit in node metadata
- prove the plugin through lowering, analysis, docs, and tests
- if the plugin owns runtime behavior, compile it through the executable plugin
  path so it participates in records, spans, metrics, and `.runs`
