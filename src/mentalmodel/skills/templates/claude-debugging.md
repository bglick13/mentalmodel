---
name: mentalmodel-debugging
description: Use when debugging a mentalmodel workflow or generated artifact. Covers IR inspection, verification flow, runtime tracing/records, and how to follow a bug from docs back to authored primitives.
---

# mentalmodel Debugging

- start with `mentalmodel check`, `docs`, and `verify`
- use `mentalmodel runs show --graph-id <graph_id>` for the latest bundle
- use `mentalmodel runs records --graph-id <graph_id> --node-id <node_id>` for semantic events
- use docs to find the suspicious node
- trace its inputs to producer nodes
- use semantic execution records before generic traces
- read `verification.json`, `records.jsonl`, and `outputs.json` before
  `otel-spans.jsonl`
