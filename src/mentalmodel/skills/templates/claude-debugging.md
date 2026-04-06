---
name: mentalmodel-debugging
description: Use when debugging a mentalmodel workflow or generated artifact. Covers IR inspection, verification flow, runtime tracing/records, and how to follow a bug from docs back to authored primitives.
---

# mentalmodel Debugging

- start with `mentalmodel check`, `docs`, and `verify`
- use `--params-json` or `--params-file` with `mentalmodel verify` when the
  entrypoint is a parameterized workflow factory
- use `--environment-entrypoint` when the runtime environment is built
  separately, and prefer `--spec` when the project already has a TOML
  invocation spec
- run `mentalmodel doctor` when setup, skills, entrypoints, or tracing may be wrong
- use `mentalmodel otel show-config` when tracing/export configuration looks suspicious
- use `mentalmodel runs latest --graph-id <graph_id>` to resolve the newest bundle
- use `mentalmodel replay --graph-id <graph_id>` to reconstruct the run timeline
- use `mentalmodel runs inputs --graph-id <graph_id> --node-id <node_id>` for persisted bound inputs
- use `mentalmodel runs outputs --graph-id <graph_id> --node-id <node_id>` for persisted outputs
- use `mentalmodel runs trace --graph-id <graph_id> --node-id <node_id>` for semantic events
- use `mentalmodel runs diff --graph-id <graph_id> --run-a <run_a> --run-b <run_b>` when debugging a regression between runs
- use `mentalmodel otel write-demo --stack lgtm --output-dir ...` when you need a fast local OTEL UI to demo traces
- use `mentalmodel runs repair --dry-run` if legacy bundle metadata looks stale
- use docs to find the suspicious node
- trace its inputs to producer nodes
- use semantic execution records before generic traces
- read `verification.json`, `records.jsonl`, and `outputs.json` before
  `otel-spans.jsonl`
- follow the recipe docs under `docs/recipes/` for structure, invariants,
  runtime failures, and run comparisons
