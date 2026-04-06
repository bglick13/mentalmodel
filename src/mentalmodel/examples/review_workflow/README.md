# Review Workflow Reference

This package is the serious Phase 23 reference workflow for `mentalmodel`.

It is meant to demonstrate a real authored shape, not a one-file primitive
demo.

## What it exercises

- reusable loop body composition with `Block` + `Use`
- sequential multi-step execution with `StepLoop`
- typed runtime binding with `RuntimeEnvironment`, `RuntimeProfile`, and
  `ResourceKey`
- spec-driven CLI verification
- frame-aware run inspection via `.runs`

## Entry points

- workflow factory:
  `mentalmodel.examples.review_workflow.program:build_program`
- runtime environment factory:
  `mentalmodel.examples.review_workflow.program:build_environment`

## Verify

Fixture profile:

```bash
uv run mentalmodel verify \
  --spec src/mentalmodel/examples/review_workflow/review_workflow_fixture.toml
```

Strict profile:

```bash
uv run mentalmodel verify \
  --spec src/mentalmodel/examples/review_workflow/review_workflow_strict.toml
```

## Inspect

```bash
uv run mentalmodel runs latest --graph-id review_workflow
uv run mentalmodel runs outputs --graph-id review_workflow --node-id queue_summary
uv run mentalmodel replay --graph-id review_workflow --loop-node-id ticket_review_loop
uv run mentalmodel runs outputs \
  --graph-id review_workflow \
  --node-id ticket_review_loop.review_step.review_audit \
  --frame-id ticket_review_loop[1]
```
