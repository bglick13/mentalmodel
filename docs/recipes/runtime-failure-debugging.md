# Runtime Failure Debugging

Use this recipe when execution fails, a node throws, or a workflow succeeds but
produces suspicious outputs.

## Goal

Answer:

- Which node failed?
- What inputs were bound for that node?
- Did the failure happen in authored logic, plugin execution, or configuration?

## Recommended flow

1. Run preflight checks.

```bash
uv run mentalmodel doctor --entrypoint <module:function>
```

2. Run verification.

```bash
uv run mentalmodel verify --entrypoint <module:function>
```

3. Resolve the newest run and inspect the bundle.

```bash
uv run mentalmodel runs latest --graph-id <graph_id>
uv run mentalmodel runs show --graph-id <graph_id>
```

4. Inspect semantic records before spans.

```bash
uv run mentalmodel replay --graph-id <graph_id>
uv run mentalmodel runs records --graph-id <graph_id> --node-id <node_id>
uv run mentalmodel runs trace --graph-id <graph_id> --node-id <node_id>
```

5. Inspect the exact node payloads.

```bash
uv run mentalmodel runs inputs --graph-id <graph_id> --node-id <node_id>
uv run mentalmodel runs outputs --graph-id <graph_id> --node-id <node_id>
```

6. Only after the semantic records are clear, inspect tracing config or spans.

```bash
uv run mentalmodel otel show-config
```

## Common failure classes

- missing upstream dependency or wrong `Ref(...)`
- effect handler exception
- invariant failure
- plugin-execution failure
- tracing/config error that does not affect semantic execution

## Rule

Semantic records are the primary debugging source. Traces are the export layer.
Do not start in OTEL unless the failure is specifically about OTEL export.
