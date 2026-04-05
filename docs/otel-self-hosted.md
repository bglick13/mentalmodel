# Self-Hosted OTEL Demo

`mentalmodel` supports first-class trace export plus local semantic run bundles.

The recommended self-hosted demo path is Grafana's LGTM stack because it gives
you:

- OTLP HTTP ingest on `http://localhost:4318`
- OTLP gRPC ingest on `http://localhost:4317`
- a Grafana UI on `http://localhost:3000`

`mentalmodel` still persists `.runs/<graph_id>/<run_id>/` locally, so the
recommended debugging story is:

1. Use Grafana or Jaeger to visualize traces.
2. Use `.runs/.../records.jsonl`, `outputs.json`, and `verification.json` for
   semantic debugging.

Important:

- for OTLP HTTP trace export, the endpoint should end in `/v1/traces`
- the packaged env files now set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces`
- if you previously sourced an older env file, re-source the new file or export
  the corrected endpoint manually

## Quick start

Write the demo assets:

```bash
uv run mentalmodel otel write-demo --stack lgtm --output-dir /tmp/mentalmodel-otel
```

Start LGTM:

```bash
cd /tmp/mentalmodel-otel
docker compose -f docker-compose.otel-lgtm.yml up -d
```

Export the tracing env:

```bash
set -a
source mentalmodel.otel.env
set +a
```

Inspect the resolved config:

```bash
uv run mentalmodel otel show-config
```

Run the async RL demo with OTLP export enabled:

```bash
uv run mentalmodel verify --entrypoint mentalmodel.examples.async_rl.demo:build_program
```

Then open:

- Grafana: `http://localhost:3000`

## Jaeger alternative

If you want a lighter traces-only UI:

```bash
uv run mentalmodel otel write-demo --stack jaeger --output-dir /tmp/mentalmodel-jaeger
cd /tmp/mentalmodel-jaeger
docker compose -f docker-compose.otel-jaeger.yml up -d
set -a
source mentalmodel.otel.jaeger.env
set +a
uv run mentalmodel verify --entrypoint mentalmodel.examples.async_rl.demo:build_program
```

Then open:

- Jaeger: `http://localhost:16686`

## Design note

This milestone exports traces only.

That is intentional. `mentalmodel` already has a strong semantic-record model,
but it does not yet have a fully designed OTel metrics or OTel logs model. A
later milestone will scope those signals explicitly instead of adding them
prematurely.
