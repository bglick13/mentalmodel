# mentalmodel OTEL Demo

This directory contains a self-hosted OpenTelemetry demo stack for
`mentalmodel`.

## Recommended stack: LGTM

Start the stack:

```bash
docker compose -f docker-compose.otel-lgtm.yml up -d
```

Export the tracing environment:

```bash
set -a
source mentalmodel.otel.env
set +a
```

Inspect the resolved tracing config:

```bash
uv run mentalmodel otel show-config
```

Run the demo workflow:

```bash
uv run mentalmodel verify --entrypoint mentalmodel.examples.async_rl.demo:build_program
```

Then open:

- Grafana UI: `http://localhost:3000`
- OTLP HTTP ingest: `http://localhost:4318`
- OTLP gRPC ingest: `http://localhost:4317`

`mentalmodel` continues to persist semantic records under `.runs/...`, so you
can debug with both the external UI and the local bundle.

## Jaeger alternative

Start the traces-only alternative:

```bash
docker compose -f docker-compose.otel-jaeger.yml up -d
```

Export the Jaeger environment:

```bash
set -a
source mentalmodel.otel.jaeger.env
set +a
```

Then open:

- Jaeger UI: `http://localhost:16686`

The Jaeger path is lighter and traces-only. LGTM is the better general demo if
you want a more complete observability UI footprint.
