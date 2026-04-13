# Durable OTel-Native Live Ingestion Redesign

## Summary

Rework `mentalmodel` live ingestion so it is:

- **non-blocking on workflow execution**
- **durable and retryable**, not memory-buffered and lossy
- **OTel-native** for spans, metrics, and semantic records-as-logs
- **multi-destination** so the same live stream can power:
  - the `mentalmodel` hosted dashboard
  - Datadog export through standard collector/exporter paths

The core direction changes from the previous plan are:

1. **No event dropping as a normal overload policy**
   - replace in-memory best-effort buffering with a **durable local outbox**
   - live export becomes at-least-once, not “drop/coalesce under pressure”
2. **Semantic records become OTel-compatible logs**
   - records are modeled and exported as structured log records with stable semantic attributes
   - this makes OTel Collector the correct front door
   - this also makes Datadog export tractable without inventing a parallel adapter format

## Architecture

### Actors

- **Workflow runtime**
  - emits semantic records, spans, and metric points
  - must never block on remote network I/O
- **Producer outbox + exporter**
  - durable local queue for live telemetry batches
  - background sender drains the outbox to OTel Collector
  - owns retry, ack tracking, and exporter health
- **OTel Collector**
  - ingress point for OTLP logs, traces, and metrics
  - batches, retries, and routes to downstream systems
- **Durable event backbone**
  - Kafka-compatible bus behind the collector for hosted-service ingestion
- **Hosted analytics storage**
  - ClickHouse stores queryable run telemetry for dashboard APIs
- **Hosted metadata/control plane**
  - Postgres keeps project links, catalog snapshots, remote operation events, run metadata
- **Datadog exporter path**
  - collector exports the same logs/traces/metrics into customer Datadog accounts when configured

### Source of truth

- **Runtime domain objects**
  - `ExecutionRecord`, `RecordedSpan`, and metric points remain the runtime source of truth in-process
- **Canonical external live representation**
  - spans -> OTel spans
  - metrics -> OTel metrics
  - semantic records -> OTel logs
- **Producer durability**
  - local durable outbox is the source of truth until a batch is acknowledged by the collector pipeline
- **Hosted query source**
  - ClickHouse is the authoritative store for dashboard reads
- **Completed-run correctness**
  - local `.runs` bundle remains the authoritative finalized artifact set and recovery path
  - completed upload also indexes into the same hosted query model

### OTel semantic record model

Refactor semantic records so their canonical serialized form is an **OTel log record**, not an ad hoc JSON blob.

Each semantic record should map to:
- **log body**
  - small human-readable summary or compact structured body
- **log attributes**
  - `mentalmodel.run_id`
  - `mentalmodel.node_id`
  - `mentalmodel.frame_id`
  - `mentalmodel.loop_node_id`
  - `mentalmodel.iteration_index`
  - `mentalmodel.event_type`
  - `mentalmodel.sequence`
  - `mentalmodel.invocation_name`
  - `mentalmodel.runtime_profile`
  - stable payload fields promoted into indexed attributes when useful
- **resource attributes**
  - project/workflow/environment/catalog identity

The current JSON export remains as a derived/debugging representation, not the primary live transport contract.

### New core types / interfaces

- `LiveIngestionConfig`
  - `outbox_dir`
  - `max_outbox_bytes`
  - `max_batch_events`
  - `max_batch_bytes`
  - `flush_interval_ms`
  - `shutdown_flush_timeout_ms`
  - `require_live_delivery`
  - `otlp_endpoint`
  - `otlp_headers`
- `TelemetryEnvelope`
  - discriminated union for:
    - `semantic_log`
    - `trace_span`
    - `metric_point`
    - `run_lifecycle`
    - `delivery_health`
- `DurableOutbox`
  - append, claim batch, ack batch, retry batch, purge acknowledged
  - persisted on local disk
- `AsyncLiveExporter`
  - implements `LiveExecutionSink`
  - workflow thread only appends to the outbox and nudges the sender
  - sender thread/process performs OTLP export
- `LiveDeliveryStats`
  - queue depth
  - outbox bytes
  - oldest pending age
  - batch size
  - send latency
  - retry count
  - ack lag
  - degraded status
  - terminal exporter error
- `OtelLogMapper`
  - lossless conversion from `ExecutionRecord` to OTel log payload/attributes
- `TelemetryIndexer`
  - canonical mapping from OTel logs/traces/metrics into ClickHouse row shapes
  - shared by both live-consumer and completed-run indexing paths

### Public entrypoints

- `run_managed(...)`
  - keeps `enable_live_execution`
  - adds `live_ingestion: LiveIngestionConfig | None`
  - `live_execution_delivery` expands to include:
    - `delivery_mode`
    - `outbox_depth`
    - `outbox_bytes`
    - `ack_lag_ms`
    - `retry_count`
    - `last_error`
    - `degraded`
- current producer-facing live HTTP session API is removed from the critical path
- producer writes OTLP-compatible telemetry to collector instead of calling:
  - `/api/remote/live/sessions/start`
  - `/api/remote/live/sessions/{run_id}`

## Reliability and failure model

### Required guarantee

Normal overload must not be handled by dropping live events.

The new normal path is:
- runtime emits event
- event is durably written to local outbox
- workflow continues
- background exporter retries until acked

### Backpressure policy

- **network/back-end slowdown**
  - never blocks workflow on synchronous send
  - backlog accumulates in durable outbox
- **collector/downstream outage**
  - exporter goes degraded
  - retry continues from durable outbox
  - workflow continues
- **outbox nearing capacity**
  - emit critical delivery-health events
  - surface warning in dashboard/CLI immediately
- **outbox hard-cap exceeded**
  - explicit policy, not silent drop:
    - if `require_live_delivery=true`: fail the managed run with a clear delivery-capacity error
    - if `require_live_delivery=false`: continue workflow, mark live delivery failed-open, and stop accepting new live telemetry
- no silent event loss in the normal degraded path

### Why this is the right contract

This avoids the current failure mode:
- workflow blocked on localhost HTTP/JSON
while also avoiding the earlier proposed failure mode:
- unbounded lossy dropping under pressure

It gives a production-grade path closer to Datadog:
- agent/outbox durability
- background export
- retry until downstream recovery

## Topology

### Producer/edge

- runtime process
- durable local outbox
- async OTLP exporter

### Ingest and routing

- **OTel Collector is the primary ingest front door**
- collector responsibilities:
  - receive OTLP logs/traces/metrics
  - batch and retry
  - route to:
    - Kafka-compatible bus for hosted ingestion
    - Datadog exporter when configured
    - optional local diagnostics/exporter for dev

### Hosted backend

- Kafka-compatible topic(s) for telemetry ingestion
- ClickHouse for telemetry queries
- Postgres for metadata/control plane only

### Datadog integration

Datadog support should be implemented through the collector/export pipeline, not by building a separate app-side Datadog writer.

That means:
- spans and metrics flow through standard OTLP
- semantic records flow through OTLP logs
- customer can point collector/export config at their Datadog account
- `mentalmodel` hosted dashboard and Datadog can consume the same telemetry stream

## Implementation plan

### Phase 1: Canonical telemetry contract

- Define the OTel mapping for:
  - semantic records -> logs
  - spans -> spans
  - metrics -> metrics
- Add one central mapper module and semantic conventions for `mentalmodel.*` attributes.
- Keep existing JSON export only as a compatibility/debug view derived from the new canonical mapping.
- Add deterministic tests proving:
  - record-to-log mapping is lossless for the fields the dashboard/debugging needs
  - metric/span mapping preserves frame/iteration/runtime-profile semantics

### Phase 2: Durable producer outbox

- Replace the current synchronous `RemoteServiceLiveExecutionSink` with `AsyncLiveExporter`.
- Implement `DurableOutbox` as a local persistent queue.
  - acceptable first implementation: SQLite or append-only segment files with ack state
  - must support crash recovery and replay
- Workflow thread responsibilities:
  - map runtime events to canonical telemetry envelopes
  - append them durably
  - never perform remote HTTP on the hot path
- Sender responsibilities:
  - batch by size/time
  - export OTLP in the background
  - track acknowledgements and retry state
- Add delivery-health metrics/events from the exporter itself.

### Phase 3: OTel Collector integration

- Introduce collector config for:
  - OTLP receive
  - batching
  - retry
  - durable queue where supported
  - routing/export
- Producer default live target becomes collector, not `mentalmodel` API.
- Keep completed-run upload as-is.
- Add deterministic local stack recipe:
  - collector
  - Kafka-compatible bus
  - ClickHouse
  - hosted API/UI

### Phase 4: Hosted ingestion and query model

- Build consumers that materialize live telemetry from Kafka-compatible topics into ClickHouse.
- Replace Postgres live-session telemetry storage as the hot query path.
- ClickHouse row model should support:
  - records/logs by run/node/frame/iteration/time
  - spans by run/node/frame/iteration/profile/time
  - metrics by run/series/iteration/time
  - lifecycle and delivery-health summaries
- Dashboard APIs read from ClickHouse for live and completed runs through one query model.

### Phase 5: Completed-run indexing unification

- Completed bundle upload continues unchanged from the producer’s perspective.
- After bundle upload, index finalized records/spans/metrics into the same ClickHouse schema.
- Mark the run committed/finalized in hosted metadata.
- Hosted dashboard queries do not care whether rows originated from:
  - live OTLP ingestion
  - completed bundle indexing

### Phase 6: Datadog export

- Ship collector/export configuration and docs for Datadog.
- Ensure Datadog receives:
  - traces
  - metrics
  - semantic records as logs
- Define the supported field mapping and naming policy so customers get predictable dashboards and log search behavior.
- Validate with a deterministic staging pipeline that a managed run produces usable Datadog artifacts without special app-side code.

### Phase 7: Removal and cleanup

- Remove the old producer live-session API path from the normal live data plane.
- Remove synchronous live HTTP batching from `run_managed(...)`.
- Retain only:
  - completed bundle upload API
  - metadata/control APIs
  - hosted query APIs
- Update docs, packaged skills, and operator guidance to describe the new live/hosted/Datadog architecture.

## Test plan

### Unit

- durable outbox append/claim/ack/retry/recovery
- crash recovery with partially sent/unacked batches
- OTel log mapping for semantic records
- exporter health accounting
- hard-cap policy behavior for `require_live_delivery` true/false

### Integration

- managed workflow with live enabled does not block on remote slowdown
- collector outage causes backlog growth and degraded exporter state, not workflow stall
- collector recovery drains backlog successfully
- hosted ClickHouse queries return live records/spans/metrics correctly
- completed upload indexes into the same query model
- Datadog export path receives logs/traces/metrics with expected fields

### Performance

- high-record, loop-heavy workflow no longer stalls with live enabled
- runtime overhead with live enabled remains bounded and materially below the current synchronous implementation
- dashboard reads for long runs remain paginated/downsampled and do not require raw payload scans

### Acceptance criteria

- Pangramanizer detached `build-variants` with live enabled continues progressing under the local hosted stack
- disabling live is no longer the operational workaround
- local collector/hosted stack can be restarted during a run without losing already accepted live telemetry
- semantic records are queryable both in `mentalmodel` dashboard and in Datadog as logs
- completed upload still produces a correct finalized run even if live delivery was degraded during execution

## Assumptions and defaults

- OTel Collector is the correct primary ingest front door.
- Semantic records will be refactored into OTel-compatible logs rather than maintaining a separate bespoke live record transport.
- Event dropping is not an acceptable normal-pressure policy.
- The first durable outbox implementation can be local-disk backed and single-producer friendly; it does not need distributed coordination.
- Completed-run upload remains part of the product even after live ingestion is productionized, because it is valuable as finalized artifact transport and recovery, not just as a temporary workaround.
