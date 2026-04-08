import type { ExecutionRecord, GenericSpan } from "../types";

/** Key/value rows for inspector panels (span drawer, record drawer, inline expand). */
export function executionRecordToRows(
  record: ExecutionRecord,
): Array<[string, string]> {
  return [
    ["event_type", record.event_type],
    ["node_id", record.node_id],
    ["frame_id", record.frame_id],
    ["timestamp (UTC)", new Date(record.timestamp_ms).toISOString()],
    ["sequence", String(record.sequence)],
    ["loop_node_id", record.loop_node_id ?? "—"],
    [
      "iteration_index",
      record.iteration_index != null ? String(record.iteration_index) : "—",
    ],
  ];
}

/** Full document for raw JSON block (matches standalone record drawer). */
export function executionRecordToDetailJson(record: ExecutionRecord): Record<string, unknown> {
  return {
    record_id: record.record_id,
    run_id: record.run_id,
    node_id: record.node_id,
    frame_id: record.frame_id,
    loop_node_id: record.loop_node_id,
    iteration_index: record.iteration_index,
    event_type: record.event_type,
    sequence: record.sequence,
    timestamp_ms: record.timestamp_ms,
    payload: record.payload,
  };
}

/** One-line description of the join keys (for explorer copy). */
export function formatSpanCorrelationScope(span: GenericSpan): string {
  const k = span.correlationKeys;
  return `run_id=${k.runId ?? "?"} · node_id=${k.nodeId} · frame_id=${k.frameId}`;
}

/**
 * Semantic records whose ``run_id``, ``node_id``, and ``frame_id`` match the span’s
 * correlation keys (same scope as one node execution in one frame).
 */
export function recordsMatchingSpanScope(
  records: ExecutionRecord[],
  span: GenericSpan,
): ExecutionRecord[] {
  const { runId, nodeId, frameId } = span.correlationKeys;
  return records
    .filter((r) => {
      if (runId != null && r.run_id !== runId) {
        return false;
      }
      if (r.node_id !== nodeId) {
        return false;
      }
      const rf = r.frame_id === "" ? "root" : r.frame_id;
      return rf === frameId;
    })
    .sort((a, b) => a.sequence - b.sequence);
}
