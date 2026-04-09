import type { GenericSpan, NodeDetail, SpanKindTag } from "../types";

/** Format nanoseconds as a human-readable duration (µs, ms, s). */
export function formatDurationNs(ns: number): string {
  if (!Number.isFinite(ns) || ns < 0) {
    return "n/a";
  }
  if (ns < 1_000) {
    return `${Math.round(ns)} ns`;
  }
  if (ns < 1_000_000) {
    return `${(ns / 1_000).toFixed(1)} µs`;
  }
  if (ns < 1_000_000_000) {
    return `${(ns / 1_000_000).toFixed(2)} ms`;
  }
  return `${(ns / 1_000_000_000).toFixed(3)} s`;
}

/** Format a wall-clock instant from nanoseconds since Unix epoch (OTel style). */
export function formatInstantNs(ns: number): string {
  if (!Number.isFinite(ns) || ns <= 0) {
    return "n/a";
  }
  const ms = ns / 1_000_000;
  try {
    return new Date(ms).toISOString();
  } catch {
    return String(ns);
  }
}

function readNumericField(
  span: Record<string, unknown>,
  keys: string[],
): number | null {
  for (const key of keys) {
    const candidate = span[key];
    if (typeof candidate === "number" && Number.isFinite(candidate)) {
      return candidate;
    }
    if (typeof candidate === "string" && candidate.length > 0) {
      const n = Number(candidate);
      if (Number.isFinite(n)) {
        return n;
      }
    }
  }
  return null;
}

function readAttributes(span: Record<string, unknown>): Record<string, unknown> {
  const raw = span.attributes;
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    return raw as Record<string, unknown>;
  }
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      /* ignore */
    }
  }
  return {};
}

function readStringField(
  attrs: Record<string, unknown>,
  keys: string[],
): string | null {
  for (const key of keys) {
    const v = attrs[key];
    if (typeof v === "string" && v.length > 0) {
      return v;
    }
  }
  return null;
}

function shortenId(s: string, max = 22): string {
  if (s.length <= max) {
    return s;
  }
  return `${s.slice(0, Math.max(12, max - 1))}…`;
}

function formatAttrValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "—";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function humanizeKey(key: string): string {
  return key
    .replace(/^mentalmodel\./, "")
    .replace(/_/g, " ")
    .trim();
}

function classifyNodeKind(kind: string | null | undefined): SpanKindTag {
  if (!kind || typeof kind !== "string") {
    return "other";
  }
  const k = kind.toLowerCase();
  if (k.includes("effect")) {
    return "effect";
  }
  if (k.includes("join")) {
    return "join";
  }
  if (k.includes("loop")) {
    return "loop";
  }
  if (k.includes("queue")) {
    return "queue";
  }
  if (k.includes("sink")) {
    return "sink";
  }
  return "other";
}

function hueFromKindAndNode(kind: string | null, nodeId: string): number {
  const s = `${kind ?? "?"}:${nodeId}`;
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (h * 31 + s.charCodeAt(i)) >>> 0;
  }
  return h % 360;
}

/** Short display title: avoid repeating the node id; prefer last dotted segments. */
function normalizeSpanTitle(rawName: string, nodeId: string): string {
  let t = rawName.trim();
  if (!t || t === nodeId) {
    return nodeId;
  }
  const prefix = `${nodeId}:`;
  if (t.startsWith(prefix)) {
    t = t.slice(prefix.length).trim();
  }
  if (t.includes(".")) {
    const parts = t.split(".").filter(Boolean);
    if (parts.length > 2) {
      return parts.slice(-2).join(".");
    }
  }
  return t.length > 0 ? t : nodeId;
}

function spanFrameId(span: Record<string, unknown>): string {
  const attrs = readAttributes(span);
  return (
    readStringField(attrs, ["mentalmodel.frame.id"]) ??
    (typeof span.frame_id === "string" ? span.frame_id : null) ??
    "root"
  );
}

function buildStructuredRows(span: Record<string, unknown>): Array<[string, string]> {
  const rows: Array<[string, string]> = [];
  const attrs = readAttributes(span);

  const durationNs =
    readNumericField(span, ["duration_ns"]) ??
    (() => {
      const end = readNumericField(span, ["end_time_ns"]);
      const start = readNumericField(span, ["start_time_ns"]);
      if (end != null && start != null && end >= start) {
        return end - start;
      }
      return null;
    })();

  const durationMs =
    readNumericField(span, ["duration_ms", "latency_ms", "duration"]) ?? 0;

  let latencyNs: number | null = durationNs;
  if (latencyNs == null && durationMs > 0) {
    latencyNs = durationMs * 1_000_000;
  }

  if (latencyNs != null && latencyNs > 0) {
    rows.push([
      "Duration",
      `${formatDurationNs(latencyNs)} (${latencyNs.toLocaleString()} ns)`,
    ]);
  } else if (durationMs > 0) {
    rows.push(["Duration", `${durationMs.toFixed(3)} ms`]);
  }

  const startNs = readNumericField(span, ["start_time_ns"]);
  const endNs = readNumericField(span, ["end_time_ns"]);
  if (startNs != null) {
    rows.push(["Start (UTC)", formatInstantNs(startNs)]);
  }
  if (endNs != null) {
    rows.push(["End (UTC)", formatInstantNs(endNs)]);
  }

  const topLevelKeys = [
    "name",
    "frame_id",
    "loop_node_id",
    "iteration_index",
    "error_type",
    "error_message",
  ] as const;
  for (const key of topLevelKeys) {
    if (span[key] === undefined || span[key] === null) {
      continue;
    }
    rows.push([humanizeKey(key), formatAttrValue(span[key])]);
  }

  const mmKeys = Object.keys(attrs).filter((k) => k.startsWith("mentalmodel."));
  mmKeys.sort((a, b) => a.localeCompare(b));
  for (const key of mmKeys) {
    rows.push([humanizeKey(key), formatAttrValue(attrs[key])]);
  }

  const otherKeys = Object.keys(attrs).filter((k) => !k.startsWith("mentalmodel."));
  otherKeys.sort((a, b) => a.localeCompare(b));
  for (const key of otherKeys) {
    rows.push([humanizeKey(key), formatAttrValue(attrs[key])]);
  }

  return rows;
}

function spanToView(span: Record<string, unknown>): GenericSpan {
  const attrs = readAttributes(span);

  const label =
    (typeof span.name === "string" && span.name.length > 0
      ? span.name
      : null) ??
    readStringField(attrs, ["mentalmodel.node.id"]) ??
    "span";

  const durationNs =
    readNumericField(span, ["duration_ns"]) ??
    (() => {
      const end = readNumericField(span, ["end_time_ns"]);
      const start = readNumericField(span, ["start_time_ns"]);
      if (end != null && start != null && end >= start) {
        return end - start;
      }
      return null;
    })();

  const durationMs =
    readNumericField(span, ["duration_ms", "latency_ms", "duration"]) ?? 0;

  let latencyNsForMs: number | null = durationNs;
  if (latencyNsForMs == null && durationMs > 0) {
    latencyNsForMs = durationMs * 1_000_000;
  }

  const latencyMs =
    latencyNsForMs != null ? latencyNsForMs / 1_000_000 : durationMs;

  const startNs = readNumericField(span, ["start_time_ns"]);
  const endNs = readNumericField(span, ["end_time_ns"]);
  const startTimeMs = startNs != null ? startNs / 1_000_000 : null;
  let endTimeMs: number | null =
    endNs != null ? endNs / 1_000_000 : null;
  if (endTimeMs == null && startTimeMs != null && latencyMs > 0) {
    endTimeMs = startTimeMs + latencyMs;
  }

  const latencyLabel =
    latencyNsForMs != null && latencyNsForMs > 0
      ? formatDurationNs(latencyNsForMs)
      : durationMs > 0
        ? `${durationMs.toFixed(3)} ms`
        : "n/a";

  const errType = span.error_type;
  const errMsg = span.error_message;
  const statusLabel =
    typeof errType === "string" && errType.length > 0
      ? errType
      : errMsg != null && String(errMsg).length > 0
        ? "error"
        : "ok";

  const runId =
    readStringField(attrs, ["mentalmodel.run_id"]) ??
    (typeof span.run_id === "string" ? span.run_id : null);
  const traceIdDisplay = runId
    ? shortenId(runId, 28)
    : shortenId(label, 40);

  const nodeId =
    readStringField(attrs, ["mentalmodel.node.id"]) ?? label.slice(0, 64);
  const frameId =
    readStringField(attrs, ["mentalmodel.frame.id"]) ??
    (typeof span.frame_id === "string" ? span.frame_id : null);

  const nodeKindRaw = readStringField(attrs, ["mentalmodel.node.kind"]);
  const kindTag = classifyNodeKind(nodeKindRaw);
  const kindHue = hueFromKindAndNode(nodeKindRaw, nodeId);
  const title = normalizeSpanTitle(label, nodeId);
  const subtitleParts: string[] = [];
  if (frameId && frameId !== "root") {
    subtitleParts.push(frameId);
  }
  const subtitle =
    subtitleParts.length > 0 ? subtitleParts.join(" · ") : null;

  const summaryLine = [title, subtitle, latencyLabel !== "n/a" ? latencyLabel : null]
    .filter(Boolean)
    .join(" · ");

  const structuredRows = buildStructuredRows(span);

  const metadata = structuredRows.slice(0, 12);

  const frameNorm =
    frameId != null && String(frameId).length > 0 ? String(frameId) : "root";

  return {
    label,
    title,
    subtitle,
    nodeKind: nodeKindRaw,
    kindTag,
    kindHue,
    latencyLabel,
    latencyMs: Math.max(0, latencyMs),
    startTimeMs,
    endTimeMs,
    statusLabel,
    traceIdDisplay,
    summaryLine,
    structuredRows,
    metadata,
    rawSpan: span,
    correlationKeys: {
      runId,
      nodeId,
      frameId: frameNorm,
    },
  };
}

/** Filter OTel span rows by ``mentalmodel.node.id`` when a node facet is set. */
export function filterSpansByExploreNode(
  spans: Record<string, unknown>[],
  exploreNodeId: string | null,
): Record<string, unknown>[] {
  if (!exploreNodeId) {
    return spans;
  }
  return spans.filter((span) => {
    const attrs = span.attributes;
    if (!attrs || typeof attrs !== "object" || Array.isArray(attrs)) {
      return false;
    }
    const id = (attrs as Record<string, unknown>)["mentalmodel.node.id"];
    return id === exploreNodeId;
  });
}

/** Match graph ``selectedFrameId`` so span rows align with semantic records. */
export function filterSpansBySelectedFrame(
  spans: Record<string, unknown>[],
  selectedFrameId: string | null,
): Record<string, unknown>[] {
  if (selectedFrameId === null) {
    return spans;
  }
  return spans.filter((span) => spanFrameId(span) === selectedFrameId);
}

/**
 * Prefer run-level ``otel-spans.jsonl`` when loaded; otherwise node-detail trace.
 * Applies ``@node_id`` and the same frame filter as semantic records when set.
 */
export function buildSpanViews(
  nodeDetail: NodeDetail | null,
  runSpans: Record<string, unknown>[] | null,
  exploreNodeId: string | null,
  selectedFrameId: string | null,
): GenericSpan[] {
  let base: Record<string, unknown>[];
  if (runSpans === null) {
    base = (nodeDetail?.trace?.spans ?? []) as Record<string, unknown>[];
  } else if (runSpans.length > 0) {
    base = runSpans;
  } else {
    base = (nodeDetail?.trace?.spans ?? []) as Record<string, unknown>[];
  }
  let filtered = filterSpansByExploreNode(base, exploreNodeId);
  filtered = filterSpansBySelectedFrame(filtered, selectedFrameId);
  return filtered.map((span) => spanToView(span));
}
