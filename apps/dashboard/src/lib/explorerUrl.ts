/** Query-string encoding for Datadog-style explorer facets (shareable URLs). */

export const EXPLORER_PARAMS = {
  spec: "spec",
  window: "window",
  run: "run",
  node: "node",
  stepStart: "is",
  stepEnd: "ie",
  /** Spans view: selected span row index (0-based). */
  spanInspect: "si",
  /** Spans view: selected semantic record id (e.g. ``run-…:38``). */
  recordInspect: "rid",
} as const;

/** Preset ids aligned with `ExploreTimePreset` in App.tsx */
export const EXPLORER_WINDOW_VALUES = [
  "15m",
  "1h",
  "4h",
  "24h",
  "7d",
  "all",
] as const;

export type ExplorerWindowParam = (typeof EXPLORER_WINDOW_VALUES)[number];

export function isExplorerWindowParam(s: string): s is ExplorerWindowParam {
  return (EXPLORER_WINDOW_VALUES as readonly string[]).includes(s);
}

export type ParsedExplorerQuery = {
  specId: string | null;
  window: ExplorerWindowParam | null;
  runId: string | null;
  /** `null` = all nodes; empty string in URL also means all */
  nodeId: string | null;
  iterationStart: number | null;
  iterationEnd: number | null;
  /** Spans inspector: OTel row index, or null if absent / invalid */
  spanInspectIndex: number | null;
  /** Spans inspector: full ``record_id`` from records.jsonl */
  recordInspectId: string | null;
};

export function parseExplorerQuery(search: string): ParsedExplorerQuery {
  const q = new URLSearchParams(
    search.startsWith("?") ? search.slice(1) : search,
  );
  const specRaw = q.get(EXPLORER_PARAMS.spec);
  const windowRaw = q.get(EXPLORER_PARAMS.window);
  const runRaw = q.get(EXPLORER_PARAMS.run);
  const nodeRaw = q.get(EXPLORER_PARAMS.node);
  const stepStartRaw = q.get(EXPLORER_PARAMS.stepStart);
  const stepEndRaw = q.get(EXPLORER_PARAMS.stepEnd);
  const siRaw = q.get(EXPLORER_PARAMS.spanInspect);
  const ridRaw = q.get(EXPLORER_PARAMS.recordInspect);

  let spanInspectIndex: number | null = null;
  if (siRaw != null && siRaw !== "") {
    const n = Number.parseInt(siRaw, 10);
    if (Number.isFinite(n) && n >= 0) {
      spanInspectIndex = n;
    }
  }

  let iterationStart: number | null = null;
  if (stepStartRaw != null && stepStartRaw !== "") {
    const n = Number.parseInt(stepStartRaw, 10);
    if (Number.isFinite(n) && n >= 0) {
      iterationStart = n;
    }
  }

  let iterationEnd: number | null = null;
  if (stepEndRaw != null && stepEndRaw !== "") {
    const n = Number.parseInt(stepEndRaw, 10);
    if (Number.isFinite(n) && n >= 0) {
      iterationEnd = n;
    }
  }

  return {
    specId: specRaw && specRaw.length > 0 ? specRaw : null,
    window:
      windowRaw && isExplorerWindowParam(windowRaw) ? windowRaw : null,
    runId: runRaw && runRaw.length > 0 ? runRaw : null,
    nodeId:
      nodeRaw === null || nodeRaw === ""
        ? null
        : nodeRaw.length > 0
          ? nodeRaw
          : null,
    iterationStart,
    iterationEnd,
    spanInspectIndex,
    recordInspectId:
      ridRaw != null && ridRaw.length > 0 ? ridRaw : null,
  };
}

export function serializeExplorerQuery(input: {
  specId: string | null;
  window: string;
  runId: string | null;
  nodeId: string | null;
  iterationStart?: number | null;
  iterationEnd?: number | null;
  /** When set, opens record inspector (takes precedence over span index). */
  recordInspectId?: string | null;
  spanInspectIndex?: number | null;
}): string {
  const q = new URLSearchParams();
  if (input.specId) {
    q.set(EXPLORER_PARAMS.spec, input.specId);
  }
  q.set(EXPLORER_PARAMS.window, input.window);
  if (input.runId) {
    q.set(EXPLORER_PARAMS.run, input.runId);
  }
  if (input.nodeId) {
    q.set(EXPLORER_PARAMS.node, input.nodeId);
  }
  if (
    input.iterationStart != null &&
    Number.isFinite(input.iterationStart) &&
    input.iterationStart >= 0
  ) {
    q.set(EXPLORER_PARAMS.stepStart, String(input.iterationStart));
  }
  if (
    input.iterationEnd != null &&
    Number.isFinite(input.iterationEnd) &&
    input.iterationEnd >= 0
  ) {
    q.set(EXPLORER_PARAMS.stepEnd, String(input.iterationEnd));
  }
  if (input.recordInspectId) {
    q.set(EXPLORER_PARAMS.recordInspect, input.recordInspectId);
  } else if (
    input.spanInspectIndex != null &&
    Number.isFinite(input.spanInspectIndex) &&
    input.spanInspectIndex >= 0
  ) {
    q.set(EXPLORER_PARAMS.spanInspect, String(input.spanInspectIndex));
  }
  const s = q.toString();
  return s.length > 0 ? `?${s}` : "";
}

export type ExplorerUrlState = {
  pathname: string;
  activeView: string;
  specId: string | null;
  window: string;
  runId: string | null;
  nodeId: string | null;
  iterationStart: number | null;
  iterationEnd: number | null;
  /** Only serialized when ``activeView === \"spans\"``. */
  spanInspectIndex: number | null;
  recordInspectId: string | null;
};

export function buildExplorerUrl(input: ExplorerUrlState): string {
  const onSpans = input.activeView === "spans";
  const search = serializeExplorerQuery({
    specId: input.specId,
    window: input.window,
    runId: input.runId,
    nodeId: input.nodeId,
    iterationStart: input.iterationStart,
    iterationEnd: input.iterationEnd,
    recordInspectId: onSpans ? input.recordInspectId : null,
    spanInspectIndex: onSpans ? input.spanInspectIndex : null,
  });
  const hash = input.activeView ? `#${input.activeView}` : "";
  return `${input.pathname}${search}${hash}`;
}

/** Update the address bar without adding a history entry (facet changes, popstate sync). */
export function replaceUrlWithExplorerState(input: ExplorerUrlState): void {
  window.history.replaceState(null, "", buildExplorerUrl(input));
}

/** Push a new history entry (e.g. primary view navigation so Back/Forward work). */
export function pushUrlWithExplorerState(input: ExplorerUrlState): void {
  window.history.pushState(null, "", buildExplorerUrl(input));
}
