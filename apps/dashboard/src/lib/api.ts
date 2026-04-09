import type {
  CatalogGraphPayload,
  CatalogEntry,
  EvaluatedCustomView,
  ExecutionSession,
  NodeDetail,
  ReplayReport,
  RunOverview,
  RunSummary,
  TimeseriesResponse,
} from "../types";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });
  const raw = await response.text();
  if (!response.ok) {
    throw new Error(raw || `Request failed with ${response.status}`);
  }
  const trimmed = raw.trim();
  if (
    trimmed.startsWith("<!DOCTYPE") ||
    trimmed.startsWith("<!doctype") ||
    trimmed.startsWith("<html")
  ) {
    throw new Error(
      `Expected JSON from ${path} but received HTML (wrong port, missing /api route, or SPA fallback). Rebuild apps/dashboard and run \`uv run mentalmodel ui\` so API and dist match.`,
    );
  }
  try {
    return JSON.parse(trimmed) as T;
  } catch (parseError) {
    throw new Error(
      `Invalid JSON from ${path}: ${trimmed.slice(0, 120)}${trimmed.length > 120 ? "…" : ""}`,
    );
  }
}

export async function fetchCatalog(): Promise<CatalogEntry[]> {
  const payload = await request<{ entries: CatalogEntry[] }>("/api/catalog");
  return payload.entries;
}

export async function fetchCatalogGraph(specId: string): Promise<CatalogGraphPayload> {
  return request(`/api/catalog/${specId}/graph`);
}

export async function registerCatalogFromPath(
  specPath: string,
): Promise<CatalogEntry> {
  const payload = await request<{ entry: CatalogEntry }>(
    "/api/catalog/from-path",
    {
      method: "POST",
      body: JSON.stringify({ spec_path: specPath }),
    },
  );
  return payload.entry;
}

export async function launchExecution(params: {
  specId?: string;
  specPath?: string;
}): Promise<ExecutionSession> {
  const body: Record<string, string> = {};
  if (params.specPath) {
    body.spec_path = params.specPath;
  } else if (params.specId) {
    body.spec_id = params.specId;
  } else {
    throw new Error("launchExecution requires specId or specPath");
  }
  return request("/api/executions", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function fetchExecution(
  executionId: string,
  afterSequence = 0,
): Promise<ExecutionSession> {
  return request(
    `/api/executions/${executionId}?after_sequence=${encodeURIComponent(afterSequence)}`,
  );
}

export async function fetchTimeseries(params: {
  graphId: string;
  invocationName: string;
  sinceMs: number;
  untilMs: number;
  rollupMs: number;
  runId?: string | null;
  nodeId?: string | null;
}): Promise<TimeseriesResponse> {
  const query = new URLSearchParams({
    graph_id: params.graphId,
    invocation_name: params.invocationName,
    since_ms: String(params.sinceMs),
    until_ms: String(params.untilMs),
    rollup_ms: String(params.rollupMs),
  });
  if (params.runId) {
    query.set("run_id", params.runId);
  }
  if (params.nodeId) {
    query.set("node_id", params.nodeId);
  }
  return request(`/api/analytics/timeseries?${query.toString()}`);
}

export async function fetchRuns(
  graphId?: string,
  invocationName?: string,
): Promise<RunSummary[]> {
  const params = new URLSearchParams();
  if (graphId) {
    params.set("graph_id", graphId);
  }
  if (invocationName) {
    params.set("invocation_name", invocationName);
  }
  const query = params.toString();
  const payload = await request<{ runs: RunSummary[] }>(
    `/api/runs${query ? `?${query}` : ""}`,
  );
  return payload.runs;
}

export async function fetchRunOverview(
  graphId: string,
  runId: string,
): Promise<RunOverview> {
  return request(`/api/runs/${graphId}/${runId}/overview`);
}

export async function fetchRunCustomView(
  specId: string,
  runId: string,
  viewId: string,
): Promise<EvaluatedCustomView> {
  return request(
    `/api/catalog/${encodeURIComponent(specId)}/runs/${encodeURIComponent(runId)}/views/${encodeURIComponent(viewId)}`,
  );
}

export async function fetchRunReplay(
  graphId: string,
  runId: string,
  loopNodeId?: string,
): Promise<ReplayReport> {
  const query = loopNodeId
    ? `?loop_node_id=${encodeURIComponent(loopNodeId)}`
    : "";
  return request(`/api/runs/${graphId}/${runId}/replay${query}`);
}

export async function fetchRunRecords(
  graphId: string,
  runId: string,
  nodeId?: string,
): Promise<ReplayReport["events"]> {
  const query = nodeId ? `?node_id=${encodeURIComponent(nodeId)}` : "";
  const payload = await request<{ records: ReplayReport["events"] }>(
    `/api/runs/${graphId}/${runId}/records${query}`,
  );
  return payload.records;
}

export async function fetchRunSpans(
  graphId: string,
  runId: string,
  nodeId?: string | null,
): Promise<{ spans: Record<string, unknown>[] }> {
  const params = new URLSearchParams();
  if (nodeId) {
    params.set("node_id", nodeId);
  }
  const q = params.toString();
  return request(
    `/api/runs/${graphId}/${runId}/spans${q ? `?${q}` : ""}`,
  );
}

export async function fetchNodeDetail(
  graphId: string,
  runId: string,
  nodeId: string,
  frameId?: string | null,
): Promise<NodeDetail> {
  const query = frameId ? `?frame_id=${encodeURIComponent(frameId)}` : "";
  return request(`/api/runs/${graphId}/${runId}/nodes/${encodeURIComponent(nodeId)}${query}`);
}
