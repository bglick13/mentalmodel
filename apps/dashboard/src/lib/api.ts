import type {
  CatalogGraphPayload,
  CatalogEntry,
  EvaluatedCustomView,
  ExecutionSession,
  ExecutionRecord,
  RunMetricGroupsResponse,
  NodeDetail,
  PageResponse,
  RemoteOperationEvent,
  ReplayReport,
  RunOverview,
  RunSummary,
  TimeseriesResponse,
} from "../types";

const inflightRequests = new Map<string, Promise<unknown>>();

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const method = init?.method?.toUpperCase() ?? "GET";
  const cacheKey = method === "GET" ? `${method}:${path}` : null;
  if (cacheKey) {
    const cached = inflightRequests.get(cacheKey);
    if (cached) {
      return cached as Promise<T>;
    }
  }
  const requestPromise = (async () => {
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
  })();
  if (cacheKey) {
    inflightRequests.set(cacheKey, requestPromise);
  }
  try {
    return await requestPromise;
  } finally {
    if (cacheKey) {
      inflightRequests.delete(cacheKey);
    }
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

export async function fetchRemoteEvents(params: {
  projectId?: string | null;
  graphId?: string | null;
  runId?: string | null;
  limit?: number;
}): Promise<RemoteOperationEvent[]> {
  const query = new URLSearchParams();
  if (params.projectId) {
    query.set("project_id", params.projectId);
  }
  if (params.graphId) {
    query.set("graph_id", params.graphId);
  }
  if (params.runId) {
    query.set("run_id", params.runId);
  }
  if (params.limit != null) {
    query.set("limit", String(params.limit));
  }
  const payload = await request<{ events: RemoteOperationEvent[] }>(
    `/api/remote/events?${query.toString()}`,
  );
  return payload.events;
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

export async function fetchRunMetricGroups(
  specId: string,
  runId: string,
  params?: {
    stepStart?: number | null;
    stepEnd?: number | null;
    maxPoints?: number;
    nodeId?: string | null;
    frameId?: string | null;
  },
): Promise<RunMetricGroupsResponse> {
  const query = new URLSearchParams();
  if (params?.stepStart != null) {
    query.set("step_start", String(params.stepStart));
  }
  if (params?.stepEnd != null) {
    query.set("step_end", String(params.stepEnd));
  }
  if (params?.maxPoints != null) {
    query.set("max_points", String(params.maxPoints));
  }
  if (params?.nodeId) {
    query.set("node_id", params.nodeId);
  }
  if (params?.frameId) {
    query.set("frame_id", params.frameId);
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return request(
    `/api/catalog/${encodeURIComponent(specId)}/runs/${encodeURIComponent(runId)}/metrics${suffix}`,
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
  params?: {
    nodeId?: string | null;
    frameId?: string | null;
    cursor?: string | null;
    limit?: number;
    includePayload?: boolean;
  },
): Promise<PageResponse<ReplayReport["events"][number]>> {
  const query = new URLSearchParams();
  if (params?.nodeId) {
    query.set("node_id", params.nodeId);
  }
  if (params?.frameId) {
    query.set("frame_id", params.frameId);
  }
  if (params?.cursor) {
    query.set("cursor", params.cursor);
  }
  if (params?.limit != null) {
    query.set("limit", String(params.limit));
  }
  if (params?.includePayload != null) {
    query.set("include_payload", params.includePayload ? "true" : "false");
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return request<PageResponse<ExecutionRecord>>(
    `/api/runs/${graphId}/${runId}/records${suffix}`,
  );
}

export async function fetchRunSpans(
  graphId: string,
  runId: string,
  options?: {
    nodeId?: string | null;
    frameId?: string | null;
    cursor?: string | null;
    limit?: number;
  },
): Promise<PageResponse<Record<string, unknown>>> {
  const query = new URLSearchParams();
  if (options?.nodeId) {
    query.set("node_id", options.nodeId);
  }
  if (options?.frameId) {
    query.set("frame_id", options.frameId);
  }
  if (options?.cursor) {
    query.set("cursor", options.cursor);
  }
  if (options?.limit != null) {
    query.set("limit", String(options.limit));
  }
  const suffix = query.size > 0 ? `?${query.toString()}` : "";
  return request(`/api/runs/${graphId}/${runId}/spans${suffix}`);
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
