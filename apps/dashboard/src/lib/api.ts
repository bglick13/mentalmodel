import type {
  CatalogEntry,
  ExecutionSession,
  GraphPayload,
  NodeDetail,
  ReplayReport,
  RunOverview,
  RunSummary,
} from "../types";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed with ${response.status}`);
  }
  return (await response.json()) as T;
}

export async function fetchCatalog(): Promise<CatalogEntry[]> {
  const payload = await request<{ entries: CatalogEntry[] }>("/api/catalog");
  return payload.entries;
}

export async function fetchCatalogGraph(
  specId: string,
): Promise<{ catalog_entry: CatalogEntry; graph: GraphPayload; analysis: unknown }> {
  return request(`/api/catalog/${specId}/graph`);
}

export async function launchExecution(specId: string): Promise<ExecutionSession> {
  return request("/api/executions", {
    method: "POST",
    body: JSON.stringify({ spec_id: specId }),
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

export async function fetchNodeDetail(
  graphId: string,
  runId: string,
  nodeId: string,
  frameId?: string | null,
): Promise<NodeDetail> {
  const query = frameId ? `?frame_id=${encodeURIComponent(frameId)}` : "";
  return request(`/api/runs/${graphId}/${runId}/nodes/${encodeURIComponent(nodeId)}${query}`);
}
