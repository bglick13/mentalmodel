export type CatalogEntry = {
  spec_id: string;
  label: string;
  description: string;
  spec_path: string;
  graph_id: string;
  invocation_name: string;
};

export type GraphNode = {
  node_id: string;
  kind: string;
  label: string;
  metadata: Record<string, string>;
};

export type GraphEdge = {
  edge_id: string;
  source_node_id: string;
  source_port: string;
  target_node_id: string;
  target_port: string;
  kind: string;
};

export type GraphPayload = {
  graph_id: string;
  metadata: Record<string, string>;
  nodes: GraphNode[];
  edges: GraphEdge[];
};

export type RunSummary = {
  schema_version: number;
  graph_id: string;
  run_id: string;
  created_at_ms: number;
  success: boolean;
  node_count: number;
  edge_count: number;
  record_count: number;
  output_count: number;
  state_count: number;
  invocation_name: string | null;
  runtime_default_profile_name: string | null;
  runtime_profile_names: string[];
  trace_mode: string;
  trace_service_name: string;
  run_dir: string;
};

export type ExecutionRecord = {
  record_id: string;
  run_id: string;
  node_id: string;
  frame_id: string;
  loop_node_id: string | null;
  iteration_index: number | null;
  event_type: string;
  sequence: number;
  timestamp_ms: number;
  payload: Record<string, unknown>;
};

export type ExecutionSession = {
  execution_id: string;
  spec: CatalogEntry;
  status: string;
  started_at_ms: number;
  finished_at_ms: number | null;
  error: string | null;
  run_id: string | null;
  run_artifacts_dir: string | null;
  latest_sequence: number;
  records: ExecutionRecord[];
  run_summary?: RunOverview;
};

export type ReplayNodeSummary = {
  node_id: string;
  frame_id: string;
  loop_node_id: string | null;
  iteration_index: number | null;
  succeeded: boolean;
  failed: boolean;
  invariant_status: string | null;
  invariant_passed: boolean | null;
  invariant_severity: string | null;
  last_event_type: string;
};

export type ReplayReport = {
  graph_id: string;
  run_id: string;
  invocation_name: string | null;
  success: boolean;
  event_count: number;
  node_count: number;
  frame_ids: string[];
  events: ExecutionRecord[];
  node_summaries: ReplayNodeSummary[];
};

export type NumericMetric = {
  node_id: string;
  path: string;
  label: string;
  value: number;
};

export type InvariantOverview = {
  node_id: string;
  frame_id: string;
  loop_node_id: string | null;
  iteration_index: number | null;
  status: string | null;
  passed: boolean | null;
  severity: string | null;
};

export type RunOverview = {
  summary: RunSummary;
  verification: Record<string, unknown> | null;
  graph: GraphPayload;
  metrics: NumericMetric[];
  invariants: InvariantOverview[];
};

export type NodeDetail = {
  node_id: string;
  frame_id: string | null;
  inputs?: unknown;
  inputs_error?: string;
  output?: unknown;
  output_error?: string;
  trace?: {
    records: ExecutionRecord[];
    spans: Record<string, unknown>[];
  };
  trace_error?: string;
  available_frames: Array<{
    frame_id: string;
    loop_node_id: string | null;
    iteration_index: number | null;
  }>;
};
