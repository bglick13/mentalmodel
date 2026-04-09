export type MetricGroup = {
  group_id: string;
  title: string;
  description: string;
  metric_path_prefixes: string[];
  max_items: number;
};

export type PinnedNode = {
  node_id: string;
  title: string;
  description: string;
};

export type TableRowSource = {
  kind: string;
  node_id: string;
  items_path: string;
  loop_node_id: string | null;
};

export type ValueSelector = {
  kind: string;
  path: string | null;
  node_id: string | null;
  event_type: string | null;
};

export type TableColumn = {
  column_id: string;
  title: string;
  description: string;
  selector: ValueSelector;
};

export type CustomView = {
  view_id: string;
  title: string;
  description: string;
  kind: string;
  row_source: TableRowSource;
  columns: TableColumn[];
};

export type CatalogEntry = {
  spec_id: string;
  label: string;
  description: string;
  spec_path: string;
  graph_id: string;
  invocation_name: string;
  project_id?: string | null;
  project_label?: string | null;
  catalog_source?: string | null;
  category: string;
  tags: string[];
  default_loop_node_id: string | null;
  metric_groups: MetricGroup[];
  pinned_nodes: PinnedNode[];
  custom_views: CustomView[];
};

export type AnalysisFinding = {
  code: string;
  severity: string;
  message: string;
  node_id: string | null;
};

export type AnalysisReport = {
  error_count: number;
  warning_count: number;
  findings: AnalysisFinding[];
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

export type CatalogGraphPayload = {
  catalog_entry: CatalogEntry;
  graph: GraphPayload;
  analysis: AnalysisReport;
};

export type TimeseriesBucket = {
  start_ms: number;
  end_ms: number;
  records_per_sec: number;
  loop_events_per_sec: number;
  unique_nodes: number;
  unique_nodes_per_sec: number;
};

export type TimeseriesResponse = {
  rollup_ms: number;
  since_ms: number;
  until_ms: number;
  graph_id: string;
  invocation_name: string;
  buckets: TimeseriesBucket[];
  runs_scanned: number;
};

export type RunSummary = {
  schema_version: number;
  graph_id: string;
  run_id: string;
  created_at_ms: number;
  status: string;
  success: boolean | null;
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
  source: string;
  execution_id?: string | null;
  availability: {
    summary: boolean;
    records: boolean;
    spans: boolean;
    replay: boolean;
    custom_views: boolean;
  };
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

export type ExecutionMessage = {
  sequence: number;
  timestamp_ms: number;
  level: string;
  message: string;
  source: string;
};

export type ExecutionSpan = {
  sequence: number;
  name: string;
  start_time_ns: number;
  end_time_ns: number;
  duration_ns: number;
  attributes: Record<string, unknown>;
  frame_id: string;
  loop_node_id: string | null;
  iteration_index: number | null;
  error_type: string | null;
  error_message: string | null;
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
  spans: ExecutionSpan[];
  messages: ExecutionMessage[];
  run_handle?: RunSummary;
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
  frame_id: string | null;
  loop_node_id: string | null;
  iteration_index: number | null;
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

export type EvaluatedCustomViewRow = {
  row_id: string;
  frame_id: string | null;
  loop_node_id: string | null;
  iteration_index: number | null;
  values: Record<string, unknown>;
};

export type EvaluatedCustomView = {
  view: CustomView;
  row_count: number;
  rows: EvaluatedCustomViewRow[];
  warnings: string[];
};

/** Coarse bucket for span coloring / icons (from ``mentalmodel.node.kind``). */
export type SpanKindTag =
  | "effect"
  | "join"
  | "loop"
  | "queue"
  | "sink"
  | "other";

/** Normalized span row for traces / spans UI (built from raw OTel-style payloads). */
export type GenericSpan = {
  /** Raw OTel/span ``name`` (before UI normalization). */
  label: string;
  /** Primary display line (short, deduped vs node id). */
  title: string;
  /** Secondary line: frame / context only (no duplicate of title). */
  subtitle: string | null;
  /** Raw ``mentalmodel.node.kind`` when present. */
  nodeKind: string | null;
  kindTag: SpanKindTag;
  /** Hue 0–360 for stripe / badge (stable from kind + node id). */
  kindHue: number;
  latencyLabel: string;
  /** Best-effort duration in milliseconds for charts. */
  latencyMs: number;
  statusLabel: string;
  /** Short id for tables (run id or trace hint). */
  traceIdDisplay: string;
  /** Legacy one-line summary; prefer ``title`` + ``subtitle`` in UI. */
  summaryLine: string;
  /** Ordered key/value rows for detail panel. */
  structuredRows: Array<[string, string]>;
  /** Compact preview rows (legacy / small summaries). */
  metadata: Array<[string, string]>;
  /** Original span object for JSON / advanced fields. */
  rawSpan: Record<string, unknown>;
  /** Align semantic ``records.jsonl`` rows with this span (same bundle scope). */
  correlationKeys: {
    runId: string | null;
    nodeId: string;
    frameId: string;
  };
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
