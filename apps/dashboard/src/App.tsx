import {
  startTransition,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type Dispatch,
  type ReactNode,
  type SetStateAction,
} from "react";

import { ExecutionDetailDrawer } from "./components/ExecutionDetailDrawer";
import {
  frameIdForNodeDetailApi,
  InspectorNodeIo,
} from "./components/InspectorNodeIo";
import { ExplorerTimeseriesChart } from "./components/ExplorerTimeseriesChart";
import { GraphPanel } from "./components/GraphPanel";
import { MetricGroupTimeseriesChart } from "./components/MetricGroupTimeseriesChart";
import { MetricSparkline } from "./components/MetricSparkline";
import { SpanLatencyInsights } from "./components/SpanLatencyInsights";
import { SpanFlamegraph } from "./components/SpanFlamegraph";
import {
  fetchCatalog,
  fetchCatalogGraph,
  fetchExecution,
  fetchNodeDetail,
  fetchRemoteEvents,
  fetchRunCustomView,
  fetchRunMetricGroups,
  fetchRunOverview,
  fetchRunRecords,
  fetchRunReplay,
  fetchRuns,
  fetchRunSpans,
  fetchTimeseries,
  launchExecution,
  registerCatalogFromPath,
} from "./lib/api";
import {
  buildExplorerUrl,
  isExplorerWindowParam,
  parseExplorerQuery,
  pushUrlWithExplorerState,
  replaceUrlWithExplorerState,
} from "./lib/explorerUrl";
import {
  executionRecordToDetailJson,
  executionRecordToRows,
  formatSpanCorrelationScope,
  recordsMatchingSpanScope,
} from "./lib/recordsForSpan";
import { buildSpanViews } from "./lib/traceSpan";
import type {
  AnalysisFinding,
  CatalogEntry,
  EvaluatedCustomView,
  EvaluatedCustomViewRow,
  ExecutionMessage,
  ExecutionRecord,
  ExecutionSession,
  GenericSpan,
  GraphEdge,
  GraphPayload,
  MetricGroupQueryResult,
  MetricSeries,
  NodeDetail,
  ReplayNodeSummary,
  ReplayReport,
  RemoteOperationEvent,
  RunOverview,
  RunSummary,
  TableColumn,
  TimeseriesResponse,
} from "./types";

type SpansInspector =
  | { kind: "span"; index: number }
  | { kind: "record"; id: string };

type ViewId =
  | "overview"
  | "views"
  | "graph"
  | "node"
  | "spans"
  | "launch";

function viewNeedsRunRecords(view: ViewId): boolean {
  return view === "node" || view === "spans";
}

function viewNeedsRunSpans(view: ViewId): boolean {
  return view === "spans";
}

type ScopeToken = {
  label: string;
  value: string;
};

const RUN_RECORDS_PAGE_SIZE = 250;
const RUN_SPANS_PAGE_SIZE = 200;

const VIEWS: Array<{
  id: ViewId;
  label: string;
  title: string;
}> = [
  {
    id: "overview",
    label: "Overview",
    title: "Health, trend charts, counters, recent runs, and bottlenecks",
  },
  {
    id: "views",
    label: "Tables",
    title: "Spec-defined metric / narrative tables for the selected run",
  },
  { id: "graph", label: "Graph", title: "Execution DAG — click nodes to sync explorer scope" },
  {
    id: "node",
    label: "Node",
    title: "Resolved I/O, frames, invariants, and cadence for the selected node",
  },
  {
    id: "spans",
    label: "Traces",
    title:
      "Trace timeline (wall-clock), OTel span list, and semantic record stream",
  },
  { id: "launch", label: "Launch", title: "Start runs and watch live semantic stream" },
];

const SPEC_PATH_STORAGE_KEY = "mentalmodel.dashboard.specPath";
const RUN_LIST_POLL_MS = 10000;
const SELECTED_RUN_POLL_MS = 2000;

function readStoredSpecPath(): string {
  try {
    const v = localStorage.getItem(SPEC_PATH_STORAGE_KEY);
    if (!v || v.length === 0) {
      return "";
    }
    return v;
  } catch {
    return "";
  }
}

type ExploreTimePreset = "15m" | "1h" | "4h" | "24h" | "7d" | "all";

function computeExploreWindow(
  preset: ExploreTimePreset,
  runs: RunSummary[],
): { sinceMs: number; untilMs: number; rollupMs: number } {
  const wallUntil = Date.now();
  let sinceMs = wallUntil - 3600000;
  let untilMs = wallUntil;
  let rollupMs = 60000;
  switch (preset) {
    case "15m":
      sinceMs = wallUntil - 15 * 60 * 1000;
      rollupMs = 15000;
      break;
    case "1h":
      sinceMs = wallUntil - 3600000;
      rollupMs = 60000;
      break;
    case "4h":
      sinceMs = wallUntil - 4 * 3600000;
      rollupMs = 120000;
      break;
    case "24h":
      sinceMs = wallUntil - 86400000;
      rollupMs = 300000;
      break;
    case "7d":
      sinceMs = wallUntil - 7 * 86400000;
      rollupMs = 3600000;
      break;
    case "all":
      if (runs.length === 0) {
        sinceMs = wallUntil - 86400000;
        untilMs = wallUntil;
        rollupMs = 300000;
      } else {
        const minT = Math.min(...runs.map((r) => r.created_at_ms));
        const maxT = Math.max(...runs.map((r) => r.created_at_ms));
        sinceMs = minT - 60_000;
        untilMs = Math.max(wallUntil, maxT + 60_000);
        const span = untilMs - sinceMs;
        rollupMs = Math.max(60_000, Math.min(3_600_000, Math.floor(span / 60)));
      }
      break;
    default:
      break;
  }
  return { sinceMs, untilMs, rollupMs };
}

function filterRunsInExploreWindow(
  runs: RunSummary[],
  sinceMs: number,
  untilMs: number,
): RunSummary[] {
  return runs
    .filter(
      (r) => r.created_at_ms >= sinceMs && r.created_at_ms <= untilMs,
    )
    .sort((a, b) => b.created_at_ms - a.created_at_ms);
}

function filterRecordsByTimeWindow(
  records: ExecutionRecord[],
  sinceMs: number,
  untilMs: number,
): ExecutionRecord[] {
  return records.filter(
    (r) => r.timestamp_ms >= sinceMs && r.timestamp_ms < untilMs,
  );
}

function formatSuccessRateForRuns(sample: RunSummary[]): string {
  const completed = sample.filter(
    (run) => run.status === "succeeded" || run.status === "failed",
  );
  if (completed.length === 0) {
    return "n/a";
  }
  const successes = completed.filter((run) => run.status === "succeeded").length;
  return `${((successes / completed.length) * 100).toFixed(1)}%`;
}

const EXPLORE_PRESET_LABEL: Record<ExploreTimePreset, string> = {
  "15m": "Past 15m",
  "1h": "Past 1h",
  "4h": "Past 4h",
  "24h": "Past 24h",
  "7d": "Past 7d",
  all: "All runs (time range)",
};

function App() {
  const [catalog, setCatalog] = useState<CatalogEntry[]>([]);
  const [selectedSpecId, setSelectedSpecId] = useState<string | null>(null);
  const [displayedSpecId, setDisplayedSpecId] = useState<string | null>(null);
  const [specSwitchLoading, setSpecSwitchLoading] = useState(false);
  const [activeView, setActiveView] = useState<ViewId>("overview");
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [graphPreview, setGraphPreview] = useState<GraphPayload | null>(null);
  const [graphFindings, setGraphFindings] = useState<AnalysisFinding[]>([]);
  const [activeRun, setActiveRun] = useState<RunOverview | null>(null);
  const [activeReplay, setActiveReplay] = useState<ReplayReport | null>(null);
  const [activeRecords, setActiveRecords] = useState<ExecutionRecord[]>([]);
  const [activeRecordsCursor, setActiveRecordsCursor] = useState<string | null>(null);
  const [activeRecordsHasMore, setActiveRecordsHasMore] = useState(false);
  const [activeRecordsTotalCount, setActiveRecordsTotalCount] = useState(0);
  const [activeRecordsLoading, setActiveRecordsLoading] = useState(false);
  const [activeRecordsLoadingMore, setActiveRecordsLoadingMore] = useState(false);
  const [activeExecution, setActiveExecution] = useState<ExecutionSession | null>(
    null,
  );
  const [activeCustomView, setActiveCustomView] =
    useState<EvaluatedCustomView | null>(null);
  const [selectedCustomViewId, setSelectedCustomViewId] = useState<string | null>(
    null,
  );
  const [customViewLoading, setCustomViewLoading] = useState(false);
  const [customViewError, setCustomViewError] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedFrameId, setSelectedFrameId] = useState<string | null>(null);
  const [nodeDetail, setNodeDetail] = useState<NodeDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [customSpecPath, setCustomSpecPath] = useState(readStoredSpecPath);
  const [exploreTimePreset, setExploreTimePreset] =
    useState<ExploreTimePreset>("1h");
  const [exploreRunId, setExploreRunId] = useState<string | null>(null);
  const [exploreNodeId, setExploreNodeId] = useState<string | null>(null);
  const [exploreIterationStart, setExploreIterationStart] = useState<string>("");
  const [exploreIterationEnd, setExploreIterationEnd] = useState<string>("");
  const [timeseries, setTimeseries] = useState<TimeseriesResponse | null>(null);
  const [timeseriesLoading, setTimeseriesLoading] = useState(false);
  const [timeseriesPollBusy, setTimeseriesPollBusy] = useState(false);
  const [timeseriesError, setTimeseriesError] = useState<string | null>(null);
  const [metricGroupsResponse, setMetricGroupsResponse] =
    useState<MetricGroupQueryResult[]>([]);
  const [metricGroupsLoading, setMetricGroupsLoading] = useState(false);
  const [metricGroupsRefreshing, setMetricGroupsRefreshing] = useState(false);
  const [runSpans, setRunSpans] = useState<Record<string, unknown>[] | null>(
    null,
  );
  const [runSpansCursor, setRunSpansCursor] = useState<string | null>(null);
  const [runSpansHasMore, setRunSpansHasMore] = useState(false);
  const [runSpansTotalCount, setRunSpansTotalCount] = useState(0);
  const [remoteEvents, setRemoteEvents] = useState<RemoteOperationEvent[]>([]);
  const [runSpansLoading, setRunSpansLoading] = useState(false);
  const [runSpansLoadingMore, setRunSpansLoadingMore] = useState(false);
  const [selectedRunRefreshTick, setSelectedRunRefreshTick] = useState(0);
  const explorerUrlHydratedRef = useRef(false);
  const prevExplorerSpecIdRef = useRef<string | null>(null);
  /** After popstate/back-forward: sync URL with React state using replace, do not push. */
  const explorerRestoringFromHistoryRef = useRef(false);
  /** Last view written to history; used to push only on real view changes. */
  const prevActiveViewForHistoryRef = useRef<ViewId | null>(null);
  /** Apply ``si`` / ``rid`` from URL once span/record lists are ready. */
  const spansInspectorHydrateRef = useRef<{
    si: number | null;
    rid: string | null;
  } | null>(null);

  const [spansInspector, setSpansInspector] = useState<SpansInspector | null>(
    null,
  );
  const [shareCopied, setShareCopied] = useState(false);

  useEffect(() => {
    try {
      localStorage.setItem(SPEC_PATH_STORAGE_KEY, customSpecPath);
    } catch {
      /* ignore */
    }
  }, [customSpecPath]);

  useEffect(() => {
    const applyHash = () => {
      const next = viewFromHash(window.location.hash);
      if (next) {
        setActiveView(next);
      }
    };
    applyHash();
    window.addEventListener("hashchange", applyHash);
    return () => window.removeEventListener("hashchange", applyHash);
  }, []);

  const requestedCatalog = useMemo(
    () => catalog.find((entry) => entry.spec_id === selectedSpecId) ?? null,
    [catalog, selectedSpecId],
  );
  const selectedCatalog = useMemo(
    () =>
      catalog.find((entry) => entry.spec_id === displayedSpecId) ??
      requestedCatalog ??
      null,
    [catalog, displayedSpecId, requestedCatalog],
  );

  useEffect(() => {
    if (!selectedCatalog) {
      return;
    }
    setCustomSpecPath(selectedCatalog.spec_path);
  }, [selectedCatalog?.spec_id, selectedCatalog?.spec_path]);

  useEffect(() => {
    setSelectedCustomViewId(selectedCatalog?.custom_views[0]?.view_id ?? null);
  }, [selectedCatalog?.spec_id, selectedCatalog?.custom_views]);

  const exploreNodeIdRef = useRef(exploreNodeId);
  exploreNodeIdRef.current = exploreNodeId;

  const exploreWindow = useMemo(
    () => computeExploreWindow(exploreTimePreset, runs),
    [exploreTimePreset, runs],
  );

  const runsInExploreWindow = useMemo(
    () =>
      filterRunsInExploreWindow(
        runs,
        exploreWindow.sinceMs,
        exploreWindow.untilMs,
      ),
    [runs, exploreWindow],
  );

  const runsForExplorerDropdown = useMemo(
    () => (runsInExploreWindow.length > 0 ? runsInExploreWindow : runs),
    [runs, runsInExploreWindow],
  );
  const liveRecords = activeExecution?.records ?? [];
  const liveMessages = activeExecution?.messages ?? [];
  const liveRunRecords = useMemo(
    () =>
      activeExecution?.run_id === exploreRunId
        ? liveRecords
        : [],
    [activeExecution?.run_id, exploreRunId, liveRecords],
  );
  const iterationBounds = useMemo(
    () =>
      collectIterationBounds({
        metricGroups: metricGroupsResponse,
        records:
          activeRun?.summary.run_id === exploreRunId ? activeRecords : liveRunRecords,
        spans:
          activeRun?.summary.run_id === exploreRunId
            ? runSpans
            : activeExecution?.run_id === exploreRunId
              ? activeExecution.spans
              : runSpans,
      }),
    [
      activeExecution?.run_id,
      activeExecution?.spans,
      activeRun?.summary.run_id,
      activeRecords,
      exploreRunId,
      liveRunRecords,
      metricGroupsResponse,
      runSpans,
    ],
  );
  const normalizedIterationRange = useMemo(
    () =>
      normalizeIterationRange({
        startInput: exploreIterationStart,
        endInput: exploreIterationEnd,
        bounds: iterationBounds,
      }),
    [exploreIterationStart, exploreIterationEnd, iterationBounds],
  );

  const loadRun = useCallback(async (
    entry: CatalogEntry,
    runId: string,
    options?: { preserveFrameSelection?: boolean },
  ) => {
    const overview = await fetchRunOverview(entry.graph_id, runId);
    setActiveRun(overview);
    if (!options?.preserveFrameSelection) {
      setSelectedFrameId(null);
    }
    const preferred = exploreNodeIdRef.current;
    setSelectedNodeId((current) => {
      if (
        preferred &&
        overview.graph.nodes.some((n) => n.node_id === preferred)
      ) {
        return preferred;
      }
      if (
        current &&
        overview.graph.nodes.some((n) => n.node_id === current)
      ) {
        return current;
      }
      return (
        entry.pinned_nodes[0]?.node_id ??
        overview.graph.nodes[0]?.node_id ??
        null
      );
    });
    setError(null);
  }, []);

  const refreshRuns = useCallback(
    async (entry: CatalogEntry): Promise<RunSummary[]> => {
      const nextRuns = await fetchRuns(entry.graph_id, entry.invocation_name);
      setRuns(nextRuns);
      setError(null);
      return nextRuns;
    },
    [],
  );

  const loadOlderRecords = useCallback(async () => {
    if (
      !selectedCatalog ||
      !activeRun ||
      !exploreRunId ||
      activeRun.summary.run_id !== exploreRunId ||
      !activeRecordsHasMore ||
      activeRecordsCursor == null
    ) {
      return;
    }
    try {
      setActiveRecordsLoadingMore(true);
      const page = await fetchRunRecords(selectedCatalog.graph_id, activeRun.summary.run_id, {
        nodeId: exploreNodeId,
        frameId: selectedFrameId,
        cursor: activeRecordsCursor,
        limit: RUN_RECORDS_PAGE_SIZE,
        includePayload: activeView === "spans",
      });
      setActiveRecords((current) => current.concat(page.items));
      setActiveRecordsCursor(page.next_cursor);
      setActiveRecordsHasMore(page.has_more);
      setActiveRecordsTotalCount(page.total_count);
      setError(null);
    } catch (recordsError) {
      setError(String(recordsError));
    } finally {
      setActiveRecordsLoadingMore(false);
    }
  }, [
    selectedCatalog,
    activeRun,
    exploreRunId,
    activeRecordsHasMore,
    activeRecordsCursor,
    exploreNodeId,
    selectedFrameId,
  ]);

  const loadOlderSpans = useCallback(async () => {
    if (
      !selectedCatalog ||
      !activeRun ||
      !exploreRunId ||
      activeRun.summary.run_id !== exploreRunId ||
      !runSpansHasMore ||
      runSpansCursor == null
    ) {
      return;
    }
    try {
      setRunSpansLoadingMore(true);
      const page = await fetchRunSpans(selectedCatalog.graph_id, activeRun.summary.run_id, {
        nodeId: exploreNodeId,
        frameId: selectedFrameId,
        cursor: runSpansCursor,
        limit: RUN_SPANS_PAGE_SIZE,
      });
      setRunSpans((current) => (current ?? []).concat(page.items));
      setRunSpansCursor(page.next_cursor);
      setRunSpansHasMore(page.has_more);
      setRunSpansTotalCount(page.total_count);
      setError(null);
    } catch (spansError) {
      setError(String(spansError));
    } finally {
      setRunSpansLoadingMore(false);
    }
  }, [
    selectedCatalog,
    activeRun,
    exploreRunId,
    runSpansHasMore,
    runSpansCursor,
    exploreNodeId,
    selectedFrameId,
  ]);

  useEffect(() => {
    void (async () => {
      try {
        const entries = await fetchCatalog();
        setCatalog(entries);
        setError(null);
        const parsed = parseExplorerQuery(window.location.search);
        const specFromUrl =
          parsed.specId &&
          entries.some((entry) => entry.spec_id === parsed.specId)
            ? parsed.specId
            : null;
        const initialSpecId = specFromUrl ?? entries[0]?.spec_id ?? null;
        setSelectedSpecId(initialSpecId);
        setDisplayedSpecId(initialSpecId);
        if (parsed.window && isExplorerWindowParam(parsed.window)) {
          setExploreTimePreset(parsed.window);
        }
        setExploreRunId(parsed.runId);
        setExploreNodeId(parsed.nodeId);
        setExploreIterationStart(
          parsed.iterationStart != null ? String(parsed.iterationStart) : "",
        );
        setExploreIterationEnd(
          parsed.iterationEnd != null ? String(parsed.iterationEnd) : "",
        );
        const hashView = viewFromHash(window.location.hash);
        if (hashView) {
          setActiveView(hashView);
        }
        spansInspectorHydrateRef.current = {
          si: parsed.spanInspectIndex,
          rid: parsed.recordInspectId,
        };
        explorerUrlHydratedRef.current = true;
      } catch (fetchError) {
        setError(String(fetchError));
      }
    })();
  }, []);

  useEffect(() => {
    if (!requestedCatalog) {
      return;
    }
    if (displayedSpecId === requestedCatalog.spec_id && graphPreview != null) {
      return;
    }
    let cancelled = false;
    const nextSpecId = requestedCatalog.spec_id;
    const prevSpecId = prevExplorerSpecIdRef.current;
    setSpecSwitchLoading(true);
    void (async () => {
      try {
        const [catalogGraph, runData] = await Promise.all([
          fetchCatalogGraph(nextSpecId),
          fetchRuns(requestedCatalog.graph_id, requestedCatalog.invocation_name),
        ]);
        if (cancelled) {
          return;
        }
        const nextRunId = runData[0]?.run_id ?? null;
        let nextOverview: RunOverview | null = null;
        if (nextRunId) {
          nextOverview = await fetchRunOverview(requestedCatalog.graph_id, nextRunId);
          if (cancelled) {
            return;
          }
        }
        setRuns(runData);
        setGraphPreview(catalogGraph.graph);
        setGraphFindings(catalogGraph.analysis.findings);
        setActiveRun(nextOverview);
        setActiveReplay(null);
        setExploreRunId(nextRunId);
        setSelectedFrameId(null);
        setNodeDetail(null);
        setActiveRecords([]);
        setActiveRecordsCursor(null);
        setActiveRecordsHasMore(false);
        setActiveRecordsTotalCount(0);
        setRunSpans(null);
        setRunSpansCursor(null);
        setRunSpansHasMore(false);
        setRunSpansTotalCount(0);
        setMetricGroupsResponse([]);
        setTimeseries(null);
        if (prevSpecId != null && prevSpecId !== nextSpecId) {
          setExploreNodeId(null);
          setExploreIterationStart("");
          setExploreIterationEnd("");
          setExploreTimePreset("1h");
          setSpansInspector(null);
        }
        setSelectedNodeId(() => {
          if (nextOverview) {
            return (
              requestedCatalog.pinned_nodes[0]?.node_id ??
              nextOverview.graph.nodes[0]?.node_id ??
              null
            );
          }
          return (
            requestedCatalog.pinned_nodes[0]?.node_id ??
            catalogGraph.graph.nodes[0]?.node_id ??
            null
          );
        });
        prevExplorerSpecIdRef.current = nextSpecId;
        setDisplayedSpecId(nextSpecId);
        setError(null);
      } catch (fetchError) {
        if (!cancelled) {
          setError(String(fetchError));
        }
      } finally {
        if (!cancelled) {
          setSpecSwitchLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [requestedCatalog, displayedSpecId, graphPreview]);

  useEffect(() => {
    if (!selectedCatalog) {
      return;
    }
    const shouldPollRunList =
      activeExecution != null ||
      runs.some(
        (run) =>
          run.status === "running" ||
          run.status === "pending" ||
          run.source === "remote-live",
      );
    if (!shouldPollRunList) {
      return;
    }
    let cancelled = false;
    const timer = window.setInterval(() => {
      if (cancelled || document.visibilityState !== "visible") {
        return;
      }
      void refreshRuns(selectedCatalog)
        .then((nextRuns) => {
          if (cancelled || nextRuns.length === 0) {
            return;
          }
          setExploreRunId((current) => current ?? nextRuns[0]!.run_id);
        })
        .catch((fetchError: unknown) => {
          if (!cancelled) {
            setError(String(fetchError));
          }
        });
    }, RUN_LIST_POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [activeExecution, refreshRuns, runs, selectedCatalog]);

  useEffect(() => {
    if (
      exploreRunId != null &&
      !runs.some((r) => r.run_id === exploreRunId) &&
      !(
        activeExecution?.run_id === exploreRunId &&
        activeExecution.status !== "succeeded" &&
        activeExecution.status !== "failed"
      )
    ) {
      setExploreRunId(null);
    }
  }, [activeExecution?.run_id, activeExecution?.status, exploreRunId, runs]);

  useEffect(() => {
    if (!selectedCatalog) {
      return;
    }
    if (exploreRunId == null) {
      setActiveRun(null);
      setActiveReplay(null);
      setActiveRecords([]);
      setActiveRecordsCursor(null);
      setActiveRecordsHasMore(false);
      setActiveRecordsTotalCount(0);
      setNodeDetail(null);
      setRunSpans(null);
      setRunSpansCursor(null);
      setRunSpansHasMore(false);
      setRunSpansTotalCount(0);
      return;
    }
    if (activeRun?.summary.run_id === exploreRunId) {
      return;
    }
    if (
      activeExecution?.run_id === exploreRunId &&
      activeExecution.status !== "succeeded" &&
      activeExecution.status !== "failed" &&
      !runs.some((run) => run.run_id === exploreRunId)
    ) {
      setActiveRun(null);
      setActiveReplay(null);
      setActiveRecords([]);
      setActiveRecordsCursor(null);
      setActiveRecordsHasMore(false);
      setActiveRecordsTotalCount(0);
      setNodeDetail(null);
      setRunSpans(null);
      setRunSpansCursor(null);
      setRunSpansHasMore(false);
      setRunSpansTotalCount(0);
      return;
    }
    void loadRun(selectedCatalog, exploreRunId);
  }, [
    selectedCatalog,
    exploreRunId,
    loadRun,
    activeExecution?.run_id,
    activeExecution?.status,
    activeRun?.summary.run_id,
    runs,
  ]);

  useEffect(() => {
    if (
      !selectedCatalog ||
      !activeRun ||
      exploreRunId == null ||
      activeRun.summary.run_id !== exploreRunId
    ) {
      setActiveReplay(null);
      return;
    }
    const shouldLoadReplay =
      activeView === "graph" ||
      activeView === "node" ||
      selectedFrameId != null;
    if (!shouldLoadReplay) {
      setActiveReplay(null);
      return;
    }
    let cancelled = false;
    void fetchRunReplay(
      selectedCatalog.graph_id,
      activeRun.summary.run_id,
      selectedCatalog.default_loop_node_id ?? undefined,
    )
      .then((replay) => {
        if (!cancelled) {
          setActiveReplay(replay);
          setError(null);
        }
      })
      .catch((replayError: unknown) => {
        if (!cancelled) {
          setError(String(replayError));
          setActiveReplay(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    activeRun,
    activeView,
    exploreRunId,
    selectedCatalog,
    selectedFrameId,
  ]);

  useEffect(() => {
    if (!selectedCatalog || !exploreRunId) {
      return;
    }
    const shouldPollSelectedRun =
      activeRun == null ||
      (activeRun.summary.run_id === exploreRunId &&
        (activeRun.summary.status === "running" ||
          activeRun.summary.status === "pending" ||
          activeRun.summary.source === "remote-live"));
    if (!shouldPollSelectedRun) {
      return;
    }
    let cancelled = false;
    const timer = window.setInterval(() => {
      if (cancelled || document.visibilityState !== "visible") {
        return;
      }
      void loadRun(selectedCatalog, exploreRunId, {
        preserveFrameSelection: true,
      })
        .then(() => {
          if (!cancelled) {
            setSelectedRunRefreshTick((current) => current + 1);
          }
        })
        .catch((loadError: unknown) => {
          if (!cancelled) {
            setError(String(loadError));
          }
        });
    }, SELECTED_RUN_POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [selectedCatalog, exploreRunId, loadRun, activeRun]);

  useEffect(() => {
    if (!selectedCatalog || !activeRun || exploreRunId == null) {
      setActiveRecords([]);
      setActiveRecordsCursor(null);
      setActiveRecordsHasMore(false);
      setActiveRecordsTotalCount(0);
      setActiveRecordsLoading(false);
      setActiveRecordsLoadingMore(false);
      return;
    }
    if (!viewNeedsRunRecords(activeView)) {
      setActiveRecords([]);
      setActiveRecordsCursor(null);
      setActiveRecordsHasMore(false);
      setActiveRecordsTotalCount(0);
      setActiveRecordsLoading(false);
      setActiveRecordsLoadingMore(false);
      return;
    }
    if (activeRun.summary.run_id !== exploreRunId) {
      return;
    }
    let cancelled = false;
    setActiveRecordsLoading(true);
    void fetchRunRecords(selectedCatalog.graph_id, activeRun.summary.run_id, {
      nodeId: exploreNodeId,
      frameId: selectedFrameId,
      limit: RUN_RECORDS_PAGE_SIZE,
      includePayload: activeView === "spans",
    })
      .then((records) => {
        if (!cancelled) {
          setActiveRecords(records.items);
          setActiveRecordsCursor(records.next_cursor);
          setActiveRecordsHasMore(records.has_more);
          setActiveRecordsTotalCount(records.total_count);
          setError(null);
        }
      })
      .catch((recordsError: unknown) => {
        if (!cancelled) {
          setError(String(recordsError));
          setActiveRecords([]);
          setActiveRecordsCursor(null);
          setActiveRecordsHasMore(false);
          setActiveRecordsTotalCount(0);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setActiveRecordsLoading(false);
          setActiveRecordsLoadingMore(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    selectedCatalog,
    activeRun?.summary.run_id,
    exploreRunId,
    exploreNodeId,
    selectedFrameId,
    activeView,
    selectedRunRefreshTick,
  ]);

  useEffect(() => {
    if (!selectedCatalog || !activeRun || exploreRunId == null) {
      setRunSpansLoading(false);
      setRunSpansLoadingMore(false);
      setRunSpans(null);
      setRunSpansCursor(null);
      setRunSpansHasMore(false);
      setRunSpansTotalCount(0);
      return;
    }
    if (!viewNeedsRunSpans(activeView)) {
      setRunSpansLoading(false);
      setRunSpansLoadingMore(false);
      setRunSpans(null);
      setRunSpansCursor(null);
      setRunSpansHasMore(false);
      setRunSpansTotalCount(0);
      return;
    }
    if (activeRun.summary.run_id !== exploreRunId) {
      return;
    }
    let cancelled = false;
    setRunSpansLoading(true);
    void fetchRunSpans(selectedCatalog.graph_id, activeRun.summary.run_id, {
      nodeId: exploreNodeId,
      frameId: selectedFrameId,
      limit: RUN_SPANS_PAGE_SIZE,
    })
      .then((payload) => {
        if (!cancelled) {
          setRunSpans(payload.items);
          setRunSpansCursor(payload.next_cursor);
          setRunSpansHasMore(payload.has_more);
          setRunSpansTotalCount(payload.total_count);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setRunSpans([]);
          setRunSpansCursor(null);
          setRunSpansHasMore(false);
          setRunSpansTotalCount(0);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setRunSpansLoading(false);
          setRunSpansLoadingMore(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    selectedCatalog,
    activeRun?.summary.run_id,
    exploreRunId,
    exploreNodeId,
    selectedFrameId,
    activeView,
    selectedRunRefreshTick,
  ]);

  useEffect(() => {
    const projectId = selectedCatalog?.project_id ?? null;
    const graphId = selectedCatalog?.graph_id ?? null;
    const runId = exploreRunId;
    if (!projectId && !graphId && !runId) {
      setRemoteEvents([]);
      return;
    }
    let cancelled = false;
    void fetchRemoteEvents({
      projectId,
      graphId,
      runId,
      limit: 20,
    })
      .then((events) => {
        if (!cancelled) {
          setRemoteEvents(events);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setRemoteEvents([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    selectedCatalog?.project_id,
    selectedCatalog?.graph_id,
    exploreRunId,
    selectedRunRefreshTick,
  ]);

  useEffect(() => {
    if (
      !selectedCatalog ||
      !activeRun ||
      exploreRunId == null ||
      selectedCustomViewId == null
    ) {
      setActiveCustomView(null);
      setCustomViewError(null);
      setCustomViewLoading(false);
      return;
    }
    if (activeRun.summary.run_id !== exploreRunId) {
      return;
    }
    let cancelled = false;
    setCustomViewLoading(true);
    void fetchRunCustomView(
      selectedCatalog.spec_id,
      activeRun.summary.run_id,
      selectedCustomViewId,
    )
      .then((payload) => {
        if (!cancelled) {
          setActiveCustomView(payload);
          setCustomViewError(null);
        }
      })
      .catch((customViewFetchError: unknown) => {
        if (!cancelled) {
          setActiveCustomView(null);
          setCustomViewError(String(customViewFetchError));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setCustomViewLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    selectedCatalog,
    activeRun?.summary.run_id,
    exploreRunId,
    selectedCustomViewId,
    selectedRunRefreshTick,
  ]);

  useEffect(() => {
    if (!selectedCatalog || !activeRun || exploreRunId == null) {
      setMetricGroupsResponse([]);
      setMetricGroupsLoading(false);
      setMetricGroupsRefreshing(false);
      return;
    }
    if (activeRun.summary.run_id !== exploreRunId) {
      return;
    }
    let cancelled = false;
    const shouldRefreshOnly = metricGroupsResponse.length > 0;
    if (!shouldRefreshOnly) {
      setMetricGroupsLoading(true);
    } else {
      setMetricGroupsRefreshing(true);
    }
    void fetchRunMetricGroups(selectedCatalog.spec_id, activeRun.summary.run_id, {
      stepStart: normalizedIterationRange.start,
      stepEnd: normalizedIterationRange.end,
      maxPoints: 120,
      nodeId: exploreNodeId,
      frameId: selectedFrameId,
    })
      .then((payload) => {
        if (!cancelled) {
          setMetricGroupsResponse(payload.groups);
          setError(null);
        }
      })
      .catch((metricGroupsError: unknown) => {
        if (!cancelled) {
          setError(String(metricGroupsError));
          setMetricGroupsResponse([]);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setMetricGroupsLoading(false);
          setMetricGroupsRefreshing(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    selectedCatalog,
    activeRun?.summary.run_id,
    exploreRunId,
    exploreNodeId,
    selectedFrameId,
    normalizedIterationRange.start,
    normalizedIterationRange.end,
    selectedRunRefreshTick,
  ]);

  useEffect(() => {
    if (!explorerUrlHydratedRef.current || catalog.length === 0) {
      return;
    }
    const urlState = {
      pathname: window.location.pathname,
      activeView,
      specId: selectedSpecId,
      window: exploreTimePreset,
      runId: exploreRunId,
      nodeId: exploreNodeId,
      iterationStart: normalizedIterationRange.start,
      iterationEnd: normalizedIterationRange.end,
      spanInspectIndex:
        activeView === "spans" && spansInspector?.kind === "span"
          ? spansInspector.index
          : null,
      recordInspectId:
        activeView === "spans" && spansInspector?.kind === "record"
          ? spansInspector.id
          : null,
    };
    if (explorerRestoringFromHistoryRef.current) {
      explorerRestoringFromHistoryRef.current = false;
      prevActiveViewForHistoryRef.current = activeView;
      return;
    }
    const prevView = prevActiveViewForHistoryRef.current;
    const viewChanged = prevView !== null && prevView !== activeView;
    if (viewChanged) {
      pushUrlWithExplorerState(urlState);
    } else {
      replaceUrlWithExplorerState(urlState);
    }
    prevActiveViewForHistoryRef.current = activeView;
  }, [
    catalog.length,
    selectedSpecId,
    exploreTimePreset,
    exploreRunId,
    exploreNodeId,
    normalizedIterationRange,
    activeView,
    spansInspector,
  ]);

  useEffect(() => {
    const onPopState = () => {
      explorerRestoringFromHistoryRef.current = true;
      const parsed = parseExplorerQuery(window.location.search);
      if (parsed.specId && catalog.some((e) => e.spec_id === parsed.specId)) {
        setSelectedSpecId(parsed.specId);
      }
      if (parsed.window && isExplorerWindowParam(parsed.window)) {
        setExploreTimePreset(parsed.window);
      }
      setExploreRunId(parsed.runId);
      setExploreNodeId(parsed.nodeId);
      setExploreIterationStart(
        parsed.iterationStart != null ? String(parsed.iterationStart) : "",
      );
      setExploreIterationEnd(
        parsed.iterationEnd != null ? String(parsed.iterationEnd) : "",
      );
      spansInspectorHydrateRef.current = {
        si: parsed.spanInspectIndex,
        rid: parsed.recordInspectId,
      };
      const v = viewFromHash(window.location.hash);
      if (v) {
        setActiveView(v);
      }
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [catalog]);

  useEffect(() => {
    if (exploreNodeId != null) {
      setSelectedNodeId(exploreNodeId);
    }
  }, [exploreNodeId]);

  useEffect(() => {
    if (!selectedCatalog) {
      return;
    }
    let cancelled = false;
    const shouldPollTimeseries =
      activeExecution != null ||
      runs.some(
        (run) =>
          run.status === "running" ||
          run.status === "pending" ||
          run.source === "remote-live",
      );

    const pull = (isPoll: boolean) => {
      const { sinceMs, untilMs, rollupMs } = computeExploreWindow(
        exploreTimePreset,
        runs,
      );
      if (!isPoll) {
        setTimeseriesLoading(true);
        setTimeseriesError(null);
      } else {
        setTimeseriesPollBusy(true);
      }
      void fetchTimeseries({
        graphId: selectedCatalog.graph_id,
        invocationName: selectedCatalog.invocation_name,
        sinceMs,
        untilMs,
        rollupMs,
        runId: exploreRunId,
        nodeId: exploreNodeId,
      })
        .then((data) => {
          if (!cancelled) {
            setTimeseries(data);
          }
        })
        .catch((err: unknown) => {
          if (!cancelled) {
            setTimeseriesError(String(err));
            setTimeseries(null);
          }
        })
        .finally(() => {
          if (!cancelled) {
            if (!isPoll) {
              setTimeseriesLoading(false);
            } else {
              setTimeseriesPollBusy(false);
            }
          }
        });
    };

    pull(false);
    let timer: number | null = null;
    if (shouldPollTimeseries) {
      const intervalMs = 15_000;
      timer = window.setInterval(() => {
        if (cancelled || document.visibilityState !== "visible") {
          return;
        }
        pull(true);
      }, intervalMs);
    }
    return () => {
      cancelled = true;
      if (timer != null) {
        window.clearInterval(timer);
      }
    };
  }, [
    activeExecution,
    selectedCatalog,
    exploreTimePreset,
    exploreRunId,
    exploreNodeId,
    runs,
  ]);

  useEffect(() => {
    if (
      !activeExecution ||
      activeExecution.status === "succeeded" ||
      activeExecution.status === "failed"
    ) {
      return;
    }

    const timer = window.setInterval(() => {
      void (async () => {
        const next = await fetchExecution(
          activeExecution.execution_id,
          activeExecution.latest_sequence,
        );
        setActiveExecution((current) => {
          if (!current) {
            return next;
          }
          return {
            ...next,
            records: [...current.records, ...next.records],
            spans: [...current.spans, ...next.spans],
            messages: [...current.messages, ...next.messages],
          };
        });
        setError(null);
        if (next.run_id && selectedCatalog) {
          setExploreRunId(next.run_id);
          await refreshRuns(selectedCatalog);
        }
      })().catch((pollError: unknown) => {
        setError(String(pollError));
      });
    }, 750);

    return () => window.clearInterval(timer);
  }, [activeExecution, refreshRuns, selectedCatalog]);

  useEffect(() => {
    if (!activeRun || !selectedNodeId) {
      setNodeDetail(null);
      return;
    }
    void (async () => {
      try {
        const detail = await fetchNodeDetail(
          activeRun.summary.graph_id,
          activeRun.summary.run_id,
          selectedNodeId,
          selectedFrameId,
        );
        setNodeDetail(detail);
        setError(null);
      } catch (detailError) {
        setError(String(detailError));
      }
    })();
  }, [activeRun, selectedNodeId, selectedFrameId]);

  function selectNodeAndExplorer(nodeId: string, frameId: string | null = null) {
    setExploreNodeId(nodeId);
    setSelectedNodeId(nodeId);
    setSelectedFrameId(frameId);
  }

  async function handleRun(specId: string) {
    try {
      setError(null);
      setActiveExecution(await launchExecution({ specId }));
    } catch (launchError) {
      setError(String(launchError));
    }
  }

  async function handleRunFromPath(specPath: string) {
    try {
      setError(null);
      setActiveExecution(
        await launchExecution({ specPath: specPath.trim() }),
      );
    } catch (launchError) {
      setError(String(launchError));
    }
  }

  const summaryGraph = activeRun?.graph ?? graphPreview;
  const metricGroups = metricGroupsResponse;
  const metricTrendGroups = useMemo(
    () => metricGroups.filter((groupView) => groupView.has_iteration_series),
    [metricGroups],
  );
  const metricCounterSummaries = useMemo(
    () => buildMetricCounterSummaries(metricGroups),
    [metricGroups],
  );
  const frameCount = useMemo(
    () =>
      activeReplay == null
        ? 0
        : new Set(
            activeReplay.node_summaries
              .filter((summary) =>
                isWithinIterationRange(
                  summary.iteration_index,
                  normalizedIterationRange,
                ),
              )
              .map((summary) => summary.frame_id),
          ).size,
    [activeReplay, normalizedIterationRange],
  );
  const availableFrameOptions = useMemo(() => {
    if (activeReplay == null) {
      return [] as string[];
    }
    return [...new Set(
      activeReplay.node_summaries
        .filter((summary) =>
          isWithinIterationRange(summary.iteration_index, normalizedIterationRange),
        )
        .map((summary) => summary.frame_id),
    )].sort((left, right) => {
      if (left === "root") {
        return -1;
      }
      if (right === "root") {
        return 1;
      }
      return left.localeCompare(right);
    });
  }, [activeReplay, normalizedIterationRange]);
  const selectedNodeSummary = useMemo(
    () => getSelectedNodeSummary(activeReplay, selectedNodeId, selectedFrameId),
    [activeReplay, selectedFrameId, selectedNodeId],
  );
  const recordsInTimeWindow = useMemo(
    () =>
      filterRecordsByIterationRange(
        filterRecordsByTimeWindow(
          activeRun?.summary.run_id === exploreRunId ? activeRecords : liveRunRecords,
          exploreWindow.sinceMs,
          exploreWindow.untilMs,
        ),
        normalizedIterationRange,
      ),
    [
      activeRecords,
      activeRun?.summary.run_id,
      exploreRunId,
      exploreWindow,
      liveRunRecords,
      normalizedIterationRange,
    ],
  );
  const selectedNodeRecords = useMemo(
    () =>
      filterRecords(recordsInTimeWindow, exploreNodeId, selectedFrameId),
    [recordsInTimeWindow, selectedFrameId, exploreNodeId],
  );
  const explorerRecordsInWindowCount = useMemo(() => {
    if (exploreRunId != null) {
      return activeRecordsTotalCount;
    }
    return runsInExploreWindow.reduce((sum, r) => sum + r.record_count, 0);
  }, [exploreRunId, activeRecordsTotalCount, runsInExploreWindow]);
  const recentRunSuccessLabel = useMemo(
    () => formatSuccessRateForRuns(runsInExploreWindow),
    [runsInExploreWindow],
  );
  const warningInvariantCount = useMemo(() => {
    if (exploreRunId == null) {
      return "—";
    }
      return String(
        (activeRun?.invariants ?? []).filter(
          (item) =>
            item.severity === "warning" &&
            isWithinIterationRange(item.iteration_index, normalizedIterationRange),
        ).length,
      );
  }, [exploreRunId, activeRun?.invariants, normalizedIterationRange]);
  const selectedNodeEdges = useMemo(
    () => getConnectedEdges(summaryGraph, selectedNodeId),
    [summaryGraph, selectedNodeId],
  );
  const spanItems = useMemo(
    () =>
      filterSpansByIterationRange(
        buildSpanViews(
          nodeDetail,
          activeRun?.summary.run_id === exploreRunId
            ? runSpans
            : activeExecution?.run_id === exploreRunId
              ? activeExecution.spans
              : runSpans,
          exploreNodeId,
          selectedFrameId,
        ),
        normalizedIterationRange,
      ),
    [
      activeExecution?.run_id,
      activeExecution?.spans,
      activeRun?.summary.run_id,
      nodeDetail,
      runSpans,
      exploreNodeId,
      selectedFrameId,
      exploreRunId,
      normalizedIterationRange,
    ],
  );

  useEffect(() => {
    setSpansInspector(null);
  }, [exploreRunId]);

  useEffect(() => {
    if (activeView !== "spans") {
      setSpansInspector(null);
    }
  }, [activeView]);

  useEffect(() => {
    if (activeView !== "spans") {
      return;
    }
    const h = spansInspectorHydrateRef.current;
    if (!h || (h.rid == null && h.si == null)) {
      return;
    }
    if (h.rid != null) {
      spansInspectorHydrateRef.current = null;
      setSpansInspector({ kind: "record", id: h.rid });
      return;
    }
    if (h.si != null && h.si >= 0 && h.si < spanItems.length) {
      spansInspectorHydrateRef.current = null;
      setSpansInspector({ kind: "span", index: h.si });
    }
  }, [activeView, spanItems]);

  const runContext = useMemo<ScopeToken[]>(() => {
    const windowLabel = EXPLORE_PRESET_LABEL[exploreTimePreset];
    const base: ScopeToken[] = activeRun
      ? [
          { label: "spec", value: selectedCatalog?.spec_id ?? "n/a" },
          { label: "graph", value: activeRun.summary.graph_id },
          { label: "run", value: activeRun.summary.run_id },
          {
            label: "steps",
            value: formatIterationRangeLabel(normalizedIterationRange),
          },
          {
            label: "invocation",
            value:
              activeRun.summary.invocation_name ??
              selectedCatalog?.invocation_name ??
              "n/a",
          },
          {
            label: "profiles",
            value:
              activeRun.summary.runtime_profile_names.join(", ") ||
              activeRun.summary.runtime_default_profile_name ||
              "n/a",
          },
        ]
      : [
          { label: "spec", value: selectedCatalog?.spec_id ?? "n/a" },
          { label: "graph", value: selectedCatalog?.graph_id ?? "n/a" },
          {
            label: "invocation",
            value: selectedCatalog?.invocation_name ?? "n/a",
          },
          {
            label: "steps",
            value: formatIterationRangeLabel(normalizedIterationRange),
          },
        ];
    /* Facet row ($window, @run_id, @node_id) lives in ExplorerScopeBar — omit here to avoid duplicate UI. */
    if (selectedCatalog) {
      return base;
    }
    return [
      ...base,
      { label: "$window", value: windowLabel },
      { label: "@run_id", value: exploreRunId ?? "all" },
      { label: "@node_id", value: exploreNodeId ?? "all" },
    ];
  }, [
    activeRun,
    selectedCatalog,
    exploreTimePreset,
    exploreRunId,
    exploreNodeId,
    normalizedIterationRange,
  ]);

  const activeViewLabel =
    VIEWS.find((view) => view.id === activeView)?.label ?? "Dashboard";

  const shareExplorerUrl = useMemo(() => {
    const path = buildExplorerUrl({
      pathname:
        typeof window !== "undefined" ? window.location.pathname : "/",
      activeView,
      specId: selectedSpecId,
      window: exploreTimePreset,
      runId: exploreRunId,
      nodeId: exploreNodeId,
      iterationStart: normalizedIterationRange.start,
      iterationEnd: normalizedIterationRange.end,
      spanInspectIndex:
        activeView === "spans" && spansInspector?.kind === "span"
          ? spansInspector.index
          : null,
      recordInspectId:
        activeView === "spans" && spansInspector?.kind === "record"
          ? spansInspector.id
          : null,
    });
    if (typeof window === "undefined") {
      return path;
    }
    return `${window.location.origin}${path}`;
  }, [
    activeView,
    selectedSpecId,
    exploreTimePreset,
    exploreRunId,
    exploreNodeId,
    normalizedIterationRange,
    spansInspector,
  ]);

  async function copyExplorerLink() {
    try {
      await navigator.clipboard.writeText(shareExplorerUrl);
      setShareCopied(true);
      window.setTimeout(() => setShareCopied(false), 2000);
    } catch {
      setError("Could not copy link to clipboard.");
    }
  }

  const openTracesAtSpanIndex = useCallback((index: number) => {
    setActiveView("spans");
    setSpansInspector({ kind: "span", index });
  }, []);

  function switchView(view: ViewId) {
    setActiveView(view);
  }

  return (
    <div className="dashboard-shell v3-shell">
      <aside className="app-nav">
        <div className="nav-brand">
          <div className="nav-mark">
            <span />
          </div>
          <div className="nav-brand-title">mentalmodel</div>
        </div>

        <nav className="nav-section">
          {VIEWS.map((view) => (
            <a
              key={view.id}
              href={`#${view.id}`}
              title={view.title}
              className={`nav-item ${activeView === view.id ? "active" : ""}`}
              onClick={(event) => {
                event.preventDefault();
                switchView(view.id);
              }}
            >
              {view.label}
            </a>
          ))}
        </nav>

        <div className="nav-section nav-section-fill">
          <label className="nav-spec-label" htmlFor="nav-spec-select">
            Spec
          </label>
          <select
            id="nav-spec-select"
            className="nav-spec-select"
            value={selectedSpecId ?? ""}
            onChange={(event) => {
              const nextSpecId = event.target.value || null;
              startTransition(() => {
                setSelectedSpecId(nextSpecId);
              });
            }}
          >
            {catalog.map((entry) => (
              <option key={entry.spec_id} value={entry.spec_id}>
                {entry.label}
              </option>
            ))}
          </select>
        </div>

        <footer className="nav-footer">
          <span className="nav-footer-label">Also via terminal</span>
          <code className="nav-footer-code" title="Same data as this dashboard">
            uv run mentalmodel ui
          </code>
        </footer>
      </aside>

      <main className="workspace v3-workspace">
        <section className="topbar-shell">
          <div className="topbar-copy">
            <div className="breadcrumbs">
              {buildBreadcrumbs(activeView, selectedCatalog, selectedNodeId).map(
                (crumb, index, all) => (
                  <span key={`${crumb}:${index}`} className="breadcrumb-part">
                    <span className={index === all.length - 1 ? "active" : ""}>
                      {crumb}
                    </span>
                    {index < all.length - 1 ? (
                      <span className="breadcrumb-sep" aria-hidden>
                        ›
                      </span>
                    ) : null}
                  </span>
                ),
              )}
            </div>
            <div className="topbar-title-row">
              <h1>{activeViewLabel}</h1>
            </div>
          </div>
          <div className="topbar-actions">
            <button
              type="button"
              className={`share-link-btn ${shareCopied ? "share-link-btn-done" : ""}`}
              onClick={() => void copyExplorerLink()}
              title="Copy a shareable URL including spec, time window, run, node, and this screen"
            >
              {shareCopied ? "Copied" : "Copy explorer link"}
            </button>
          </div>
        </section>

        {error ? <div className="error-banner">{error}</div> : null}

        {selectedCatalog ? (
          <ExplorerScopeBar
          exploreNodeId={exploreNodeId}
          exploreIterationEnd={exploreIterationEnd}
          exploreIterationStart={exploreIterationStart}
          exploreRunId={exploreRunId}
          exploreTimePreset={exploreTimePreset}
          selectedFrameId={selectedFrameId}
          frameOptions={availableFrameOptions}
          iterationBounds={iterationBounds}
          graphNodes={summaryGraph?.nodes ?? []}
          runs={runsForExplorerDropdown}
          setExploreNodeId={setExploreNodeId}
          setExploreIterationEnd={setExploreIterationEnd}
          setExploreIterationStart={setExploreIterationStart}
          setExploreRunId={setExploreRunId}
          setExploreTimePreset={setExploreTimePreset}
          setSelectedFrameId={setSelectedFrameId}
        />
        ) : null}

        {renderCurrentView({
          activeExecution,
          activeCustomView,
          activeRun,
          activeReplay,
          activeView,
          catalog,
          explorerRecordsInWindowCount,
          exploreNodeId,
          filteredCustomView: filterCustomViewByIterationRange(
            activeCustomView,
            normalizedIterationRange,
          ),
          exploreRunId,
          frameCount,
          graphFindings,
          handleRun,
          handleRunFromPath,
          customViewError,
          customViewLoading,
          liveMessages,
          liveRecords,
          metricCounterSummaries,
          metricGroups,
          metricTrendGroups,
          metricGroupsLoading,
          metricGroupsRefreshing,
          remoteEvents,
          activeRecordsHasMore,
          activeRecordsLoading,
          activeRecordsLoadingMore,
          activeRecordsTotalCount,
          nodeDetail,
          recentRunSuccessLabel,
          runContext,
          runs,
          runsForExplorerList: runsForExplorerDropdown,
          selectNodeAndExplorer,
          selectedCatalog,
          selectedCustomViewId,
          selectedFrameId,
          selectedNodeEdges,
          selectedNodeId,
          recordsInTimeWindow,
          selectedNodeRecords,
          selectedNodeSummary,
          iterationBounds,
          normalizedIterationRange,
          setActiveView,
          setCatalog,
          setSelectedCustomViewId,
          setExploreRunId,
          setSelectedFrameId,
          setSelectedNodeId,
          setSelectedSpecId,
          setSpansInspector,
          loadOlderRecords,
          loadOlderSpans,
          spanItems,
          spansInspector,
          summaryGraph,
          warningInvariantCount,
          customSpecPath,
          setCustomSpecPath,
          timeseries,
          timeseriesError,
          timeseriesLoading,
          timeseriesPollBusy,
          runSpansLoading,
          runSpansHasMore,
          runSpansLoadingMore,
          runSpansTotalCount,
          openTracesAtSpanIndex,
          specSwitchLoading,
        })}
      </main>
    </div>
  );
}

function renderCurrentView({
  activeExecution,
  activeCustomView,
  activeRun,
  activeReplay,
  activeView,
  catalog,
  explorerRecordsInWindowCount,
  exploreNodeId,
  filteredCustomView,
  exploreRunId,
  frameCount,
  graphFindings,
  handleRun,
  handleRunFromPath,
  customViewError,
  customViewLoading,
  liveMessages,
  liveRecords,
  metricCounterSummaries,
  metricGroups,
  metricTrendGroups,
  metricGroupsLoading,
  metricGroupsRefreshing,
  remoteEvents,
  nodeDetail,
  recentRunSuccessLabel,
  runContext,
  runs,
  runsForExplorerList,
  activeRecordsHasMore,
  activeRecordsLoading,
  activeRecordsLoadingMore,
  activeRecordsTotalCount,
  selectNodeAndExplorer,
  selectedCatalog,
  selectedCustomViewId,
  selectedFrameId,
  selectedNodeEdges,
  selectedNodeId,
  recordsInTimeWindow,
  selectedNodeRecords,
  selectedNodeSummary,
  iterationBounds,
  normalizedIterationRange,
  setActiveView,
  setCatalog,
  setSelectedCustomViewId,
  setExploreRunId,
  setSelectedFrameId,
  setSelectedNodeId,
  setSelectedSpecId,
  setSpansInspector,
  loadOlderRecords,
  loadOlderSpans,
  spanItems,
  spansInspector,
  summaryGraph,
  warningInvariantCount,
  customSpecPath,
  setCustomSpecPath,
  timeseries,
  timeseriesError,
  timeseriesLoading,
  timeseriesPollBusy,
  runSpansLoading,
  runSpansHasMore,
  runSpansLoadingMore,
  runSpansTotalCount,
  openTracesAtSpanIndex,
  specSwitchLoading,
}: {
  activeExecution: ExecutionSession | null;
  activeCustomView: EvaluatedCustomView | null;
  activeRun: RunOverview | null;
  activeReplay: ReplayReport | null;
  activeView: ViewId;
  catalog: CatalogEntry[];
  explorerRecordsInWindowCount: number;
  exploreNodeId: string | null;
  filteredCustomView: EvaluatedCustomView | null;
  exploreRunId: string | null;
  frameCount: number;
  graphFindings: AnalysisFinding[];
  handleRun: (specId: string) => Promise<void>;
  handleRunFromPath: (specPath: string) => Promise<void>;
  customViewError: string | null;
  customViewLoading: boolean;
  liveMessages: ExecutionMessage[];
  liveRecords: ExecutionRecord[];
  metricCounterSummaries: MetricSummary[];
  metricGroups: MetricGroupQueryResult[];
  metricTrendGroups: MetricGroupQueryResult[];
  metricGroupsLoading: boolean;
  metricGroupsRefreshing: boolean;
  remoteEvents: RemoteOperationEvent[];
  activeRecordsHasMore: boolean;
  activeRecordsLoading: boolean;
  activeRecordsLoadingMore: boolean;
  activeRecordsTotalCount: number;
  nodeDetail: NodeDetail | null;
  recentRunSuccessLabel: string;
  runContext: ScopeToken[];
  runs: RunSummary[];
  runsForExplorerList: RunSummary[];
  selectNodeAndExplorer: (nodeId: string, frameId?: string | null) => void;
  selectedCatalog: CatalogEntry | null;
  selectedCustomViewId: string | null;
  selectedFrameId: string | null;
  selectedNodeEdges: { upstream: GraphEdge[]; downstream: GraphEdge[] };
  selectedNodeId: string | null;
  recordsInTimeWindow: ExecutionRecord[];
  selectedNodeRecords: ExecutionRecord[];
  selectedNodeSummary: ReplayNodeSummary | null;
  iterationBounds: IterationBounds | null;
  normalizedIterationRange: NormalizedIterationRange;
  setActiveView: (view: ViewId) => void;
  setCatalog: (entries: CatalogEntry[]) => void;
  setSelectedCustomViewId: (viewId: string | null) => void;
  setExploreRunId: (runId: string | null) => void;
  setSelectedFrameId: (frameId: string | null) => void;
  setSelectedNodeId: (nodeId: string | null) => void;
  setSelectedSpecId: (specId: string | null) => void;
  setSpansInspector: Dispatch<SetStateAction<SpansInspector | null>>;
  loadOlderRecords: () => void;
  loadOlderSpans: () => void;
  spanItems: GenericSpan[];
  spansInspector: SpansInspector | null;
  summaryGraph: GraphPayload | null;
  warningInvariantCount: string;
  customSpecPath: string;
  setCustomSpecPath: (path: string) => void;
  timeseries: TimeseriesResponse | null;
  timeseriesError: string | null;
  timeseriesLoading: boolean;
  timeseriesPollBusy: boolean;
  runSpansLoading: boolean;
  runSpansHasMore: boolean;
  runSpansLoadingMore: boolean;
  runSpansTotalCount: number;
  openTracesAtSpanIndex: (spanIndex: number) => void;
  specSwitchLoading: boolean;
}) {
  switch (activeView) {
    case "overview":
      return (
        <OverviewView
          activeExecution={activeExecution}
          activeCustomView={filteredCustomView}
          activeRun={activeRun}
          exploreRunId={exploreRunId}
          explorerRecordsInWindowCount={explorerRecordsInWindowCount}
          frameCount={frameCount}
          iterationBounds={iterationBounds}
          graphFindings={graphFindings}
          handleRun={handleRun}
          liveMessages={liveMessages}
          liveRecords={liveRecords}
          metricCounterSummaries={metricCounterSummaries}
          metricGroups={metricGroups}
          metricTrendGroups={metricTrendGroups}
          metricGroupsLoading={metricGroupsLoading}
          metricGroupsRefreshing={metricGroupsRefreshing}
          remoteEvents={remoteEvents}
          recentRunSuccessLabel={recentRunSuccessLabel}
          recordsInTimeWindow={recordsInTimeWindow}
          runContext={runContext}
          runsForExplorerList={runsForExplorerList}
          selectNodeAndExplorer={selectNodeAndExplorer}
          selectedCatalog={selectedCatalog}
          setActiveView={setActiveView}
          setExploreRunId={setExploreRunId}
          setSelectedFrameId={setSelectedFrameId}
          timeseries={timeseries}
          timeseriesError={timeseriesError}
          timeseriesLoading={timeseriesLoading}
          timeseriesPollBusy={timeseriesPollBusy}
          warningInvariantCount={warningInvariantCount}
          spanItems={spanItems}
          specSwitchLoading={specSwitchLoading}
        />
      );
    case "views":
      return (
        <CustomViewsView
          activeCustomView={filteredCustomView}
          activeRun={activeRun}
          customViewError={customViewError}
          customViewLoading={customViewLoading}
          runContext={runContext}
          selectedCatalog={selectedCatalog}
          selectedCustomViewId={selectedCustomViewId}
          setSelectedCustomViewId={setSelectedCustomViewId}
        />
      );
    case "graph":
      return (
        <GraphView
          activeRun={activeRun}
          activeReplay={activeReplay}
          runContext={runContext}
          selectedCatalog={selectedCatalog}
          selectedFrameId={selectedFrameId}
          selectedNodeId={selectedNodeId}
          selectedNodeSummary={selectedNodeSummary}
          selectNodeAndExplorer={selectNodeAndExplorer}
          setActiveView={setActiveView}
          summaryGraph={summaryGraph}
        />
      );
    case "node":
      return (
        <NodeDetailView
          activeRun={activeRun}
          nodeDetail={nodeDetail}
          openTracesAtSpanIndex={openTracesAtSpanIndex}
          runContext={runContext}
          selectedFrameId={selectedFrameId}
          selectedNodeEdges={selectedNodeEdges}
          selectedNodeId={selectedNodeId}
          selectedNodeRecords={selectedNodeRecords}
          selectedNodeSummary={selectedNodeSummary}
          setSelectedFrameId={setSelectedFrameId}
          spanItems={spanItems}
        />
      );
    case "spans":
      return (
        <SpansRecordsView
          activeRun={activeRun}
          exploreNodeId={exploreNodeId}
          exploreRunId={exploreRunId}
          nodeDetail={nodeDetail}
          runFailureMessage={activeRun?.runtime_error ?? null}
          runContext={runContext}
          runRecordsInWindow={recordsInTimeWindow}
          runRecordsHasMore={activeRecordsHasMore}
          runRecordsLoading={activeRecordsLoading}
          runRecordsLoadingMore={activeRecordsLoadingMore}
          runRecordsTotalCount={activeRecordsTotalCount}
          runSpansLoading={runSpansLoading}
          runSpansHasMore={runSpansHasMore}
          runSpansLoadingMore={runSpansLoadingMore}
          runSpansTotalCount={runSpansTotalCount}
          selectedNodeId={selectedNodeId}
          selectedFrameId={selectedFrameId}
          selectedNodeRecords={selectedNodeRecords}
          onLoadOlderRecords={loadOlderRecords}
          onLoadOlderSpans={loadOlderSpans}
          setSpansInspector={setSpansInspector}
          spanItems={spanItems}
          spansInspector={spansInspector}
        />
      );
    case "launch":
      return (
        <LaunchCompareView
          activeExecution={activeExecution}
          catalog={catalog}
          customSpecPath={customSpecPath}
          handleRun={handleRun}
          handleRunFromPath={handleRunFromPath}
          liveMessages={liveMessages}
          liveRecords={liveRecords}
          runs={runs}
          selectedCatalog={selectedCatalog}
          setCatalog={setCatalog}
          setCustomSpecPath={setCustomSpecPath}
          setExploreRunId={setExploreRunId}
          setSelectedSpecId={setSelectedSpecId}
        />
      );
    default:
      return null;
  }
}

function OverviewView({
  activeExecution,
  activeCustomView,
  activeRun,
  exploreRunId,
  explorerRecordsInWindowCount,
  frameCount,
  iterationBounds,
  graphFindings,
  handleRun,
  liveMessages,
  liveRecords,
  metricCounterSummaries,
  metricGroups,
  metricTrendGroups,
  metricGroupsLoading,
  metricGroupsRefreshing,
  remoteEvents,
  recentRunSuccessLabel,
  recordsInTimeWindow,
  runContext,
  runsForExplorerList,
  selectNodeAndExplorer,
  selectedCatalog,
  setActiveView,
  setExploreRunId,
  setSelectedFrameId,
  timeseries,
  timeseriesError,
  timeseriesLoading,
  timeseriesPollBusy,
  warningInvariantCount,
  spanItems,
  specSwitchLoading,
}: {
  activeExecution: ExecutionSession | null;
  activeCustomView: EvaluatedCustomView | null;
  activeRun: RunOverview | null;
  exploreRunId: string | null;
  explorerRecordsInWindowCount: number;
  frameCount: number;
  iterationBounds: IterationBounds | null;
  graphFindings: AnalysisFinding[];
  handleRun: (specId: string) => Promise<void>;
  liveMessages: ExecutionMessage[];
  liveRecords: ExecutionRecord[];
  metricCounterSummaries: MetricSummary[];
  metricGroups: MetricGroupQueryResult[];
  metricTrendGroups: MetricGroupQueryResult[];
  metricGroupsLoading: boolean;
  metricGroupsRefreshing: boolean;
  remoteEvents: RemoteOperationEvent[];
  recentRunSuccessLabel: string;
  recordsInTimeWindow: ExecutionRecord[];
  runContext: ScopeToken[];
  runsForExplorerList: RunSummary[];
  selectNodeAndExplorer: (nodeId: string, frameId?: string | null) => void;
  selectedCatalog: CatalogEntry | null;
  setActiveView: (view: ViewId) => void;
  setExploreRunId: (runId: string | null) => void;
  setSelectedFrameId: (frameId: string | null) => void;
  timeseries: TimeseriesResponse | null;
  timeseriesError: string | null;
  timeseriesLoading: boolean;
  timeseriesPollBusy: boolean;
  warningInvariantCount: string;
  spanItems: GenericSpan[];
  specSwitchLoading: boolean;
}) {
  const currentRecordCount = String(explorerRecordsInWindowCount);
  const frameCountKpi =
    exploreRunId == null ? "—" : String(frameCount);
  const visibleStepCount =
    iterationBounds == null
      ? "—"
      : String(iterationBounds.max - iterationBounds.min + 1);
  const latestIterationLabel =
    iterationBounds == null ? "—" : `i${iterationBounds.max}`;
  const previewRecords = recordsInTimeWindow.slice(0, 8);
  const previewCustomRows = activeCustomView?.rows.slice(-6).reverse() ?? [];
  const runFailed =
    activeRun?.summary.status === "failed" ||
    activeRun?.verification_success === false ||
    activeRun?.runtime_error != null;
  const failureCards: Array<{ label: string; copy: string }> = [];
  if (runFailed && activeRun != null && activeRun.runtime_error) {
    failureCards.push({
      label: "runtime failure",
      copy:
        activeRun.summary.output_count === 0
          ? `Run failed before outputs were fully persisted. ${activeRun.runtime_error}`
          : activeRun.runtime_error,
    });
  }
  if (
    runFailed &&
    activeRun != null &&
    activeRun.summary.output_count === 0 &&
    !activeRun.runtime_error
  ) {
    failureCards.push({
      label: "missing outputs",
      copy:
        "Run failed before frame or node outputs were written to the persisted bundle.",
    });
  }

  return (
    <>
      <RunContextStrip tokens={runContext} />

      {failureCards.length > 0 ? (
        <section className="analysis-stack">
          {failureCards.map((card) => (
            <AlertCard
              key={`${card.label}:${card.copy}`}
              copy={card.copy}
              label={card.label}
              tone="error"
            />
          ))}
        </section>
      ) : null}

      {specSwitchLoading ? (
        <section className="analysis-stack">
          <AlertCard
            copy="Switching specs. Keeping the current surface visible until the next spec hydrates to avoid destructive flicker."
            label="loading next spec"
            tone="warning"
          />
        </section>
      ) : null}

      <section className="hero-grid v3-hero-grid">
        <KpiCard
          label="Visible steps"
          value={visibleStepCount}
          source="source: selected run + current @step range"
          tone="accent"
        />
        <KpiCard
          label="Recent run success"
          value={recentRunSuccessLabel}
          source="source: runs in explorer $window"
          tone="ok"
        />
        <KpiCard
          label="Persisted records"
          value={currentRecordCount}
          source={
            exploreRunId == null
              ? "source: sum of summary record_count for runs in $window (pick @run_id for one bundle)"
              : "source: loaded run + $window + @node_id"
          }
          tone="accent"
        />
        <KpiCard
          label="Warning invariants"
          value={warningInvariantCount}
          source="source: invariant.checked"
          tone="warning"
        />
        <KpiCard
          label="Latest visible step"
          value={latestIterationLabel}
          source="source: selected run + current @step range"
        />
        <KpiCard
          label="Visible frames"
          value={frameCountKpi}
          source="source: replay frames in current @step range"
        />
      </section>

      <section className="overview-layout">
        <div className="stack">
          <Panel className="semantic-rate-panel" title="Semantic event rate">
            <ExplorerTimeseriesChart
              error={timeseriesError}
              loading={timeseriesLoading}
              pollBusy={timeseriesPollBusy}
              timeseries={timeseries}
            />
          </Panel>

          <Panel
            title="Learning and stability trends"
            subtitle="Iteration-scoped metric groups answer the training questions that matter: is reward moving, is loss stable, is KL under control, and are updates drifting."
          >
            {metricGroupsLoading && metricTrendGroups.length === 0 ? (
              <EmptyState copy="Loading grouped metrics…" />
            ) : metricTrendGroups.length > 0 ? (
              <div className="metric-groups-stack">
                {metricTrendGroups.map((groupView) => (
                  <MetricTrendPanel
                    key={groupView.group_id}
                    group={groupView}
                    refreshing={metricGroupsRefreshing}
                    onInspectMetric={(series) => {
                      selectNodeAndExplorer(
                        series.node_id,
                        series.frame_id && series.frame_id !== "root"
                          ? series.frame_id
                          : null,
                      );
                      setActiveView("node");
                    }}
                  />
                ))}
              </div>
            ) : (
              <EmptyState copy="This spec does not expose iteration-scoped metric groups in the current explorer range." />
            )}
          </Panel>

          <Panel
            title="Latency and bottlenecks"
            subtitle="Use this panel to answer “what is the latency per step?” and “what is blocking the step right now?”"
          >
            <SpanLatencyInsights
              spans={spanItems}
              onInspectNode={(nodeId) => {
                selectNodeAndExplorer(nodeId, null);
                setSelectedFrameId(null);
                setActiveView("spans");
              }}
            />
          </Panel>
        </div>

        <div className="stack narrow">
          <Panel title="Runs (explorer scope)">
            <div className="interactive-list">
              {runsForExplorerList.length > 0 ? (
                runsForExplorerList.slice(0, 8).map((run) => (
                  <button
                    key={run.run_id}
                    className={`list-card ${activeRun?.summary.run_id === run.run_id ? "active" : ""}`}
                    onClick={() => {
                      setExploreRunId(run.run_id);
                    }}
                  >
                    <div className="list-card-head">
                      <span>{run.invocation_name ?? run.run_id.slice(0, 12)}</span>
                      <StatusChip
                        label={run.status}
                        tone={
                          run.status === "succeeded"
                            ? "ok"
                            : run.status === "failed"
                              ? "error"
                              : "accent"
                        }
                      />
                    </div>
                    <div className="list-card-copy">
                      {new Date(run.created_at_ms).toLocaleTimeString()} · {run.record_count} records ·{" "}
                      {run.runtime_profile_names.join(", ") || run.runtime_default_profile_name}
                    </div>
                  </button>
                ))
              ) : (
                <EmptyState copy="This spec has no persisted runs yet. Launch one from the workspace." />
              )}
            </div>
          </Panel>

          <Panel
            title="Run counters and gauges"
            subtitle="Summary widgets are reserved for counters, snapshots, and sparse gauges. Trend-heavy metrics stay in charts above."
          >
            {metricGroupsLoading && metricCounterSummaries.length === 0 ? (
              <EmptyState copy="Loading counter and gauge summaries…" />
            ) : metricCounterSummaries.length > 0 ? (
              <div className="metric-counter-grid">
                {metricCounterSummaries.map((summary) => (
                  <button
                    key={summary.key}
                    className="metric-counter-card"
                    title={`${summary.label} · ${summary.kindLabel}`}
                    onClick={() => {
                      selectNodeAndExplorer(
                        summary.series.node_id,
                        summary.series.frame_id && summary.series.frame_id !== "root"
                          ? summary.series.frame_id
                          : null,
                      );
                      setActiveView("node");
                    }}
                  >
                    <div className="metric-counter-head">
                      <span className="metric-counter-label">{summary.label}</span>
                      <span className="metric-counter-kind">{summary.seriesKind}</span>
                    </div>
                    <div className="metric-counter-primary">
                      {formatMetricValueWithUnit(summary.latestValue, summary.unit)}
                    </div>
                    <MetricSparkline series={summary.series} />
                    <div className="metric-counter-meta">
                      {summary.pointCount > 1 ? (
                        <strong>
                          {formatMetricDelta(
                            summary.series.summary.window_delta,
                            summary.unit,
                          )}{" "}
                          visible window
                        </strong>
                      ) : (
                        <span>single visible point</span>
                      )}
                      <span>{summary.pointsLabel}</span>
                    </div>
                    <div className="metric-counter-foot">
                      <span>{summary.kindLabel}</span>
                      <span>{summary.extremaLabel}</span>
                    </div>
                  </button>
                ))}
              </div>
            ) : (
              <EmptyState copy="No cumulative counter-like metrics are available in the current explorer range." />
            )}
          </Panel>

          <Panel
            title={activeCustomView?.view.title ?? "Behavior preview"}
            subtitle="Compact preview of the currently selected table view. Open Tables for full row detail."
          >
            {!activeRun ? (
              <EmptyState copy="Pick a run in Explorer to preview run tables." />
            ) : activeCustomView ? (
              <div className="custom-view-preview">
                <div className="chip-row wrap">
                  <StatusChip label={`${activeCustomView.row_count} rows`} tone="accent" />
                  <StatusChip label={activeCustomView.view.kind} />
                </div>
                <div className="records-table-wrap custom-view-preview-wrap">
                  <table className="records-table custom-view-table custom-view-preview-table">
                    <thead>
                      <tr>
                        {activeCustomView.view.columns.slice(0, 4).map((column) => (
                          <th key={column.column_id}>{column.title}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {previewCustomRows.map((row) => (
                        <tr key={row.row_id}>
                          {activeCustomView.view.columns.slice(0, 4).map((column) => (
                            <td key={`${row.row_id}:${column.column_id}`}>
                              {formatCustomViewCellPreview(row.values[column.column_id], column)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <button
                  type="button"
                  className="primary-action secondary"
                  onClick={() => setActiveView("views")}
                >
                  Open full table
                </button>
              </div>
            ) : (
              <EmptyState copy="This spec does not expose any custom views yet." />
            )}
          </Panel>
        </div>
      </section>

      <section className="overview-bottom">
        <div className="stack">
          <Panel title="Records">
            <RecordConsole
              records={previewRecords}
              fallbackRecords={activeExecution?.records.slice(-8) ?? []}
            />
          </Panel>

          <Panel
            title="Live run"
            aside={
              selectedCatalog ? (
                <button
                  className="primary-action"
                  disabled={!selectedCatalog.launch_enabled}
                  onClick={() => void handleRun(selectedCatalog.spec_id)}
                >
                  Run verification
                </button>
              ) : null
            }
          >
            {activeExecution ? (
              <div className="live-panel">
                <div className="chip-row">
                  <StatusChip
                    label={activeExecution.status}
                    tone={
                      activeExecution.status === "succeeded"
                        ? "ok"
                        : activeExecution.status === "failed"
                          ? "error"
                          : "accent"
                    }
                  />
                  <StatusChip label={activeExecution.spec.label} />
                </div>
                <div className="live-list">
                  {(liveRecords.slice(-5) ?? []).map((record) => (
                    <button
                      key={record.record_id}
                      className="list-card"
                      onClick={() => {
                        selectNodeAndExplorer(
                          record.node_id,
                          record.frame_id !== "root" ? record.frame_id : null,
                        );
                        setActiveView("node");
                      }}
                    >
                      <div className="list-card-head">
                        <span>{record.node_id}</span>
                        <span className="event-pill">{record.event_type}</span>
                      </div>
                      <div className="list-card-copy">
                        {record.frame_id}
                        {record.loop_node_id ? ` · ${record.loop_node_id}` : ""}
                      </div>
                    </button>
                  ))}
                  {liveMessages.length > 0 ? (
                    liveMessages.slice(-5).map((message) => (
                      <div
                        key={`${message.sequence}:${message.timestamp_ms}`}
                        className="list-card"
                      >
                        <div className="list-card-head">
                          <span>{message.source}</span>
                          <span className="event-pill">{message.level}</span>
                        </div>
                        <div className="list-card-copy">{message.message}</div>
                      </div>
                    ))
                  ) : null}
                  {liveRecords.length === 0 &&
                  liveMessages.length === 0 &&
                  activeExecution.spec.project_id &&
                  activeExecution.spec.project_id !== "mentalmodel-examples" ? (
                    <div className="empty-state">
                      External run started. Live records have not arrived yet, so
                      the worker is likely still in setup before the workflow
                      begins emitting semantic events.
                    </div>
                  ) : null}
                </div>
              </div>
            ) : (
              <EmptyState copy="Launch a run to watch the live semantic stream." />
            )}
          </Panel>
        </div>

        <div className="stack">
          <Panel title="Remote delivery">
            {activeRun?.remote_delivery || activeExecution?.live_execution_delivery ? (
              <div className="stack compact">
                {activeRun?.remote_delivery ? (
                  <div className="list-card">
                    <div className="list-card-head">
                      <span>Service health</span>
                      <StatusChip
                        label={activeRun.remote_delivery.last_status ?? "unknown"}
                        tone={
                          activeRun.remote_delivery.last_status === "succeeded"
                            ? "ok"
                            : activeRun.remote_delivery.last_status === "failed"
                              ? "error"
                              : "accent"
                        }
                      />
                    </div>
                    <div className="list-card-copy">
                      {activeRun.remote_delivery.last_kind ?? "no events"} · failures (24h):{" "}
                      {activeRun.remote_delivery.recent_failure_count} · successes (24h):{" "}
                      {activeRun.remote_delivery.recent_success_count}
                    </div>
                    {activeRun.remote_delivery.last_error_message ? (
                      <div className="list-card-copy">
                        {activeRun.remote_delivery.last_error_message}
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {activeExecution?.live_execution_delivery ? (
                  <div className="list-card">
                    <div className="list-card-head">
                      <span>Producer delivery</span>
                      <StatusChip
                        label={
                          activeExecution.live_execution_delivery.success
                            ? "succeeded"
                            : "failed"
                        }
                        tone={
                          activeExecution.live_execution_delivery.success ? "ok" : "error"
                        }
                      />
                    </div>
                    <div className="list-card-copy">
                      start attempts:{" "}
                      {String(activeExecution.live_execution_delivery.start_attempt_count)} · update attempts:{" "}
                      {String(activeExecution.live_execution_delivery.update_attempt_count)}
                    </div>
                    {activeExecution.live_execution_delivery.error ? (
                      <div className="list-card-copy">
                        {activeExecution.live_execution_delivery.error}
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {remoteEvents.length > 0 ? (
                  <div className="interactive-list">
                    {remoteEvents.slice(0, 6).map((event) => (
                      <div key={event.event_id} className="list-card">
                        <div className="list-card-head">
                          <span>{event.kind}</span>
                          <StatusChip
                            label={event.status}
                            tone={event.status === "succeeded" ? "ok" : "error"}
                          />
                        </div>
                        <div className="list-card-copy">
                          {new Date(event.occurred_at_ms).toLocaleTimeString()}
                          {event.error_message ? ` · ${event.error_message}` : ""}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : (
              <EmptyState copy="No remote delivery diagnostics available for the current scope." />
            )}
          </Panel>

          <Panel title="Static analysis">
            {graphFindings.length > 0 ? (
              <div className="analysis-stack">
                {graphFindings.slice(0, 6).map((finding) => (
                  <div
                    key={`${finding.code}:${finding.node_id ?? "root"}:${finding.message}`}
                    className={`analysis-card ${finding.severity}`}
                  >
                    <div className="analysis-card-title">{finding.code}</div>
                    <div className="analysis-card-copy">{finding.message}</div>
                    <div className="analysis-card-meta">
                      {finding.severity}
                      {finding.node_id ? ` · ${finding.node_id}` : ""}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState copy="No structural findings for the selected spec." />
            )}
          </Panel>
        </div>
      </section>
    </>
  );
}

function CustomViewsView({
  activeCustomView,
  activeRun,
  customViewError,
  customViewLoading,
  runContext,
  selectedCatalog,
  selectedCustomViewId,
  setSelectedCustomViewId,
}: {
  activeCustomView: EvaluatedCustomView | null;
  activeRun: RunOverview | null;
  customViewError: string | null;
  customViewLoading: boolean;
  runContext: ScopeToken[];
  selectedCatalog: CatalogEntry | null;
  selectedCustomViewId: string | null;
  setSelectedCustomViewId: (viewId: string | null) => void;
}) {
  const availableViews = selectedCatalog?.custom_views ?? [];
  const [selectedRowId, setSelectedRowId] = useState<string | null>(null);
  const [rowShowCount, setRowShowCount] = useState(50);

  useEffect(() => {
    setSelectedRowId(null);
  }, [activeCustomView?.view.view_id, activeRun?.summary.run_id]);

  useEffect(() => {
    setRowShowCount(50);
  }, [activeCustomView?.view.view_id, activeRun?.summary.run_id]);

  const selectedRow = useMemo(() => {
    if (!activeCustomView || !selectedRowId) {
      return null;
    }
    return activeCustomView.rows.find((r) => r.row_id === selectedRowId) ?? null;
  }, [activeCustomView, selectedRowId]);

  const selectedRowIndex =
    selectedRow && activeCustomView
      ? activeCustomView.rows.findIndex((r) => r.row_id === selectedRow.row_id)
      : -1;
  const displayedRows = activeCustomView?.rows.slice(-rowShowCount) ?? [];
  const showInlineLoading = customViewLoading && activeRun != null && activeCustomView != null;
  const showBlockingLoading = customViewLoading && activeCustomView == null;
  const showInlineError = customViewError != null && activeCustomView != null;

  return (
    <>
      <RunContextStrip tokens={runContext} />
      <section className="overview-layout custom-views-layout">
        <div className="stack wide">
          <Panel
            title={activeCustomView?.view.title ?? "Custom View"}
            aside={
              activeRun ? (
                <div className="chip-row wrap">
                  <StatusChip label={activeRun.summary.run_id.slice(0, 12)} tone="accent" />
                  {showInlineLoading ? (
                    <StatusChip label="refreshing view…" tone="accent" />
                  ) : null}
                </div>
              ) : null
            }
          >
            {!activeRun ? (
              <EmptyState copy="Pick a run in Explorer to evaluate a custom view." />
            ) : activeCustomView ? (
              <div className="custom-view-stack">
                {showInlineError ? (
                  <AlertCard
                    copy={customViewError ?? "Custom view refresh failed."}
                    label="view refresh failed"
                    tone="error"
                  />
                ) : null}
                {activeCustomView.view.description ? (
                  <div className="panel-copy">{activeCustomView.view.description}</div>
                ) : null}
                <div className="chip-row wrap">
                  <StatusChip label={`${activeCustomView.row_count} rows`} tone="accent" />
                  {activeCustomView.rows.length > displayedRows.length ? (
                    <StatusChip
                      label={`${displayedRows.length}/${activeCustomView.rows.length} loaded`}
                    />
                  ) : null}
                </div>
                {activeCustomView.warnings.length > 0 ? (
                  <div className="analysis-stack">
                    {activeCustomView.warnings.slice(0, 4).map((warning) => (
                      <div key={warning} className="analysis-card warning">
                        <div className="analysis-card-title">view warning</div>
                        <div className="analysis-card-copy">{warning}</div>
                      </div>
                    ))}
                  </div>
                ) : null}
                <p className="custom-view-table-hint">
                  Click a row for the full prompt, generated text, and metrics in the side panel.
                </p>
                <div className="records-table-wrap custom-view-table-wrap">
                  <table className="records-table custom-view-table">
                    <thead>
                      <tr>
                        {activeCustomView.view.columns.map((column) => (
                          <th
                            key={column.column_id}
                            className={customViewColumnClassName(column)}
                          >
                            {column.title}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {displayedRows.map((row) => (
                        <tr
                          key={row.row_id}
                          className={`custom-view-row ${
                            selectedRowId === row.row_id ? "selected" : ""
                          }`}
                          aria-selected={selectedRowId === row.row_id}
                          tabIndex={0}
                          onClick={() => setSelectedRowId(row.row_id)}
                          onKeyDown={(event) => {
                            if (event.key === "Enter" || event.key === " ") {
                              event.preventDefault();
                              setSelectedRowId(row.row_id);
                            }
                          }}
                        >
                          {activeCustomView.view.columns.map((column) => (
                            <td
                              key={`${row.row_id}:${column.column_id}`}
                              className={customViewColumnClassName(column)}
                            >
                              <div
                                className={`custom-view-cell ${customViewCellTone(
                                  row.values[column.column_id],
                                )}`}
                              >
                                {formatCustomViewCellPreview(
                                  row.values[column.column_id],
                                  column,
                                )}
                              </div>
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {activeCustomView.rows.length > displayedRows.length ? (
                  <button
                    type="button"
                    className="load-more-btn"
                    onClick={() => setRowShowCount((current) => current + 50)}
                  >
                    Load older rows ({activeCustomView.rows.length - displayedRows.length} not shown)
                  </button>
                ) : null}
              </div>
            ) : customViewError ? (
              <EmptyState copy={customViewError} />
            ) : showBlockingLoading ? (
              <EmptyState copy="Loading evaluated run view…" />
            ) : (
              <EmptyState copy="Select a custom view to render it for the current run." />
            )}
          </Panel>
        </div>

        <div className="stack narrow custom-views-rail">
          <Panel title="Available Views">
            {availableViews.length > 0 ? (
              <div className="interactive-list">
                {availableViews.map((view) => (
                  <button
                    key={view.view_id}
                    className={`list-card ${
                      selectedCustomViewId === view.view_id ? "active" : ""
                    }`}
                    onClick={() => setSelectedCustomViewId(view.view_id)}
                  >
                    <div className="list-card-head">
                      <span>{view.title}</span>
                      <StatusChip label={view.kind} tone="accent" />
                    </div>
                    <div className="list-card-copy">
                      {view.description || "Provider-defined custom run view."}
                    </div>
                  </button>
                ))}
              </div>
            ) : (
              <EmptyState copy="This spec does not declare any custom views yet." />
            )}
          </Panel>
        </div>
      </section>

      {selectedRow && activeCustomView ? (
        <ExecutionDetailDrawer
          badge={activeCustomView.view.title}
          heading={
            selectedRowIndex >= 0
              ? `${activeCustomView.view.title} · row ${selectedRowIndex + 1}`
              : `${activeCustomView.view.title} · sample`
          }
          onClose={() => setSelectedRowId(null)}
          open
          rawJson={JSON.stringify(selectedRow, null, 2)}
          rawTitle="Row JSON"
          rows={customViewRowToDetailRows(selectedRow, activeCustomView.view.columns)}
          subheading={customViewRowSubheading(selectedRow)}
        />
      ) : null}
    </>
  );
}

function GraphView({
  activeRun,
  activeReplay,
  runContext,
  selectedCatalog,
  selectedFrameId,
  selectedNodeId,
  selectedNodeSummary,
  selectNodeAndExplorer,
  setActiveView,
  summaryGraph,
}: {
  activeRun: RunOverview | null;
  activeReplay: ReplayReport | null;
  runContext: ScopeToken[];
  selectedCatalog: CatalogEntry | null;
  selectedFrameId: string | null;
  selectedNodeId: string | null;
  selectedNodeSummary: ReplayNodeSummary | null;
  selectNodeAndExplorer: (nodeId: string, frameId?: string | null) => void;
  setActiveView: (view: ViewId) => void;
  summaryGraph: GraphPayload | null;
}) {
  return (
    <>
      <RunContextStrip tokens={runContext} />
      <section className="graph-layout">
        <div className="graph-left">
          {summaryGraph ? (
            <GraphPanel
              graph={summaryGraph}
              nodeSummaries={activeReplay?.node_summaries ?? []}
              selectedNodeId={selectedNodeId}
              onSelectNode={(nodeId) => {
                selectNodeAndExplorer(nodeId, null);
              }}
            />
          ) : (
            <Panel title="Execution graph">
              <EmptyState copy="Loading graph preview…" />
            </Panel>
          )}
        </div>

        <div className="graph-rail">
          <Panel title="Inspector">
            <div className="inspector-header-block">
              <div>
                <div className="inspector-node-title">
                  {selectedNodeId ?? "Select a node"}
                </div>
                <div className="inspector-node-copy">
                  {selectedCatalog?.pinned_nodes.find((node) => node.node_id === selectedNodeId)
                    ?.description ?? "Selected node in the current graph."}
                </div>
              </div>
              <div className="mini-kpi-row">
                <MiniKpi
                  value={String(selectedNodeSummary ? 1 : 0)}
                  label="selection"
                />
                <MiniKpi
                  value={selectedFrameId ?? "root"}
                  label="frame"
                />
                <MiniKpi
                  value={selectedNodeSummary?.invariant_status ?? "n/a"}
                  label="invariant"
                />
              </div>
            </div>
          </Panel>

          <Panel title="Runtime">
            <KeyValueList
              rows={[
                [
                  "runtime_profile_names",
                  activeRun?.summary.runtime_profile_names.join(", ") ||
                    activeRun?.summary.runtime_default_profile_name ||
                    "n/a",
                ],
                [
                  "runtime_default_profile",
                  activeRun?.summary.runtime_default_profile_name ?? "n/a",
                ],
                [
                  "invocation_name",
                  activeRun?.summary.invocation_name ??
                    selectedCatalog?.invocation_name ??
                    "n/a",
                ],
                ["trace_service_name", activeRun?.summary.trace_service_name ?? "n/a"],
              ]}
            />
          </Panel>

          <Panel title="Shortcuts">
            <ActionRail
              rows={[
                ["Open node detail", "Inputs, outputs, frames, invariants", "node"],
                ["Open traces", "Trace timeline, OTel list, semantic records", "spans"],
              ]}
              onSelect={(target) => setActiveView(target as ViewId)}
            />
          </Panel>
        </div>
      </section>
    </>
  );
}

function NodeDetailView({
  activeRun,
  nodeDetail,
  openTracesAtSpanIndex,
  runContext,
  selectedFrameId,
  selectedNodeEdges,
  selectedNodeId,
  selectedNodeRecords,
  selectedNodeSummary,
  setSelectedFrameId,
  spanItems,
}: {
  activeRun: RunOverview | null;
  nodeDetail: NodeDetail | null;
  openTracesAtSpanIndex: (spanIndex: number) => void;
  runContext: ScopeToken[];
  selectedFrameId: string | null;
  selectedNodeEdges: { upstream: GraphEdge[]; downstream: GraphEdge[] };
  selectedNodeId: string | null;
  selectedNodeRecords: ExecutionRecord[];
  selectedNodeSummary: ReplayNodeSummary | null;
  setSelectedFrameId: (frameId: string | null) => void;
  spanItems: GenericSpan[];
}) {
  const traceCount = nodeDetail?.trace?.spans.length ?? spanItems.length;
  return (
    <>
      <RunContextStrip tokens={runContext} />
      <section className="node-hero">
        <div>
          <div className="node-title-row">
            <h2>{selectedNodeId ?? "Select a node"}</h2>
            <div className="chip-row">
              <StatusChip
                label={selectedNodeSummary?.succeeded ? "healthy" : "unknown"}
                tone={selectedNodeSummary?.failed ? "error" : "ok"}
              />
              <StatusChip label={`frame: ${selectedFrameId ?? "root"}`} tone="accent" />
              <StatusChip
                label={`start: ${activeRun ? new Date(activeRun.summary.created_at_ms).toLocaleTimeString() : "n/a"}`}
              />
            </div>
          </div>
        </div>
      </section>

      <section className="node-layout">
        <div className="stack">
          <Panel title="Throughput">
            <div className="chip-row">
              <StatusChip label="source: semantic event cadence" tone="accent" />
              <StatusChip label="source: trace spans" />
            </div>
            <BarSeries values={buildRecordCadenceBars(selectedNodeRecords)} />
          </Panel>

          <Panel title="I/O">
            <CodeSurface
              lines={[
                nodeDetail?.inputs
                  ? `inputs: ${truncateJson(nodeDetail.inputs, 240)}`
                  : nodeDetail?.inputs_error
                    ? `inputs_error: ${nodeDetail.inputs_error}`
                    : "inputs: unavailable",
                nodeDetail?.output
                  ? `output: ${truncateJson(nodeDetail.output, 240)}`
                  : nodeDetail?.output_error
                    ? `output_error: ${nodeDetail.output_error}`
                    : "output: unavailable",
                `trace_spans: ${traceCount}`,
                `records: ${selectedNodeRecords.length}`,
              ]}
            />
            {nodeDetail?.available_frames.length ? (
              <div className="chip-row">
                {nodeDetail.available_frames.map((frame) => (
                  <button
                    key={`${frame.frame_id}:${frame.iteration_index ?? "root"}`}
                    className={`chip-button ${
                      (selectedFrameId ?? "root") === frame.frame_id ? "active" : ""
                    }`}
                    onClick={() =>
                      setSelectedFrameId(
                        frame.frame_id !== "root" ? frame.frame_id : null,
                      )
                    }
                  >
                    {frame.frame_id}
                  </button>
                ))}
              </div>
            ) : null}
          </Panel>
        </div>

        <div className="stack wide">
          <Panel title="Alerts">
            <AlertCard
              tone="warning"
              label="warning"
              copy={
                selectedNodeSummary?.invariant_severity === "warning"
                  ? "warning-level invariant surfaced on the selected node or frame."
                  : "No warning-level invariant surfaced for the current selection."
              }
            />
            <AlertCard
              tone="error"
              label="invariant"
              copy={
                selectedNodeSummary?.failed
                  ? "A hard invariant failure would appear here when the selected node actually fails."
                  : "No hard invariant failure for the current selection."
              }
            />
          </Panel>

          <Panel title="Traces">
            <TraceTable
              onOpenInTraces={openTracesAtSpanIndex}
              spans={spanItems}
            />
          </Panel>

          <Panel title="Edges">
            <KeyValueList
              rows={[
                [
                  "upstream",
                  selectedNodeEdges.upstream.map((edge) => edge.source_node_id).join(", ") ||
                    "none",
                ],
                [
                  "downstream",
                  selectedNodeEdges.downstream
                    .map((edge) => edge.target_node_id)
                    .join(", ") || "none",
                ],
                [
                  "record lens",
                  selectedNodeId
                    ? `${selectedNodeId} @ ${selectedFrameId ?? "root"}`
                    : "select a node",
                ],
              ]}
            />
          </Panel>
        </div>
      </section>
    </>
  );
}

function nodeDetailIoPrefetchForSpan(
  nodeDetail: NodeDetail | null,
  exploreRunId: string | null,
  activeRun: RunOverview | null,
  exploreNodeId: string | null,
  selectedFrameId: string | null,
  span: GenericSpan,
): NodeDetail | null {
  if (!nodeDetail || !activeRun || exploreRunId !== activeRun.summary.run_id) {
    return null;
  }
  if (exploreNodeId !== span.correlationKeys.nodeId) {
    return null;
  }
  const spanFrame = frameIdForNodeDetailApi(span.correlationKeys.frameId);
  const explorerFrame = frameIdForNodeDetailApi(selectedFrameId ?? "root");
  if (spanFrame !== explorerFrame) {
    return null;
  }
  return nodeDetail;
}

function nodeDetailIoPrefetchForRecord(
  nodeDetail: NodeDetail | null,
  exploreRunId: string | null,
  activeRun: RunOverview | null,
  exploreNodeId: string | null,
  selectedFrameId: string | null,
  record: ExecutionRecord,
): NodeDetail | null {
  if (!nodeDetail || !activeRun || exploreRunId !== activeRun.summary.run_id) {
    return null;
  }
  if (exploreNodeId !== record.node_id) {
    return null;
  }
  const recFrame = frameIdForNodeDetailApi(record.frame_id);
  const explorerFrame = frameIdForNodeDetailApi(selectedFrameId ?? "root");
  if (recFrame !== explorerFrame) {
    return null;
  }
  return nodeDetail;
}

function SpansRecordsView({
  activeRun,
  exploreNodeId,
  exploreRunId,
  nodeDetail,
  runFailureMessage,
  runContext,
  runRecordsInWindow,
  runRecordsHasMore,
  runRecordsLoading,
  runRecordsLoadingMore,
  runRecordsTotalCount,
  runSpansLoading,
  runSpansHasMore,
  runSpansLoadingMore,
  runSpansTotalCount,
  selectedNodeId,
  selectedFrameId,
  selectedNodeRecords,
  onLoadOlderRecords,
  onLoadOlderSpans,
  setSpansInspector,
  spanItems,
  spansInspector,
}: {
  activeRun: RunOverview | null;
  exploreNodeId: string | null;
  exploreRunId: string | null;
  /** Latest node-detail payload for the explorer’s node + frame (same API the drawers call). */
  nodeDetail: NodeDetail | null;
  runFailureMessage: string | null;
  runContext: ScopeToken[];
  /** Time-window slice of the loaded run’s semantic records (same source as list; used to join spans). */
  runRecordsInWindow: ExecutionRecord[];
  runRecordsHasMore: boolean;
  runRecordsLoading: boolean;
  runRecordsLoadingMore: boolean;
  runRecordsTotalCount: number;
  runSpansLoading: boolean;
  runSpansHasMore: boolean;
  runSpansLoadingMore: boolean;
  runSpansTotalCount: number;
  selectedNodeId: string | null;
  selectedFrameId: string | null;
  selectedNodeRecords: ExecutionRecord[];
  onLoadOlderRecords: () => void;
  onLoadOlderSpans: () => void;
  setSpansInspector: Dispatch<SetStateAction<SpansInspector | null>>;
  spanItems: GenericSpan[];
  spansInspector: SpansInspector | null;
}) {
  const explorerNodeLabel = exploreNodeId ?? "all";

  const [traceFullscreenOpen, setTraceFullscreenOpen] = useState(false);

  useEffect(() => {
    if (!traceFullscreenOpen) {
      return;
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setTraceFullscreenOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
    };
  }, [traceFullscreenOpen]);

  const spanDetail =
    spansInspector?.kind === "span"
      ? (spanItems[spansInspector.index] ?? null)
      : null;

  const spanRelatedRecords = useMemo(() => {
    if (!spanDetail) {
      return [];
    }
    return recordsMatchingSpanScope(runRecordsInWindow, spanDetail);
  }, [spanDetail, runRecordsInWindow]);

  const recordDetail =
    spansInspector?.kind === "record"
      ? (runRecordsInWindow.find((r) => r.record_id === spansInspector.id) ??
          selectedNodeRecords.find((r) => r.record_id === spansInspector.id) ??
          null)
      : null;

  const ioPrefetchForOpenSpan = useMemo(
    () =>
      spanDetail != null
        ? nodeDetailIoPrefetchForSpan(
            nodeDetail,
            exploreRunId,
            activeRun,
            exploreNodeId,
            selectedFrameId,
            spanDetail,
          )
        : null,
    [
      spanDetail,
      nodeDetail,
      exploreRunId,
      activeRun,
      exploreNodeId,
      selectedFrameId,
    ],
  );

  const ioPrefetchForOpenRecord = useMemo(
    () =>
      recordDetail != null
        ? nodeDetailIoPrefetchForRecord(
            nodeDetail,
            exploreRunId,
            activeRun,
            exploreNodeId,
            selectedFrameId,
            recordDetail,
          )
        : null,
    [
      recordDetail,
      nodeDetail,
      exploreRunId,
      activeRun,
      exploreNodeId,
      selectedFrameId,
    ],
  );

  const recordsEmptyReason =
    exploreRunId == null
      ? "Select a run (@run_id) to load semantic records from the bundle."
      : selectedNodeRecords.length === 0
        ? "No semantic records in this scope. Records are log lines from records.jsonl; spans are OTel timings—they can differ in count. Try clearing @node_id / frame filters or another run."
        : null;

  return (
    <>
      <RunContextStrip tokens={runContext} />
      <section className="spans-layout">
        <div className="spans-trace-hero">
          <Panel
            aside={
              spanItems.length > 0 ? (
                <div className="trace-panel-actions">
                  <button
                    type="button"
                    className="share-link-btn trace-fs-open-btn"
                    onClick={() => setTraceFullscreenOpen(true)}
                  >
                    Fullscreen
                  </button>
                </div>
              ) : undefined
            }
            subtitle="Duration on the horizontal axis; overlapping work stacks into rows (cf. distributed trace flame / waterfall views)."
            title="Trace timeline"
          >
            {spanItems.length === 0 && runSpansLoading ? (
              <div className="flamegraph-skeleton" aria-busy>
                <span className="explorer-skeleton-chart" />
              </div>
            ) : spanItems.length === 0 ? (
              <EmptyState
                compact
                copy="No spans in this scope yet. Ensure a run is selected and span export is enabled."
              />
            ) : (
              <SpanFlamegraph
                density="compact"
                onSelectSpan={(index) =>
                  setSpansInspector({ kind: "span", index })
                }
                selectedIndex={
                  spansInspector?.kind === "span"
                    ? spansInspector.index
                    : null
                }
                spans={spanItems}
              />
            )}
            <p className="spans-detail-hint spans-trace-footer">
              Select a span block above or a row below; full fields and bundle I/O open in the
              drawer (same detail as the Node view).{" "}
              <span className="spans-trace-fs-hint">
                Use <strong>Fullscreen</strong> for a taller trace layout.
              </span>
            </p>
          </Panel>
        </div>

        <div className="spans-lists-row">
          <div className="stack">
            <Panel title="OTel spans">
              <div className="chip-row">
                <StatusChip label={`@node_id: ${explorerNodeLabel}`} tone="accent" />
                <StatusChip label={`frame: ${selectedFrameId ?? "all"}`} />
                <StatusChip label={`${spanItems.length} / ${runSpansTotalCount} spans`} />
                <StatusChip label={`trace: ${activeRun?.summary.trace_mode ?? "n/a"}`} />
                {runSpansLoadingMore && exploreRunId ? (
                  <StatusChip label="loading older spans…" tone="accent" />
                ) : runSpansLoading && exploreRunId ? (
                  <StatusChip label="loading span data…" tone="accent" />
                ) : null}
              </div>
              <p className="spans-explainer">
                Sequential span list (like a Datadog span list). Click a row for full
                attributes.
              </p>
              <TraceList
                indexOffset={0}
                selectedIndex={
                  spansInspector?.kind === "span" ? spansInspector.index : null
                }
                spans={spanItems}
                onSelectSpan={(index) =>
                  setSpansInspector({ kind: "span", index })
                }
              />
              {runSpansHasMore ? (
                <button
                  type="button"
                  className="load-more-btn"
                  onClick={onLoadOlderSpans}
                  disabled={runSpansLoadingMore}
                >
                  {runSpansLoadingMore ? "Loading older spans…" : "Load older spans"}
                </button>
              ) : null}
            </Panel>
          </div>

          <div className="stack">
            <Panel title="Semantic stream">
              <div className="chip-row">
                <StatusChip label={`${selectedNodeRecords.length} / ${runRecordsTotalCount} records`} tone="accent" />
                <StatusChip
                  label={`scope: @node_id ${explorerNodeLabel} · frame ${selectedFrameId ?? "all"}`}
                />
                {runRecordsLoadingMore ? (
                  <StatusChip label="loading older records…" tone="accent" />
                ) : runRecordsLoading && exploreRunId ? (
                  <StatusChip label="loading record data…" tone="accent" />
                ) : null}
              </div>
              <p className="spans-explainer">
                Execution events from <span className="mono">records.jsonl</span>. Not every
                span has a matching record line.
              </p>
              <SemanticRecordList
                emptyHint={recordsEmptyReason}
                records={selectedNodeRecords}
                selectedRecordId={
                  spansInspector?.kind === "record" ? spansInspector.id : null
                }
                onSelectRecord={(id) =>
                  setSpansInspector({ kind: "record", id })
                }
              />
              {runRecordsHasMore ? (
                <button
                  type="button"
                  className="load-more-btn"
                  onClick={onLoadOlderRecords}
                  disabled={runRecordsLoadingMore}
                >
                  {runRecordsLoadingMore ? "Loading older records…" : "Load older records"}
                </button>
              ) : null}
            </Panel>
          </div>
        </div>
      </section>

      {spanDetail ? (
        <ExecutionDetailDrawer
          afterFields={
            <InspectorNodeIo
              enabled
              frameId={spanDetail.correlationKeys.frameId}
              graphId={activeRun?.summary.graph_id ?? null}
              nodeId={spanDetail.correlationKeys.nodeId}
              prefetchedDetail={ioPrefetchForOpenSpan}
              runId={spanDetail.correlationKeys.runId ?? exploreRunId}
              runFailureMessage={runFailureMessage}
            />
          }
          badge={spanDetail.kindTag.toUpperCase()}
          heading={spanDetail.title}
          kindHue={spanDetail.kindHue}
          onClose={() => setSpansInspector(null)}
          open
          rawJson={JSON.stringify(spanDetail.rawSpan, null, 2)}
          relatedRecords={spanRelatedRecords}
          relatedScopeLabel={formatSpanCorrelationScope(spanDetail)}
          rows={spanDetail.structuredRows}
          subheading={spanDetail.subtitle ?? undefined}
        />
      ) : null}
      {recordDetail ? (
        <ExecutionDetailDrawer
          afterFields={
            <InspectorNodeIo
              enabled
              frameId={recordDetail.frame_id}
              graphId={activeRun?.summary.graph_id ?? null}
              nodeId={recordDetail.node_id}
              prefetchedDetail={ioPrefetchForOpenRecord}
              runId={recordDetail.run_id}
              runFailureMessage={runFailureMessage}
            />
          }
          badge={recordDetail.event_type}
          heading={`${recordDetail.event_type} · ${recordDetail.node_id}`}
          onClose={() => setSpansInspector(null)}
          open
          rawJson={JSON.stringify(
            executionRecordToDetailJson(recordDetail),
            null,
            2,
          )}
          rows={executionRecordToRows(recordDetail)}
          subheading={new Date(recordDetail.timestamp_ms).toISOString()}
        />
      ) : null}

      {traceFullscreenOpen && spanItems.length > 0 ? (
        <div
          className="trace-fs-overlay"
          role="presentation"
          onClick={() => setTraceFullscreenOpen(false)}
        >
          <div
            aria-labelledby="trace-fs-title"
            aria-modal="true"
            className="trace-fs-dialog panel v3-panel"
            role="dialog"
            onClick={(e) => e.stopPropagation()}
          >
            <header className="trace-fs-header">
              <div>
                <h2 className="trace-fs-title" id="trace-fs-title">
                  Trace timeline
                </h2>
                <p className="panel-subtitle trace-fs-subtitle">
                  Full-width layout with taller rows. Press Esc or click outside to
                  close.
                </p>
              </div>
              <button
                type="button"
                className="share-link-btn trace-fs-close-btn"
                onClick={() => setTraceFullscreenOpen(false)}
              >
                Close
              </button>
            </header>
            <div className="trace-fs-chart">
              <SpanFlamegraph
                density="comfortable"
                onSelectSpan={(index) => {
                  setSpansInspector({ kind: "span", index });
                  setTraceFullscreenOpen(false);
                }}
                selectedIndex={
                  spansInspector?.kind === "span"
                    ? spansInspector.index
                    : null
                }
                spans={spanItems}
              />
            </div>
            <p className="spans-detail-hint trace-fs-footer">
              Selecting a span closes fullscreen and opens the detail drawer.
            </p>
          </div>
        </div>
      ) : null}
    </>
  );
}

function TraceList({
  indexOffset = 0,
  onSelectSpan,
  selectedIndex,
  spans,
}: {
  indexOffset?: number;
  onSelectSpan: (index: number) => void;
  selectedIndex: number | null;
  spans: GenericSpan[];
}) {
  return spans.length > 0 ? (
    <div className="trace-list">
      {spans.map((span, index) => {
        const globalIndex = indexOffset + index;
        return (
        <button
          key={`${span.label}:${globalIndex}`}
          type="button"
          className={`trace-row-card ${selectedIndex === globalIndex ? "active" : ""}`}
          onClick={() => onSelectSpan(globalIndex)}
        >
          <span
            className="trace-kind-stripe"
            style={{ background: `hsl(${span.kindHue} 58% 52%)` }}
            aria-hidden
          />
          <div className="trace-row-main">
            <div className="trace-row-top">
              <span className="trace-kind-pill mono">{span.kindTag}</span>
              <strong className="trace-row-title">{span.title}</strong>
            </div>
            {span.subtitle ? (
              <span className="trace-row-sub mono">{span.subtitle}</span>
            ) : null}
          </div>
          <div className="trace-row-stats">
            <span className="mono trace-lat">{span.latencyLabel}</span>
            <span
              className={`trace-status ${span.statusLabel === "ok" ? "ok" : "err"}`}
            >
              {span.statusLabel}
            </span>
          </div>
        </button>
      );
      })}
    </div>
  ) : (
    <EmptyState copy="No spans in this scope." compact />
  );
}

function SemanticRecordList({
  emptyHint,
  onSelectRecord,
  records,
  selectedRecordId,
}: {
  emptyHint: string | null;
  onSelectRecord: (recordId: string) => void;
  records: ExecutionRecord[];
  selectedRecordId: string | null;
}) {
  return records.length > 0 ? (
    <div className="record-console record-console-interactive">
      {records.map((record) => (
        <button
          key={record.record_id}
          type="button"
          className={`record-console-line record-console-line-btn ${
            selectedRecordId === record.record_id ? "active" : ""
          }`}
          onClick={() => onSelectRecord(record.record_id)}
        >
          <span className="record-console-time mono">
            {new Date(record.timestamp_ms).toLocaleTimeString()}
          </span>
          <span className="record-console-pill mono">{record.event_type}</span>
          <span className="record-console-node mono">{record.node_id}</span>
        </button>
      ))}
    </div>
  ) : (
    <EmptyState
      copy={emptyHint ?? "No records in this scope."}
      compact
    />
  );
}

function LaunchCompareView({
  activeExecution,
  catalog,
  customSpecPath,
  handleRun,
  handleRunFromPath,
  liveMessages,
  liveRecords,
  runs,
  selectedCatalog,
  setCatalog,
  setCustomSpecPath,
  setExploreRunId,
  setSelectedSpecId,
}: {
  activeExecution: ExecutionSession | null;
  catalog: CatalogEntry[];
  customSpecPath: string;
  handleRun: (specId: string) => Promise<void>;
  handleRunFromPath: (specPath: string) => Promise<void>;
  liveMessages: ExecutionMessage[];
  liveRecords: ExecutionRecord[];
  runs: RunSummary[];
  selectedCatalog: CatalogEntry | null;
  setCatalog: (entries: CatalogEntry[]) => void;
  setCustomSpecPath: (path: string) => void;
  setExploreRunId: (runId: string | null) => void;
  setSelectedSpecId: (specId: string | null) => void;
}) {
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadingSpec, setLoadingSpec] = useState(false);

  async function handleLoadSpecPath() {
    setLoadError(null);
    setLoadingSpec(true);
    try {
      const registered = await registerCatalogFromPath(customSpecPath.trim());
      const entries = await fetchCatalog();
      setCatalog(entries);
      setSelectedSpecId(registered.spec_id);
    } catch (err) {
      setLoadError(String(err));
    } finally {
      setLoadingSpec(false);
    }
  }

  return (
    <>
      <section className="launch-layout launch-layout-single">
        <Panel title="Verify TOML path">
          <p className="launch-hint">
            Absolute path to a <code>mentalmodel verify --spec</code> file. The
            field follows the currently selected catalog entry by default, but
            you can also paste any other verify TOML path and load it manually.
          </p>
          <label className="launch-path-label" htmlFor="custom-spec-path">
            Spec path
          </label>
          <input
            id="custom-spec-path"
            type="text"
            className="launch-path-input"
            spellCheck={false}
            value={customSpecPath}
            onChange={(event) => setCustomSpecPath(event.target.value)}
            placeholder={selectedCatalog?.spec_path ?? "/absolute/path/to/verify.toml"}
          />
          {loadError ? <div className="launch-error">{loadError}</div> : null}
          <div className="launch-actions">
            <button
              type="button"
              className="primary-action secondary"
              disabled={loadingSpec || !customSpecPath.trim()}
              onClick={() => void handleLoadSpecPath()}
            >
              {loadingSpec ? "Loading…" : "Load spec"}
            </button>
            <button
              type="button"
              className="primary-action"
              disabled={!customSpecPath.trim()}
              onClick={() => void handleRunFromPath(customSpecPath)}
            >
              Run verification
            </button>
          </div>
          {selectedCatalog ? (
            <div className="launch-meta">
              <div>
                <span className="eyebrow">Loaded</span>
                <div className="launch-meta-value">{selectedCatalog.label}</div>
              </div>
              <div>
                <span className="eyebrow">Graph</span>
                <div className="launch-meta-value">{selectedCatalog.graph_id}</div>
              </div>
              <div>
                <span className="eyebrow">Invocation</span>
                <div className="launch-meta-value">
                  {selectedCatalog.invocation_name}
                </div>
              </div>
              <div>
                <span className="eyebrow">Execution</span>
                <div className="launch-meta-value">
                  {selectedCatalog.launch_enabled ? "local/dashboard" : "hosted read-only"}
                </div>
              </div>
            </div>
          ) : null}

          {selectedCatalog && !selectedCatalog.launch_enabled ? (
            <div className="launch-hint">
              This catalog entry was published from a remote snapshot. Inspect runs here, but
              launch verification from the producer repo instead of the hosted dashboard.
            </div>
          ) : null}

          {selectedCatalog ? (
            <div className="launch-secondary-actions">
              <button
                type="button"
                className="primary-action secondary"
                disabled={!selectedCatalog.launch_enabled}
                onClick={() => void handleRun(selectedCatalog.spec_id)}
              >
                Run again (selected catalog)
              </button>
              {runs[0] ? (
                <button
                  type="button"
                  className="primary-action secondary"
                  onClick={() => setExploreRunId(runs[0].run_id)}
                >
                  Open latest run
                </button>
              ) : null}
            </div>
          ) : null}

          {activeExecution ? (
            <>
              <div className="launch-meta launch-live-meta">
                <div>
                  <span className="eyebrow">Execution</span>
                  <div className="launch-meta-value">
                    <StatusChip
                      label={activeExecution.status}
                      tone={
                        activeExecution.status === "succeeded"
                          ? "ok"
                          : activeExecution.status === "failed"
                            ? "error"
                            : "accent"
                      }
                    />
                  </div>
                </div>
                <div>
                  <span className="eyebrow">Live records</span>
                  <div className="launch-meta-value">{liveRecords.length}</div>
                </div>
                <div>
                  <span className="eyebrow">Live messages</span>
                  <div className="launch-meta-value">{liveMessages.length}</div>
                </div>
                <div>
                  <span className="eyebrow">Run id</span>
                  <div className="launch-meta-value">
                    {activeExecution.run_id ?? "pending"}
                  </div>
                </div>
              </div>

              <div className="record-console launch-console">
                {liveMessages.length > 0 ? (
                  liveMessages.slice(-4).map((message) => (
                    <div
                      key={`${message.sequence}:${message.timestamp_ms}`}
                      className="record-console-line"
                    >
                      <span className="record-console-pill mono">
                        {message.level}
                      </span>
                      <span className="record-console-node mono">
                        {message.source}
                      </span>
                      <span>{message.message}</span>
                    </div>
                  ))
                ) : liveRecords.length > 0 ? (
                  liveRecords.slice(-4).map((record) => (
                    <div key={record.record_id} className="record-console-line">
                      <span className="record-console-pill mono">
                        {record.event_type}
                      </span>
                      <span className="record-console-node mono">
                        {record.node_id}
                      </span>
                      <span>{record.frame_id}</span>
                    </div>
                  ))
                ) : (
                  <div className="empty-state compact">
                    Launch requested. Waiting for the first worker message or
                    semantic record.
                  </div>
                )}
              </div>
            </>
          ) : null}
        </Panel>

        <Panel title="Built-in catalog">
          <div className="catalog-compact">
            {catalog
              .filter((e) => !e.tags.includes("spec-path"))
              .map((entry) => (
                <button
                  key={entry.spec_id}
                  type="button"
                  className={`catalog-pill ${selectedCatalog?.spec_id === entry.spec_id ? "active" : ""}`}
                  onClick={() => setSelectedSpecId(entry.spec_id)}
                >
                  {entry.label}
                </button>
              ))}
          </div>
        </Panel>
      </section>
    </>
  );
}

function RunContextStrip({ tokens }: { tokens: ScopeToken[] }) {
  return (
    <section className="run-context-strip">
      <div className="run-context-grid">
        {tokens.map((token) => (
          <div key={`${token.label}:${token.value}`} className="context-token">
            <span>{token.label}:</span>
            <strong>{token.value}</strong>
          </div>
        ))}
      </div>
    </section>
  );
}

function Panel({
  aside,
  children,
  className,
  subtitle,
  title,
}: {
  aside?: ReactNode;
  children: ReactNode;
  className?: string;
  subtitle?: string;
  title: string;
}) {
  return (
    <section className={`panel v3-panel ${className ?? ""}`.trim()}>
      <header className="panel-header">
        <div>
          <div className="panel-title">{title}</div>
          {subtitle ? <div className="panel-subtitle">{subtitle}</div> : null}
        </div>
        {aside}
      </header>
      {children}
    </section>
  );
}

function KpiCard({
  label,
  source,
  tone = "default",
  value,
}: {
  label: string;
  source: string;
  tone?: "accent" | "default" | "error" | "ok" | "warning";
  value: string;
}) {
  return (
    <div className={`summary-card kpi-card ${tone}`}>
      <div className="eyebrow">{label}</div>
      <div className="summary-value">{value}</div>
      <div className="kpi-source">{source}</div>
    </div>
  );
}

function MiniKpi({ label, value }: { label: string; value: string }) {
  return (
    <div className="mini-kpi">
      <div className="mini-kpi-value">{value}</div>
      <div className="mini-kpi-label">{label}</div>
    </div>
  );
}

function StatusChip({
  label,
  tone = "muted",
}: {
  label: string;
  tone?: "accent" | "error" | "muted" | "ok" | "warning";
}) {
  return (
    <span className={`status-chip ${tone}`}>
      <span className="status-dot" />
      {label}
    </span>
  );
}

function MetricTrendPanel({
  group,
  refreshing,
  onInspectMetric,
}: {
  group: MetricGroupQueryResult;
  refreshing: boolean;
  onInspectMetric: (series: MetricSeries) => void;
}) {
  const summaries = summarizeMetricGroup(group);

  return (
    <div className="metric-trend-panel">
      <div className="metric-group-head">
        <div>
          <div className="panel-title">{group.title}</div>
          <div className="panel-subtitle">{group.description}</div>
        </div>
        {refreshing ? <StatusChip label="refreshing metrics…" tone="accent" /> : null}
      </div>
      <MetricGroupTimeseriesChart group={group} />
      <div className="metric-summary-grid compact">
        {summaries.map((summary) => (
          <button
            key={summary.key}
            className="metric-summary-card"
            onClick={() => onInspectMetric(summary.series)}
          >
            <div className="metric-summary-header">
              <span className="metric-summary-label">{summary.label}</span>
              {summary.rangeLabel ? (
                <span className="metric-summary-range">{summary.rangeLabel}</span>
              ) : null}
            </div>
            <div className="metric-summary-primary">
              {formatMetricValueWithUnit(summary.latestValue, summary.unit)}
            </div>
            <div className="metric-summary-meta">
              <span>{summary.kindLabel}</span>
              {summary.deltaLabel ? <strong>{summary.deltaLabel}</strong> : null}
            </div>
            <div className="metric-summary-foot">
              <span>{summary.extremaLabel}</span>
              <span>{summary.pointsLabel}</span>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}

type MetricSummary = {
  key: string;
  label: string;
  latestValue: number;
  deltaLabel: string | null;
  extremaLabel: string;
  pointsLabel: string;
  kindLabel: string;
  rangeLabel: string | null;
  unit: MetricSeries["unit"];
  series: MetricSeries;
  pointCount: number;
  seriesKind: "counter" | "gauge" | "trend";
};

function summarizeMetricGroup(group: MetricGroupQueryResult): MetricSummary[] {
  return group.series
    .map((series) => {
      const summary = series.summary;
      return {
        key: series.series_id,
        label: series.label,
        latestValue: summary.latest,
        deltaLabel:
          summary.delta == null ? null : formatMetricDelta(summary.delta, series.unit),
        extremaLabel: `min ${formatMetricValueWithUnit(summary.min, series.unit)} · max ${formatMetricValueWithUnit(summary.max, series.unit)}`,
        pointsLabel:
          summary.latest_iteration == null
            ? `${summary.point_count} gauges`
            : `${summary.point_count} samples`,
        kindLabel: metricKindLabel(series),
        rangeLabel:
          summary.latest_iteration == null
            ? null
            : buildSeriesRangeLabel(series),
        unit: series.unit,
        series,
        pointCount: summary.point_count,
        seriesKind: summary.semantic_kind,
      };
    })
    .sort((left, right) => {
      const leftIteration = left.series.summary.latest_iteration ?? -1;
      const rightIteration = right.series.summary.latest_iteration ?? -1;
      if (leftIteration !== rightIteration) {
        return rightIteration - leftIteration;
      }
      return left.label.localeCompare(right.label);
    });
}

function buildMetricCounterSummaries(
  metricGroups: MetricGroupQueryResult[],
): MetricSummary[] {
  return metricGroups
    .flatMap((group) => summarizeMetricGroup(group))
    .filter((summary) => summary.seriesKind !== "trend")
    .sort((left, right) => {
      const leftPriority = left.seriesKind === "counter" ? 0 : 1;
      const rightPriority = right.seriesKind === "counter" ? 0 : 1;
      if (leftPriority !== rightPriority) {
        return leftPriority - rightPriority;
      }
      return right.latestValue - left.latestValue;
    })
    .slice(0, 10);
}

function metricKindLabel(series: MetricSeries): string {
  if (series.semantic_kind === "counter") {
    return "counter";
  }
  if (series.render_hint === "bar") {
    return "sparse gauge";
  }
  if (series.summary.latest_iteration == null) {
    return "snapshot gauge";
  }
  return "gauge";
}

function buildSeriesRangeLabel(series: MetricSeries): string | null {
  const iterations = series.points
    .map((point) => point.iteration_index)
    .filter((value): value is number => value != null);
  if (iterations.length === 0) {
    return null;
  }
  return formatIterationSeriesRange(iterations[0] ?? null, iterations[iterations.length - 1] ?? null);
}

function formatMetricValueWithUnit(value: number, unit: MetricSeries["unit"]): string {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  if (unit === "ms") {
    return `${formatMetricValue(value)} ms`;
  }
  if (unit === "s") {
    return `${formatMetricValue(value)} s`;
  }
  if (unit === "pct") {
    return `${formatMetricValue(value)}%`;
  }
  if (unit === "bytes") {
    return `${formatMetricValue(value)}`;
  }
  return formatMetricValue(value);
}

function formatMetricDelta(delta: number, unit: MetricSeries["unit"]): string {
  const sign = delta > 0 ? "+" : "";
  return `${sign}${formatMetricValueWithUnit(delta, unit)}`;
}

function formatIterationSeriesRange(
  start: number | null,
  end: number | null,
): string {
  if (start == null || end == null) {
    return "snapshot";
  }
  if (start === end) {
    return `i${end}`;
  }
  return `i${start}–i${end}`;
}

function BarSeries({ values }: { values: number[] }) {
  return (
    <div className="bar-series">
      {values.map((value, index) => (
        <div key={`${value}:${index}`} className="bar-series-column">
          <div
            className={`bar-series-bar ${value === 0 ? "ghost" : ""}`}
            style={{ height: `${Math.max(value, 10)}px` }}
          />
        </div>
      ))}
    </div>
  );
}

function ExplorerScopeBar({
  exploreNodeId,
  exploreIterationEnd,
  exploreIterationStart,
  exploreRunId,
  exploreTimePreset,
  selectedFrameId,
  frameOptions,
  iterationBounds,
  graphNodes,
  runs,
  setExploreNodeId,
  setExploreIterationEnd,
  setExploreIterationStart,
  setExploreRunId,
  setExploreTimePreset,
  setSelectedFrameId,
}: {
  exploreNodeId: string | null;
  exploreIterationEnd: string;
  exploreIterationStart: string;
  exploreRunId: string | null;
  exploreTimePreset: ExploreTimePreset;
  selectedFrameId: string | null;
  frameOptions: string[];
  iterationBounds: IterationBounds | null;
  graphNodes: GraphPayload["nodes"];
  runs: RunSummary[];
  setExploreNodeId: (id: string | null) => void;
  setExploreIterationEnd: (value: string) => void;
  setExploreIterationStart: (value: string) => void;
  setExploreRunId: (id: string | null) => void;
  setExploreTimePreset: (p: ExploreTimePreset) => void;
  setSelectedFrameId: (frameId: string | null) => void;
}) {
  return (
    <section className="explorer-scope-bar" aria-label="Explorer facets">
      <div className="explorer-scope-title">
        <span className="explorer-scope-product">Explorer</span>
        <span
          className="explorer-scope-hint"
          title={
            "Time window scopes the chart and run list. “All runs” aggregates the chart; pick a run to load the bundle, metrics, and node drill-down."
          }
        >
          Window · run · node — shareable via Copy explorer link
        </span>
      </div>
      <div className="explorer-facets">
        <label className="explorer-facet">
          <span className="explorer-facet-key">$window</span>
          <select
            className="explorer-facet-input"
            value={exploreTimePreset}
            onChange={(event) =>
              setExploreTimePreset(event.target.value as ExploreTimePreset)
            }
          >
            <option value="15m">Past 15m</option>
            <option value="1h">Past 1h</option>
            <option value="4h">Past 4h</option>
            <option value="24h">Past 24h</option>
            <option value="7d">Past 7d</option>
            <option value="all">All runs (data range)</option>
          </select>
        </label>
        <label className="explorer-facet">
          <span className="explorer-facet-key">@run_id</span>
          <select
            className="explorer-facet-input"
            value={exploreRunId ?? ""}
            onChange={(event) =>
              setExploreRunId(event.target.value ? event.target.value : null)
            }
          >
            <option value="">All runs</option>
            {runs.map((run) => (
              <option key={run.run_id} value={run.run_id}>
                {run.run_id.slice(0, 14)}
                {run.run_id.length > 14 ? "…" : ""}
              </option>
            ))}
          </select>
        </label>
        <label className="explorer-facet">
          <span className="explorer-facet-key">@node_id</span>
          <select
            className="explorer-facet-input"
            value={exploreNodeId ?? ""}
            onChange={(event) =>
              setExploreNodeId(event.target.value ? event.target.value : null)
            }
          >
            <option value="">All nodes</option>
            {graphNodes.map((node) => (
              <option key={node.node_id} value={node.node_id}>
                {node.node_id}
              </option>
            ))}
          </select>
        </label>
        <label className="explorer-facet">
          <span className="explorer-facet-key">@frame_id</span>
          <select
            className="explorer-facet-input"
            value={selectedFrameId ?? ""}
            onChange={(event) =>
              setSelectedFrameId(event.target.value ? event.target.value : null)
            }
          >
            <option value="">All frames</option>
            {frameOptions.filter((frameId) => frameId !== "root").map((frameId) => (
              <option key={frameId} value={frameId === "root" ? "" : frameId}>
                {frameId}
              </option>
            ))}
          </select>
        </label>
        <label className="explorer-facet explorer-facet-range">
          <span className="explorer-facet-key">@step_start</span>
          <input
            className="explorer-facet-input"
            inputMode="numeric"
            type="number"
            min={iterationBounds?.min ?? 0}
            max={iterationBounds?.max ?? undefined}
            placeholder={iterationBounds ? String(iterationBounds.min) : "all"}
            value={exploreIterationStart}
            onChange={(event) => setExploreIterationStart(event.target.value)}
          />
        </label>
        <label className="explorer-facet explorer-facet-range">
          <span className="explorer-facet-key">@step_end</span>
          <input
            className="explorer-facet-input"
            inputMode="numeric"
            type="number"
            min={iterationBounds?.min ?? 0}
            max={iterationBounds?.max ?? undefined}
            placeholder={iterationBounds ? String(iterationBounds.max) : "all"}
            value={exploreIterationEnd}
            onChange={(event) => setExploreIterationEnd(event.target.value)}
          />
        </label>
      </div>
    </section>
  );
}

function RecordConsole({
  fallbackRecords,
  records,
}: {
  fallbackRecords: ExecutionRecord[];
  records: ExecutionRecord[];
}) {
  const source = [...(records.length > 0 ? records : fallbackRecords)].sort(
    (left, right) => right.timestamp_ms - left.timestamp_ms,
  );
  return source.length > 0 ? (
    <div className="record-console">
      {source.slice(0, 400).map((record) => (
        <div key={record.record_id} className="record-console-line">
          {formatRecordLine(record)}
        </div>
      ))}
    </div>
  ) : (
    <EmptyState copy="No records captured for the current scope yet." compact />
  );
}

function TraceTable({
  onOpenInTraces,
  spans,
}: {
  onOpenInTraces: (spanIndex: number) => void;
  spans: GenericSpan[];
}) {
  return spans.length > 0 ? (
    <div className="trace-table">
      <div className="trace-table-head">
        <span>Run / trace</span>
        <span>Latency</span>
        <span>Status</span>
        <span>Action</span>
      </div>
      {spans.map((span, index) => (
        <button
          key={`${span.label}:${index}`}
          type="button"
          className="trace-table-row trace-table-row-btn"
          onClick={() => onOpenInTraces(index)}
        >
          <span className="mono trace-table-id">{span.traceIdDisplay}</span>
          <span className="mono">{span.latencyLabel}</span>
          <span>{span.statusLabel}</span>
          <span className="trace-table-action">Open in Traces →</span>
        </button>
      ))}
    </div>
  ) : (
    <EmptyState copy="No traces available for the current selection." compact />
  );
}

function ActionRail({
  onSelect,
  rows,
}: {
  onSelect: (target: string) => void;
  rows: Array<[string, string, string]>;
}) {
  return (
    <div className="action-rail">
      {rows.map(([label, detail, target]) => (
        <button
          key={label}
          className="list-card"
          onClick={() => onSelect(target)}
        >
          <div className="list-card-head">
            <span>{label}</span>
          </div>
          <div className="list-card-copy">{detail}</div>
        </button>
      ))}
    </div>
  );
}

function AlertCard({
  copy,
  label,
  tone,
}: {
  copy: string;
  label: string;
  tone: "error" | "warning";
}) {
  return (
    <div className={`alert-card ${tone}`}>
      <StatusChip label={label} tone={tone} />
      <div className="alert-copy">{copy}</div>
    </div>
  );
}

function CodeSurface({ lines }: { lines: string[] }) {
  return (
    <div className="code-surface">
      {lines.map((line, index) => (
        <div key={`${line}:${index}`} className="code-line">
          {line}
        </div>
      ))}
    </div>
  );
}

function FormField({ label, value }: { label: string; value: string }) {
  return (
    <div className="form-field">
      <div className="eyebrow">{label}</div>
      <div className="form-value">{value}</div>
    </div>
  );
}

function KeyValueList({
  rows,
}: {
  rows: Array<[string, string]>;
}) {
  return (
    <div className="key-value-list">
      {rows.map(([label, value]) => (
        <div key={`${label}:${value}`} className="key-value-row">
          <span>{label}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </div>
  );
}

function StructuredKeyValueList({
  rows,
}: {
  rows: Array<[string, string]>;
}) {
  return (
    <div className="structured-kv-list">
      {rows.map(([label, value], index) => (
        <div key={`${index}:${label}`} className="structured-kv-row">
          <span className="structured-kv-key">{label}</span>
          <div className="structured-kv-value mono">{value}</div>
        </div>
      ))}
    </div>
  );
}

function EmptyState({
  compact = false,
  copy,
}: {
  compact?: boolean;
  copy: string;
}) {
  return <div className={`empty-state ${compact ? "compact" : ""}`}>{copy}</div>;
}

type IterationBounds = {
  min: number;
  max: number;
};

type NormalizedIterationRange = {
  start: number | null;
  end: number | null;
};

function collectIterationBounds(input: {
  metricGroups: MetricGroupQueryResult[];
  records: ExecutionRecord[];
  spans: Array<{ iteration_index?: unknown }> | null;
}): IterationBounds | null {
  const values: number[] = [];
  for (const group of input.metricGroups) {
    for (const series of group.series) {
      for (const point of series.points) {
        if (point.iteration_index != null) {
          values.push(point.iteration_index);
        }
      }
    }
  }
  for (const record of input.records) {
    if (record.iteration_index != null) {
      values.push(record.iteration_index);
    }
  }
  for (const span of input.spans ?? []) {
    const raw = span.iteration_index;
    if (typeof raw === "number" && Number.isFinite(raw)) {
      values.push(raw);
    }
  }
  if (values.length === 0) {
    return null;
  }
  return {
    min: Math.min(...values),
    max: Math.max(...values),
  };
}

function normalizeIterationRange(input: {
  startInput: string;
  endInput: string;
  bounds: IterationBounds | null;
}): NormalizedIterationRange {
  const parse = (value: string): number | null => {
    if (value.trim() === "") {
      return null;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) {
      return null;
    }
    return Math.max(0, Math.floor(parsed));
  };
  const startParsed = parse(input.startInput);
  const endParsed = parse(input.endInput);
  if (input.bounds == null) {
    return { start: startParsed, end: endParsed };
  }
  const boundedStart =
    startParsed == null
      ? null
      : Math.min(Math.max(startParsed, input.bounds.min), input.bounds.max);
  const boundedEnd =
    endParsed == null
      ? null
      : Math.min(Math.max(endParsed, input.bounds.min), input.bounds.max);
  if (
    boundedStart != null &&
    boundedEnd != null &&
    boundedStart > boundedEnd
  ) {
    return { start: boundedEnd, end: boundedStart };
  }
  return { start: boundedStart, end: boundedEnd };
}

function isWithinIterationRange(
  iterationIndex: number | null,
  range: NormalizedIterationRange,
): boolean {
  if (iterationIndex == null) {
    return true;
  }
  if (range.start != null && iterationIndex < range.start) {
    return false;
  }
  if (range.end != null && iterationIndex > range.end) {
    return false;
  }
  return true;
}

function filterRecordsByIterationRange(
  records: ExecutionRecord[],
  range: NormalizedIterationRange,
): ExecutionRecord[] {
  return records.filter((record) =>
    isWithinIterationRange(record.iteration_index, range),
  );
}

function filterSpansByIterationRange(
  spans: GenericSpan[],
  range: NormalizedIterationRange,
): GenericSpan[] {
  return spans.filter((span) =>
    isWithinIterationRange(span.iterationIndex, range),
  );
}

function filterCustomViewByIterationRange(
  view: EvaluatedCustomView | null,
  range: NormalizedIterationRange,
): EvaluatedCustomView | null {
  if (view == null) {
    return null;
  }
  const rows = view.rows.filter((row) =>
    isWithinIterationRange(row.iteration_index, range),
  );
  return {
    ...view,
    row_count: rows.length,
    rows,
  };
}

function formatIterationRangeLabel(range: NormalizedIterationRange): string {
  if (range.start == null && range.end == null) {
    return "all";
  }
  if (range.start != null && range.end != null) {
    return `i${range.start}–i${range.end}`;
  }
  if (range.start != null) {
    return `i${range.start}+`;
  }
  return `≤ i${range.end}`;
}

function formatMetricValue(value: number) {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value.toFixed(2);
}

function normalizeCustomViewString(value: string, columnId: string): string {
  const stripped = value
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/\*\*/g, "")
    .replace(/\r\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  if (columnId.includes("prompt") || columnId.includes("completion")) {
    return stripped;
  }
  return stripped.replace(/\s+/g, " ");
}

function truncateCustomViewString(
  compact: string,
  columnId: string,
  limits: { narrative: number; other: number },
): string {
  const limit = columnId.includes("prompt") || columnId.includes("completion")
    ? limits.narrative
    : limits.other;
  if (compact.length <= limit) {
    return compact;
  }
  return `${compact.slice(0, limit).trimEnd()}…`;
}

function formatCustomViewValueFull(
  value: unknown,
  column: { column_id: string },
): string {
  if (value == null) {
    return "—";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : value.toFixed(4);
  }
  if (typeof value === "string") {
    return normalizeCustomViewString(value, column.column_id);
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  return normalizeCustomViewString(JSON.stringify(value), column.column_id);
}

/** Compact text for the table; full strings live in the row detail drawer. */
function formatCustomViewCellPreview(
  value: unknown,
  column: { column_id: string },
): string {
  if (value == null) {
    return "—";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : value.toFixed(4);
  }
  if (typeof value === "string") {
    const normalized = normalizeCustomViewString(value, column.column_id);
    return truncateCustomViewString(normalized, column.column_id, {
      narrative: 220,
      other: 72,
    });
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  return truncateCustomViewString(
    normalizeCustomViewString(JSON.stringify(value), column.column_id),
    column.column_id,
    { narrative: 160, other: 72 },
  );
}

function customViewRowToDetailRows(
  row: EvaluatedCustomViewRow,
  columns: TableColumn[],
): Array<[string, string]> {
  const meta: Array<[string, string]> = [];
  if (row.frame_id != null) {
    meta.push(["Frame", row.frame_id]);
  }
  if (row.loop_node_id != null) {
    meta.push(["Loop node", row.loop_node_id]);
  }
  if (row.iteration_index != null) {
    meta.push(["Iteration", String(row.iteration_index)]);
  }
  meta.push(["Row id", row.row_id]);
  return [...meta, ...columns.map((col) => [col.title, formatCustomViewValueFull(row.values[col.column_id], col)] as [string, string])];
}

function customViewRowSubheading(row: EvaluatedCustomViewRow): string | undefined {
  const parts: string[] = [];
  if (row.frame_id) {
    parts.push(`frame ${row.frame_id}`);
  }
  if (row.loop_node_id) {
    parts.push(`loop ${row.loop_node_id}`);
  }
  if (row.iteration_index != null) {
    parts.push(`iteration ${row.iteration_index}`);
  }
  return parts.length > 0 ? parts.join(" · ") : undefined;
}

function customViewCellTone(value: unknown) {
  return typeof value === "number" ? "numeric" : "text";
}

function customViewColumnClassName(column: { column_id: string; title: string }) {
  const label = `${column.column_id} ${column.title}`.toLowerCase();
  if (
    label.includes("score") ||
    label.includes("reward") ||
    label.includes("metric") ||
    label.includes("loss") ||
    label.includes("rate")
  ) {
    return "custom-view-col numeric";
  }
  if (label.includes("prompt") || label.includes("sample") || label.includes("text")) {
    return "custom-view-col narrative";
  }
  return "custom-view-col";
}

function buildRecordCadenceBars(records: ExecutionRecord[]) {
  if (records.length === 0) {
    return [28, 44, 52, 66, 72, 64, 20, 38];
  }
  const buckets = new Array<number>(8).fill(0);
  records.forEach((record, index) => {
    buckets[index % buckets.length] += 1;
  });
  const peak = Math.max(...buckets, 1);
  return buckets.map((value) => Math.round((value / peak) * 120));
}

function formatRecordLine(record: ExecutionRecord) {
  return `${new Date(record.timestamp_ms).toLocaleTimeString()}  ${record.event_type.padEnd(18, " ")} ${record.node_id}  frame=${record.frame_id}`;
}

function truncateJson(value: unknown, maxLength: number) {
  const serialized = JSON.stringify(value, null, 2);
  if (serialized.length <= maxLength) {
    return serialized;
  }
  return `${serialized.slice(0, maxLength)}…`;
}

function getSelectedNodeSummary(
  replay: ReplayReport | null,
  nodeId: string | null,
  frameId: string | null,
) {
  if (!replay || !nodeId) {
    return null;
  }
  return (
    replay.node_summaries.find(
      (summary) =>
        summary.node_id === nodeId &&
        (frameId === null || summary.frame_id === frameId),
    ) ?? null
  );
}

function filterRecords(
  records: ExecutionRecord[],
  nodeId: string | null,
  frameId: string | null,
) {
  if (!nodeId) {
    return records;
  }
  return records.filter(
    (record) =>
      record.node_id === nodeId &&
      (frameId === null || record.frame_id === frameId),
  );
}

function getConnectedEdges(graph: GraphPayload | null, nodeId: string | null) {
  if (!graph || !nodeId) {
    return { upstream: [], downstream: [] };
  }
  return {
    upstream: graph.edges.filter((edge) => edge.target_node_id === nodeId),
    downstream: graph.edges.filter((edge) => edge.source_node_id === nodeId),
  };
}

function buildBreadcrumbs(
  activeView: ViewId,
  selectedCatalog: CatalogEntry | null,
  selectedNodeId: string | null,
) {
  const root = [
    "Home",
    activeView === "launch" ? "Catalog" : "Runs",
    selectedCatalog?.invocation_name ?? selectedCatalog?.spec_id ?? "Dashboard",
  ];
  switch (activeView) {
    case "overview":
      return [...root, "Overview"];
    case "views":
      return [...root, "Tables"];
    case "graph":
      return [...root, "Graph"];
    case "node":
      return [...root, "Node", selectedNodeId ?? "—"];
    case "spans":
      return [...root, "Traces"];
    case "launch":
      return ["Home", "Catalog", "Launch"];
    default:
      return root;
  }
}

function viewFromHash(hash: string): ViewId | null {
  const normalized = hash.replace(/^#/, "");
  return VIEWS.some((view) => view.id === normalized)
    ? (normalized as ViewId)
    : null;
}

export default App;
