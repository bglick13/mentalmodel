import {
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
import { GraphPanel } from "./components/GraphPanel";
import {
  fetchCatalog,
  fetchCatalogGraph,
  fetchExecution,
  fetchNodeDetail,
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
  ExecutionRecord,
  ExecutionSession,
  GenericSpan,
  GraphEdge,
  GraphPayload,
  MetricGroup,
  NodeDetail,
  NumericMetric,
  ReplayNodeSummary,
  ReplayReport,
  RunOverview,
  RunSummary,
  TimeseriesResponse,
} from "./types";

type SpansInspector =
  | { kind: "span"; index: number }
  | { kind: "record"; id: string };

type MetricGroupView = {
  group: MetricGroup;
  metrics: NumericMetric[];
};

type ViewId =
  | "overview"
  | "graph"
  | "node"
  | "spans"
  | "launch";

type ScopeToken = {
  label: string;
  value: string;
};

const VIEWS: Array<{
  id: ViewId;
  label: string;
}> = [
  { id: "overview", label: "Overview" },
  { id: "graph", label: "Graph" },
  { id: "node", label: "Node" },
  { id: "spans", label: "Records" },
  { id: "launch", label: "Launch" },
];

const SPEC_PATH_STORAGE_KEY = "mentalmodel.dashboard.specPath";
const DEFAULT_PANGRAM_VERIFY3 =
  "/Users/ben/repos/pangramanizer/pangramanizer/mentalmodel_training/verification/real_verify3.toml";

function readStoredSpecPath(): string {
  try {
    const v = localStorage.getItem(SPEC_PATH_STORAGE_KEY);
    return v && v.length > 0 ? v : DEFAULT_PANGRAM_VERIFY3;
  } catch {
    return DEFAULT_PANGRAM_VERIFY3;
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
  if (sample.length === 0) {
    return "n/a";
  }
  const successes = sample.filter((run) => run.success).length;
  return `${((successes / sample.length) * 100).toFixed(1)}%`;
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
  const [activeView, setActiveView] = useState<ViewId>("overview");
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [graphPreview, setGraphPreview] = useState<GraphPayload | null>(null);
  const [graphFindings, setGraphFindings] = useState<AnalysisFinding[]>([]);
  const [activeRun, setActiveRun] = useState<RunOverview | null>(null);
  const [activeReplay, setActiveReplay] = useState<ReplayReport | null>(null);
  const [activeRecords, setActiveRecords] = useState<ExecutionRecord[]>([]);
  const [activeExecution, setActiveExecution] = useState<ExecutionSession | null>(
    null,
  );
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedFrameId, setSelectedFrameId] = useState<string | null>(null);
  const [nodeDetail, setNodeDetail] = useState<NodeDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [customSpecPath, setCustomSpecPath] = useState(readStoredSpecPath);
  const [exploreTimePreset, setExploreTimePreset] =
    useState<ExploreTimePreset>("1h");
  const [exploreRunId, setExploreRunId] = useState<string | null>(null);
  const [exploreNodeId, setExploreNodeId] = useState<string | null>(null);
  const [timeseries, setTimeseries] = useState<TimeseriesResponse | null>(null);
  const [timeseriesLoading, setTimeseriesLoading] = useState(false);
  const [timeseriesError, setTimeseriesError] = useState<string | null>(null);
  const [runSpans, setRunSpans] = useState<Record<string, unknown>[] | null>(
    null,
  );
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

  const selectedCatalog = useMemo(
    () => catalog.find((entry) => entry.spec_id === selectedSpecId) ?? null,
    [catalog, selectedSpecId],
  );

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

  const loadRun = useCallback(async (entry: CatalogEntry, runId: string) => {
    const [overview, replay] = await Promise.all([
      fetchRunOverview(entry.graph_id, runId),
      fetchRunReplay(entry.graph_id, runId, entry.default_loop_node_id ?? undefined),
    ]);
    setActiveRun(overview);
    setActiveReplay(replay);
    setSelectedFrameId(null);
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
  }, []);

  useEffect(() => {
    void (async () => {
      try {
        const entries = await fetchCatalog();
        setCatalog(entries);
        const parsed = parseExplorerQuery(window.location.search);
        const specFromUrl =
          parsed.specId &&
          entries.some((entry) => entry.spec_id === parsed.specId)
            ? parsed.specId
            : null;
        setSelectedSpecId(specFromUrl ?? entries[0]?.spec_id ?? null);
        if (parsed.window && isExplorerWindowParam(parsed.window)) {
          setExploreTimePreset(parsed.window);
        }
        setExploreRunId(parsed.runId);
        setExploreNodeId(parsed.nodeId);
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
    if (!selectedCatalog) {
      return;
    }
    const specId = selectedCatalog.spec_id;
    const prevSpec = prevExplorerSpecIdRef.current;
    prevExplorerSpecIdRef.current = specId;
    if (prevSpec != null && prevSpec !== specId) {
      setExploreRunId(null);
      setExploreNodeId(null);
      setExploreTimePreset("1h");
      setSpansInspector(null);
    }
    setError(null);
    setRuns([]);
    setGraphPreview(null);
    setGraphFindings([]);
    setActiveRun(null);
    setActiveReplay(null);
    setActiveRecords([]);
    setSelectedFrameId(null);
    setNodeDetail(null);
    setTimeseries(null);
    setRunSpans(null);
    void (async () => {
      try {
        const [catalogGraph, runData] = await Promise.all([
          fetchCatalogGraph(selectedCatalog.spec_id),
          fetchRuns(selectedCatalog.graph_id, selectedCatalog.invocation_name),
        ]);
        setGraphPreview(catalogGraph.graph);
        setGraphFindings(catalogGraph.analysis.findings);
        setRuns(runData);
        if (runData.length === 0) {
          setSelectedNodeId(
            selectedCatalog.pinned_nodes[0]?.node_id ??
              catalogGraph.graph.nodes[0]?.node_id ??
              null,
          );
        }
      } catch (fetchError) {
        setError(String(fetchError));
      }
    })();
  }, [selectedCatalog]);

  useEffect(() => {
    if (
      exploreRunId != null &&
      runs.length > 0 &&
      !runs.some((r) => r.run_id === exploreRunId)
    ) {
      setExploreRunId(null);
    }
  }, [exploreRunId, runs]);

  useEffect(() => {
    if (!selectedCatalog) {
      return;
    }
    if (exploreRunId == null) {
      setActiveRun(null);
      setActiveReplay(null);
      setActiveRecords([]);
      setNodeDetail(null);
      setRunSpans(null);
      return;
    }
    if (activeRun?.summary.run_id === exploreRunId) {
      return;
    }
    void loadRun(selectedCatalog, exploreRunId);
  }, [selectedCatalog, exploreRunId, loadRun, activeRun?.summary.run_id]);

  useEffect(() => {
    if (!selectedCatalog || !activeRun || exploreRunId == null) {
      return;
    }
    if (activeRun.summary.run_id !== exploreRunId) {
      return;
    }
    let cancelled = false;
    void fetchRunRecords(
      selectedCatalog.graph_id,
      activeRun.summary.run_id,
      exploreNodeId ?? undefined,
    )
      .then((records) => {
        if (!cancelled) {
          setActiveRecords(records);
        }
      })
      .catch((recordsError: unknown) => {
        if (!cancelled) {
          setError(String(recordsError));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedCatalog, activeRun?.summary.run_id, exploreRunId, exploreNodeId]);

  useEffect(() => {
    if (!selectedCatalog || !activeRun || exploreRunId == null) {
      return;
    }
    if (activeRun.summary.run_id !== exploreRunId) {
      return;
    }
    let cancelled = false;
    void fetchRunSpans(selectedCatalog.graph_id, activeRun.summary.run_id)
      .then((payload) => {
        if (!cancelled) {
          setRunSpans(payload.spans);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setRunSpans([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedCatalog, activeRun?.summary.run_id, exploreRunId]);

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
    const { sinceMs, untilMs, rollupMs } = computeExploreWindow(
      exploreTimePreset,
      runs,
    );
    let cancelled = false;
    setTimeseriesLoading(true);
    setTimeseriesError(null);
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
          setTimeseriesLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
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
          };
        });
        if (next.run_id && selectedCatalog) {
          setExploreRunId(next.run_id);
          const refreshedRuns = await fetchRuns(
            selectedCatalog.graph_id,
            selectedCatalog.invocation_name,
          );
          setRuns(refreshedRuns);
        }
      })().catch((pollError: unknown) => {
        setError(String(pollError));
      });
    }, 750);

    return () => window.clearInterval(timer);
  }, [activeExecution, selectedCatalog]);

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
  const metricGroups = useMemo(() => {
    const groups = buildMetricGroups(
      selectedCatalog,
      activeRun?.metrics ?? [],
    );
    if (!exploreNodeId) {
      return groups;
    }
    return groups
      .map((groupView) => ({
        ...groupView,
        metrics: groupView.metrics.filter(
          (metric) => metric.node_id === exploreNodeId,
        ),
      }))
      .filter((groupView) => groupView.metrics.length > 0);
  }, [selectedCatalog, activeRun?.metrics, exploreNodeId]);
  const frameCount = activeReplay?.frame_ids.length ?? 0;
  const selectedNodeSummary = useMemo(
    () => getSelectedNodeSummary(activeReplay, selectedNodeId, selectedFrameId),
    [activeReplay, selectedFrameId, selectedNodeId],
  );
  const recordsInTimeWindow = useMemo(
    () =>
      filterRecordsByTimeWindow(
        activeRecords,
        exploreWindow.sinceMs,
        exploreWindow.untilMs,
      ),
    [activeRecords, exploreWindow],
  );
  const selectedNodeRecords = useMemo(
    () =>
      filterRecords(recordsInTimeWindow, exploreNodeId, selectedFrameId),
    [recordsInTimeWindow, selectedFrameId, exploreNodeId],
  );
  const explorerRecordsInWindowCount = useMemo(() => {
    if (exploreRunId != null) {
      return recordsInTimeWindow.length;
    }
    return runsInExploreWindow.reduce((sum, r) => sum + r.record_count, 0);
  }, [exploreRunId, recordsInTimeWindow, runsInExploreWindow]);
  const liveRecords = activeExecution?.records ?? [];
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
        (item) => item.severity === "warning",
      ).length,
    );
  }, [exploreRunId, activeRun?.invariants]);
  const selectedNodeEdges = useMemo(
    () => getConnectedEdges(summaryGraph, selectedNodeId),
    [summaryGraph, selectedNodeId],
  );
  const spanItems = useMemo(
    () =>
      buildSpanViews(
        nodeDetail,
        runSpans,
        exploreNodeId,
        selectedFrameId,
      ),
    [nodeDetail, runSpans, exploreNodeId, selectedFrameId],
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

  const runContext = useMemo<ScopeToken[]>(
    () => {
      const windowLabel = EXPLORE_PRESET_LABEL[exploreTimePreset];
      const base: ScopeToken[] = activeRun
        ? [
            { label: "spec", value: selectedCatalog?.spec_id ?? "n/a" },
            { label: "graph", value: activeRun.summary.graph_id },
            { label: "run", value: activeRun.summary.run_id },
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
          ];
      const explorer: ScopeToken[] = [
        { label: "$window", value: windowLabel },
        {
          label: "@run_id",
          value: exploreRunId ?? "all",
        },
        {
          label: "@node_id",
          value: exploreNodeId ?? "all",
        },
      ];
      return [...base, ...explorer];
    },
    [
      activeRun,
      selectedCatalog,
      exploreTimePreset,
      exploreRunId,
      exploreNodeId,
    ],
  );

  const activeViewLabel =
    VIEWS.find((view) => view.id === activeView)?.label ?? "Dashboard";

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
            onChange={(event) => setSelectedSpecId(event.target.value || null)}
          >
            {catalog.map((entry) => (
              <option key={entry.spec_id} value={entry.spec_id}>
                {entry.label}
              </option>
            ))}
          </select>
        </div>

        <footer className="nav-footer">
          <code className="nav-footer-code">uv run mentalmodel ui --help</code>
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
          <label className="scope-search-wrap">
            <span className="sr-only">Search and filter scope</span>
            <input
              type="search"
              className="scope-search"
              readOnly
              placeholder={searchScopePlaceholder(activeView)}
              aria-label={`Filter scope for this view: ${searchScopePlaceholder(activeView)}`}
            />
          </label>
        </section>

        {error ? <div className="error-banner">{error}</div> : null}

        {selectedCatalog ? (
          <ExplorerScopeBar
            exploreNodeId={exploreNodeId}
            exploreRunId={exploreRunId}
            exploreTimePreset={exploreTimePreset}
            graphNodes={summaryGraph?.nodes ?? []}
            runs={runsForExplorerDropdown}
            setExploreNodeId={setExploreNodeId}
            setExploreRunId={setExploreRunId}
            setExploreTimePreset={setExploreTimePreset}
          />
        ) : null}

        {        renderCurrentView({
          activeExecution,
          activeRun,
          activeReplay,
          activeView,
          catalog,
          explorerRecordsInWindowCount,
          exploreNodeId,
          exploreRunId,
          frameCount,
          graphFindings,
          handleRun,
          handleRunFromPath,
          liveRecords,
          metricGroups,
          nodeDetail,
          recentRunSuccessLabel,
          runContext,
          runs,
          runsForExplorerList: runsForExplorerDropdown,
          selectNodeAndExplorer,
          selectedCatalog,
          selectedFrameId,
          selectedNodeEdges,
          selectedNodeId,
          recordsInTimeWindow,
          selectedNodeRecords,
          selectedNodeSummary,
          setActiveView,
          setCatalog,
          setExploreRunId,
          setSelectedFrameId,
          setSelectedNodeId,
          setSelectedSpecId,
          setSpansInspector,
          spanItems,
          spansInspector,
          summaryGraph,
          warningInvariantCount,
          customSpecPath,
          setCustomSpecPath,
          timeseries,
          timeseriesError,
          timeseriesLoading,
        })}
      </main>
    </div>
  );
}

function renderCurrentView({
  activeExecution,
  activeRun,
  activeReplay,
  activeView,
  catalog,
  explorerRecordsInWindowCount,
  exploreNodeId,
  exploreRunId,
  frameCount,
  graphFindings,
  handleRun,
  handleRunFromPath,
  liveRecords,
  metricGroups,
  nodeDetail,
  recentRunSuccessLabel,
  runContext,
  runs,
  runsForExplorerList,
  selectNodeAndExplorer,
  selectedCatalog,
  selectedFrameId,
  selectedNodeEdges,
  selectedNodeId,
  recordsInTimeWindow,
  selectedNodeRecords,
  selectedNodeSummary,
  setActiveView,
  setCatalog,
  setExploreRunId,
  setSelectedFrameId,
  setSelectedNodeId,
  setSelectedSpecId,
  setSpansInspector,
  spanItems,
  spansInspector,
  summaryGraph,
  warningInvariantCount,
  customSpecPath,
  setCustomSpecPath,
  timeseries,
  timeseriesError,
  timeseriesLoading,
}: {
  activeExecution: ExecutionSession | null;
  activeRun: RunOverview | null;
  activeReplay: ReplayReport | null;
  activeView: ViewId;
  catalog: CatalogEntry[];
  explorerRecordsInWindowCount: number;
  exploreNodeId: string | null;
  exploreRunId: string | null;
  frameCount: number;
  graphFindings: AnalysisFinding[];
  handleRun: (specId: string) => Promise<void>;
  handleRunFromPath: (specPath: string) => Promise<void>;
  liveRecords: ExecutionRecord[];
  metricGroups: MetricGroupView[];
  nodeDetail: NodeDetail | null;
  recentRunSuccessLabel: string;
  runContext: ScopeToken[];
  runs: RunSummary[];
  runsForExplorerList: RunSummary[];
  selectNodeAndExplorer: (nodeId: string, frameId?: string | null) => void;
  selectedCatalog: CatalogEntry | null;
  selectedFrameId: string | null;
  selectedNodeEdges: { upstream: GraphEdge[]; downstream: GraphEdge[] };
  selectedNodeId: string | null;
  recordsInTimeWindow: ExecutionRecord[];
  selectedNodeRecords: ExecutionRecord[];
  selectedNodeSummary: ReplayNodeSummary | null;
  setActiveView: (view: ViewId) => void;
  setCatalog: (entries: CatalogEntry[]) => void;
  setExploreRunId: (runId: string | null) => void;
  setSelectedFrameId: (frameId: string | null) => void;
  setSelectedNodeId: (nodeId: string | null) => void;
  setSelectedSpecId: (specId: string | null) => void;
  setSpansInspector: Dispatch<SetStateAction<SpansInspector | null>>;
  spanItems: GenericSpan[];
  spansInspector: SpansInspector | null;
  summaryGraph: GraphPayload | null;
  warningInvariantCount: string;
  customSpecPath: string;
  setCustomSpecPath: (path: string) => void;
  timeseries: TimeseriesResponse | null;
  timeseriesError: string | null;
  timeseriesLoading: boolean;
}) {
  switch (activeView) {
    case "overview":
      return (
        <OverviewView
          activeExecution={activeExecution}
          activeRun={activeRun}
          exploreRunId={exploreRunId}
          explorerRecordsInWindowCount={explorerRecordsInWindowCount}
          graphFindings={graphFindings}
          handleRun={handleRun}
          liveRecords={liveRecords}
          metricGroups={metricGroups}
          recentRunSuccessLabel={recentRunSuccessLabel}
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
          warningInvariantCount={warningInvariantCount}
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
          runContext={runContext}
          runRecordsInWindow={recordsInTimeWindow}
          selectNodeAndExplorer={selectNodeAndExplorer}
          selectedNodeId={selectedNodeId}
          selectedFrameId={selectedFrameId}
          selectedNodeRecords={selectedNodeRecords}
          setActiveView={setActiveView}
          setSpansInspector={setSpansInspector}
          spanItems={spanItems}
          spansInspector={spansInspector}
        />
      );
    case "launch":
      return (
        <LaunchCompareView
          catalog={catalog}
          customSpecPath={customSpecPath}
          handleRun={handleRun}
          handleRunFromPath={handleRunFromPath}
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
  activeRun,
  exploreRunId,
  explorerRecordsInWindowCount,
  graphFindings,
  handleRun,
  liveRecords,
  metricGroups,
  recentRunSuccessLabel,
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
  warningInvariantCount,
}: {
  activeExecution: ExecutionSession | null;
  activeRun: RunOverview | null;
  exploreRunId: string | null;
  explorerRecordsInWindowCount: number;
  graphFindings: AnalysisFinding[];
  handleRun: (specId: string) => Promise<void>;
  liveRecords: ExecutionRecord[];
  metricGroups: MetricGroupView[];
  recentRunSuccessLabel: string;
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
  warningInvariantCount: string;
}) {
  const currentRecordCount = String(explorerRecordsInWindowCount);
  const frameCountKpi =
    exploreRunId == null
      ? "—"
      : String(
          new Set(activeRun?.metrics.map((m) => m.frame_id) ?? []).size,
        );

  return (
    <>
      <RunContextStrip tokens={runContext} />

      <section className="hero-grid v3-hero-grid">
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
          label="Visible frames"
          value={frameCountKpi}
          source="source: replay frames (requires @run_id)"
        />
      </section>

      <section className="overview-layout">
        <div className="stack">
          <Panel title="Semantic event rate">
            <ExplorerTimeseriesChart
              error={timeseriesError}
              loading={timeseriesLoading}
              timeseries={timeseries}
            />
          </Panel>
        </div>

        <div className="stack narrow">
          <Panel title="Runs (explorer scope)">
            <div className="interactive-list">
              {runsForExplorerList.length > 0 ? (
                runsForExplorerList.slice(0, 5).map((run) => (
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
                        label={run.success ? "pass" : "fail"}
                        tone={run.success ? "ok" : "error"}
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

          <Panel title="Metrics">
            {metricGroups.length > 0 ? (
              metricGroups.map((groupView) => (
                <MetricGroupRail
                  key={groupView.group.group_id}
                  group={groupView.group}
                  metrics={groupView.metrics}
                  onInspectMetric={(metric) => {
                    selectNodeAndExplorer(
                      metric.node_id,
                      metric.frame_id && metric.frame_id !== "root"
                        ? metric.frame_id
                        : null,
                    );
                    setActiveView("node");
                  }}
                />
              ))
            ) : (
              <EmptyState copy="This spec does not expose grouped numeric outputs yet." />
            )}
          </Panel>
        </div>
      </section>

      <section className="overview-bottom">
        <Panel title="Records">
          <RecordConsole records={activeRun ? activeRun.graph.nodes.length > 0 ? liveRecords.length > 0 ? liveRecords.slice(-4) : [] : [] : []} fallbackRecords={activeExecution?.records.slice(-4) ?? []} />
        </Panel>

        <Panel
          title="Live run"
          aside={
            selectedCatalog ? (
              <button
                className="primary-action"
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
              </div>
            </div>
          ) : (
            <EmptyState copy="Launch a run to watch the live semantic stream." />
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
      </section>
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
                ["Open node detail", "Inspect inputs and outputs", "node"],
                ["Open spans & records", "Pivot into traces and event stream", "spans"],
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
              <StatusChip label="source: queue_summary.*" tone="accent" />
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
            <TraceTable spans={spanItems} />
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

function SpansRecordsView({
  activeRun,
  exploreNodeId,
  exploreRunId,
  runContext,
  runRecordsInWindow,
  selectNodeAndExplorer,
  selectedNodeId,
  selectedFrameId,
  selectedNodeRecords,
  setActiveView,
  setSpansInspector,
  spanItems,
  spansInspector,
}: {
  activeRun: RunOverview | null;
  exploreNodeId: string | null;
  exploreRunId: string | null;
  runContext: ScopeToken[];
  /** Time-window slice of the loaded run’s semantic records (same source as list; used to join spans). */
  runRecordsInWindow: ExecutionRecord[];
  selectNodeAndExplorer: (nodeId: string, frameId?: string | null) => void;
  selectedNodeId: string | null;
  selectedFrameId: string | null;
  selectedNodeRecords: ExecutionRecord[];
  setActiveView: (view: ViewId) => void;
  setSpansInspector: Dispatch<SetStateAction<SpansInspector | null>>;
  spanItems: GenericSpan[];
  spansInspector: SpansInspector | null;
}) {
  const explorerNodeLabel = exploreNodeId ?? "all";

  const displayedRecords = useMemo(
    () => selectedNodeRecords.slice(-400),
    [selectedNodeRecords],
  );

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
          displayedRecords.find((r) => r.record_id === spansInspector.id) ??
          null)
      : null;

  function openNodeDetailForRecord(record: ExecutionRecord) {
    const fid =
      record.frame_id != null && record.frame_id !== "root"
        ? record.frame_id
        : null;
    selectNodeAndExplorer(record.node_id, fid);
    setActiveView("node");
    setSpansInspector(null);
  }

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
        <div className="stack">
          <Panel title="Spans (OTel)">
            <div className="chip-row">
              <StatusChip label={`@node_id: ${explorerNodeLabel}`} tone="accent" />
              <StatusChip label={`frame: ${selectedFrameId ?? "all"}`} />
              <StatusChip label={`${spanItems.length} spans`} />
              <StatusChip label={`trace: ${activeRun?.summary.trace_mode ?? "n/a"}`} />
            </div>
            <p className="spans-explainer">
              Timings from mirrored OTel spans. Click a row for full attributes.
            </p>
            <TraceList
              selectedIndex={
                spansInspector?.kind === "span" ? spansInspector.index : null
              }
              spans={spanItems}
              onSelectSpan={(index) =>
                setSpansInspector({ kind: "span", index })
              }
            />
          </Panel>

          <Panel title="Records (semantic log)">
            <div className="chip-row">
              <StatusChip label={`${displayedRecords.length} records`} tone="accent" />
              <StatusChip
                label={`scope: @node_id ${explorerNodeLabel} · frame ${selectedFrameId ?? "all"}`}
              />
            </div>
            <p className="spans-explainer">
              Execution events from <span className="mono">records.jsonl</span>. Not every
              span has a matching record line.
            </p>
            <SemanticRecordList
              emptyHint={recordsEmptyReason}
              records={displayedRecords}
              selectedRecordId={
                spansInspector?.kind === "record" ? spansInspector.id : null
              }
              onSelectRecord={(id) =>
                setSpansInspector({ kind: "record", id })
              }
            />
          </Panel>
        </div>

        <div className="stack wide">
          <Panel title="Timeline">
            <TimelineLens spans={spanItems} />
          </Panel>

          <Panel title="Inspector">
            <p className="spans-detail-hint">
              Click any span or record on the left to open a side panel with fields and raw
              JSON—similar to Datadog’s span / log inspector.
            </p>
          </Panel>
        </div>
      </section>

      {spanDetail ? (
        <ExecutionDetailDrawer
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
            recordDetail.event_type === "effect.completed" ? (
              <section className="exec-detail-section">
                <h3 className="exec-detail-section-title">Structured output</h3>
                <p className="spans-explainer">
                  Semantic records only record an output type hint for effects. The full
                  return value is stored in the run bundle{" "}
                  <span className="mono">outputs.json</span> and shown under{" "}
                  <strong>Node</strong> for this node and frame.
                </p>
                <button
                  type="button"
                  className="primary-action secondary"
                  onClick={() => openNodeDetailForRecord(recordDetail)}
                >
                  Open node detail (inputs / outputs)
                </button>
              </section>
            ) : null
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
    </>
  );
}

function TraceList({
  onSelectSpan,
  selectedIndex,
  spans,
}: {
  onSelectSpan: (index: number) => void;
  selectedIndex: number | null;
  spans: GenericSpan[];
}) {
  return spans.length > 0 ? (
    <div className="trace-list">
      {spans.map((span, index) => (
        <button
          key={`${span.label}:${index}`}
          type="button"
          className={`trace-row-card ${selectedIndex === index ? "active" : ""}`}
          onClick={() => onSelectSpan(index)}
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
      ))}
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
  catalog,
  customSpecPath,
  handleRun,
  handleRunFromPath,
  runs,
  selectedCatalog,
  setCatalog,
  setCustomSpecPath,
  setExploreRunId,
  setSelectedSpecId,
}: {
  catalog: CatalogEntry[];
  customSpecPath: string;
  handleRun: (specId: string) => Promise<void>;
  handleRunFromPath: (specPath: string) => Promise<void>;
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
            Absolute path to a <code>mentalmodel verify --spec</code> file. For
            pangramanizer verify3, your checkout must be on{" "}
            <code>PYTHONPATH</code> (e.g. run the UI from the pangram repo with{" "}
            <code>uv run mentalmodel ui</code>).
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
            placeholder={DEFAULT_PANGRAM_VERIFY3}
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
            </div>
          ) : null}

          {selectedCatalog ? (
            <div className="launch-secondary-actions">
              <button
                type="button"
                className="primary-action secondary"
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

function MetricGroupRail({
  group,
  metrics,
  onInspectMetric,
}: {
  group: MetricGroup;
  metrics: NumericMetric[];
  onInspectMetric: (metric: NumericMetric) => void;
}) {
  const maxValue = Math.max(...metrics.map((metric) => metric.value), 1);

  return (
    <div className="metric-group-rail">
      <div className="metric-group-head">
        <div className="panel-title">{group.title}</div>
        <div className="panel-subtitle">{group.description}</div>
      </div>
      <div className="metric-rail-list">
        {metrics.map((metric) => (
          <button
            key={`${metric.node_id}:${metric.path}:${metric.frame_id ?? "root"}`}
            className="metric-rail-row"
            onClick={() => onInspectMetric(metric)}
          >
            <div className="metric-rail-meta">
              <span>{metric.path}</span>
              <strong>{formatMetricValue(metric.value)}</strong>
            </div>
            <div className="metric-rail-track">
              <div
                className="metric-rail-fill"
                style={{ width: `${Math.max((metric.value / maxValue) * 100, 8)}%` }}
              />
            </div>
          </button>
        ))}
      </div>
    </div>
  );
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
  exploreRunId,
  exploreTimePreset,
  graphNodes,
  runs,
  setExploreNodeId,
  setExploreRunId,
  setExploreTimePreset,
}: {
  exploreNodeId: string | null;
  exploreRunId: string | null;
  exploreTimePreset: ExploreTimePreset;
  graphNodes: GraphPayload["nodes"];
  runs: RunSummary[];
  setExploreNodeId: (id: string | null) => void;
  setExploreRunId: (id: string | null) => void;
  setExploreTimePreset: (p: ExploreTimePreset) => void;
}) {
  return (
    <section className="explorer-scope-bar" aria-label="Explorer facets">
      <div className="explorer-scope-title">
        <span className="explorer-scope-product">Explorer</span>
        <span className="explorer-scope-hint">
          $window scopes the chart and run list. &quot;All runs&quot; for @run_id
          aggregates the chart across runs in $window; pick a run to load bundle,
          metrics, and node drill-down.
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
      </div>
    </section>
  );
}

function ExplorerTimeseriesChart({
  error,
  loading,
  timeseries,
}: {
  error: string | null;
  loading: boolean;
  timeseries: TimeseriesResponse | null;
}) {
  const { paths, maxY, w, h } = useMemo(() => {
    const width = 920;
    const height = 232;
    const padL = 52;
    const padR = 20;
    const padT = 20;
    const padB = 44;
    if (!timeseries || timeseries.buckets.length === 0) {
      return {
        paths: null as {
          records: string;
          loops: string;
          nodes: string;
        } | null,
        maxY: 1,
        w: width,
        h: height,
      };
    }
    const buckets = timeseries.buckets;
    const innerW = width - padL - padR;
    const innerH = height - padT - padB;
    const maxVal = Math.max(
      0.000_001,
      ...buckets.flatMap((b) => [
        b.records_per_sec,
        b.loop_events_per_sec,
        b.unique_nodes_per_sec,
      ]),
    );
    const n = buckets.length;
    const stepX = n > 1 ? innerW / (n - 1) : 0;
    const toPoints = (pick: (b: (typeof buckets)[0]) => number) =>
      buckets
        .map((b, i) => {
          const x = padL + (n === 1 ? innerW / 2 : i * stepX);
          const y = padT + innerH * (1 - pick(b) / maxVal);
          return `${x.toFixed(1)},${y.toFixed(1)}`;
        })
        .join(" ");
    return {
      paths: {
        records: toPoints((b) => b.records_per_sec),
        loops: toPoints((b) => b.loop_events_per_sec),
        nodes: toPoints((b) => b.unique_nodes_per_sec),
      },
      maxY: maxVal,
      w: width,
      h: height,
    };
  }, [timeseries]);

  if (loading && !timeseries) {
    return <div className="explorer-chart-loading">Loading timeseries…</div>;
  }
  if (error) {
    return <div className="explorer-chart-error">{error}</div>;
  }
  if (!timeseries || !paths || timeseries.buckets.length === 0) {
    return (
      <EmptyState
        compact
        copy="No data in this window. Try a wider time range or clear facets."
      />
    );
  }

  const first = timeseries.buckets[0];
  const last = timeseries.buckets[timeseries.buckets.length - 1];
  const t0 = new Date(first.start_ms).toLocaleString();
  const t1 = new Date(last.end_ms).toLocaleString();

  return (
    <div className="explorer-timeseries">
      <div className="explorer-ts-legend">
        <span className="explorer-ts-series records">
          <span className="explorer-ts-dot" /> semantic records/s
        </span>
        <span className="explorer-ts-series loops">
          <span className="explorer-ts-dot" /> loop iterations/s
        </span>
        <span className="explorer-ts-series nodes">
          <span className="explorer-ts-dot" /> distinct nodes/s
        </span>
        <span className="explorer-ts-meta">
          rollup {Math.round(timeseries.rollup_ms / 1000)}s · scanned{" "}
          {timeseries.runs_scanned} run bundle(s)
        </span>
      </div>
      <svg
        className="explorer-ts-svg"
        viewBox={`0 0 ${w} ${h}`}
        preserveAspectRatio="xMidYMid meet"
      >
        <text className="explorer-ts-y-label" x="8" y="28">
          {maxY.toFixed(3)}/s
        </text>
        <polyline
          className="explorer-ts-line records"
          fill="none"
          points={paths.records}
        />
        <polyline
          className="explorer-ts-line loops"
          fill="none"
          points={paths.loops}
        />
        <polyline
          className="explorer-ts-line nodes"
          fill="none"
          points={paths.nodes}
        />
      </svg>
      <div className="explorer-ts-xaxis">
        <span>{t0}</span>
        <span>{t1}</span>
      </div>
    </div>
  );
}

function RecordConsole({
  fallbackRecords,
  records,
}: {
  fallbackRecords: ExecutionRecord[];
  records: ExecutionRecord[];
}) {
  const source = records.length > 0 ? records : fallbackRecords;
  return source.length > 0 ? (
    <div className="record-console">
      {source.slice(-400).map((record) => (
        <div key={record.record_id} className="record-console-line">
          {formatRecordLine(record)}
        </div>
      ))}
    </div>
  ) : (
    <EmptyState copy="No records captured for the current scope yet." compact />
  );
}

function TimelineLens({ spans }: { spans: GenericSpan[] }) {
  const maxMs = useMemo(
    () => Math.max(...spans.map((s) => s.latencyMs), 0.000_001),
    [spans],
  );
  return spans.length > 0 ? (
    <div className="timeline-list">
      {spans.map((span, index) => (
        <div key={`${span.label}:${index}`} className="timeline-row">
          <div className="timeline-label">{span.title}</div>
          <div className="timeline-track">
            <div
              className={`timeline-fill ${index === 2 ? "warning" : index === 3 ? "error" : ""}`}
              style={{
                width: `${Math.min(
                  92,
                  Math.max((span.latencyMs / maxMs) * 100, 10),
                )}%`,
              }}
            />
          </div>
        </div>
      ))}
    </div>
  ) : (
    <EmptyState copy="No timeline data for the current span selection." compact />
  );
}

function TraceTable({ spans }: { spans: GenericSpan[] }) {
  return spans.length > 0 ? (
    <div className="trace-table">
      <div className="trace-table-head">
        <span>Run / trace</span>
        <span>Latency</span>
        <span>Status</span>
        <span>Action</span>
      </div>
      {spans.map((span, index) => (
        <div key={`${span.label}:${index}`} className="trace-table-row">
          <span className="mono trace-table-id">{span.traceIdDisplay}</span>
          <span className="mono">{span.latencyLabel}</span>
          <span>{span.statusLabel}</span>
          <span className="trace-table-action">Open</span>
        </div>
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

function buildMetricGroups(
  catalog: CatalogEntry | null,
  metrics: NumericMetric[],
): MetricGroupView[] {
  if (!catalog) {
    return [];
  }
  return catalog.metric_groups
    .map((group) => ({
      group,
      metrics: dedupeMetrics(
        metrics.filter((metric) =>
          group.metric_path_prefixes.some(
            (prefix) =>
              metric.path.startsWith(prefix) || metric.label.startsWith(prefix),
          ),
        ),
      )
        .sort((left, right) => right.value - left.value)
        .slice(0, group.max_items),
    }))
    .filter((groupView) => groupView.metrics.length > 0);
}

function formatMetricValue(value: number) {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value.toFixed(2);
}

function dedupeMetrics(metrics: NumericMetric[]) {
  const seen = new Set<string>();
  return metrics.filter((metric) => {
    const key = [
      metric.node_id,
      metric.path,
      metric.label,
      metric.frame_id ?? "root",
      metric.loop_node_id ?? "none",
      metric.iteration_index ?? "none",
    ].join("::");
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
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
    case "graph":
      return [...root, "Execution Graph"];
    case "node":
      return [...root, "Node Detail", selectedNodeId ?? "selection"];
    case "spans":
      return [...root, "Spans & Records"];
    case "launch":
      return ["Home", "Catalog", "Launch & Compare"];
    default:
      return root;
  }
}

function searchScopePlaceholder(view: ViewId) {
  switch (view) {
    case "overview":
      return "Use Explorer ($window, @run_id, @node_id) above — scopes chart, run bundle, and tables";
    case "graph":
      return "Explorer @run_id loads the graph; @node_id syncs selection";
    case "node":
      return "Node + frame follow Explorer @node_id and drill-down";
    case "spans":
      return "Records filtered by Explorer run, node facet, and $window on timestamps";
    case "launch":
      return "Launch is independent of Explorer until you open a run";
    default:
      return "scope: current view";
  }
}

function viewFromHash(hash: string): ViewId | null {
  const normalized = hash.replace(/^#/, "");
  return VIEWS.some((view) => view.id === normalized)
    ? (normalized as ViewId)
    : null;
}

export default App;
