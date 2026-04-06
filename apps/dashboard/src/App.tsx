import { useEffect, useMemo, useState } from "react";

import { GraphPanel } from "./components/GraphPanel";
import { RecordsPanel } from "./components/RecordsPanel";
import {
  fetchCatalog,
  fetchCatalogGraph,
  fetchExecution,
  fetchNodeDetail,
  fetchRunOverview,
  fetchRunRecords,
  fetchRunReplay,
  fetchRuns,
  launchExecution,
} from "./lib/api";
import type {
  CatalogEntry,
  ExecutionRecord,
  ExecutionSession,
  NodeDetail,
  ReplayReport,
  RunOverview,
  RunSummary,
} from "./types";

function App() {
  const [catalog, setCatalog] = useState<CatalogEntry[]>([]);
  const [selectedSpecId, setSelectedSpecId] = useState<string | null>(null);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [activeRun, setActiveRun] = useState<RunOverview | null>(null);
  const [activeReplay, setActiveReplay] = useState<ReplayReport | null>(null);
  const [activeRecords, setActiveRecords] = useState<ExecutionRecord[]>([]);
  const [activeExecution, setActiveExecution] = useState<ExecutionSession | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedFrameId, setSelectedFrameId] = useState<string | null>(null);
  const [nodeDetail, setNodeDetail] = useState<NodeDetail | null>(null);
  const [graphPreview, setGraphPreview] = useState<RunOverview["graph"] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const selectedCatalog = useMemo(
    () => catalog.find((entry) => entry.spec_id === selectedSpecId) ?? null,
    [catalog, selectedSpecId],
  );

  useEffect(() => {
    void (async () => {
      try {
        const entries = await fetchCatalog();
        setCatalog(entries);
        if (entries.length > 0) {
          setSelectedSpecId(entries[0].spec_id);
        }
      } catch (fetchError) {
        setError(String(fetchError));
      }
    })();
  }, []);

  useEffect(() => {
    if (!selectedCatalog) {
      return;
    }
    void (async () => {
      try {
        const [graphData, runData] = await Promise.all([
          fetchCatalogGraph(selectedCatalog.spec_id),
          fetchRuns(selectedCatalog.graph_id, selectedCatalog.invocation_name),
        ]);
        setGraphPreview(graphData.graph);
        setRuns(runData);
      } catch (fetchError) {
        setError(String(fetchError));
      }
    })();
  }, [selectedCatalog]);

  useEffect(() => {
    if (!selectedCatalog || runs.length === 0) {
      setActiveRun(null);
      setActiveReplay(null);
      setActiveRecords([]);
      return;
    }
    const latest = runs[0];
    void loadRun(selectedCatalog.graph_id, latest.run_id);
  }, [runs, selectedCatalog]);

  useEffect(() => {
    if (!activeExecution || activeExecution.status === "succeeded" || activeExecution.status === "failed") {
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
          await loadRun(selectedCatalog.graph_id, next.run_id);
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

  async function loadRun(graphId: string, runId: string) {
    const [overview, replay, records] = await Promise.all([
      fetchRunOverview(graphId, runId),
      fetchRunReplay(graphId, runId, "ticket_review_loop"),
      fetchRunRecords(graphId, runId),
    ]);
    setActiveRun(overview);
    setActiveReplay(replay);
    setActiveRecords(records);
    if (!selectedNodeId && overview.graph.nodes.length > 0) {
      setSelectedNodeId(overview.graph.nodes[0].node_id);
    }
  }

  async function handleRun(specId: string) {
    try {
      setError(null);
      setActiveExecution(await launchExecution(specId));
    } catch (launchError) {
      setError(String(launchError));
    }
  }

  const queueMetrics = (activeRun?.metrics ?? []).filter((metric) =>
    metric.label.startsWith("queue_summary."),
  );
  const liveRecords = activeExecution?.records ?? [];

  return (
    <div className="dashboard-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-kicker">mentalmodel</div>
          <h1>Run Dashboard</h1>
          <p>Graph, run, invariant, and records views for local and hosted workflows.</p>
        </div>
        <section className="sidebar-section">
          <div className="sidebar-title">Launchable Specs</div>
          {catalog.map((entry) => (
            <button
              key={entry.spec_id}
              className={`spec-card ${selectedSpecId === entry.spec_id ? "active" : ""}`}
              onClick={() => setSelectedSpecId(entry.spec_id)}
            >
              <div className="spec-label">{entry.label}</div>
              <div className="spec-description">{entry.description}</div>
              <div className="spec-tags">
                <span>{entry.graph_id}</span>
                <span>{entry.invocation_name}</span>
              </div>
            </button>
          ))}
        </section>
        <section className="sidebar-section">
          <div className="sidebar-title">Recent Runs</div>
          {runs.map((run) => (
            <button
              key={run.run_id}
              className={`run-row ${activeRun?.summary.run_id === run.run_id ? "active" : ""}`}
              onClick={() => {
                if (selectedCatalog) {
                  void loadRun(selectedCatalog.graph_id, run.run_id);
                }
              }}
            >
              <div>{run.run_id.slice(0, 12)}</div>
              <div className={`status-pill ${run.success ? "ok" : "error"}`}>
                {run.success ? "pass" : "fail"}
              </div>
            </button>
          ))}
        </section>
      </aside>
      <main className="workspace">
        <header className="workspace-header">
          <div>
            <div className="eyebrow">Phase 26 Proof UI</div>
            <h2>{selectedCatalog?.label ?? "Select a spec"}</h2>
            <p>
              Run either ticket review environment, inspect graph state, drill into node
              outputs, and follow the full semantic record stream.
            </p>
          </div>
          <div className="header-actions">
            {selectedCatalog ? (
              <button className="primary-button" onClick={() => void handleRun(selectedCatalog.spec_id)}>
                Run {selectedCatalog.label}
              </button>
            ) : null}
          </div>
        </header>

        {error ? <div className="error-banner">{error}</div> : null}

        <section className="hero-grid">
          <div className="panel">
            <div className="panel-header">
              <div>
                <div className="panel-title">Latest Run</div>
                <div className="panel-subtitle">
                  Invocation, runtime profiles, and verification state.
                </div>
              </div>
            </div>
            {activeRun ? (
              <div className="summary-grid">
                <SummaryCard label="Invocation" value={activeRun.summary.invocation_name ?? "n/a"} />
                <SummaryCard label="Run ID" value={activeRun.summary.run_id.slice(0, 16)} />
                <SummaryCard label="Profiles" value={activeRun.summary.runtime_profile_names.join(", ")} />
                <SummaryCard label="Records" value={String(activeRun.summary.record_count)} />
              </div>
            ) : (
              <div className="empty-state">No completed run loaded yet.</div>
            )}
          </div>
          <div className="panel">
            <div className="panel-header">
              <div>
                <div className="panel-title">Resolution Metrics</div>
                <div className="panel-subtitle">
                  Numeric highlights derived from persisted run outputs.
                </div>
              </div>
            </div>
            <div className="summary-grid">
              {queueMetrics.length > 0 ? (
                queueMetrics.map((metric) => (
                  <SummaryCard
                    key={metric.label}
                    label={metric.label.replace("queue_summary.", "")}
                    value={String(metric.value)}
                  />
                ))
              ) : (
                <div className="empty-state">Run the review workflow to populate queue metrics.</div>
              )}
            </div>
          </div>
          <div className="panel">
            <div className="panel-header">
              <div>
                <div className="panel-title">Invariant Results</div>
                <div className="panel-subtitle">
                  Root and loop invariant outcomes from replay summaries.
                </div>
              </div>
            </div>
            <div className="invariant-list">
              {(activeRun?.invariants ?? []).map((item) => (
                <button
                  key={`${item.node_id}:${item.frame_id ?? "root"}`}
                  className={`invariant-row ${item.passed ? "ok" : "warn"}`}
                  onClick={() => {
                    setSelectedNodeId(item.node_id);
                    setSelectedFrameId(item.frame_id);
                  }}
                >
                  <div>{item.node_id}</div>
                  <div>
                    {item.status} {item.frame_id ? `@ ${item.frame_id}` : ""}
                  </div>
                </button>
              ))}
            </div>
          </div>
        </section>

        <section className="content-grid">
          <div className="content-main">
            {activeRun?.graph || graphPreview ? (
              <GraphPanel
                graph={activeRun?.graph ?? graphPreview!}
                nodeSummaries={activeReplay?.node_summaries ?? []}
                selectedNodeId={selectedNodeId}
                onSelectNode={(nodeId) => {
                  setSelectedNodeId(nodeId);
                  setSelectedFrameId(null);
                }}
              />
            ) : (
              <div className="panel empty-state">Loading graph preview…</div>
            )}
            <RecordsPanel
              title="Run Records"
              records={activeRecords}
            />
          </div>
          <div className="content-side">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <div className="panel-title">Live Run Stream</div>
                  <div className="panel-subtitle">
                    Polling live execution records from the running verification session.
                  </div>
                </div>
              </div>
              {activeExecution ? (
                <>
                  <div className="live-status">
                    <span className={`status-pill ${activeExecution.status === "succeeded" ? "ok" : activeExecution.status === "failed" ? "error" : "running"}`}>
                      {activeExecution.status}
                    </span>
                    <span>{activeExecution.spec.label}</span>
                  </div>
                  <div className="live-records">
                    {liveRecords.slice(-12).map((record) => (
                      <div key={record.record_id} className="live-record">
                        <div>
                          <strong>{record.node_id}</strong> {record.event_type}
                        </div>
                        <div className="muted">{record.frame_id}</div>
                      </div>
                    ))}
                  </div>
                </>
              ) : (
                <div className="empty-state">Launch a run to watch the live event stream.</div>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <div className="panel-title">Node Inspector</div>
                  <div className="panel-subtitle">
                    Inputs, outputs, and trace for the selected node.
                  </div>
                </div>
              </div>
              {nodeDetail ? (
                <div className="node-detail">
                  <div className="node-header">
                    <strong>{nodeDetail.node_id}</strong>
                    {nodeDetail.available_frames.length > 0 ? (
                      <select
                        value={selectedFrameId ?? ""}
                        onChange={(event) =>
                          setSelectedFrameId(event.target.value || null)
                        }
                      >
                        <option value="">latest frame</option>
                        {nodeDetail.available_frames.map((frame) => (
                          <option key={frame.frame_id} value={frame.frame_id}>
                            {frame.frame_id}
                          </option>
                        ))}
                      </select>
                    ) : null}
                  </div>
                  <InspectorSection title="Inputs">
                    {nodeDetail.inputs ? (
                      <pre>{JSON.stringify(nodeDetail.inputs, null, 2)}</pre>
                    ) : (
                      <div className="muted">{nodeDetail.inputs_error ?? "No inputs."}</div>
                    )}
                  </InspectorSection>
                  <InspectorSection title="Output">
                    {nodeDetail.output ? (
                      <pre>{JSON.stringify(nodeDetail.output, null, 2)}</pre>
                    ) : (
                      <div className="muted">{nodeDetail.output_error ?? "No output."}</div>
                    )}
                  </InspectorSection>
                  <InspectorSection title="Trace">
                    {nodeDetail.trace ? (
                      <pre>{JSON.stringify(nodeDetail.trace.records.slice(-6), null, 2)}</pre>
                    ) : (
                      <div className="muted">{nodeDetail.trace_error ?? "No trace."}</div>
                    )}
                  </InspectorSection>
                </div>
              ) : (
                <div className="empty-state">Select a node from the graph or invariant list.</div>
              )}
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="summary-card">
      <div className="summary-label">{label}</div>
      <div className="summary-value">{value}</div>
    </div>
  );
}

function InspectorSection({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="inspector-section">
      <div className="inspector-title">{title}</div>
      {children}
    </section>
  );
}

export default App;
