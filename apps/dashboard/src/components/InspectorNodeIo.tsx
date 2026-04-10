import { useEffect, useMemo, useState } from "react";

import { fetchNodeDetail } from "../lib/api";
import type { NodeDetail } from "../types";

/** Map correlation / record ``frame_id`` to the node-detail API (omit query for root). */
export function frameIdForNodeDetailApi(
  frameId: string | null | undefined,
): string | null {
  if (frameId == null || frameId === "" || frameId === "root") {
    return null;
  }
  return frameId;
}

function normalizeFrame(frameId: string | null | undefined): string | null {
  return frameIdForNodeDetailApi(frameId);
}

function detailMatchesScope(
  detail: NodeDetail,
  nodeId: string,
  frameForApi: string | null,
): boolean {
  return (
    detail.node_id === nodeId &&
    normalizeFrame(detail.frame_id) === frameForApi
  );
}

type InspectorNodeIoProps = {
  enabled: boolean;
  graphId: string | null;
  runId: string | null;
  nodeId: string | null;
  frameId: string | null;
  runFailureMessage: string | null;
  /** Explorer’s loaded node detail when scope matches this inspector (avoids a duplicate GET). */
  prefetchedDetail: NodeDetail | null;
};

export function InspectorNodeIo({
  enabled,
  graphId,
  runId,
  nodeId,
  frameId,
  runFailureMessage,
  prefetchedDetail,
}: InspectorNodeIoProps) {
  const frameForApi = useMemo(
    () => frameIdForNodeDetailApi(frameId),
    [frameId],
  );

  const [detail, setDetail] = useState<NodeDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled) {
      return;
    }
    if (!graphId || !runId || !nodeId) {
      setDetail(null);
      setError(null);
      setLoading(false);
      return;
    }

    if (
      prefetchedDetail != null &&
      detailMatchesScope(prefetchedDetail, nodeId, frameForApi)
    ) {
      setDetail(prefetchedDetail);
      setError(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);
    void (async () => {
      try {
        const d = await fetchNodeDetail(graphId, runId, nodeId, frameForApi);
        if (!cancelled) {
          setDetail(d);
        }
      } catch (e) {
        if (!cancelled) {
          setError(String(e));
          setDetail(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [
    enabled,
    graphId,
    runId,
    nodeId,
    frameForApi,
    prefetchedDetail,
  ]);

  if (!enabled) {
    return null;
  }

  if (!graphId || !runId || !nodeId) {
    return (
      <section className="exec-detail-section">
        <h3 className="exec-detail-section-title">Inputs / output</h3>
        <p className="spans-explainer">
          Pick a run in the explorer scope so inputs and outputs can load from the bundle.
        </p>
      </section>
    );
  }

  if (loading) {
    return (
      <section className="exec-detail-section">
        <h3 className="exec-detail-section-title">Inputs / output</h3>
        <p className="mono exec-detail-io-muted">Loading…</p>
      </section>
    );
  }

  if (error) {
    return (
      <section className="exec-detail-section">
        <h3 className="exec-detail-section-title">Inputs / output</h3>
        <p className="exec-detail-io-error">{error}</p>
      </section>
    );
  }

  if (!detail) {
    return null;
  }

  const frameLabel = frameId && frameId !== "root" ? frameId : "root";

  return (
    <section className="exec-detail-section">
      <h3 className="exec-detail-section-title">Inputs / output</h3>
      <p className="exec-detail-io-scope mono">
        {runId} · {nodeId} · {frameLabel}
      </p>
      <div className="exec-detail-io-blocks">
        <div className="exec-detail-io-block">
          <h4 className="exec-detail-related-expanded-title">Inputs</h4>
          {detail.inputs_error ? (
            <pre className="exec-detail-pre exec-detail-pre-nested">
              {detail.inputs_error}
            </pre>
          ) : detail.inputs !== undefined ? (
            <pre className="exec-detail-pre exec-detail-pre-nested">
              {JSON.stringify(detail.inputs, null, 2)}
            </pre>
          ) : (
            <p className="exec-detail-io-muted">Unavailable</p>
          )}
        </div>
        <div className="exec-detail-io-block">
          <h4 className="exec-detail-related-expanded-title">Output</h4>
          {detail.output_error ? (
            <pre className="exec-detail-pre exec-detail-pre-nested">
              {runFailureMessage
                ? `Run failed before outputs were fully persisted.\nRuntime error: ${runFailureMessage}\n\n${detail.output_error}`
                : detail.output_error}
            </pre>
          ) : detail.output !== undefined ? (
            <pre className="exec-detail-pre exec-detail-pre-nested">
              {JSON.stringify(detail.output, null, 2)}
            </pre>
          ) : (
            <p className="exec-detail-io-muted">Unavailable</p>
          )}
        </div>
      </div>
    </section>
  );
}
