import { useCallback, useMemo, useRef, useState } from "react";

import type { TimeseriesResponse } from "../types";

type ExplorerTimeseriesChartProps = {
  error: string | null;
  loading: boolean;
  /** Background refresh (no skeleton). */
  pollBusy?: boolean;
  timeseries: TimeseriesResponse | null;
};

export function ExplorerTimeseriesChart({
  error,
  loading,
  pollBusy = false,
  timeseries,
}: ExplorerTimeseriesChartProps) {
  const [hover, setHover] = useState<{
    bucketIndex: number;
    px: number;
    py: number;
  } | null>(null);

  const svgRef = useRef<SVGSVGElement | null>(null);

  const chart = useMemo(() => {
    const width = 920;
    const height = 260;
    const padL = 52;
    const padR = 20;
    const padT = 22;
    const padB = 40;
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
        padL,
        padT,
        padB,
        innerW: width - padL - padR,
        innerH: height - padT - padB,
        buckets: [] as TimeseriesResponse["buckets"],
        stepX: 0,
        n: 0,
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
      padL,
      padT,
      padB,
      innerW,
      innerH,
      buckets,
      stepX,
      n,
    };
  }, [timeseries]);

  const {
    paths,
    maxY,
    w,
    h,
    padL,
    padT,
    padB,
    innerW,
    buckets,
    stepX,
    n,
  } = chart;

  const onSvgMove = useCallback(
    (event: React.MouseEvent<SVGSVGElement>) => {
      if (!timeseries || n === 0 || !svgRef.current) {
        return;
      }
      const rect = svgRef.current.getBoundingClientRect();
      const vx =
        ((event.clientX - rect.left) / Math.max(rect.width, 1)) * w;
      const rel = vx - padL;
      let i = 0;
      if (n === 1) {
        i = 0;
      } else {
        i = Math.round(rel / Math.max(stepX, 1e-9));
      }
      i = Math.max(0, Math.min(n - 1, i));
      setHover({
        bucketIndex: i,
        px: event.clientX - rect.left,
        py: event.clientY - rect.top,
      });
    },
    [n, padL, stepX, timeseries, w],
  );

  const onSvgLeave = useCallback(() => setHover(null), []);

  if (loading && !timeseries) {
    return (
      <div className="explorer-chart-loading explorer-chart-skeleton">
        <span className="explorer-skeleton-line" />
        <span className="explorer-skeleton-chart" />
      </div>
    );
  }
  if (error) {
    return <div className="explorer-chart-error">{error}</div>;
  }
  if (!timeseries || !paths || timeseries.buckets.length === 0) {
    return (
      <div className="empty-state compact">
        No data in this window. Try a wider time range or clear facets.
      </div>
    );
  }

  const first = timeseries.buckets[0];
  const last = timeseries.buckets[timeseries.buckets.length - 1];
  const t0 = new Date(first.start_ms).toLocaleString();
  const t1 = new Date(last.end_ms).toLocaleString();

  const hb = hover ? buckets[hover.bucketIndex] : null;
  const hx =
    hb && hover && n > 0
      ? padL + (n === 1 ? innerW / 2 : hover.bucketIndex * stepX)
      : 0;

  return (
    <div className="explorer-timeseries explorer-timeseries-filled">
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
          {loading || pollBusy ? " · refreshing…" : ""}
        </span>
      </div>
      <div className="explorer-ts-chart-wrap">
        <svg
          ref={svgRef}
          className="explorer-ts-svg"
          viewBox={`0 0 ${w} ${h}`}
          preserveAspectRatio="xMidYMin meet"
          onMouseLeave={onSvgLeave}
          onMouseMove={onSvgMove}
        >
          <text className="explorer-ts-y-label" x="8" y="28">
            {maxY.toFixed(3)}/s
          </text>
          {hover && hb ? (
            <line
              className="explorer-ts-crosshair"
              x1={hx}
              y1={padT}
              x2={hx}
              y2={h - padB}
            />
          ) : null}
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
        {hover && hb ? (
          <div
            className="explorer-ts-tooltip"
            style={{
              left: Math.min(
                Math.max(hover.px + 12, 8),
                (svgRef.current?.getBoundingClientRect().width ?? 400) - 200,
              ),
              top: Math.max(hover.py - 8, 8),
            }}
          >
            <div className="explorer-ts-tooltip-title">
              {new Date(hb.start_ms).toLocaleTimeString()} —{" "}
              {new Date(hb.end_ms).toLocaleTimeString()}
            </div>
            <div className="explorer-ts-tooltip-row">
              <span className="records">records/s</span>{" "}
              <strong>{hb.records_per_sec.toFixed(4)}</strong>
            </div>
            <div className="explorer-ts-tooltip-row">
              <span className="loops">loops/s</span>{" "}
              <strong>{hb.loop_events_per_sec.toFixed(4)}</strong>
            </div>
            <div className="explorer-ts-tooltip-row">
              <span className="nodes">nodes/s</span>{" "}
              <strong>{hb.unique_nodes_per_sec.toFixed(4)}</strong>
            </div>
          </div>
        ) : null}
      </div>
      <div className="explorer-ts-xaxis">
        <span>{t0}</span>
        <span>{t1}</span>
      </div>
    </div>
  );
}
