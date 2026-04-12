import { useMemo } from "react";
import {
  Brush,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  buildIterationLatencyRows,
  buildOperationLatencyChartModel,
  buildOperationLatencySummaries,
} from "../lib/traceAnalytics";
import type { GenericSpan } from "../types";

type SpanLatencyInsightsProps = {
  spans: GenericSpan[];
  onInspectNode?: (nodeId: string) => void;
};

const SERIES_COLORS = {
  p50: "#8ED5FF",
  p95: "#FFCF86",
  p99: "#FF8A80",
  max: "#C0C1FF",
};

const OPERATION_COLORS = ["#56E5A9", "#8ED5FF", "#FFCF86", "#C0C1FF"];

export function SpanLatencyInsights({
  spans,
  onInspectNode,
}: SpanLatencyInsightsProps) {
  const iterationRows = useMemo(() => buildIterationLatencyRows(spans), [spans]);
  const operationSummaries = useMemo(
    () => buildOperationLatencySummaries(spans, 8),
    [spans],
  );
  const operationChart = useMemo(
    () => buildOperationLatencyChartModel(spans, 4),
    [spans],
  );

  if (spans.length === 0) {
    return (
      <div className="metric-group-hint">
        No span data in the current scope yet. Select a run or widen the current
        scope to inspect latency percentiles and bottlenecks.
      </div>
    );
  }

  return (
    <div className="latency-insights">
      <div className="latency-insights-grid">
        <div className="latency-chart-card">
          <div className="metric-timeseries-meta">
            <span className="metric-timeseries-eyebrow">Step latency envelope</span>
            <span className="metric-timeseries-copy">
              p50 / p95 / p99 / max latency across all spans in each iteration
            </span>
          </div>
          {iterationRows.length > 0 ? (
            <ResponsiveContainer width="100%" height={260}>
              <LineChart
                data={iterationRows}
                margin={{ top: 8, right: 12, left: 0, bottom: 0 }}
              >
                <CartesianGrid
                  stroke="rgba(142, 213, 255, 0.08)"
                  strokeDasharray="3 3"
                />
                <XAxis
                  dataKey="iteration"
                  tick={{ fill: "#8d9ab9", fontSize: 11 }}
                  tickLine={false}
                  axisLine={{ stroke: "rgba(142, 213, 255, 0.12)" }}
                />
                <YAxis
                  tick={{ fill: "#8d9ab9", fontSize: 11 }}
                  tickLine={false}
                  width={56}
                  axisLine={{ stroke: "rgba(142, 213, 255, 0.12)" }}
                  tickFormatter={(value) => formatMs(value)}
                />
                <Tooltip
                  contentStyle={tooltipStyle}
                  labelFormatter={(value) => `iteration ${String(value)}`}
                  formatter={(value, name) => [formatMs(Number(value)), String(name)]}
                />
                <Legend wrapperStyle={{ fontSize: 12, color: "#aab7d7" }} />
                <Line
                  dataKey="p50Ms"
                  name="p50"
                  stroke={SERIES_COLORS.p50}
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4 }}
                  isAnimationActive={false}
                />
                <Line
                  dataKey="p95Ms"
                  name="p95"
                  stroke={SERIES_COLORS.p95}
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4 }}
                  isAnimationActive={false}
                />
                <Line
                  dataKey="p99Ms"
                  name="p99"
                  stroke={SERIES_COLORS.p99}
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4 }}
                  isAnimationActive={false}
                />
                <Line
                  dataKey="maxMs"
                  name="max"
                  stroke={SERIES_COLORS.max}
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4 }}
                  isAnimationActive={false}
                />
                {iterationRows.length > 24 ? (
                  <Brush
                    dataKey="iteration"
                    height={18}
                    stroke="rgba(142, 213, 255, 0.25)"
                    travellerWidth={10}
                  />
                ) : null}
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="metric-group-hint">
              Iteration latency charts appear automatically when spans include
              iteration metadata.
            </div>
          )}
        </div>

        <div className="latency-chart-card">
          <div className="metric-timeseries-meta">
            <span className="metric-timeseries-eyebrow">Top operations</span>
            <span className="metric-timeseries-copy">
              Highest-latency operations over time
              {operationChart?.hiddenSeriesCount
                ? ` · +${operationChart.hiddenSeriesCount} more`
                : ""}
            </span>
          </div>
          {operationChart ? (
            <ResponsiveContainer width="100%" height={260}>
              <LineChart
                data={operationChart.rows}
                margin={{ top: 8, right: 12, left: 0, bottom: 0 }}
              >
                <CartesianGrid
                  stroke="rgba(142, 213, 255, 0.08)"
                  strokeDasharray="3 3"
                />
                <XAxis
                  dataKey="iteration"
                  tick={{ fill: "#8d9ab9", fontSize: 11 }}
                  tickLine={false}
                  axisLine={{ stroke: "rgba(142, 213, 255, 0.12)" }}
                />
                <YAxis
                  tick={{ fill: "#8d9ab9", fontSize: 11 }}
                  tickLine={false}
                  width={56}
                  axisLine={{ stroke: "rgba(142, 213, 255, 0.12)" }}
                  tickFormatter={(value) => formatMs(value)}
                />
                <Tooltip
                  contentStyle={tooltipStyle}
                  labelFormatter={(value) => `iteration ${String(value)}`}
                  formatter={(value, name) => [formatMs(Number(value)), String(name)]}
                />
                <Legend wrapperStyle={{ fontSize: 12, color: "#aab7d7" }} />
                {operationChart.series.map((series, index) => (
                  <Line
                    key={series.key}
                    dataKey={series.key}
                    name={series.label}
                    stroke={OPERATION_COLORS[index % OPERATION_COLORS.length]}
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4 }}
                    isAnimationActive={false}
                  />
                ))}
                {operationChart.rows.length > 24 ? (
                  <Brush
                    dataKey="iteration"
                    height={18}
                    stroke="rgba(142, 213, 255, 0.25)"
                    travellerWidth={10}
                  />
                ) : null}
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="metric-group-hint">
              Operation timelines appear when the current span scope includes loop
              iterations.
            </div>
          )}
        </div>
      </div>

      <div className="latency-summary-table">
        <div className="latency-summary-head">
          <span>Operation</span>
          <span>P50</span>
          <span>P95</span>
          <span>P99</span>
          <span>Max</span>
          <span>Latest</span>
        </div>
        {operationSummaries.map((summary) => (
          <button
            key={summary.nodeId}
            type="button"
            className="latency-summary-row"
            onClick={() => onInspectNode?.(summary.nodeId)}
          >
            <span className="latency-summary-title">
              <strong>{summary.title}</strong>
              <em>
                {summary.runtimeProfile
                  ? `${summary.runtimeProfile} · ${summary.count} spans`
                  : `${summary.count} spans`}
              </em>
            </span>
            <span>{formatMs(summary.p50Ms)}</span>
            <span>{formatMs(summary.p95Ms)}</span>
            <span>{formatMs(summary.p99Ms)}</span>
            <span>{formatMs(summary.maxMs)}</span>
            <span>
              {formatMs(summary.latestMs)}
              {summary.latestIteration != null ? ` · i${summary.latestIteration}` : ""}
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

function formatMs(value: number): string {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  if (value >= 1000) {
    return `${(value / 1000).toFixed(2)} s`;
  }
  if (value >= 100) {
    return `${value.toFixed(0)} ms`;
  }
  if (value >= 10) {
    return `${value.toFixed(1)} ms`;
  }
  return `${value.toFixed(2)} ms`;
}

const tooltipStyle = {
  borderRadius: 8,
  border: "1px solid rgba(142, 213, 255, 0.2)",
  background: "rgba(11, 19, 38, 0.96)",
  boxShadow: "0 8px 28px rgba(6, 14, 32, 0.38)",
  color: "#dae2fd",
} as const;
