import { useMemo } from "react";
import {
  Area,
  Bar,
  CartesianGrid,
  ComposedChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { MetricGroupQueryResult, MetricSeries } from "../types";

type MetricGroupChartProps = {
  group: MetricGroupQueryResult;
};

type ChartRow = {
  iteration: number;
  [seriesKey: string]: number;
};

const SERIES_COLORS = ["#6EE7B7", "#7DD3FC", "#FBBF24", "#C4B5FD", "#FCA5A5"];

export function MetricGroupTimeseriesChart({ group }: MetricGroupChartProps) {
  const model = useMemo(() => buildChartModel(group), [group]);
  if (model == null) {
    return null;
  }
  return (
    <div className="metric-timeseries">
      <div className="metric-timeseries-meta">
        <span className="metric-timeseries-eyebrow">{model.eyebrow}</span>
        <span className="metric-timeseries-copy">{model.copy}</span>
      </div>
      <div className="metric-timeseries-chart-wrap">
        <ResponsiveContainer width="100%" height={220}>
          <ComposedChart data={model.rows} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
            <CartesianGrid stroke="rgba(142, 213, 255, 0.08)" strokeDasharray="3 3" />
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
              tickFormatter={(value) => formatMetricValue(Number(value), model.unit)}
            />
            <Tooltip
              contentStyle={{
                borderRadius: 8,
                border: "1px solid rgba(142, 213, 255, 0.2)",
                background: "rgba(11, 19, 38, 0.96)",
                boxShadow: "0 8px 28px rgba(6, 14, 32, 0.38)",
                color: "#dae2fd",
              }}
              labelFormatter={(value) => `iteration ${String(value)}`}
              formatter={(value, name) => [
                formatMetricValue(Number(value), model.unit),
                String(name),
              ]}
            />
            {model.series.map((series, index) =>
              series.render_hint === "bar" ? (
                <Bar
                  key={series.series_id}
                  dataKey={series.series_id}
                  name={series.label}
                  fill={SERIES_COLORS[index % SERIES_COLORS.length]}
                  radius={[4, 4, 0, 0]}
                  isAnimationActive={false}
                />
              ) : series.render_hint === "area" ? (
                <Area
                  key={series.series_id}
                  type="monotone"
                  dataKey={series.series_id}
                  name={series.label}
                  stroke={SERIES_COLORS[index % SERIES_COLORS.length]}
                  fill={SERIES_COLORS[index % SERIES_COLORS.length]}
                  fillOpacity={0.14}
                  strokeWidth={2}
                  isAnimationActive={false}
                />
              ) : (
                <Area
                  key={series.series_id}
                  type="monotone"
                  dataKey={series.series_id}
                  name={series.label}
                  stroke={SERIES_COLORS[index % SERIES_COLORS.length]}
                  fillOpacity={0}
                  strokeWidth={2}
                  isAnimationActive={false}
                />
              ),
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      <div className="metric-timeseries-footer">
        {model.series.map((series, index) => (
          <div key={series.series_id} className="metric-timeseries-footer-item">
            <span
              className="metric-timeseries-footer-swatch"
              style={{ background: SERIES_COLORS[index % SERIES_COLORS.length] }}
            />
            <span className="metric-timeseries-footer-label">{series.label}</span>
            <strong className="metric-timeseries-footer-value">
              {formatMetricValue(series.summary.latest, model.unit)}
            </strong>
          </div>
        ))}
      </div>
    </div>
  );
}

function buildChartModel(group: MetricGroupQueryResult) {
  const chartSeries = group.series.filter(
    (series) =>
      series.render_hint !== "stat" &&
      series.points.some((point) => point.iteration_index != null),
  );
  if (chartSeries.length === 0) {
    return null;
  }
  const limitedSeries = chartSeries.slice(0, 4);
  const unit = limitedSeries[0]?.unit ?? "generic";
  const rowsByIteration = new Map<number, ChartRow>();
  for (const series of limitedSeries) {
    for (const point of series.points) {
      if (point.iteration_index == null) {
        continue;
      }
      const existing = rowsByIteration.get(point.iteration_index) ?? {
        iteration: point.iteration_index,
      };
      existing[series.series_id] = point.value;
      rowsByIteration.set(point.iteration_index, existing);
    }
  }
  const rows = [...rowsByIteration.values()].sort((left, right) => left.iteration - right.iteration);
  const hasCounter = limitedSeries.some((series) => series.semantic_kind === "counter");
  return {
    rows,
    unit,
    series: limitedSeries,
    eyebrow: hasCounter ? "Counter and rate series" : "Timeseries",
    copy: hasCounter
      ? "Area/bar treatments preserve cumulative or sparse behavior instead of flattening everything into text cards."
      : "Lines show change over visible steps. Hover for exact values and use the summary cards below for current state.",
  };
}

function formatMetricValue(
  value: number,
  unit: MetricSeries["unit"],
): string {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  const compact = new Intl.NumberFormat("en-US", {
    notation: Math.abs(value) >= 1000 ? "compact" : "standard",
    maximumFractionDigits: Math.abs(value) >= 100 ? 1 : 2,
  }).format(value);
  if (unit === "ms") {
    return `${compact} ms`;
  }
  if (unit === "s") {
    return `${compact} s`;
  }
  if (unit === "pct") {
    return `${compact}%`;
  }
  return compact;
}
