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

import type { MetricGroup, NumericMetric } from "../types";

type MetricSeriesChartProps = {
  group: MetricGroup;
  metrics: NumericMetric[];
};

type SeriesPoint = {
  iteration: number;
  value: number;
  frameId: string | null;
};

type SeriesDefinition = {
  key: string;
  label: string;
  points: SeriesPoint[];
  latestValue: number;
  latestIteration: number;
  deltaValue: number | null;
  minValue: number;
  maxValue: number;
};

type ChartRow = {
  iteration: number;
  [seriesKey: string]: number;
};

const SERIES_COLORS = [
  "#8ED5FF",
  "#56E5A9",
  "#FFCF86",
  "#C0C1FF",
  "#FFB4AB",
  "#7DD3FC",
];

export function MetricGroupTimeseriesChart({
  group,
  metrics,
}: MetricSeriesChartProps) {
  const model = useMemo(() => buildChartModel(group, metrics), [group, metrics]);

  if (model == null) {
    return null;
  }

  return (
    <div className="metric-timeseries">
      <div className="metric-timeseries-meta">
        <span className="metric-timeseries-eyebrow">Iteration series</span>
        <span className="metric-timeseries-copy">
          x-axis: loop iteration · y-axis: numeric metric value
          {model.hiddenSeriesCount > 0 ? ` · +${model.hiddenSeriesCount} more series` : ""}
        </span>
      </div>
      <div className="metric-timeseries-chart-wrap">
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={model.rows} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
            <CartesianGrid stroke="rgba(142, 213, 255, 0.08)" strokeDasharray="3 3" />
            <XAxis
              dataKey="iteration"
              tick={{ fill: "#8d9ab9", fontSize: 11 }}
              tickLine={false}
              axisLine={{ stroke: "rgba(142, 213, 255, 0.12)" }}
              label={{
                value: "iteration",
                position: "insideBottomRight",
                offset: -4,
                fill: "#8d9ab9",
                fontSize: 11,
              }}
            />
            <YAxis
              tick={{ fill: "#8d9ab9", fontSize: 11 }}
              tickLine={false}
              width={52}
              axisLine={{ stroke: "rgba(142, 213, 255, 0.12)" }}
              tickFormatter={(value) => formatMetricValue(value, model.unit)}
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
            <Legend
              wrapperStyle={{ fontSize: 12, color: "#aab7d7" }}
              formatter={(value) => String(value)}
            />
            {model.series.map((series, index) => (
              <Line
                key={series.key}
                type="monotone"
                dataKey={series.key}
                name={series.label}
                stroke={SERIES_COLORS[index % SERIES_COLORS.length]}
                strokeWidth={2}
                dot={{ r: 2 }}
                activeDot={{ r: 4 }}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
            {model.rows.length > 24 ? (
              <Brush
                dataKey="iteration"
                height={18}
                stroke="rgba(142, 213, 255, 0.25)"
                travellerWidth={10}
              />
            ) : null}
          </LineChart>
        </ResponsiveContainer>
      </div>
      <div className="metric-timeseries-footer">
        {model.series.map((series, index) => (
          <div key={series.key} className="metric-timeseries-footer-item">
            <span
              className="metric-timeseries-footer-swatch"
              style={{ background: SERIES_COLORS[index % SERIES_COLORS.length] }}
            />
            <span className="metric-timeseries-footer-label">{series.label}</span>
            <strong className="metric-timeseries-footer-value">
              {formatMetricValue(series.latestValue, model.unit)}
            </strong>
            {series.deltaValue != null ? (
              <span className="metric-timeseries-footer-value">
                {formatSignedDelta(series.deltaValue, model.unit)}
              </span>
            ) : null}
          </div>
        ))}
      </div>
    </div>
  );
}

function buildChartModel(group: MetricGroup, metrics: NumericMetric[]) {
  const iterationMetrics = metrics.filter(
    (metric) => metric.iteration_index != null && Number.isFinite(metric.value),
  );
  if (iterationMetrics.length === 0) {
    return null;
  }

  const seriesByKey = new Map<string, SeriesDefinition>();
  for (const metric of iterationMetrics) {
    const iteration = metric.iteration_index;
    if (iteration == null) {
      continue;
    }
    const key = normalizeMetricLabel(metric);
    const label = displayLabelForMetric(group, metric, key);
    const existing = seriesByKey.get(key);
    if (existing == null) {
      seriesByKey.set(key, {
        key,
        label,
        points: [
          {
            iteration,
            value: metric.value,
            frameId: metric.frame_id,
          },
        ],
        latestValue: metric.value,
        latestIteration: iteration,
        deltaValue: null,
        minValue: metric.value,
        maxValue: metric.value,
      });
      continue;
    }
    const duplicateIndex = existing.points.findIndex((point) => point.iteration === iteration);
    if (duplicateIndex >= 0) {
      existing.points[duplicateIndex] = {
        iteration,
        value: metric.value,
        frameId: metric.frame_id,
      };
    } else {
      existing.points.push({
        iteration,
        value: metric.value,
        frameId: metric.frame_id,
      });
    }
    if (iteration >= existing.latestIteration) {
      existing.deltaValue = metric.value - existing.latestValue;
      existing.latestIteration = iteration;
      existing.latestValue = metric.value;
    }
    existing.minValue = Math.min(existing.minValue, metric.value);
    existing.maxValue = Math.max(existing.maxValue, metric.value);
  }

  const allSeries = [...seriesByKey.values()]
    .map((series) => ({
      ...series,
      points: [...series.points].sort((left, right) => left.iteration - right.iteration),
    }))
    .sort((left, right) => {
      if (right.latestIteration !== left.latestIteration) {
        return right.latestIteration - left.latestIteration;
      }
      if (Math.abs(right.latestValue) !== Math.abs(left.latestValue)) {
        return Math.abs(right.latestValue) - Math.abs(left.latestValue);
      }
      return left.label.localeCompare(right.label);
    });

  const maxSeries = Math.min(group.max_items, 6);
  const series = allSeries.slice(0, maxSeries);
  if (series.length === 0) {
    return null;
  }

  const iterationSet = new Set<number>();
  for (const entry of series) {
    for (const point of entry.points) {
      iterationSet.add(point.iteration);
    }
  }
  const iterations = [...iterationSet].sort((left, right) => left - right);
  const rows: ChartRow[] = iterations.map((iteration) => {
    const row: ChartRow = { iteration };
    for (const entry of series) {
      const point = entry.points.find((candidate) => candidate.iteration === iteration);
      if (point != null) {
        row[entry.key] = point.value;
      }
    }
    return row;
  });

  return {
    rows,
    series,
    hiddenSeriesCount: Math.max(0, allSeries.length - series.length),
    unit: inferMetricUnit(metrics),
  };
}

function normalizeMetricLabel(metric: NumericMetric): string {
  if (metric.frame_id && metric.frame_id !== "root") {
    return metric.label.replace(`${metric.frame_id}.`, "");
  }
  return metric.label;
}

function displayLabelForMetric(
  group: MetricGroup,
  metric: NumericMetric,
  normalizedLabel: string,
): string {
  const candidates = [
    normalizedLabel,
    `${metric.node_id}.${metric.path}`,
    metric.path,
  ];
  for (const candidate of candidates) {
    for (const prefix of [...group.metric_path_prefixes].sort(
      (left, right) => right.length - left.length,
    )) {
      if (candidate.startsWith(prefix)) {
        const trimmed = candidate.slice(prefix.length).replace(/^\./, "");
        if (trimmed !== "") {
          return trimmed;
        }
      }
    }
  }
  return metric.path || normalizedLabel;
}

function inferMetricUnit(metrics: NumericMetric[]): "count" | "ms" | "pct" | "generic" {
  const combined = metrics
    .map((metric) => `${metric.path} ${metric.label}`.toLowerCase())
    .join(" ");
  if (
    combined.includes("latency") ||
    combined.includes("duration") ||
    combined.includes("_ms") ||
    combined.includes(".ms")
  ) {
    return "ms";
  }
  if (
    combined.includes("percent") ||
    combined.includes("_pct") ||
    combined.includes(".pct") ||
    combined.includes("accuracy")
  ) {
    return "pct";
  }
  return metrics.every((metric) => Number.isInteger(metric.value)) ? "count" : "generic";
}

function formatMetricValue(
  value: number,
  unit: "count" | "ms" | "pct" | "generic" = "generic",
): string {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  let rendered: string;
  if (Math.abs(value) >= 1000) {
    rendered = value.toFixed(0);
  } else if (Number.isInteger(value)) {
    rendered = String(value);
  } else if (Math.abs(value) >= 10) {
    rendered = value.toFixed(2);
  } else {
    rendered = value.toFixed(4);
  }
  if (unit === "ms") {
    return `${rendered} ms`;
  }
  if (unit === "pct") {
    return `${rendered}%`;
  }
  return rendered;
}

function formatSignedDelta(
  value: number,
  unit: "count" | "ms" | "pct" | "generic",
): string {
  const sign = value > 0 ? "+" : "";
  return `${sign}${formatMetricValue(value, unit)}`;
}
