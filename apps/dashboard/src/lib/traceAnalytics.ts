import type { GenericSpan } from "../types";

export type IterationLatencyRow = {
  iteration: number;
  count: number;
  totalMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  maxMs: number;
};

export type OperationLatencySummary = {
  nodeId: string;
  title: string;
  runtimeProfile: string | null;
  count: number;
  latestIteration: number | null;
  latestMs: number;
  totalMs: number;
  avgMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  maxMs: number;
};

export type OperationLatencySeries = {
  key: string;
  nodeId: string;
  label: string;
  valuesByIteration: Map<number, number>;
};

export type LatencyOperationChartModel = {
  rows: Array<{ iteration: number; [seriesKey: string]: number }>;
  series: OperationLatencySeries[];
  hiddenSeriesCount: number;
};

export function buildIterationLatencyRows(spans: GenericSpan[]): IterationLatencyRow[] {
  const grouped = new Map<number, number[]>();
  for (const span of spans) {
    if (span.iterationIndex == null || !Number.isFinite(span.latencyMs)) {
      continue;
    }
    const current = grouped.get(span.iterationIndex) ?? [];
    current.push(span.latencyMs);
    grouped.set(span.iterationIndex, current);
  }
  return [...grouped.entries()]
    .sort((left, right) => left[0] - right[0])
    .map(([iteration, values]) => {
      const sorted = [...values].sort((left, right) => left - right);
      return {
        iteration,
        count: sorted.length,
        totalMs: sum(sorted),
        p50Ms: percentile(sorted, 0.5),
        p95Ms: percentile(sorted, 0.95),
        p99Ms: percentile(sorted, 0.99),
        maxMs: sorted[sorted.length - 1] ?? 0,
      };
    });
}

export function buildOperationLatencySummaries(
  spans: GenericSpan[],
  maxItems = 8,
): OperationLatencySummary[] {
  const grouped = new Map<string, GenericSpan[]>();
  for (const span of spans) {
    if (!Number.isFinite(span.latencyMs)) {
      continue;
    }
    const key = span.nodeId;
    const current = grouped.get(key) ?? [];
    current.push(span);
    grouped.set(key, current);
  }
  return [...grouped.entries()]
    .map(([nodeId, items]) => {
      const sorted = items
        .map((item) => item.latencyMs)
        .filter((value) => Number.isFinite(value))
        .sort((left, right) => left - right);
      const latest = [...items].sort((left, right) => {
        const leftIteration = left.iterationIndex ?? -1;
        const rightIteration = right.iterationIndex ?? -1;
        if (rightIteration !== leftIteration) {
          return rightIteration - leftIteration;
        }
        const leftTime = left.startTimeMs ?? 0;
        const rightTime = right.startTimeMs ?? 0;
        return rightTime - leftTime;
      })[0];
      return {
        nodeId,
        title: latest?.title ?? nodeId,
        runtimeProfile: latest?.runtimeProfile ?? null,
        count: sorted.length,
        latestIteration: latest?.iterationIndex ?? null,
        latestMs: latest?.latencyMs ?? 0,
        totalMs: sum(sorted),
        avgMs: average(sorted),
        p50Ms: percentile(sorted, 0.5),
        p95Ms: percentile(sorted, 0.95),
        p99Ms: percentile(sorted, 0.99),
        maxMs: sorted[sorted.length - 1] ?? 0,
      };
    })
    .sort((left, right) => {
      if (right.p95Ms !== left.p95Ms) {
        return right.p95Ms - left.p95Ms;
      }
      if (right.totalMs !== left.totalMs) {
        return right.totalMs - left.totalMs;
      }
      return left.title.localeCompare(right.title);
    })
    .slice(0, maxItems);
}

export function buildOperationLatencyChartModel(
  spans: GenericSpan[],
  maxSeries = 4,
): LatencyOperationChartModel | null {
  const summaries = buildOperationLatencySummaries(spans, Math.max(maxSeries, 1) + 4);
  if (summaries.length === 0) {
    return null;
  }
  const topNodeIds = new Set(summaries.slice(0, maxSeries).map((summary) => summary.nodeId));
  const seriesByKey = new Map<string, OperationLatencySeries>();
  for (const span of spans) {
    if (span.iterationIndex == null || !topNodeIds.has(span.nodeId)) {
      continue;
    }
    const current = seriesByKey.get(span.nodeId);
    if (current == null) {
      seriesByKey.set(span.nodeId, {
        key: span.nodeId,
        nodeId: span.nodeId,
        label: span.title,
        valuesByIteration: new Map([[span.iterationIndex, span.latencyMs]]),
      });
      continue;
    }
    current.valuesByIteration.set(span.iterationIndex, span.latencyMs);
  }
  const series = [...seriesByKey.values()].sort((left, right) => {
    const leftSummary = summaries.find((summary) => summary.nodeId === left.nodeId);
    const rightSummary = summaries.find((summary) => summary.nodeId === right.nodeId);
    return (rightSummary?.p95Ms ?? 0) - (leftSummary?.p95Ms ?? 0);
  });
  const iterationSet = new Set<number>();
  for (const item of series) {
    for (const iteration of item.valuesByIteration.keys()) {
      iterationSet.add(iteration);
    }
  }
  const iterations = [...iterationSet].sort((left, right) => left - right);
  if (iterations.length === 0) {
    return null;
  }
  const rows = iterations.map((iteration) => {
    const row: { iteration: number; [seriesKey: string]: number } = { iteration };
    for (const item of series) {
      const value = item.valuesByIteration.get(iteration);
      if (value != null) {
        row[item.key] = value;
      }
    }
    return row;
  });
  return {
    rows,
    series,
    hiddenSeriesCount: Math.max(0, summaries.length - series.length),
  };
}

function sum(values: number[]): number {
  return values.reduce((total, value) => total + value, 0);
}

function average(values: number[]): number {
  if (values.length === 0) {
    return 0;
  }
  return sum(values) / values.length;
}

function percentile(sortedValues: number[], percentileValue: number): number {
  if (sortedValues.length === 0) {
    return 0;
  }
  const position = Math.min(
    sortedValues.length - 1,
    Math.max(0, Math.ceil(percentileValue * sortedValues.length) - 1),
  );
  return sortedValues[position] ?? 0;
}
