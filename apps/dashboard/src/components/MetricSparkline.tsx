import { Area, AreaChart, Bar, BarChart, ResponsiveContainer } from "recharts";

import type { MetricSeries } from "../types";

type MetricSparklineProps = {
  series: MetricSeries;
};

export function MetricSparkline({ series }: MetricSparklineProps) {
  if (series.points.length <= 1) {
    return null;
  }
  const rows = series.points.map((point) => ({
    x: point.iteration_index ?? point.bucket_end ?? 0,
    value: point.value,
  }));
  return (
    <div className="metric-sparkline" aria-hidden>
      <ResponsiveContainer width="100%" height={44}>
        {series.render_hint === "bar" ? (
          <BarChart data={rows}>
            <Bar dataKey="value" fill="rgba(142, 213, 255, 0.78)" radius={[3, 3, 0, 0]} isAnimationActive={false} />
          </BarChart>
        ) : (
          <AreaChart data={rows}>
            <Area
              type="monotone"
              dataKey="value"
              stroke="rgba(142, 213, 255, 0.92)"
              fill="rgba(142, 213, 255, 0.16)"
              strokeWidth={2}
              isAnimationActive={false}
            />
          </AreaChart>
        )}
      </ResponsiveContainer>
    </div>
  );
}
