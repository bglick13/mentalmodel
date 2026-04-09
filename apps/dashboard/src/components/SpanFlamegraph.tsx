import { useMemo } from "react";

import type { GenericSpan } from "../types";

type FlameBox = {
  index: number;
  x: number;
  w: number;
  y: number;
  h: number;
  span: GenericSpan;
};

function layoutProportional(spans: GenericSpan[], innerW: number, rowH: number): FlameBox[] {
  const total = spans.reduce((s, x) => s + Math.max(x.latencyMs, 1e-6), 0);
  let x = 0;
  const boxes: FlameBox[] = [];
  spans.forEach((span, index) => {
    const frac = Math.max(span.latencyMs, 1e-6) / total;
    const w = index === spans.length - 1 ? innerW - x : Math.max(innerW * frac, 2);
    boxes.push({
      index,
      x,
      w,
      y: 0,
      h: rowH,
      span,
    });
    x += w;
  });
  return boxes;
}

function layoutTimedAll(
  spans: GenericSpan[],
  innerW: number,
  rowH: number,
  gap: number,
): FlameBox[] {
  const segs = spans.map((span, index) => {
    const start = span.startTimeMs!;
    const end =
      span.endTimeMs ?? start + Math.max(span.latencyMs, 0.001);
    return { index, start, end, span };
  });
  const t0 = Math.min(...segs.map((t) => t.start));
  const t1 = Math.max(...segs.map((t) => t.end));
  const spanT = Math.max(t1 - t0, 1e-6);

  const sorted = [...segs].sort((a, b) => a.start - b.start);
  const rowEnds: number[] = [];
  const boxes: FlameBox[] = [];

  for (const seg of sorted) {
    const x0 = ((seg.start - t0) / spanT) * innerW;
    const x1 = ((seg.end - t0) / spanT) * innerW;
    const w = Math.max(x1 - x0, 2);

    let row = 0;
    for (;;) {
      const last = rowEnds[row];
      if (last == null || seg.start >= last - 0.0001) {
        rowEnds[row] = seg.end;
        boxes.push({
          index: seg.index,
          x: x0,
          w,
          y: row * (rowH + gap),
          h: rowH,
          span: seg.span,
        });
        break;
      }
      row += 1;
    }
  }

  return boxes.sort((a, b) => a.y - b.y || a.x - b.x);
}

function allSpansHaveWallClock(spans: GenericSpan[]): boolean {
  return spans.every((s) => {
    if (s.startTimeMs == null) {
      return false;
    }
    const end =
      s.endTimeMs ?? s.startTimeMs + Math.max(s.latencyMs, 0.001);
    return end > s.startTimeMs;
  });
}

export type FlamegraphDensity = "compact" | "comfortable";

type SpanFlamegraphProps = {
  onSelectSpan: (index: number) => void;
  selectedIndex: number | null;
  spans: GenericSpan[];
  /** `comfortable` uses taller rows — use in fullscreen or expanded layouts. Default `compact`. */
  density?: FlamegraphDensity;
};

const INNER_W = 900;
const PAD = 10;

const DENSITY_LAYERS: Record<
  FlamegraphDensity,
  { rowH: number; rowGap: number; minChart: number; labelFont: number }
> = {
  /** Inline hero: readable on wide viewports without excessive letterboxing. */
  compact: { rowH: 34, rowGap: 8, minChart: 100, labelFont: 12 },
  /** Fullscreen / expanded: Datadog-like row height for deep traces. */
  comfortable: { rowH: 48, rowGap: 12, minChart: 180, labelFont: 13 },
};

function formatWallDurationLabel(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) {
    return "";
  }
  if (ms >= 1000) {
    return `${(ms / 1000).toFixed(2)} s trace`;
  }
  if (ms < 1) {
    return `${Math.round(ms * 1000)} µs trace`;
  }
  return `${ms < 10 ? ms.toFixed(2) : ms.toFixed(1)} ms trace`;
}

export function SpanFlamegraph({
  onSelectSpan,
  selectedIndex,
  spans,
  density = "compact",
}: SpanFlamegraphProps) {
  const dims = DENSITY_LAYERS[density];

  const { boxes, mode, vbHeight, wallDurationMs } = useMemo(() => {
    if (spans.length === 0) {
      return {
        boxes: [] as FlameBox[],
        mode: "empty" as const,
        vbHeight: 40,
        wallDurationMs: undefined as number | undefined,
      };
    }

    const inner = INNER_W - PAD * 2;
    const useWall = allSpansHaveWallClock(spans);
    const { rowH, rowGap, minChart } = DENSITY_LAYERS[density];

    if (useWall) {
      const timedBoxes = layoutTimedAll(spans, inner, rowH, rowGap);
      const contentBottom = Math.max(
        ...timedBoxes.map((b) => b.y + b.h),
        rowH,
      );
      const chartInnerH = Math.max(contentBottom, minChart);
      const vbH = PAD + chartInnerH + PAD;
      const starts = spans.map((s) => s.startTimeMs!);
      const ends = spans.map((s) => {
        const e =
          s.endTimeMs ?? s.startTimeMs! + Math.max(s.latencyMs, 0.001);
        return e;
      });
      const t0 = Math.min(...starts);
      const t1 = Math.max(...ends);
      return {
        boxes: timedBoxes,
        mode: "time" as const,
        vbHeight: vbH,
        wallDurationMs: Math.max(t1 - t0, 0),
      };
    }

    const prop = layoutProportional(spans, inner, rowH);
    const chartInnerH = Math.max(rowH, minChart);
    const vbH = PAD + chartInnerH + PAD;
    return {
      boxes: prop,
      mode: "proportional" as const,
      vbHeight: vbH,
      wallDurationMs: undefined as number | undefined,
    };
  }, [spans, density]);

  if (spans.length === 0) {
    return null;
  }

  const inner = INNER_W - PAD * 2;
  /** Target plot height (viewBox units); may be larger than raw layout for a sane aspect ratio. */
  const chartInnerH = vbHeight - PAD * 2;
  const contentExtent =
    boxes.length === 0
      ? dims.rowH
      : Math.max(...boxes.map((b) => b.y + b.h));
  /** Map layout rows into full chart height without SVG `scaleY` (which stretches text). */
  const sy = chartInnerH / Math.max(contentExtent, 1e-6);
  const labelMinH = density === "comfortable" ? 20 : 15;

  const durationLabel =
    wallDurationMs != null ? formatWallDurationLabel(wallDurationMs) : "";

  return (
    <div className="flamegraph-wrap">
      <div className="flamegraph-meta">
        <span className="flamegraph-mode-badge">
          {mode === "time" ? "Wall-clock timeline" : "Relative duration strip"}
        </span>
        {durationLabel.length > 0 ? (
          <span className="flamegraph-duration-pill">{durationLabel}</span>
        ) : null}
        <span className="flamegraph-meta-hint">
          {mode === "time"
            ? "X-axis is wall time; rows are concurrent spans (like a trace waterfall). Click a block for details."
            : "Widths reflect duration when wall-clock times are missing. Click a block to inspect."}
        </span>
      </div>
      <div className="flamegraph-chart-surface">
        <svg
          className="flamegraph-svg"
          viewBox={`0 0 ${INNER_W} ${vbHeight}`}
          preserveAspectRatio="xMinYMin meet"
          role="img"
          aria-label="Trace timeline: span durations"
        >
        <rect
          className="flamegraph-bg"
          x={PAD}
          y={PAD}
          width={inner}
          height={chartInnerH}
          rx={4}
        />
        <g transform={`translate(${PAD}, ${PAD})`}>
          {boxes.map((b) => {
            const hue = b.span.kindHue;
            const selected = selectedIndex === b.index;
            const y = b.y * sy;
            const h = b.h * sy;
            const label =
              b.span.title.length > 28
                ? `${b.span.title.slice(0, 26)}…`
                : b.span.title;
            const showLabel = b.w > 56 && h >= labelMinH;
            return (
              <g
                key={`${b.index}:${b.x}:${b.y}`}
                role="button"
                tabIndex={0}
                className="flamegraph-hit"
                aria-label={`${b.span.title}, ${b.span.latencyLabel}`}
                onClick={() => onSelectSpan(b.index)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    onSelectSpan(b.index);
                  }
                }}
              >
                <title>{`${b.span.title} · ${b.span.latencyLabel}`}</title>
                <rect
                  className={`flamegraph-block ${selected ? "flamegraph-block-selected" : ""}`}
                  x={b.x}
                  y={y}
                  width={b.w}
                  height={h}
                  rx={Math.min(3, h / 2)}
                  fill={`hsla(${hue}, 52%, 46%, 0.92)`}
                  stroke={
                    selected ? "rgba(142, 213, 255, 0.95)" : "rgba(255,255,255,0.08)"
                  }
                  strokeWidth={selected ? 2 : 1}
                />
                {showLabel ? (
                  <text
                    className="flamegraph-block-label"
                    x={b.x + 4}
                    y={y + Math.min(h * 0.72, h - 3)}
                    fill="rgba(248, 250, 252, 0.92)"
                    fontSize={dims.labelFont}
                  >
                    {label}
                  </text>
                ) : null}
              </g>
            );
          })}
        </g>
        </svg>
      </div>
    </div>
  );
}
