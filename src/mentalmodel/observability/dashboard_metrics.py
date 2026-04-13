from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.runtime.runs import RunFrameScope
from mentalmodel.ui.catalog import DashboardMetricGroup

MetricUnit = str
MetricSemanticKind = str
MetricRenderHint = str


@dataclass(slots=True, frozen=True)
class IndexedMetricRow:
    node_id: str
    path: str
    label: str
    normalized_label: str
    metric_node_path: str
    frame_id: str | None
    loop_node_id: str | None
    iteration_index: int | None
    value: float
    unit: MetricUnit
    semantic_kind: MetricSemanticKind


def metric_rows_from_outputs_payload(
    outputs_payload: dict[str, JsonValue],
) -> tuple[IndexedMetricRow, ...]:
    metrics: list[IndexedMetricRow] = []
    outputs = outputs_payload.get("outputs")
    if isinstance(outputs, dict):
        for node_id, output in outputs.items():
            if not isinstance(node_id, str):
                continue
            metrics.extend(
                _metrics_from_output(
                    node_id=node_id,
                    output=output,
                    frame_scope=RunFrameScope(frame_id="root"),
                )
            )
    framed_outputs = outputs_payload.get("framed_outputs")
    if isinstance(framed_outputs, list):
        for item in framed_outputs:
            if not isinstance(item, dict):
                continue
            raw_node_id = item.get("node_id")
            raw_frame_id = item.get("frame_id")
            if not isinstance(raw_node_id, str) or not isinstance(raw_frame_id, str):
                continue
            if raw_frame_id == "root":
                continue
            loop_node_id = item.get("loop_node_id")
            iteration_index = item.get("iteration_index")
            metrics.extend(
                _metrics_from_output(
                    node_id=raw_node_id,
                    output=_as_json_value(item.get("value")),
                    frame_scope=RunFrameScope(
                        frame_id=raw_frame_id,
                        loop_node_id=loop_node_id if isinstance(loop_node_id, str) else None,
                        iteration_index=(
                            iteration_index if isinstance(iteration_index, int) else None
                        ),
                    ),
                )
            )
    return tuple(metrics)


def metric_rows_from_live_records(
    records: Sequence[dict[str, object]],
) -> tuple[IndexedMetricRow, ...]:
    metrics: list[IndexedMetricRow] = []
    for record in records:
        if record.get("event_type") != "node.succeeded":
            continue
        payload = record.get("payload")
        node_id = record.get("node_id")
        frame_id = record.get("frame_id")
        if not isinstance(payload, dict) or not isinstance(node_id, str):
            continue
        output = payload.get("output")
        if output is None:
            continue
        loop_node_id = record.get("loop_node_id")
        iteration_index = record.get("iteration_index")
        metrics.extend(
            _metrics_from_output(
                node_id=node_id,
                output=_as_json_value(output),
                frame_scope=RunFrameScope(
                    frame_id=frame_id if isinstance(frame_id, str) else "root",
                    loop_node_id=loop_node_id if isinstance(loop_node_id, str) else None,
                    iteration_index=(
                        iteration_index if isinstance(iteration_index, int) else None
                    ),
                ),
            )
        )
    return tuple(metrics)


def evaluate_metric_groups(
    *,
    groups: Sequence[DashboardMetricGroup],
    metric_rows: Sequence[IndexedMetricRow],
    step_start: int | None,
    step_end: int | None,
    node_id: str | None = None,
    frame_id: str | None = None,
    max_points: int = 120,
) -> list[dict[str, JsonValue]]:
    filtered_rows = tuple(
        row
        for row in metric_rows
        if _matches_metric_filters(
            row=row,
            step_start=step_start,
            step_end=step_end,
            node_id=node_id,
            frame_id=frame_id,
        )
    )
    payload: list[dict[str, JsonValue]] = []
    for group in groups:
        rows = tuple(row for row in filtered_rows if metric_row_matches_group(row, group))
        series = build_metric_series(rows=rows, max_points=max_points)
        if not series:
            continue
        payload.append(
            {
                "group_id": group.group_id,
                "title": group.title,
                "description": group.description,
                "series": cast(JsonValue, series),
                "has_iteration_series": any(
                    isinstance(series_item["summary"], dict)
                    and isinstance(series_item["summary"].get("latest_iteration"), int)
                    for series_item in series
                ),
            }
        )
    return payload


def metric_row_matches_group(
    row: IndexedMetricRow,
    group: DashboardMetricGroup,
) -> bool:
    return any(
        row.path.startswith(prefix)
        or row.metric_node_path.startswith(prefix)
        or row.label.startswith(prefix)
        or row.normalized_label.startswith(prefix)
        for prefix in group.metric_path_prefixes
    )


def build_metric_series(
    *,
    rows: Sequence[IndexedMetricRow],
    max_points: int,
) -> list[dict[str, JsonValue]]:
    by_key: dict[str, list[IndexedMetricRow]] = defaultdict(list)
    for row in rows:
        key = _metric_series_key(row)
        by_key[key].append(row)

    series_payload: list[dict[str, JsonValue]] = []
    for key, series_rows in by_key.items():
        ordered = sorted(
            series_rows,
            key=lambda item: (
                -1 if item.iteration_index is None else item.iteration_index,
                item.value,
            ),
        )
        latest = ordered[-1]
        summary = _series_summary(ordered)
        render_hint = _series_render_hint(
            ordered,
            cast(MetricSemanticKind, summary["semantic_kind"]),
        )
        points = _downsample_series_points(ordered, max_points=max_points)
        series_payload.append(
            {
                "series_id": key,
                "label": _display_label_for_series(latest),
                "path": latest.path,
                "node_id": latest.node_id,
                "frame_id": latest.frame_id,
                "loop_node_id": latest.loop_node_id,
                "unit": latest.unit,
                "semantic_kind": summary["semantic_kind"],
                "render_hint": render_hint,
                "points": cast(JsonValue, points),
                "summary": summary,
            }
        )
    series_payload.sort(
        key=lambda item: (
            0 if item["semantic_kind"] == "trend" else 1,
            str(item["label"]),
        )
    )
    return series_payload


def _metric_series_key(row: IndexedMetricRow) -> str:
    if row.iteration_index is not None:
        return "::".join(
            [
                row.node_id,
                row.path,
                row.loop_node_id or "iterative",
            ]
        )
    return "::".join(
        [
            row.node_id,
            row.path,
            row.frame_id or "root",
            row.loop_node_id or "none",
        ]
    )


def _downsample_series_points(
    rows: Sequence[IndexedMetricRow],
    *,
    max_points: int,
) -> list[dict[str, JsonValue]]:
    if not rows:
        return []
    if rows[-1].iteration_index is None:
        latest = rows[-1]
        return [
            {
                "bucket_start": None,
                "bucket_end": None,
                "iteration_index": None,
                "value": latest.value,
                "min": latest.value,
                "max": latest.value,
                "avg": latest.value,
                "count": 1,
            }
        ]

    series_rows = [row for row in rows if row.iteration_index is not None]
    if not series_rows:
        return []
    min_iteration = min(cast(int, row.iteration_index) for row in series_rows)
    max_iteration = max(cast(int, row.iteration_index) for row in series_rows)
    bucket_width = 1
    if len(series_rows) > max_points and max_points > 0:
        bucket_width = max(
            1,
            math.ceil((max_iteration - min_iteration + 1) / max_points),
        )

    buckets: dict[int, list[IndexedMetricRow]] = defaultdict(list)
    for row in series_rows:
        iteration_index = cast(int, row.iteration_index)
        bucket_index = (iteration_index - min_iteration) // bucket_width
        buckets[bucket_index].append(row)

    points: list[dict[str, JsonValue]] = []
    for bucket_index in sorted(buckets):
        bucket_rows = sorted(
            buckets[bucket_index],
            key=lambda item: cast(int, item.iteration_index),
        )
        values = [row.value for row in bucket_rows]
        bucket_start = min(cast(int, row.iteration_index) for row in bucket_rows)
        bucket_end = max(cast(int, row.iteration_index) for row in bucket_rows)
        latest = bucket_rows[-1]
        points.append(
            {
                "bucket_start": bucket_start,
                "bucket_end": bucket_end,
                "iteration_index": latest.iteration_index,
                "value": latest.value,
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
                "count": len(values),
            }
        )
    return points


def _series_summary(rows: Sequence[IndexedMetricRow]) -> dict[str, JsonValue]:
    latest = rows[-1]
    first = rows[0]
    values = [row.value for row in rows]
    previous = rows[-2] if len(rows) > 1 else None
    semantic_kind = _infer_metric_semantic_kind(rows)
    return {
        "latest": latest.value,
        "first": first.value,
        "delta": None if previous is None else latest.value - previous.value,
        "window_delta": latest.value - first.value,
        "min": min(values),
        "max": max(values),
        "avg": sum(values) / len(values),
        "point_count": len(rows),
        "latest_iteration": latest.iteration_index,
        "semantic_kind": semantic_kind,
    }


def _display_label_for_series(row: IndexedMetricRow) -> str:
    return row.path or row.label


def _matches_metric_filters(
    *,
    row: IndexedMetricRow,
    step_start: int | None,
    step_end: int | None,
    node_id: str | None,
    frame_id: str | None,
) -> bool:
    if node_id is not None and row.node_id != node_id:
        return False
    if frame_id is not None and row.frame_id != frame_id:
        return False
    if (
        step_start is not None
        and row.iteration_index is not None
        and row.iteration_index < step_start
    ):
        return False
    if (
        step_end is not None
        and row.iteration_index is not None
        and row.iteration_index > step_end
    ):
        return False
    return True


def _infer_metric_semantic_kind(rows: Sequence[IndexedMetricRow]) -> MetricSemanticKind:
    latest = rows[-1]
    normalized = f"{latest.path} {latest.label}".lower()
    if len(rows) <= 1 or latest.iteration_index is None:
        return "gauge"
    counterish = (
        "total" in normalized
        or "cumulative" in normalized
        or "completed" in normalized
        or "failures" in normalized
        or "promotions" in normalized
    )
    monotonic = all(
        current.value >= previous.value
        for previous, current in zip(rows, rows[1:], strict=False)
    )
    if counterish and monotonic:
        return "counter"
    return "trend"


def _series_render_hint(
    rows: Sequence[IndexedMetricRow],
    semantic_kind: MetricSemanticKind,
) -> MetricRenderHint:
    if len(rows) <= 1 or rows[-1].iteration_index is None:
        return "stat"
    if semantic_kind == "counter":
        return "area"
    coverage = _iteration_coverage(rows)
    if coverage < 0.45:
        return "bar"
    return "line"


def _iteration_coverage(rows: Sequence[IndexedMetricRow]) -> float:
    iteration_rows = [row for row in rows if row.iteration_index is not None]
    if len(iteration_rows) <= 1:
        return 1.0
    min_iteration = min(cast(int, row.iteration_index) for row in iteration_rows)
    max_iteration = max(cast(int, row.iteration_index) for row in iteration_rows)
    span = max_iteration - min_iteration + 1
    if span <= 0:
        return 1.0
    unique_points = len({cast(int, row.iteration_index) for row in iteration_rows})
    return unique_points / span


def _metrics_from_output(
    *,
    node_id: str,
    output: JsonValue,
    frame_scope: RunFrameScope,
) -> list[IndexedMetricRow]:
    metrics: list[IndexedMetricRow] = []
    for metric_path, metric_value in _flatten_numeric_values(output):
        label = f"{node_id}.{metric_path}"
        if frame_scope.frame_id is not None and frame_scope.frame_id != "root":
            label = f"{frame_scope.frame_id}.{label}"
        normalized_label = (
            label[len(frame_scope.frame_id) + 1 :]
            if frame_scope.frame_id and frame_scope.frame_id != "root"
            else label
        )
        unit = _infer_metric_unit(metric_path=metric_path, label=label, value=metric_value)
        metrics.append(
            IndexedMetricRow(
                node_id=node_id,
                path=metric_path,
                label=label,
                normalized_label=normalized_label,
                metric_node_path=f"{node_id}.{metric_path}",
                frame_id=frame_scope.frame_id,
                loop_node_id=frame_scope.loop_node_id,
                iteration_index=frame_scope.iteration_index,
                value=metric_value,
                unit=unit,
                semantic_kind="gauge",
            )
        )
    return metrics


def _infer_metric_unit(*, metric_path: str, label: str, value: float) -> MetricUnit:
    normalized = f"{metric_path} {label}".lower()
    if (
        "latency" in normalized
        or "duration" in normalized
        or normalized.endswith("_ms")
        or "_ms_" in normalized
        or ".ms" in normalized
    ):
        return "ms"
    if (
        normalized.endswith("_seconds")
        or "_seconds_" in normalized
        or ".seconds" in normalized
        or normalized.endswith("_seconds_max")
        or normalized.endswith("_seconds_mean")
    ):
        return "s"
    if "percent" in normalized or normalized.endswith("_pct") or ".pct" in normalized:
        return "pct"
    if (
        "ratio" in normalized
        or "score" in normalized
        or "loss" in normalized
        or "reward" in normalized
        or "kl" in normalized
    ):
        return "ratio"
    if "bytes" in normalized:
        return "bytes"
    if float(value).is_integer():
        return "count"
    return "generic"


def _flatten_numeric_values(
    value: JsonValue,
    *,
    prefix: str = "",
) -> Iterable[tuple[str, float]]:
    if isinstance(value, bool):
        return
    if isinstance(value, int):
        yield prefix or "value", float(value)
        return
    if isinstance(value, float):
        if math.isfinite(value):
            yield prefix or "value", value
        return
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str) or not key:
                continue
            child_prefix = key if prefix == "" else f"{prefix}.{key}"
            yield from _flatten_numeric_values(child, prefix=child_prefix)
        return
    if isinstance(value, list):
        return


def _as_json_value(value: object) -> JsonValue:
    if value is None or isinstance(value, (str, bool, int, float)):
        return cast(JsonValue, value)
    if isinstance(value, list):
        return [_as_json_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _as_json_value(item)
            for key, item in value.items()
            if isinstance(key, str)
        }
    return str(value)
