from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypeGuard

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.runtime.replay import build_replay_report
from mentalmodel.runtime.runs import (
    load_run_node_inputs,
    load_run_node_output,
    load_run_records,
)


@dataclass(slots=True, frozen=True)
class DashboardTableRowSource:
    """Declarative row source for one custom table view."""

    kind: str
    node_id: str
    items_path: str
    loop_node_id: str | None = None

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "kind": self.kind,
            "node_id": self.node_id,
            "items_path": self.items_path,
            "loop_node_id": self.loop_node_id,
        }


@dataclass(slots=True, frozen=True)
class DashboardValueSelector:
    """Declarative selector for one table cell value."""

    kind: str
    path: str | None = None
    node_id: str | None = None
    event_type: str | None = None

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "kind": self.kind,
            "path": self.path,
            "node_id": self.node_id,
            "event_type": self.event_type,
        }


@dataclass(slots=True, frozen=True)
class DashboardTableColumn:
    """One displayed column in a custom table view."""

    column_id: str
    title: str
    selector: DashboardValueSelector
    description: str = ""

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "column_id": self.column_id,
            "title": self.title,
            "description": self.description,
            "selector": self.selector.as_dict(),
        }


@dataclass(slots=True, frozen=True)
class DashboardCustomView:
    """One provider-declared custom view rendered for a persisted run."""

    view_id: str
    title: str
    description: str
    kind: str
    row_source: DashboardTableRowSource
    columns: tuple[DashboardTableColumn, ...]

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "view_id": self.view_id,
            "title": self.title,
            "description": self.description,
            "kind": self.kind,
            "row_source": self.row_source.as_dict(),
            "columns": [column.as_dict() for column in self.columns],
        }


@dataclass(slots=True, frozen=True)
class EvaluatedCustomViewRow:
    """One normalized row returned by the evaluated custom view API."""

    row_id: str
    frame_id: str | None
    loop_node_id: str | None
    iteration_index: int | None
    values: dict[str, JsonValue]

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "row_id": self.row_id,
            "frame_id": self.frame_id,
            "loop_node_id": self.loop_node_id,
            "iteration_index": self.iteration_index,
            "values": dict(self.values),
        }


@dataclass(slots=True, frozen=True)
class EvaluatedCustomView:
    """Normalized payload returned for one evaluated custom view."""

    view: DashboardCustomView
    rows: tuple[EvaluatedCustomViewRow, ...]
    warnings: tuple[str, ...]

    def as_dict(self) -> dict[str, JsonValue]:
        return {
            "view": self.view.as_dict(),
            "row_count": len(self.rows),
            "rows": [row.as_dict() for row in self.rows],
            "warnings": list(self.warnings),
        }


@dataclass(slots=True, frozen=True)
class _ResolvedRowScope:
    row_id: str
    frame_id: str | None
    loop_node_id: str | None
    iteration_index: int | None
    item: JsonValue


_MISSING = object()


def evaluate_custom_view(
    *,
    runs_dir: Path,
    graph_id: str,
    run_id: str,
    view: DashboardCustomView,
) -> EvaluatedCustomView:
    warnings: list[str] = []
    warning_keys: set[str] = set()
    rows = _resolve_row_scopes(
        runs_dir=runs_dir,
        graph_id=graph_id,
        run_id=run_id,
        view=view,
        warnings=warnings,
        warning_keys=warning_keys,
    )
    evaluated_rows: list[EvaluatedCustomViewRow] = []
    for row in rows:
        values: dict[str, JsonValue] = {}
        for column in view.columns:
            value = _evaluate_selector(
                runs_dir=runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                selector=column.selector,
                row=row,
                warnings=warnings,
                warning_keys=warning_keys,
            )
            values[column.column_id] = value
        evaluated_rows.append(
            EvaluatedCustomViewRow(
                row_id=row.row_id,
                frame_id=row.frame_id,
                loop_node_id=row.loop_node_id,
                iteration_index=row.iteration_index,
                values=values,
            )
        )
    return EvaluatedCustomView(
        view=view,
        rows=tuple(evaluated_rows),
        warnings=tuple(warnings),
    )


def _resolve_row_scopes(
    *,
    runs_dir: Path,
    graph_id: str,
    run_id: str,
    view: DashboardCustomView,
    warnings: list[str],
    warning_keys: set[str],
) -> tuple[_ResolvedRowScope, ...]:
    source = view.row_source
    replay = build_replay_report(runs_dir=runs_dir, graph_id=graph_id, run_id=run_id)
    matching_summaries = [
        summary
        for summary in replay.node_summaries
        if summary.node_id == source.node_id
        and (source.loop_node_id is None or summary.loop_node_id == source.loop_node_id)
    ]
    if not matching_summaries:
        matching_summaries = []
    deduped_scopes: list[tuple[str | None, str | None, int | None]] = []
    seen_scopes: set[tuple[str | None, str | None, int | None]] = set()
    if matching_summaries:
        ordered = sorted(
            matching_summaries,
            key=lambda summary: (
                -1 if summary.iteration_index is None else summary.iteration_index,
                summary.frame_id,
            ),
        )
        for summary in ordered:
            key = (
                None if summary.frame_id == "root" else summary.frame_id,
                summary.loop_node_id,
                summary.iteration_index,
            )
            if key in seen_scopes:
                continue
            seen_scopes.add(key)
            deduped_scopes.append(key)
    else:
        deduped_scopes.append((None, source.loop_node_id, None))

    resolved_rows: list[_ResolvedRowScope] = []
    for frame_id, loop_node_id, iteration_index in deduped_scopes:
        try:
            output = load_run_node_output(
                runs_dir=runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=source.node_id,
                frame_id=frame_id,
                loop_node_id=loop_node_id,
                iteration_index=iteration_index,
            )
        except RunInspectionError as exc:
            _add_warning(
                warnings,
                warning_keys,
                (
                    "row-source-output",
                    source.node_id,
                    frame_id or "root",
                    str(iteration_index),
                ),
                f"Row source {source.node_id!r} could not be loaded for "
                f"{frame_id or 'root'}: {exc}",
            )
            continue
        items = _extract_value(output, source.items_path)
        if items is _MISSING:
            _add_warning(
                warnings,
                warning_keys,
                ("row-source-path", source.node_id, source.items_path),
                f"Row source path {source.items_path!r} was not found on node {source.node_id!r}.",
            )
            continue
        if not isinstance(items, list):
            _add_warning(
                warnings,
                warning_keys,
                ("row-source-list", source.node_id, source.items_path),
                f"Row source path {source.items_path!r} on node {source.node_id!r} is not a list.",
            )
            continue
        for index, item in enumerate(items):
            if not _is_json_value(item):
                _add_warning(
                    warnings,
                    warning_keys,
                    ("row-source-item", source.node_id, source.items_path, str(index)),
                    "Custom view row items must be JSON-compatible values.",
                )
                continue
            resolved_rows.append(
                _ResolvedRowScope(
                    row_id=f"{frame_id or 'root'}:{index}",
                    frame_id=frame_id,
                    loop_node_id=loop_node_id,
                    iteration_index=iteration_index,
                    item=item,
                )
            )
    return tuple(resolved_rows)


def _evaluate_selector(
    *,
    runs_dir: Path,
    graph_id: str,
    run_id: str,
    selector: DashboardValueSelector,
    row: _ResolvedRowScope,
    warnings: list[str],
    warning_keys: set[str],
) -> JsonValue:
    source: JsonValue | object
    if selector.kind == "row_item":
        source = row.item
    elif selector.kind == "scope":
        scope_payload: dict[str, JsonValue] = {
            "frame_id": row.frame_id,
            "loop_node_id": row.loop_node_id,
            "iteration_index": row.iteration_index,
        }
        source = scope_payload
    elif selector.kind == "node_output":
        if selector.node_id is None:
            return None
        try:
            source = load_run_node_output(
                runs_dir=runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=selector.node_id,
                frame_id=row.frame_id,
                loop_node_id=row.loop_node_id,
                iteration_index=row.iteration_index,
            )
        except RunInspectionError as exc:
            _add_warning(
                warnings,
                warning_keys,
                ("node-output", selector.node_id, row.row_id),
                f"Node output {selector.node_id!r} could not be loaded for row {row.row_id}: {exc}",
            )
            return None
    elif selector.kind == "node_input":
        if selector.node_id is None:
            return None
        try:
            source = load_run_node_inputs(
                runs_dir=runs_dir,
                graph_id=graph_id,
                run_id=run_id,
                node_id=selector.node_id,
                frame_id=row.frame_id,
                loop_node_id=row.loop_node_id,
                iteration_index=row.iteration_index,
            )
        except RunInspectionError as exc:
            _add_warning(
                warnings,
                warning_keys,
                ("node-input", selector.node_id, row.row_id),
                f"Node input {selector.node_id!r} could not be loaded for row {row.row_id}: {exc}",
            )
            return None
    elif selector.kind == "record_payload":
        if selector.node_id is None:
            return None
        records = load_run_records(
            runs_dir=runs_dir,
            graph_id=graph_id,
            run_id=run_id,
            node_id=selector.node_id,
            event_type=selector.event_type,
            frame_id=row.frame_id,
            loop_node_id=row.loop_node_id,
            iteration_index=row.iteration_index,
        )
        if not records:
            _add_warning(
                warnings,
                warning_keys,
                (
                    "record-payload",
                    selector.node_id,
                    selector.event_type or "*",
                    row.row_id,
                ),
                f"Record payload {selector.node_id!r}/{selector.event_type or '*'} "
                f"was not found for row {row.row_id}.",
            )
            return None
        payload = records[-1].get("payload")
        source = payload if _is_json_value(payload) else _MISSING
    else:
        return None
    if selector.path is None or selector.path == "":
        return cast_json_value(source)
    value = _extract_value(source, selector.path)
    if value is _MISSING:
        _add_warning(
            warnings,
            warning_keys,
            ("selector-path", selector.kind, selector.node_id or "", selector.path, row.row_id),
            f"Selector path {selector.path!r} was not found for row {row.row_id}.",
        )
        return None
    return cast_json_value(value)


def cast_json_value(value: JsonValue | object) -> JsonValue:
    if _is_json_value(value):
        return value
    return None


def _extract_value(source: JsonValue | object, path: str) -> JsonValue | object:
    current: JsonValue | object = source
    for segment in path.split("."):
        if segment == "":
            continue
        if isinstance(current, dict):
            if segment not in current:
                return _MISSING
            current = current[segment]
            continue
        if isinstance(current, list):
            try:
                index = int(segment)
            except ValueError:
                return _MISSING
            if index < 0 or index >= len(current):
                return _MISSING
            current = current[index]
            continue
        return _MISSING
    return current


def _add_warning(
    warnings: list[str],
    warning_keys: set[str],
    key: tuple[str, ...],
    message: str,
) -> None:
    serialized = "::".join(key)
    if serialized in warning_keys:
        return
    warning_keys.add(serialized)
    warnings.append(message)


def _is_json_value(value: object) -> TypeGuard[JsonValue]:
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, list):
        return all(_is_json_value(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(key, str) and _is_json_value(item) for key, item in value.items())
    return False
