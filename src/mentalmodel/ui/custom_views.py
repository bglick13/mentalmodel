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

    @classmethod
    def from_dict(cls, payload: object) -> DashboardTableRowSource:
        raw = _require_object(payload, "DashboardTableRowSource")
        kind = _require_str(raw, "kind", "DashboardTableRowSource")
        node_id = _require_str(raw, "node_id", "DashboardTableRowSource")
        items_path = _require_str(raw, "items_path", "DashboardTableRowSource")
        loop_node_id = _optional_str(raw, "loop_node_id", "DashboardTableRowSource")
        return cls(
            kind=kind,
            node_id=node_id,
            items_path=items_path,
            loop_node_id=loop_node_id,
        )


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

    @classmethod
    def from_dict(cls, payload: object) -> DashboardValueSelector:
        raw = _require_object(payload, "DashboardValueSelector")
        return cls(
            kind=_require_str(raw, "kind", "DashboardValueSelector"),
            path=_optional_str(raw, "path", "DashboardValueSelector"),
            node_id=_optional_str(raw, "node_id", "DashboardValueSelector"),
            event_type=_optional_str(raw, "event_type", "DashboardValueSelector"),
        )


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

    @classmethod
    def from_dict(cls, payload: object) -> DashboardTableColumn:
        raw = _require_object(payload, "DashboardTableColumn")
        selector = raw.get("selector")
        return cls(
            column_id=_require_str(raw, "column_id", "DashboardTableColumn"),
            title=_require_str(raw, "title", "DashboardTableColumn"),
            description=_optional_str(raw, "description", "DashboardTableColumn") or "",
            selector=DashboardValueSelector.from_dict(selector),
        )


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

    @classmethod
    def from_dict(cls, payload: object) -> DashboardCustomView:
        raw = _require_object(payload, "DashboardCustomView")
        columns_payload = raw.get("columns")
        if not isinstance(columns_payload, list):
            raise TypeError("DashboardCustomView.columns must be a list.")
        return cls(
            view_id=_require_str(raw, "view_id", "DashboardCustomView"),
            title=_require_str(raw, "title", "DashboardCustomView"),
            description=_optional_str(raw, "description", "DashboardCustomView") or "",
            kind=_require_str(raw, "kind", "DashboardCustomView"),
            row_source=DashboardTableRowSource.from_dict(raw.get("row_source")),
            columns=tuple(DashboardTableColumn.from_dict(item) for item in columns_payload),
        )


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


def evaluate_custom_view_from_records(
    *,
    records: list[dict[str, object]],
    run_id: str,
    view: DashboardCustomView,
) -> EvaluatedCustomView:
    warnings: list[str] = []
    warning_keys: set[str] = set()
    rows = _resolve_row_scopes_from_records(
        records=records,
        run_id=run_id,
        view=view,
        warnings=warnings,
        warning_keys=warning_keys,
    )
    evaluated_rows: list[EvaluatedCustomViewRow] = []
    for row in rows:
        values: dict[str, JsonValue] = {}
        for column in view.columns:
            value = _evaluate_selector_from_records(
                records=records,
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


def _resolve_row_scopes_from_records(
    *,
    records: list[dict[str, object]],
    run_id: str,
    view: DashboardCustomView,
    warnings: list[str],
    warning_keys: set[str],
) -> tuple[_ResolvedRowScope, ...]:
    source = view.row_source
    outputs_by_scope = _latest_live_outputs_for_node(
        records=records,
        run_id=run_id,
        node_id=source.node_id,
    )
    matching_scopes = [
        scope
        for scope in outputs_by_scope
        if source.loop_node_id is None or scope[1] == source.loop_node_id
    ]
    if not matching_scopes:
        _add_warning(
            warnings,
            warning_keys,
            ("row-source-live-output", source.node_id),
            f"Row source {source.node_id!r} is not available from the live record stream yet.",
        )
        return ()

    ordered_scopes = sorted(
        matching_scopes,
        key=lambda scope: (
            -1 if scope[2] is None else scope[2],
            "" if scope[0] is None else scope[0],
        ),
    )
    resolved_rows: list[_ResolvedRowScope] = []
    for frame_id, loop_node_id, iteration_index in ordered_scopes:
        output = outputs_by_scope[(frame_id, loop_node_id, iteration_index)]
        items = _extract_value(output, source.items_path)
        if items is _MISSING:
            _add_warning(
                warnings,
                warning_keys,
                ("row-source-live-path", source.node_id, source.items_path),
                (
                    f"Row source path {source.items_path!r} was not found on live "
                    f"node output {source.node_id!r}."
                ),
            )
            continue
        if not isinstance(items, list):
            _add_warning(
                warnings,
                warning_keys,
                ("row-source-live-list", source.node_id, source.items_path),
                (
                    f"Row source path {source.items_path!r} on live node "
                    f"{source.node_id!r} is not a list."
                ),
            )
            continue
        for index, item in enumerate(items):
            if not _is_json_value(item):
                _add_warning(
                    warnings,
                    warning_keys,
                    ("row-source-live-item", source.node_id, source.items_path, str(index)),
                    "Live custom view row items must be JSON-compatible values.",
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


def _evaluate_selector_from_records(
    *,
    records: list[dict[str, object]],
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
        source = {
            "frame_id": row.frame_id,
            "loop_node_id": row.loop_node_id,
            "iteration_index": row.iteration_index,
        }
    elif selector.kind == "node_output":
        if selector.node_id is None:
            return None
        output = _latest_live_output_for_scope(
            records=records,
            run_id=run_id,
            node_id=selector.node_id,
            frame_id=row.frame_id,
            loop_node_id=row.loop_node_id,
            iteration_index=row.iteration_index,
        )
        if output is _MISSING:
            _add_warning(
                warnings,
                warning_keys,
                ("live-node-output", selector.node_id, row.row_id),
                f"Live node output {selector.node_id!r} was not found for row {row.row_id}.",
            )
            return None
        source = output
    elif selector.kind == "node_input":
        if selector.node_id is None:
            return None
        inputs = _latest_live_inputs_for_scope(
            records=records,
            run_id=run_id,
            node_id=selector.node_id,
            frame_id=row.frame_id,
            loop_node_id=row.loop_node_id,
            iteration_index=row.iteration_index,
        )
        if inputs is _MISSING:
            _add_warning(
                warnings,
                warning_keys,
                ("live-node-input", selector.node_id, row.row_id),
                f"Live node input {selector.node_id!r} was not found for row {row.row_id}.",
            )
            return None
        source = inputs
    elif selector.kind == "record_payload":
        if selector.node_id is None:
            return None
        payload = _latest_live_record_payload_for_scope(
            records=records,
            run_id=run_id,
            node_id=selector.node_id,
            event_type=selector.event_type,
            frame_id=row.frame_id,
            loop_node_id=row.loop_node_id,
            iteration_index=row.iteration_index,
        )
        if payload is _MISSING:
            _add_warning(
                warnings,
                warning_keys,
                ("live-record-payload", selector.node_id, selector.event_type or "*", row.row_id),
                (
                    f"Live record payload {selector.node_id!r}/"
                    f"{selector.event_type or '*'} was not found for row "
                    f"{row.row_id}."
                ),
            )
            return None
        source = payload
    else:
        return None
    if selector.path is None or selector.path == "":
        return cast_json_value(source)
    value = _extract_value(source, selector.path)
    if value is _MISSING:
        _add_warning(
            warnings,
            warning_keys,
            (
                "live-selector-path",
                selector.kind,
                selector.node_id or "",
                selector.path,
                row.row_id,
            ),
            f"Selector path {selector.path!r} was not found for live row {row.row_id}.",
        )
        return None
    return cast_json_value(value)


def _latest_live_outputs_for_node(
    *,
    records: list[dict[str, object]],
    run_id: str,
    node_id: str,
) -> dict[tuple[str | None, str | None, int | None], JsonValue]:
    outputs: dict[tuple[str | None, str | None, int | None], JsonValue] = {}
    for record in records:
        if not _live_record_matches(
            record,
            run_id=run_id,
            node_id=node_id,
            event_type="node.succeeded",
        ):
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        output = payload.get("output")
        if not _is_json_value(output):
            continue
        outputs[_live_scope_key(record)] = output
    return outputs


def _latest_live_output_for_scope(
    *,
    records: list[dict[str, object]],
    run_id: str,
    node_id: str,
    frame_id: str | None,
    loop_node_id: str | None,
    iteration_index: int | None,
) -> JsonValue | object:
    output = _MISSING
    for record in records:
        if not _live_record_matches(
            record,
            run_id=run_id,
            node_id=node_id,
            event_type="node.succeeded",
            frame_id=frame_id,
            loop_node_id=loop_node_id,
            iteration_index=iteration_index,
        ):
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        candidate = payload.get("output")
        if _is_json_value(candidate):
            output = candidate
    return output


def _latest_live_inputs_for_scope(
    *,
    records: list[dict[str, object]],
    run_id: str,
    node_id: str,
    frame_id: str | None,
    loop_node_id: str | None,
    iteration_index: int | None,
) -> JsonValue | object:
    inputs = _MISSING
    for record in records:
        if not _live_record_matches(
            record,
            run_id=run_id,
            node_id=node_id,
            frame_id=frame_id,
            loop_node_id=loop_node_id,
            iteration_index=iteration_index,
        ):
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        for key in ("inputs", "resolved_inputs"):
            candidate = payload.get(key)
            if _is_json_value(candidate):
                inputs = candidate
    return inputs


def _latest_live_record_payload_for_scope(
    *,
    records: list[dict[str, object]],
    run_id: str,
    node_id: str,
    event_type: str | None,
    frame_id: str | None,
    loop_node_id: str | None,
    iteration_index: int | None,
) -> JsonValue | object:
    payload_value = _MISSING
    for record in records:
        if not _live_record_matches(
            record,
            run_id=run_id,
            node_id=node_id,
            event_type=event_type,
            frame_id=frame_id,
            loop_node_id=loop_node_id,
            iteration_index=iteration_index,
        ):
            continue
        payload = record.get("payload")
        if _is_json_value(payload):
            payload_value = payload
    return payload_value


def _live_record_matches(
    record: dict[str, object],
    *,
    run_id: str,
    node_id: str,
    event_type: str | None = None,
    frame_id: str | None = None,
    loop_node_id: str | None = None,
    iteration_index: int | None = None,
) -> bool:
    if record.get("run_id") != run_id or record.get("node_id") != node_id:
        return False
    if event_type is not None and record.get("event_type") != event_type:
        return False
    rec_frame_id, rec_loop_node_id, rec_iteration_index = _live_scope_key(record)
    if rec_frame_id != frame_id:
        return False
    if rec_loop_node_id != loop_node_id:
        return False
    if rec_iteration_index != iteration_index:
        return False
    return True


def _live_scope_key(
    record: dict[str, object],
) -> tuple[str | None, str | None, int | None]:
    frame_id_value = record.get("frame_id")
    loop_node_id_value = record.get("loop_node_id")
    iteration_index_value = record.get("iteration_index")
    frame_id = (
        frame_id_value
        if isinstance(frame_id_value, str) and frame_id_value != "root"
        else None
    )
    loop_node_id = loop_node_id_value if isinstance(loop_node_id_value, str) else None
    iteration_index = (
        iteration_index_value
        if isinstance(iteration_index_value, int) and not isinstance(iteration_index_value, bool)
        else None
    )
    return (frame_id, loop_node_id, iteration_index)


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


def _require_object(payload: object, kind: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise TypeError(f"{kind} must decode from a JSON object.")
    return payload


def _require_str(payload: dict[str, object], key: str, kind: str) -> str:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    raise TypeError(f"{kind}.{key} must be a non-empty string.")


def _optional_str(payload: dict[str, object], key: str, kind: str) -> str | None:
    value = payload.get(key)
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value
    raise TypeError(f"{kind}.{key} must be a string when present.")
