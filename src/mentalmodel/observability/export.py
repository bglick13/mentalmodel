from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import fields, is_dataclass
from pathlib import Path

from mentalmodel.core.interfaces import JsonValue, RuntimeValue
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.tracing import RecordedSpan


def serialize_runtime_value(value: RuntimeValue) -> JsonValue:
    """Convert a runtime value into a JSON-safe representation."""

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: serialize_runtime_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): serialize_runtime_value(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
        }
    if isinstance(value, tuple):
        return [serialize_runtime_value(item) for item in value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [serialize_runtime_value(item) for item in value]
    return {
        "type": type(value).__name__,
        "repr": repr(value),
    }


def execution_record_to_json(record: ExecutionRecord) -> dict[str, JsonValue]:
    """Return a JSON-safe projection of one execution record."""

    return {
        "record_id": record.record_id,
        "run_id": record.run_id,
        "node_id": record.node_id,
        "frame_id": record.frame.frame_id,
        "frame_path": serialize_runtime_value(record.frame.path),
        "loop_node_id": record.frame.loop_node_id,
        "iteration_index": record.frame.iteration_index,
        "event_type": record.event_type,
        "sequence": record.sequence,
        "timestamp_ms": record.timestamp_ms,
        "payload": serialize_runtime_value(record.payload),
    }


def recorded_span_to_json(span: RecordedSpan) -> dict[str, JsonValue]:
    """Return a JSON-safe projection of one recorded span."""

    return {
        "name": span.name,
        "start_time_ns": span.start_time_ns,
        "end_time_ns": span.end_time_ns,
        "duration_ns": span.end_time_ns - span.start_time_ns,
        "attributes": serialize_runtime_value(span.attributes),
        "frame_id": span.frame_id,
        "loop_node_id": span.loop_node_id,
        "iteration_index": span.iteration_index,
        "error_type": span.error_type,
        "error_message": span.error_message,
    }


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    """Write one JSON document with stable formatting."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {key: serialize_runtime_value(value) for key, value in payload.items()},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

def write_jsonl(path: Path, rows: Iterable[Mapping[str, object]]) -> None:
    """Write newline-delimited JSON with stable formatting."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded_rows = [
        json.dumps(
            {key: serialize_runtime_value(value) for key, value in row.items()},
            sort_keys=True,
        )
        for row in rows
    ]
    content = "\n".join(encoded_rows)
    if content:
        content += "\n"
    path.write_text(content, encoding="utf-8")
