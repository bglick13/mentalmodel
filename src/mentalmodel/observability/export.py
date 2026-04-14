from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.observability.serialization import serialize_runtime_value
from mentalmodel.observability.telemetry import (
    TelemetryMapper,
    execution_record_json_from_log,
    recorded_span_json_from_span,
)
from mentalmodel.observability.tracing import RecordedSpan

_MAPPER = TelemetryMapper()


def execution_record_to_json(record: ExecutionRecord) -> dict[str, JsonValue]:
    """Return a JSON-safe projection of one execution record."""

    return execution_record_json_from_log(_MAPPER.execution_record_to_log(record))


def recorded_span_to_json(span: RecordedSpan) -> dict[str, JsonValue]:
    """Return a JSON-safe projection of one recorded span."""

    return recorded_span_json_from_span(_MAPPER.recorded_span_to_span(span))


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
