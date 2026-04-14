from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass
from pathlib import Path

from mentalmodel.core.interfaces import JsonValue, RuntimeValue


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
