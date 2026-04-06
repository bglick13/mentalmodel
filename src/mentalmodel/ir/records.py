from __future__ import annotations

from dataclasses import dataclass, field

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.runtime.frame import ROOT_FRAME, ExecutionFrame


@dataclass(slots=True, frozen=True)
class ExecutionRecord:
    """Milestone 1 placeholder for future runtime execution records."""

    record_id: str
    run_id: str
    node_id: str
    event_type: str
    sequence: int
    timestamp_ms: int
    frame: ExecutionFrame = field(default_factory=lambda: ROOT_FRAME)
    payload: dict[str, JsonValue] = field(default_factory=dict)
