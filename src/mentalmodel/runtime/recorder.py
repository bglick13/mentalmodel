from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.ir.records import ExecutionRecord
from mentalmodel.runtime.frame import ROOT_FRAME, ExecutionFrame


@dataclass(slots=True)
class ExecutionRecorder:
    """In-memory recorder for semantic execution events."""

    records: list[ExecutionRecord] = field(default_factory=list)
    last_run_id: str | None = None
    _sequence: int = 0

    def record(
        self,
        *,
        run_id: str,
        node_id: str,
        event_type: str,
        timestamp_ms: int,
        frame: ExecutionFrame = ROOT_FRAME,
        payload: Mapping[str, JsonValue] | None = None,
    ) -> ExecutionRecord:
        self.last_run_id = run_id
        self._sequence += 1
        record = ExecutionRecord(
            record_id=f"{run_id}:{self._sequence}",
            run_id=run_id,
            node_id=node_id,
            event_type=event_type,
            sequence=self._sequence,
            timestamp_ms=timestamp_ms,
            frame=frame,
            payload=dict(payload or {}),
        )
        self.records.append(record)
        return record
