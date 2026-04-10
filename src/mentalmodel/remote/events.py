from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.remote.contracts import RemoteContractError


class RemoteOperationKind(StrEnum):
    """Stable service-side operation identifiers for remote delivery events."""

    PROJECT_LINK = "project.link"
    CATALOG_PUBLISH = "project.publish_catalog"
    RUN_UPLOAD = "run.upload"
    LIVE_START = "live.start"
    LIVE_UPDATE = "live.update"
    LIVE_COMMIT = "live.commit"


class RemoteOperationStatus(StrEnum):
    """Outcome status for one remote delivery event."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(slots=True, frozen=True)
class RemoteOperationEvent:
    """Structured event emitted by the service for one remote-facing operation."""

    event_id: str
    occurred_at_ms: int
    kind: RemoteOperationKind
    status: RemoteOperationStatus
    project_id: str | None = None
    graph_id: str | None = None
    run_id: str | None = None
    invocation_name: str | None = None
    error_category: str | None = None
    error_message: str | None = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.event_id:
            raise RemoteContractError("RemoteOperationEvent.event_id cannot be empty.")
        if self.occurred_at_ms < 0:
            raise RemoteContractError(
                "RemoteOperationEvent.occurred_at_ms cannot be negative."
            )
        if self.error_category == "":
            raise RemoteContractError(
                "RemoteOperationEvent.error_category cannot be empty."
            )
        if self.error_message == "":
            raise RemoteContractError(
                "RemoteOperationEvent.error_message cannot be empty."
            )
        if self.status is RemoteOperationStatus.FAILED and self.error_message is None:
            raise RemoteContractError(
                "RemoteOperationEvent.error_message is required for failed events."
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "occurred_at_ms": self.occurred_at_ms,
            "kind": self.kind.value,
            "status": self.status.value,
            "project_id": self.project_id,
            "graph_id": self.graph_id,
            "run_id": self.run_id,
            "invocation_name": self.invocation_name,
            "error_category": self.error_category,
            "error_message": self.error_message,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RemoteOperationEvent:
        event_id = payload.get("event_id")
        occurred_at_ms = payload.get("occurred_at_ms")
        kind = payload.get("kind")
        status = payload.get("status")
        metadata = payload.get("metadata", {})
        if not isinstance(event_id, str):
            raise RemoteContractError("RemoteOperationEvent.event_id must be a string.")
        if not isinstance(occurred_at_ms, int):
            raise RemoteContractError(
                "RemoteOperationEvent.occurred_at_ms must be an integer."
            )
        if not isinstance(kind, str):
            raise RemoteContractError("RemoteOperationEvent.kind must be a string.")
        if not isinstance(status, str):
            raise RemoteContractError("RemoteOperationEvent.status must be a string.")
        if not isinstance(metadata, dict):
            raise RemoteContractError("RemoteOperationEvent.metadata must be an object.")
        return cls(
            event_id=event_id,
            occurred_at_ms=occurred_at_ms,
            kind=RemoteOperationKind(kind),
            status=RemoteOperationStatus(status),
            project_id=_optional_str(payload, "project_id"),
            graph_id=_optional_str(payload, "graph_id"),
            run_id=_optional_str(payload, "run_id"),
            invocation_name=_optional_str(payload, "invocation_name"),
            error_category=_optional_str(payload, "error_category"),
            error_message=_optional_str(payload, "error_message"),
            metadata={str(key): value for key, value in metadata.items()},
        )


@dataclass(slots=True, frozen=True)
class RemoteDeliveryHealthSummary:
    """Aggregated service-side health view for one project or run."""

    last_event_at_ms: int | None
    last_status: RemoteOperationStatus | None
    last_kind: RemoteOperationKind | None
    last_error_message: str | None
    recent_success_count: int
    recent_failure_count: int

    def __post_init__(self) -> None:
        if self.recent_success_count < 0:
            raise RemoteContractError(
                "RemoteDeliveryHealthSummary.recent_success_count cannot be negative."
            )
        if self.recent_failure_count < 0:
            raise RemoteContractError(
                "RemoteDeliveryHealthSummary.recent_failure_count cannot be negative."
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "last_event_at_ms": self.last_event_at_ms,
            "last_status": None if self.last_status is None else self.last_status.value,
            "last_kind": None if self.last_kind is None else self.last_kind.value,
            "last_error_message": self.last_error_message,
            "recent_success_count": self.recent_success_count,
            "recent_failure_count": self.recent_failure_count,
        }


def _optional_str(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RemoteContractError(f"RemoteOperationEvent.{key} must be a string.")
    return value
