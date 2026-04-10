from __future__ import annotations

from dataclasses import dataclass

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.runtime.runs import RunSummary


@dataclass(slots=True, frozen=True)
class DashboardRunAvailability:
    """Projection availability for one run as surfaced to the dashboard."""

    summary: bool
    records: bool
    spans: bool
    replay: bool
    custom_views: bool

    def as_dict(self) -> dict[str, bool]:
        return {
            "summary": self.summary,
            "records": self.records,
            "spans": self.spans,
            "replay": self.replay,
            "custom_views": self.custom_views,
        }


@dataclass(slots=True, frozen=True)
class DashboardRunHandle:
    """Unified run row for both active and persisted dashboard reads."""

    schema_version: int
    graph_id: str
    run_id: str
    created_at_ms: int
    status: str
    success: bool | None
    node_count: int
    edge_count: int
    record_count: int
    output_count: int
    state_count: int
    invocation_name: str | None
    runtime_default_profile_name: str | None
    runtime_profile_names: tuple[str, ...]
    trace_mode: str
    trace_service_name: str
    run_dir: str
    source: str
    execution_id: str | None = None
    availability: DashboardRunAvailability = DashboardRunAvailability(
        summary=True,
        records=True,
        spans=True,
        replay=True,
        custom_views=True,
    )

    def as_dict(self) -> dict[str, JsonValue]:
        availability: dict[str, JsonValue] = {
            "summary": self.availability.summary,
            "records": self.availability.records,
            "spans": self.availability.spans,
            "replay": self.availability.replay,
            "custom_views": self.availability.custom_views,
        }
        return {
            "schema_version": self.schema_version,
            "graph_id": self.graph_id,
            "run_id": self.run_id,
            "created_at_ms": self.created_at_ms,
            "status": self.status,
            "success": self.success,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "record_count": self.record_count,
            "output_count": self.output_count,
            "state_count": self.state_count,
            "invocation_name": self.invocation_name,
            "runtime_default_profile_name": self.runtime_default_profile_name,
            "runtime_profile_names": list(self.runtime_profile_names),
            "trace_mode": self.trace_mode,
            "trace_service_name": self.trace_service_name,
            "run_dir": self.run_dir,
            "source": self.source,
            "execution_id": self.execution_id,
            "availability": availability,
        }


def persisted_run_handle(summary: RunSummary) -> DashboardRunHandle:
    """Normalize one persisted bundle summary into the dashboard run shape."""

    return DashboardRunHandle(
        schema_version=summary.schema_version,
        graph_id=summary.graph_id,
        run_id=summary.run_id,
        created_at_ms=summary.created_at_ms,
        status="succeeded" if summary.success else "failed",
        success=summary.success,
        node_count=summary.node_count,
        edge_count=summary.edge_count,
        record_count=summary.record_count,
        output_count=summary.output_count,
        state_count=summary.state_count,
        invocation_name=summary.invocation_name,
        runtime_default_profile_name=summary.runtime_default_profile_name,
        runtime_profile_names=summary.runtime_profile_names,
        trace_mode=summary.trace_mode,
        trace_service_name=summary.trace_service_name,
        run_dir=str(summary.run_dir),
        source="persisted",
    )
