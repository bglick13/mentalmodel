from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from mentalmodel.errors import MentalModelError


class DashboardCatalogError(MentalModelError):
    """Raised when a dashboard catalog entry cannot be resolved."""


@dataclass(slots=True, frozen=True)
class DashboardCatalogEntry:
    """One launchable workflow configuration exposed in the dashboard."""

    spec_id: str
    label: str
    description: str
    spec_path: Path
    graph_id: str
    invocation_name: str

    def as_dict(self) -> dict[str, str]:
        return {
            "spec_id": self.spec_id,
            "label": self.label,
            "description": self.description,
            "spec_path": str(self.spec_path),
            "graph_id": self.graph_id,
            "invocation_name": self.invocation_name,
        }


def default_dashboard_catalog() -> tuple[DashboardCatalogEntry, ...]:
    """Return the built-in dashboard proof catalog."""

    package_root = files("mentalmodel.examples.review_workflow")
    fixture_path = Path(str(package_root.joinpath("review_workflow_fixture.toml")))
    strict_path = Path(str(package_root.joinpath("review_workflow_strict.toml")))
    return (
        DashboardCatalogEntry(
            spec_id="review-workflow-fixture",
            label="Ticket Review Fixture",
            description=(
                "Runs the ticket review workflow with the baseline fixture review "
                "policy."
            ),
            spec_path=fixture_path,
            graph_id="review_workflow",
            invocation_name="review_workflow_fixture",
        ),
        DashboardCatalogEntry(
            spec_id="review-workflow-strict",
            label="Ticket Review Strict",
            description=(
                "Runs the ticket review workflow with the stricter escalation "
                "policy."
            ),
            spec_path=strict_path,
            graph_id="review_workflow",
            invocation_name="review_workflow_strict",
        ),
    )


def resolve_catalog_entry(spec_id: str) -> DashboardCatalogEntry:
    """Resolve one built-in catalog entry by id."""

    for entry in default_dashboard_catalog():
        if entry.spec_id == spec_id:
            return entry
    raise DashboardCatalogError(f"Unknown dashboard catalog entry {spec_id!r}.")
