from __future__ import annotations

import hashlib
import importlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import cast

from mentalmodel.errors import EntrypointLoadError, MentalModelError
from mentalmodel.invocation import load_workflow_subject, read_verify_invocation_spec
from mentalmodel.ir.lowering import lower_program


class DashboardCatalogError(MentalModelError):
    """Raised when a dashboard catalog entry cannot be resolved."""


@dataclass(slots=True, frozen=True)
class DashboardMetricGroup:
    """One overview metric group rendered by the dashboard."""

    group_id: str
    title: str
    metric_path_prefixes: tuple[str, ...]
    description: str = ""
    max_items: int = 8

    def as_dict(self) -> dict[str, object]:
        return {
            "group_id": self.group_id,
            "title": self.title,
            "description": self.description,
            "metric_path_prefixes": list(self.metric_path_prefixes),
            "max_items": self.max_items,
        }


@dataclass(slots=True, frozen=True)
class DashboardPinnedNode:
    """One node the dashboard should surface as a quick drill-down target."""

    node_id: str
    title: str
    description: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "node_id": self.node_id,
            "title": self.title,
            "description": self.description,
        }


@dataclass(slots=True, frozen=True)
class DashboardCatalogEntry:
    """One launchable workflow configuration exposed in the dashboard."""

    spec_id: str
    label: str
    description: str
    spec_path: Path
    graph_id: str
    invocation_name: str
    project_id: str | None = None
    project_label: str | None = None
    catalog_source: str | None = None
    category: str = "default"
    tags: tuple[str, ...] = ()
    default_loop_node_id: str | None = None
    metric_groups: tuple[DashboardMetricGroup, ...] = ()
    pinned_nodes: tuple[DashboardPinnedNode, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "spec_id": self.spec_id,
            "label": self.label,
            "description": self.description,
            "spec_path": str(self.spec_path),
            "graph_id": self.graph_id,
            "invocation_name": self.invocation_name,
            "project_id": self.project_id,
            "project_label": self.project_label,
            "catalog_source": self.catalog_source,
            "category": self.category,
            "tags": list(self.tags),
            "default_loop_node_id": self.default_loop_node_id,
            "metric_groups": [group.as_dict() for group in self.metric_groups],
            "pinned_nodes": [node.as_dict() for node in self.pinned_nodes],
        }


def default_dashboard_catalog() -> tuple[DashboardCatalogEntry, ...]:
    """Return the built-in dashboard proof catalog."""

    package_root = files("mentalmodel.examples.review_workflow")
    fixture_path = Path(str(package_root.joinpath("review_workflow_fixture.toml")))
    strict_path = Path(str(package_root.joinpath("review_workflow_strict.toml")))
    queue_metrics = DashboardMetricGroup(
        group_id="resolution-metrics",
        title="Resolution Metrics",
        description="Run outcome metrics derived from persisted queue summary outputs.",
        metric_path_prefixes=("queue_summary.",),
        max_items=6,
    )
    review_nodes = (
        DashboardPinnedNode(
            node_id="queue_summary",
            title="Queue Summary",
            description="Root run summary with processed, escalated, and auto-published counts.",
        ),
        DashboardPinnedNode(
            node_id="review_ticket",
            title="Review Decisions",
            description="Loop body decision node for per-ticket inspection.",
        ),
    )
    return validate_dashboard_catalog(
        (
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
                project_id="mentalmodel-examples",
                project_label="Mentalmodel Examples",
                catalog_source="builtin",
                category="examples",
                tags=("fixture", "review"),
                default_loop_node_id="ticket_review_loop",
                metric_groups=(queue_metrics,),
                pinned_nodes=review_nodes,
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
                project_id="mentalmodel-examples",
                project_label="Mentalmodel Examples",
                catalog_source="builtin",
                category="examples",
                tags=("strict", "review"),
                default_loop_node_id="ticket_review_loop",
                metric_groups=(queue_metrics,),
                pinned_nodes=review_nodes,
            ),
        )
    )


def validate_dashboard_catalog(
    entries: Sequence[DashboardCatalogEntry],
) -> tuple[DashboardCatalogEntry, ...]:
    """Validate and normalize one dashboard catalog."""

    seen_spec_ids: set[str] = set()
    seen_run_keys: set[tuple[str, str]] = set()
    normalized: list[DashboardCatalogEntry] = []
    for entry in entries:
        if entry.spec_id in seen_spec_ids:
            raise DashboardCatalogError(
                f"Duplicate dashboard spec_id {entry.spec_id!r}."
            )
        seen_spec_ids.add(entry.spec_id)
        run_key = (entry.project_id, entry.graph_id, entry.invocation_name)
        if run_key in seen_run_keys:
            raise DashboardCatalogError(
                "Duplicate dashboard run key "
                f"{entry.project_id!r}/{entry.graph_id!r}/{entry.invocation_name!r}."
            )
        seen_run_keys.add(run_key)
        if not entry.spec_path.is_file():
            raise DashboardCatalogError(
                f"Dashboard spec path does not exist: {entry.spec_path}"
            )
        if not entry.label:
            raise DashboardCatalogError("Dashboard catalog entries require a label.")
        if not entry.metric_groups and not entry.pinned_nodes:
            normalized.append(entry)
            continue
        _validate_metric_groups(entry)
        normalized.append(entry)
    return tuple(normalized)


def resolve_catalog_entry(
    spec_id: str,
    entries: Sequence[DashboardCatalogEntry],
) -> DashboardCatalogEntry:
    """Resolve one dashboard catalog entry by id."""

    for entry in entries:
        if entry.spec_id == spec_id:
            return entry
    raise DashboardCatalogError(f"Unknown dashboard catalog entry {spec_id!r}.")


def catalog_entry_from_spec_path(spec_path: Path) -> DashboardCatalogEntry:
    """Build a catalog entry from an on-disk verify TOML (any absolute or resolved path)."""

    path = spec_path.expanduser().resolve()
    if not path.is_file():
        raise DashboardCatalogError(f"Spec file not found: {path}")
    try:
        invocation = read_verify_invocation_spec(path)
        _module, program = load_workflow_subject(invocation.program)
    except Exception as exc:
        raise DashboardCatalogError(
            f"Failed to load verify spec {path!r}: {exc}"
        ) from exc
    graph = lower_program(program)
    inv_name = invocation.invocation_name or "verify"
    digest = hashlib.sha256(str(path).encode()).hexdigest()[:12]
    spec_id = f"path-{digest}"
    return DashboardCatalogEntry(
        spec_id=spec_id,
        label=path.stem,
        description=str(path),
        spec_path=path,
        graph_id=graph.graph_id,
        invocation_name=inv_name,
        catalog_source="spec-path",
        category="custom",
        tags=("spec-path",),
        default_loop_node_id=None,
        metric_groups=(),
        pinned_nodes=(),
    )


def load_dashboard_catalog_subject(
    raw: str,
) -> tuple[object, tuple[DashboardCatalogEntry, ...]]:
    """Load one dashboard catalog provider entrypoint."""

    module_name, separator, attribute_name = raw.partition(":")
    if not separator or not module_name or not attribute_name:
        raise EntrypointLoadError(
            "Dashboard catalog entrypoints must use 'module:attribute' format."
        )
    module = importlib.import_module(module_name)
    subject = getattr(module, attribute_name, None)
    if subject is None:
        raise EntrypointLoadError(
            f"Module {module_name!r} does not define {attribute_name!r}."
        )
    if callable(subject):
        provided = subject()
    else:
        provided = subject
    entries = _coerce_catalog_entries(provided)
    return module, validate_dashboard_catalog(entries)


def _coerce_catalog_entries(
    provided: object,
) -> tuple[DashboardCatalogEntry, ...]:
    if isinstance(provided, DashboardCatalogEntry):
        return (provided,)
    if not isinstance(provided, Iterable) or isinstance(provided, (str, bytes)):
        raise EntrypointLoadError(
            "Dashboard catalog providers must return a sequence of DashboardCatalogEntry objects."
        )
    entries = tuple(cast(object, item) for item in provided)
    for item in entries:
        if not isinstance(item, DashboardCatalogEntry):
            raise EntrypointLoadError(
                "Dashboard catalog providers must return only DashboardCatalogEntry objects."
            )
    return cast(tuple[DashboardCatalogEntry, ...], entries)


def _validate_metric_groups(entry: DashboardCatalogEntry) -> None:
    seen_group_ids: set[str] = set()
    for group in entry.metric_groups:
        if not group.group_id:
            raise DashboardCatalogError(
                f"Dashboard metric group ids cannot be empty for {entry.spec_id!r}."
            )
        if group.group_id in seen_group_ids:
            raise DashboardCatalogError(
                f"Duplicate dashboard metric group id {group.group_id!r} for {entry.spec_id!r}."
            )
        seen_group_ids.add(group.group_id)
        if not group.metric_path_prefixes:
            raise DashboardCatalogError(
                "Dashboard metric group "
                f"{group.group_id!r} must declare at least one metric path prefix."
            )
        if group.max_items <= 0:
            raise DashboardCatalogError(
                f"Dashboard metric group {group.group_id!r} max_items must be positive."
            )
