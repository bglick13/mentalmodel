from __future__ import annotations

import importlib
from dataclasses import replace

from mentalmodel.errors import EntrypointLoadError
from mentalmodel.remote import ProjectCatalog, WorkspaceConfig
from mentalmodel.ui.catalog import DashboardCatalogEntry, validate_dashboard_catalog


def load_project_catalog_subject(raw: str) -> tuple[object, ProjectCatalog]:
    """Load one project catalog provider entrypoint."""

    module_name, separator, attribute_name = raw.partition(":")
    if not separator or not module_name or not attribute_name:
        raise EntrypointLoadError(
            "Project catalog entrypoints must use 'module:attribute' format."
        )
    module = importlib.import_module(module_name)
    subject = getattr(module, attribute_name, None)
    if subject is None:
        raise EntrypointLoadError(
            f"Module {module_name!r} does not define {attribute_name!r}."
        )
    provided = subject() if callable(subject) else subject
    if not isinstance(provided, ProjectCatalog):
        raise EntrypointLoadError(
            "Project catalog entrypoints must return a ProjectCatalog."
        )
    return module, provided


def flatten_project_catalogs(
    project_catalogs: tuple[ProjectCatalog, ...],
) -> tuple[DashboardCatalogEntry, ...]:
    """Flatten project catalogs into one validated dashboard catalog."""

    entries: list[DashboardCatalogEntry] = []
    for project_catalog in project_catalogs:
        for entry in project_catalog.entries:
            entries.append(
                replace(
                    entry,
                    project_id=project_catalog.project.project_id,
                    project_label=project_catalog.project.label,
                    catalog_source=entry.catalog_source or "module-provider",
                )
            )
    return validate_dashboard_catalog(tuple(entries))


def workspace_catalog_entries(workspace: WorkspaceConfig) -> tuple[DashboardCatalogEntry, ...]:
    """Resolve all enabled project catalogs for one workspace config."""

    project_catalogs: list[ProjectCatalog] = []
    for project in workspace.projects:
        if not project.enabled or project.catalog_provider is None:
            continue
        _module, catalog = load_project_catalog_subject(project.catalog_provider)
        project_catalogs.append(catalog)
    return flatten_project_catalogs(tuple(project_catalogs))


def workspace_projects_payload(workspace: WorkspaceConfig) -> tuple[dict[str, object], ...]:
    """Return UI-safe project registry metadata for one workspace."""

    payload: list[dict[str, object]] = []
    for project in workspace.projects:
        payload.append(project.as_dict())
    return tuple(payload)
