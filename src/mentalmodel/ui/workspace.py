from __future__ import annotations

import importlib
import importlib.util
import json
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import cast

from mentalmodel.errors import EntrypointLoadError
from mentalmodel.remote import ProjectCatalog, ProjectRegistration, WorkspaceConfig
from mentalmodel.ui.catalog import (
    DashboardCatalogEntry,
    DashboardMetricGroup,
    DashboardPinnedNode,
    catalog_entry_from_spec_path,
    validate_dashboard_catalog,
)
from mentalmodel.ui.custom_views import (
    DashboardCustomView,
    DashboardTableColumn,
    DashboardTableRowSource,
    DashboardValueSelector,
)


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


def resolve_project_catalog(project: ProjectRegistration) -> ProjectCatalog:
    """Resolve one project registration into a validated project catalog."""

    if project.catalog_provider is None:
        raise EntrypointLoadError(
            f"Project {project.project_id!r} does not declare a catalog provider."
        )
    with _temporary_import_root(project.root_dir):
        module_name, separator, attribute_name = project.catalog_provider.partition(":")
        if not separator or not module_name or not attribute_name:
            raise EntrypointLoadError(
                "Project catalog entrypoints must use 'module:attribute' format."
            )
        module = _import_provider_module(module_name, project.root_dir)
        subject = getattr(module, attribute_name, None)
        if subject is None:
            raise EntrypointLoadError(
                f"Module {module_name!r} does not define {attribute_name!r}."
            )
        provided = subject() if callable(subject) else subject
        if isinstance(provided, ProjectCatalog):
            if provided.project.project_id != project.project_id:
                raise EntrypointLoadError(
                    "Project catalog provider returned a mismatched project_id."
                )
            return ProjectCatalog(
                project=project,
                entries=provided.entries,
                description=provided.description,
                default_entry_id=provided.default_entry_id,
            )
        entries = _coerce_project_catalog_entries(provided, project)
        return ProjectCatalog(project=project, entries=entries)


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
        project_catalogs.append(resolve_project_catalog(project))
    return flatten_project_catalogs(tuple(project_catalogs))


def workspace_project_catalogs(workspace: WorkspaceConfig) -> tuple[ProjectCatalog, ...]:
    """Resolve all enabled project catalogs for one workspace config."""

    catalogs: list[ProjectCatalog] = []
    for project in workspace.projects:
        if not project.enabled or project.catalog_provider is None:
            continue
        catalogs.append(resolve_project_catalog(project))
    return tuple(catalogs)


def workspace_projects_payload(workspace: WorkspaceConfig) -> tuple[dict[str, object], ...]:
    """Return UI-safe project registry metadata for one workspace."""

    payload: list[dict[str, object]] = []
    for project in workspace.projects:
        payload.append(project.as_dict())
    return tuple(payload)


def _coerce_project_catalog_entries(
    provided: object,
    project: ProjectRegistration,
) -> tuple[DashboardCatalogEntry, ...]:
    entries = _coerce_dashboard_entries(provided)
    if entries is not None:
        return entries
    if not isinstance(provided, Iterable) or isinstance(provided, (str, bytes)):
        raise EntrypointLoadError(
            "Project catalog providers must return ProjectCatalog, DashboardCatalogEntry "
            "items, or spec-catalog entries."
        )
    synthesized: list[DashboardCatalogEntry] = []
    for item in tuple(cast(object, entry) for entry in provided):
        synthesized.append(_catalog_entry_from_spec_object(project, item))
    return validate_dashboard_catalog(tuple(synthesized))


def _coerce_dashboard_entries(
    provided: object,
) -> tuple[DashboardCatalogEntry, ...] | None:
    if isinstance(provided, DashboardCatalogEntry):
        return (provided,)
    if not isinstance(provided, Iterable) or isinstance(provided, (str, bytes)):
        return None
    items = tuple(cast(object, item) for item in provided)
    if not items:
        return ()
    if all(isinstance(item, DashboardCatalogEntry) for item in items):
        return validate_dashboard_catalog(
            cast(tuple[DashboardCatalogEntry, ...], items)
        )
    return None


def _catalog_entry_from_spec_object(
    project: ProjectRegistration,
    payload: object,
) -> DashboardCatalogEntry:
    label = _required_object_str(payload, "label")
    spec_path = Path(_required_object_str(payload, "spec_path")).expanduser().resolve()
    graph_id = _optional_object_str(payload, "graph_id")
    category = _optional_object_str(payload, "category") or "custom"
    description = _optional_object_str(payload, "description") or str(spec_path)
    invocation_name = _optional_object_str(payload, "invocation_name")
    default_loop_node_id = _optional_object_str(payload, "default_loop_node_id")
    tags = _optional_object_str_sequence(payload, "tags") or (category,)
    metric_groups = _optional_metric_groups(payload)
    pinned_nodes = _optional_pinned_nodes(payload)
    custom_views = _optional_custom_views(payload)
    spec_id = f"{project.project_id}:{_slugify(label)}"
    if graph_id is not None:
        entry = _catalog_entry_from_declared_spec(
            spec_id=spec_id,
            spec_path=spec_path,
            graph_id=graph_id,
            invocation_name=invocation_name,
            category=category,
            description=description,
        )
    else:
        try:
            entry = catalog_entry_from_spec_path(spec_path)
        except Exception as exc:
            entry = _catalog_entry_from_external_spec(
                project=project,
                spec_path=spec_path,
                invocation_name=invocation_name,
                category=category,
                description=description,
                cause=exc,
            )
    return replace(
        entry,
        spec_id=spec_id,
        label=label,
        description=description,
        invocation_name=invocation_name or entry.invocation_name,
        category=category,
        tags=tags,
        default_loop_node_id=default_loop_node_id,
        metric_groups=metric_groups,
        pinned_nodes=pinned_nodes,
        custom_views=custom_views,
    )


@contextmanager
def _temporary_import_root(root_dir: Path) -> Iterator[None]:
    resolved = str(root_dir.expanduser().resolve())
    if resolved in sys.path:
        yield
        return
    sys.path.insert(0, resolved)
    try:
        yield
    finally:
        if resolved in sys.path:
            sys.path.remove(resolved)


def _import_provider_module(module_name: str, root_dir: Path) -> ModuleType:
    try:
        return importlib.import_module(module_name)
    except Exception as exc:
        module = _load_module_from_root(module_name, root_dir)
        if module is None:
            raise exc
        return module


def _load_module_from_root(module_name: str, root_dir: Path) -> ModuleType | None:
    module_path = root_dir / Path(*module_name.split("."))
    candidate = module_path.with_suffix(".py")
    if not candidate.is_file():
        return None
    synthetic_name = f"_mentalmodel_external_{module_name.replace('.', '_')}"
    spec = importlib.util.spec_from_file_location(synthetic_name, candidate)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[synthetic_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(synthetic_name, None)
        raise
    return module


def _required_object_str(payload: object, field_name: str) -> str:
    value = _optional_object_str(payload, field_name)
    if value is None:
        raise EntrypointLoadError(
            f"Project catalog spec entry is missing required field {field_name!r}."
        )
    return value


def _optional_object_str(payload: object, field_name: str) -> str | None:
    if isinstance(payload, dict):
        value = payload.get(field_name)
    else:
        value = getattr(payload, field_name, None)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise EntrypointLoadError(
        f"Project catalog spec entry field {field_name!r} must be a string."
    )


def _optional_object_str_sequence(
    payload: object,
    field_name: str,
) -> tuple[str, ...] | None:
    if isinstance(payload, dict):
        value = payload.get(field_name)
    else:
        value = getattr(payload, field_name, None)
    if value is None:
        return None
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
        raise EntrypointLoadError(
            f"Project catalog spec entry field {field_name!r} must be a sequence of strings."
        )
    items: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise EntrypointLoadError(
                f"Project catalog spec entry field {field_name!r} must contain only strings."
            )
        items.append(item)
    return tuple(items)


def _optional_metric_groups(payload: object) -> tuple[DashboardMetricGroup, ...]:
    raw_groups = _optional_object_sequence(payload, "metric_groups")
    if raw_groups is None:
        return ()
    groups: list[DashboardMetricGroup] = []
    for group in raw_groups:
        group_id = _required_object_str(group, "group_id")
        title = _required_object_str(group, "title")
        metric_path_prefixes = _optional_object_str_sequence(group, "metric_path_prefixes")
        if not metric_path_prefixes:
            raise EntrypointLoadError(
                "Project catalog metric_groups entries require metric_path_prefixes."
            )
        description = _optional_object_str(group, "description") or ""
        max_items = _optional_object_int(group, "max_items") or 8
        groups.append(
            DashboardMetricGroup(
                group_id=group_id,
                title=title,
                description=description,
                metric_path_prefixes=metric_path_prefixes,
                max_items=max_items,
            )
        )
    return tuple(groups)


def _optional_pinned_nodes(payload: object) -> tuple[DashboardPinnedNode, ...]:
    raw_nodes = _optional_object_sequence(payload, "pinned_nodes")
    if raw_nodes is None:
        return ()
    nodes: list[DashboardPinnedNode] = []
    for node in raw_nodes:
        nodes.append(
            DashboardPinnedNode(
                node_id=_required_object_str(node, "node_id"),
                title=_required_object_str(node, "title"),
                description=_optional_object_str(node, "description") or "",
            )
        )
    return tuple(nodes)


def _optional_custom_views(payload: object) -> tuple[DashboardCustomView, ...]:
    raw_views = _optional_object_sequence(payload, "custom_views")
    if raw_views is None:
        return ()
    views: list[DashboardCustomView] = []
    for view in raw_views:
        kind = _required_object_str(view, "kind")
        row_source = _required_object(view, "row_source")
        raw_columns = _optional_object_sequence(view, "columns")
        if not raw_columns:
            raise EntrypointLoadError("Project catalog custom views require columns.")
        columns: list[DashboardTableColumn] = []
        for column in raw_columns:
            selector_payload = _required_object(column, "selector")
            columns.append(
                DashboardTableColumn(
                    column_id=_required_object_str(column, "column_id"),
                    title=_required_object_str(column, "title"),
                    description=_optional_object_str(column, "description") or "",
                    selector=DashboardValueSelector(
                        kind=_required_object_str(selector_payload, "kind"),
                        path=_optional_object_str(selector_payload, "path"),
                        node_id=_optional_object_str(selector_payload, "node_id"),
                        event_type=_optional_object_str(selector_payload, "event_type"),
                    ),
                )
            )
        views.append(
            DashboardCustomView(
                view_id=_required_object_str(view, "view_id"),
                title=_required_object_str(view, "title"),
                description=_optional_object_str(view, "description") or "",
                kind=kind,
                row_source=DashboardTableRowSource(
                    kind=_required_object_str(row_source, "kind"),
                    node_id=_required_object_str(row_source, "node_id"),
                    items_path=_required_object_str(row_source, "items_path"),
                    loop_node_id=_optional_object_str(row_source, "loop_node_id"),
                ),
                columns=tuple(columns),
            )
        )
    return tuple(views)


def _optional_object_sequence(payload: object, field_name: str) -> tuple[object, ...] | None:
    if isinstance(payload, dict):
        value = payload.get(field_name)
    else:
        value = getattr(payload, field_name, None)
    if value is None:
        return None
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
        raise EntrypointLoadError(
            f"Project catalog spec entry field {field_name!r} must be a sequence."
        )
    return tuple(cast(object, item) for item in value)


def _required_object(payload: object, field_name: str) -> object:
    if isinstance(payload, dict):
        value = payload.get(field_name)
    else:
        value = getattr(payload, field_name, None)
    if value is None:
        raise EntrypointLoadError(
            f"Project catalog spec entry is missing required field {field_name!r}."
        )
    return cast(object, value)


def _optional_object_int(payload: object, field_name: str) -> int | None:
    if isinstance(payload, dict):
        value = payload.get(field_name)
    else:
        value = getattr(payload, field_name, None)
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise EntrypointLoadError(
        f"Project catalog spec entry field {field_name!r} must be an integer."
    )


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return normalized or "entry"


def _catalog_entry_from_external_spec(
    *,
    project: ProjectRegistration,
    spec_path: Path,
    invocation_name: str | None,
    category: str,
    description: str,
    cause: Exception,
) -> DashboardCatalogEntry:
    payload = _load_external_spec_metadata(project.root_dir, spec_path, cause)
    graph_id = payload.get("graph_id")
    resolved_invocation_name = payload.get("invocation_name")
    if not isinstance(graph_id, str) or not graph_id:
        raise EntrypointLoadError("External spec metadata must include graph_id.")
    if resolved_invocation_name is not None and not isinstance(resolved_invocation_name, str):
        raise EntrypointLoadError(
            "External spec metadata invocation_name must be a string when present."
        )
    return DashboardCatalogEntry(
        spec_id=f"path-{_slugify(spec_path.stem)}",
        label=spec_path.stem,
        description=description,
        spec_path=spec_path,
        graph_id=graph_id,
        invocation_name=invocation_name or resolved_invocation_name or "verify",
        catalog_source="module-provider",
        category=category,
        tags=(category,),
    )


def _catalog_entry_from_declared_spec(
    *,
    spec_id: str,
    spec_path: Path,
    graph_id: str,
    invocation_name: str | None,
    category: str,
    description: str,
) -> DashboardCatalogEntry:
    if not spec_path.is_file():
        raise EntrypointLoadError(f"Spec file not found: {spec_path}")
    if invocation_name is None:
        raise EntrypointLoadError(
            "Project catalog spec entries that declare graph_id must also declare invocation_name."
        )
    return DashboardCatalogEntry(
        spec_id=spec_id,
        label=spec_path.stem,
        description=description,
        spec_path=spec_path,
        graph_id=graph_id,
        invocation_name=invocation_name,
        catalog_source="module-provider",
        category=category,
        tags=(category,),
    )


def _load_external_spec_metadata(
    root_dir: Path,
    spec_path: Path,
    cause: Exception,
) -> dict[str, object]:
    command = [
        "uv",
        "run",
        "--directory",
        str(root_dir),
        "python",
        "-c",
        _EXTERNAL_SPEC_METADATA_SCRIPT,
        str(spec_path),
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or str(cause)
        raise EntrypointLoadError(
            f"Failed to load verify spec {spec_path!r}: {message}"
        ) from cause
    decoded = json.loads(completed.stdout)
    if not isinstance(decoded, dict):
        raise EntrypointLoadError(
            f"Failed to load verify spec {spec_path!r}: external helper returned non-object JSON."
        )
    return cast(dict[str, object], decoded)


_EXTERNAL_SPEC_METADATA_SCRIPT = """
import json
import sys
from pathlib import Path

from mentalmodel.invocation import load_workflow_subject, read_verify_invocation_spec
from mentalmodel.ir.lowering import lower_program

spec_path = Path(sys.argv[1])
invocation = read_verify_invocation_spec(spec_path)
_, program = load_workflow_subject(invocation.program)
graph = lower_program(program)
print(
    json.dumps(
        {
            "graph_id": graph.graph_id,
            "invocation_name": invocation.invocation_name,
        }
    )
)
""".strip()
