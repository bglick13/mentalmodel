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
    catalog_entry_from_spec_path,
    validate_dashboard_catalog,
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
    category = _optional_object_str(payload, "category") or "custom"
    description = _optional_object_str(payload, "description") or str(spec_path)
    invocation_name = _optional_object_str(payload, "invocation_name")
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
    spec_id = f"{project.project_id}:{_slugify(label)}"
    return replace(
        entry,
        spec_id=spec_id,
        label=label,
        description=description,
        invocation_name=invocation_name or entry.invocation_name,
        category=category,
        tags=(category,),
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
