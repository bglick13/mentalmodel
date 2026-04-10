"""Dashboard and UI surface exports."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "DashboardCatalogEntry",
    "DashboardCustomView",
    "DashboardMetricGroup",
    "DashboardPinnedNode",
    "DashboardTableColumn",
    "DashboardTableRowSource",
    "DashboardValueSelector",
    "DashboardExecutionSession",
    "DashboardService",
    "create_dashboard_app",
    "default_dashboard_catalog",
    "flatten_project_catalogs",
    "load_dashboard_catalog_subject",
    "load_project_catalog_subject",
    "validate_dashboard_catalog",
    "workspace_catalog_entries",
]

_LAZY_EXPORTS = {
    "create_dashboard_app": ("mentalmodel.ui.api", "create_dashboard_app"),
    "DashboardCatalogEntry": ("mentalmodel.ui.catalog", "DashboardCatalogEntry"),
    "DashboardMetricGroup": ("mentalmodel.ui.catalog", "DashboardMetricGroup"),
    "DashboardPinnedNode": ("mentalmodel.ui.catalog", "DashboardPinnedNode"),
    "default_dashboard_catalog": ("mentalmodel.ui.catalog", "default_dashboard_catalog"),
    "load_dashboard_catalog_subject": (
        "mentalmodel.ui.catalog",
        "load_dashboard_catalog_subject",
    ),
    "validate_dashboard_catalog": ("mentalmodel.ui.catalog", "validate_dashboard_catalog"),
    "DashboardCustomView": ("mentalmodel.ui.custom_views", "DashboardCustomView"),
    "DashboardTableColumn": ("mentalmodel.ui.custom_views", "DashboardTableColumn"),
    "DashboardTableRowSource": ("mentalmodel.ui.custom_views", "DashboardTableRowSource"),
    "DashboardValueSelector": ("mentalmodel.ui.custom_views", "DashboardValueSelector"),
    "DashboardExecutionSession": ("mentalmodel.ui.service", "DashboardExecutionSession"),
    "DashboardService": ("mentalmodel.ui.service", "DashboardService"),
    "flatten_project_catalogs": ("mentalmodel.ui.workspace", "flatten_project_catalogs"),
    "load_project_catalog_subject": (
        "mentalmodel.ui.workspace",
        "load_project_catalog_subject",
    ),
    "workspace_catalog_entries": ("mentalmodel.ui.workspace", "workspace_catalog_entries"),
}


def __getattr__(name: str) -> object:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_EXPORTS[name]
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
