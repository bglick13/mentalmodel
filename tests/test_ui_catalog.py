from __future__ import annotations

import sys
import types
import unittest

from mentalmodel.ui.catalog import (
    DashboardCatalogEntry,
    DashboardCatalogError,
    default_dashboard_catalog,
    load_dashboard_catalog_subject,
    validate_dashboard_catalog,
)


class DashboardCatalogTest(unittest.TestCase):
    def test_validate_dashboard_catalog_rejects_duplicate_spec_ids(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        duplicate = DashboardCatalogEntry(
            spec_id=fixture_entry.spec_id,
            label="Duplicate",
            description="duplicate",
            spec_path=fixture_entry.spec_path,
            graph_id="other_graph",
            invocation_name="other_invocation",
        )
        with self.assertRaises(DashboardCatalogError):
            validate_dashboard_catalog((fixture_entry, duplicate))

    def test_validate_dashboard_catalog_rejects_duplicate_run_keys(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        duplicate = DashboardCatalogEntry(
            spec_id="duplicate-spec",
            label="Duplicate",
            description="duplicate",
            spec_path=fixture_entry.spec_path,
            graph_id=fixture_entry.graph_id,
            invocation_name=fixture_entry.invocation_name,
        )
        with self.assertRaises(DashboardCatalogError):
            validate_dashboard_catalog((fixture_entry, duplicate))

    def test_load_dashboard_catalog_subject_accepts_callable_provider(self) -> None:
        fixture_entry = default_dashboard_catalog()[0]
        module_name = "mentalmodel.tests.synthetic_dashboard_catalog"
        module = types.ModuleType(module_name)
        module.__dict__["dashboard_catalog"] = lambda: (fixture_entry,)
        sys.modules[module_name] = module
        try:
            loaded_module, entries = load_dashboard_catalog_subject(
                f"{module_name}:dashboard_catalog"
            )
        finally:
            sys.modules.pop(module_name, None)
        self.assertIs(loaded_module, module)
        self.assertEqual(entries, (fixture_entry,))
