from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.remote.backend import (
    InMemoryArtifactStore,
    InMemoryManifestIndex,
    InMemoryProjectIndex,
    RemoteCompletedRunSink,
    RemoteProjectStore,
    RemoteRunStore,
)
from mentalmodel.remote.contracts import (
    ProjectCatalogSnapshot,
    RemoteProjectCatalogPublishRequest,
    RemoteProjectLinkRequest,
)
from mentalmodel.remote.store import FileRemoteRunStore
from mentalmodel.remote.sync import build_run_bundle_upload
from mentalmodel.testing import run_verification
from mentalmodel.ui.catalog import default_dashboard_catalog


class RemoteStoreTest(unittest.TestCase):
    def test_file_remote_run_store_ingests_uploaded_bundle(self) -> None:
        with (
            tempfile.TemporaryDirectory() as local_tmp,
            tempfile.TemporaryDirectory() as remote_tmp,
        ):
            local_root = Path(local_tmp)
            remote_root = Path(remote_tmp)
            report = run_verification(build_program(), runs_dir=local_root)
            self.assertTrue(report.success)
            self.assertIsNotNone(report.runtime.run_id)
            assert report.runtime.run_id is not None
            upload = build_run_bundle_upload(
                runs_dir=local_root,
                graph_id="async_rl_demo",
                run_id=report.runtime.run_id,
                project_id="mentalmodel-examples",
                project_label="Mentalmodel Examples",
            )
            store = FileRemoteRunStore(root_dir=remote_root)
            run_dir = store.ingest(upload)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue(
                (
                    remote_root
                    / ".remote"
                    / "manifests"
                    / "async_rl_demo"
                    / f"{report.runtime.run_id}.json"
                ).exists()
            )

    def test_remote_run_store_indexes_and_materializes_uploaded_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as local_tmp, tempfile.TemporaryDirectory() as cache_tmp:
            local_root = Path(local_tmp)
            cache_root = Path(cache_tmp)
            report = run_verification(build_program(), runs_dir=local_root)
            self.assertTrue(report.success)
            self.assertIsNotNone(report.runtime.run_id)
            assert report.runtime.run_id is not None
            upload = build_run_bundle_upload(
                runs_dir=local_root,
                graph_id="async_rl_demo",
                run_id=report.runtime.run_id,
                project_id="mentalmodel-examples",
                project_label="Mentalmodel Examples",
            )
            store = RemoteRunStore(
                manifest_index=InMemoryManifestIndex(),
                artifact_store=InMemoryArtifactStore(),
                cache_dir=cache_root,
            )
            run_dir = store.ingest(upload)
            self.assertTrue((run_dir / "summary.json").exists())
            summaries = store.list_run_summaries(graph_id="async_rl_demo")
            self.assertEqual(len(summaries), 1)
            self.assertEqual(summaries[0].run_id, report.runtime.run_id)
            resolved = store.resolve_run_summary(
                graph_id="async_rl_demo",
                run_id=report.runtime.run_id,
            )
            self.assertEqual(resolved.graph_id, "async_rl_demo")
            self.assertTrue(
                (cache_root / ".runs" / "async_rl_demo" / report.runtime.run_id).exists()
            )

    def test_remote_completed_run_sink_indexes_existing_local_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as local_tmp, tempfile.TemporaryDirectory() as cache_tmp:
            local_root = Path(local_tmp)
            cache_root = Path(cache_tmp)
            report = run_verification(build_program(), runs_dir=local_root)
            self.assertTrue(report.success)
            self.assertIsNotNone(report.runtime.run_id)
            self.assertIsNotNone(report.runtime.run_artifacts_dir)
            assert report.runtime.run_id is not None
            assert report.runtime.run_artifacts_dir is not None
            manifest_index = InMemoryManifestIndex()
            store = RemoteRunStore(
                manifest_index=manifest_index,
                artifact_store=InMemoryArtifactStore(),
                cache_dir=cache_root,
            )
            sink = RemoteCompletedRunSink(
                store,
                project_id="mentalmodel-examples",
                project_label="Mentalmodel Examples",
            )
            run_dir = Path(report.runtime.run_artifacts_dir)
            sink.publish_run_dir(run_dir)
            summaries = store.list_run_summaries(
                graph_id="async_rl_demo",
                invocation_name=report.runtime.invocation_name,
            )
            self.assertEqual(len(summaries), 1)
            indexed = manifest_index.get_run(
                graph_id="async_rl_demo",
                run_id=report.runtime.run_id,
            )
            self.assertEqual(indexed.manifest.project_id, "mentalmodel-examples")
            self.assertEqual(indexed.manifest.project_label, "Mentalmodel Examples")

    def test_remote_project_store_links_and_lists_projects(self) -> None:
        store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        payload = RemoteProjectLinkRequest(
            project_id="pangramanizer",
            label="Pangramanizer",
            default_environment="prod",
            catalog_provider="pangramanizer.dashboard:catalog",
            default_runs_dir=".runs",
            default_verify_spec="verification/real_smoke.toml",
            catalog_snapshot=ProjectCatalogSnapshot(
                project_id="pangramanizer",
                provider="pangramanizer.dashboard:catalog",
                published_at_ms=1000,
                entries=(),
            ),
        )
        linked = store.link_project(payload)
        self.assertEqual(linked.project_id, "pangramanizer")
        self.assertTrue(linked.catalog_published)
        listed = store.list_projects()
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0].project_id, "pangramanizer")

    def test_remote_project_store_publishes_catalog_without_relinking(self) -> None:
        store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        linked = store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                default_environment="prod",
                catalog_provider="pangramanizer.dashboard:catalog",
            )
        )
        entry = default_dashboard_catalog()[0]
        published = store.publish_catalog(
            RemoteProjectCatalogPublishRequest(
                project_id="pangramanizer",
                catalog_provider="pangramanizer.dashboard:catalog",
                catalog_snapshot=ProjectCatalogSnapshot(
                    project_id="pangramanizer",
                    provider="pangramanizer.dashboard:catalog",
                    published_at_ms=2000,
                    entries=(entry.as_dict(),),
                    default_entry_id=entry.spec_id,
                    version=2,
                ),
            )
        )
        self.assertEqual(published.project_id, "pangramanizer")
        self.assertEqual(published.linked_at_ms, linked.linked_at_ms)
        self.assertTrue(published.catalog_published)
        self.assertEqual(published.catalog_version, 2)
        self.assertEqual(published.catalog_entry_count, 1)

    def test_remote_project_store_records_last_completed_run_upload(self) -> None:
        store = RemoteProjectStore(project_index=InMemoryProjectIndex())
        linked = store.link_project(
            RemoteProjectLinkRequest(
                project_id="pangramanizer",
                label="Pangramanizer",
                default_environment="prod",
                catalog_provider="pangramanizer.dashboard:catalog",
            )
        )
        updated = store.record_completed_run_upload(
            project_id="pangramanizer",
            graph_id="pangramanizer_training",
            run_id="run-123",
            invocation_name="pangram_real_smoke",
            uploaded_at_ms=linked.linked_at_ms + 1,
        )
        self.assertEqual(updated.last_completed_run_upload_at_ms, linked.linked_at_ms + 1)
        self.assertEqual(updated.last_completed_run_graph_id, "pangramanizer_training")
        self.assertEqual(updated.last_completed_run_id, "run-123")


if __name__ == "__main__":
    unittest.main()
