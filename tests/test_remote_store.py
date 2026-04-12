from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.remote.backend import (
    InMemoryArtifactStore,
    InMemoryLiveSessionIndex,
    InMemoryManifestIndex,
    InMemoryPersistedRunIndex,
    InMemoryProjectIndex,
    RemoteCompletedRunSink,
    RemoteLiveSessionStore,
    RemoteProjectStore,
    RemoteRunStore,
)
from mentalmodel.remote.contracts import (
    ProjectCatalogSnapshot,
    RemoteLiveSessionStartRequest,
    RemoteLiveSessionStatus,
    RemoteLiveSessionUpdateRequest,
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
            persisted_index = InMemoryPersistedRunIndex()
            store = RemoteRunStore(
                manifest_index=InMemoryManifestIndex(),
                artifact_store=InMemoryArtifactStore(),
                persisted_run_index=persisted_index,
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
            records_page = store.get_records_page(
                graph_id="async_rl_demo",
                run_id=report.runtime.run_id,
                cursor=None,
                limit=5,
            )
            self.assertGreater(records_page.total_count, 0)
            self.assertLessEqual(len(records_page.items), 5)
            self.assertEqual(
                records_page.total_count,
                len(persisted_index._records[("async_rl_demo", report.runtime.run_id)]),
            )
            bucket_rows = store.aggregate_record_timeseries(
                graph_id="async_rl_demo",
                invocation_name=report.runtime.invocation_name or "async_rl_demo",
                since_ms=resolved.created_at_ms - 1,
                until_ms=resolved.created_at_ms + 60_000,
                rollup_ms=5_000,
                run_id=report.runtime.run_id,
            )
            self.assertTrue(bucket_rows)

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

    def test_remote_live_session_store_tracks_records_and_bundle_commit(self) -> None:
        store = RemoteLiveSessionStore(live_session_index=InMemoryLiveSessionIndex())
        started = store.start_session(
            RemoteLiveSessionStartRequest(
                project_id="pangramanizer",
                graph_id="pangramanizer_training",
                run_id="run-live-123",
                invocation_name="pangram_real_smoke",
                environment_name="prod",
                started_at_ms=1000,
                graph={
                    "graph_id": "pangramanizer_training",
                    "metadata": {},
                    "nodes": [{"node_id": "source", "kind": "actor", "label": "Source"}],
                    "edges": [],
                },
                analysis={"error_count": 0, "warning_count": 0, "findings": []},
                runtime_default_profile_name="real",
                runtime_profile_names=("real",),
            )
        )
        self.assertEqual(started.status, RemoteLiveSessionStatus.RUNNING)
        updated = store.apply_update(
            RemoteLiveSessionUpdateRequest(
                graph_id="pangramanizer_training",
                run_id="run-live-123",
                updated_at_ms=1100,
                records=(
                    {
                        "record_id": "run-live-123:1",
                        "run_id": "run-live-123",
                        "node_id": "source",
                        "frame_id": "root",
                        "frame_path": ["root"],
                        "loop_node_id": None,
                        "iteration_index": None,
                        "event_type": "node.succeeded",
                        "sequence": 1,
                        "timestamp_ms": 1100,
                        "payload": {"output": {"reward": 1.0}},
                    },
                ),
                spans=(
                    {
                        "span_id": "span-1:root:100:actor:source",
                        "sequence": 1,
                        "name": "actor:source",
                        "start_time_ns": 100,
                        "end_time_ns": 200,
                        "duration_ns": 100,
                        "attributes": {"mentalmodel.node.id": "source"},
                        "frame_id": "root",
                        "loop_node_id": None,
                        "iteration_index": None,
                        "error_type": None,
                        "error_message": None,
                    },
                ),
            )
        )
        self.assertEqual(len(updated.records), 1)
        self.assertEqual(len(updated.spans), 1)
        closed = store.apply_update(
            RemoteLiveSessionUpdateRequest(
                graph_id="pangramanizer_training",
                run_id="run-live-123",
                updated_at_ms=1200,
                status=RemoteLiveSessionStatus.SUCCEEDED,
            )
        )
        self.assertEqual(closed.status, RemoteLiveSessionStatus.SUCCEEDED)
        committed = store.mark_bundle_committed(
            graph_id="pangramanizer_training",
            run_id="run-live-123",
            committed_at_ms=1300,
        )
        assert committed is not None
        self.assertEqual(committed.bundle_committed_at_ms, 1300)


if __name__ == "__main__":
    unittest.main()
