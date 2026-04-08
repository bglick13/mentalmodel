from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.remote import (
    FileRemoteRunStore,
    InMemoryArtifactStore,
    InMemoryManifestIndex,
    RemoteCompletedRunSink,
    RemoteRunStore,
    build_run_bundle_upload,
)
from mentalmodel.testing import run_verification


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


if __name__ == "__main__":
    unittest.main()
