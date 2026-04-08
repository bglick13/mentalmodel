from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.remote import FileRemoteRunStore, build_run_bundle_upload
from mentalmodel.testing import run_verification


class RemoteStoreTest(unittest.TestCase):
    def test_file_remote_run_store_ingests_uploaded_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as local_tmp, tempfile.TemporaryDirectory() as remote_tmp:
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


if __name__ == "__main__":
    unittest.main()
