from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from pathlib import Path

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.runtime.runs import (
    list_run_summaries,
    load_run_payload,
    load_run_records,
    load_run_summary,
    resolve_run_summary,
)
from mentalmodel.testing import run_verification


class RunsTest(unittest.TestCase):
    def test_run_helpers_load_latest_materialized_run(self) -> None:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report = run_verification(build_program(), module=module, runs_dir=root)
            self.assertTrue(report.success)

            summaries = list_run_summaries(runs_dir=root, graph_id="async_rl_demo")
            self.assertEqual(len(summaries), 1)
            summary = resolve_run_summary(runs_dir=root, graph_id="async_rl_demo")
            self.assertEqual(summary.graph_id, "async_rl_demo")
            self.assertEqual(summary.run_id, report.runtime.run_id)

            verification = load_run_payload(
                runs_dir=root,
                graph_id="async_rl_demo",
                filename="verification.json",
            )
            self.assertEqual(verification["graph_id"], "async_rl_demo")

            records = load_run_records(
                runs_dir=root,
                graph_id="async_rl_demo",
                node_id="staleness_invariant",
            )
            self.assertGreaterEqual(len(records), 1)
            self.assertTrue(all(record["node_id"] == "staleness_invariant" for record in records))

    def test_load_run_summary_supports_legacy_summary_without_created_at_ms(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / ".runs" / "legacy_graph" / "run-legacy"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "graph_id": "legacy_graph",
                        "run_id": "run-legacy",
                        "success": True,
                        "node_count": 1,
                        "edge_count": 0,
                        "record_count": 0,
                        "output_count": 0,
                        "state_count": 0,
                        "trace_sink_configured": False,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            summary = load_run_summary(run_dir)
            self.assertEqual(summary.graph_id, "legacy_graph")
            self.assertEqual(summary.run_id, "run-legacy")
            self.assertGreater(summary.created_at_ms, 0)

            summaries = list_run_summaries(runs_dir=Path(tmpdir), graph_id="legacy_graph")
            self.assertEqual(len(summaries), 1)
            self.assertEqual(summaries[0].run_id, "run-legacy")


if __name__ == "__main__":
    unittest.main()
