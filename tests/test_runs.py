from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.errors import RunInspectionError
from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.runtime.replay import build_replay_report, build_run_diff
from mentalmodel.runtime.runs import (
    RUN_SCHEMA_VERSION,
    apply_run_repairs,
    iter_run_dirs,
    list_run_summaries,
    load_run_node_inputs,
    load_run_node_output,
    load_run_node_trace,
    load_run_payload,
    load_run_records,
    load_run_summary,
    plan_run_repairs,
    resolve_run_summary,
)
from mentalmodel.testing import run_verification


class RunsTest(unittest.TestCase):
    def _materialize_demo_run(
        self,
        root: Path,
        *,
        group_size: int = 4,
        sampler_lag: int = 0,
        max_off_policy_steps: int = 0,
    ) -> str:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        report = run_verification(
            build_program(
                group_size=group_size,
                sampler_lag=sampler_lag,
                max_off_policy_steps=max_off_policy_steps,
            ),
            module=module,
            runs_dir=root,
        )
        run_id = report.runtime.run_id
        assert run_id is not None
        return run_id

    def test_run_helpers_load_latest_materialized_run(self) -> None:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report = run_verification(build_program(), module=module, runs_dir=root)
            self.assertTrue(report.success)

            summaries = list_run_summaries(runs_dir=root, graph_id="async_rl_demo")
            self.assertEqual(len(summaries), 1)
            summary = resolve_run_summary(runs_dir=root, graph_id="async_rl_demo")
            self.assertEqual(summary.schema_version, RUN_SCHEMA_VERSION)
            self.assertEqual(summary.graph_id, "async_rl_demo")
            self.assertEqual(summary.run_id, report.runtime.run_id)
            self.assertEqual(summary.trace_mode, "disk")
            self.assertTrue(summary.trace_mirror_to_disk)

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

            output = load_run_node_output(
                runs_dir=root,
                graph_id="async_rl_demo",
                node_id="staleness_invariant",
            )
            self.assertIsInstance(output, dict)

            trace = load_run_node_trace(
                runs_dir=root,
                graph_id="async_rl_demo",
                node_id="staleness_invariant",
            )
            self.assertEqual(trace.node_id, "staleness_invariant")
            self.assertGreaterEqual(len(trace.records), 1)

    def test_load_run_node_inputs_returns_persisted_input_payload(self) -> None:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report = run_verification(build_program(), module=module, runs_dir=root)
            self.assertTrue(report.success)

            inputs = load_run_node_inputs(
                runs_dir=root,
                graph_id="async_rl_demo",
                node_id="staleness_invariant",
            )
            self.assertIsInstance(inputs, dict)
            input_map = cast(dict[str, JsonValue], inputs)
            rollout_join = input_map["rollout_join"]
            self.assertIsInstance(rollout_join, dict)
            rollout_join_map = cast(dict[str, JsonValue], rollout_join)
            self.assertEqual(rollout_join_map["current_policy_version"], 3)
            self.assertEqual(rollout_join_map["sampled_policy_version"], 3)

    def test_load_run_node_output_fails_for_missing_node(self) -> None:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report = run_verification(build_program(), module=module, runs_dir=root)
            self.assertTrue(report.success)

            with self.assertRaises(RunInspectionError):
                load_run_node_output(
                    runs_dir=root,
                    graph_id="async_rl_demo",
                    node_id="missing_node",
                )

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
            self.assertEqual(summary.schema_version, RUN_SCHEMA_VERSION)
            self.assertEqual(summary.graph_id, "legacy_graph")
            self.assertEqual(summary.run_id, "run-legacy")
            self.assertGreater(summary.created_at_ms, 0)

            summaries = list_run_summaries(runs_dir=Path(tmpdir), graph_id="legacy_graph")
            self.assertEqual(len(summaries), 1)
            self.assertEqual(summaries[0].run_id, "run-legacy")

    def test_plan_and_apply_run_repairs_for_legacy_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / ".runs" / "legacy_graph" / "run-legacy"
            run_dir.mkdir(parents=True, exist_ok=True)
            summary_path = run_dir / "summary.json"
            summary_path.write_text(
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

            self.assertEqual(iter_run_dirs(runs_dir=root, graph_id="legacy_graph"), (run_dir,))

            plan = plan_run_repairs(runs_dir=root, graph_id="legacy_graph")
            self.assertTrue(plan.has_actions)
            self.assertEqual(len(plan.actions), 1)
            action = plan.actions[0]
            self.assertEqual(action.from_schema_version, 1)
            self.assertEqual(action.to_schema_version, RUN_SCHEMA_VERSION)
            self.assertIn("schema_version", action.updates)
            self.assertIn("created_at_ms", action.updates)

            apply_run_repairs(plan)
            repaired = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(repaired["schema_version"], RUN_SCHEMA_VERSION)
            self.assertIn("created_at_ms", repaired)

            repaired_plan = plan_run_repairs(runs_dir=root, graph_id="legacy_graph")
            self.assertFalse(repaired_plan.has_actions)

    def test_build_replay_report_returns_semantic_timeline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_id = self._materialize_demo_run(root)

            report = build_replay_report(
                runs_dir=root,
                graph_id="async_rl_demo",
                run_id=run_id,
            )

            self.assertEqual(report.summary.run_id, run_id)
            self.assertTrue(report.verification_success)
            self.assertGreaterEqual(len(report.events), 1)
            self.assertIn("staleness_invariant", report.output_node_ids)
            node_summaries = {
                node_summary.node_id: node_summary for node_summary in report.node_summaries
            }
            self.assertTrue(node_summaries["staleness_invariant"].invariant_passed)
            self.assertEqual(node_summaries["staleness_invariant"].invariant_status, "pass")
            self.assertEqual(node_summaries["staleness_invariant"].invariant_severity, "error")

    def test_build_run_diff_detects_changed_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_a = self._materialize_demo_run(root, group_size=2)
            run_b = self._materialize_demo_run(root, group_size=4)

            diff = build_run_diff(
                runs_dir=root,
                graph_id="async_rl_demo",
                run_a=run_a,
                run_b=run_b,
                node_id="sample_policy",
            )

            self.assertTrue(diff.differs)
            self.assertEqual(len(diff.node_diffs), 1)
            node_diff = diff.node_diffs[0]
            self.assertEqual(node_diff.node_id, "sample_policy")
            self.assertFalse(node_diff.outputs_equal)
            self.assertTrue(node_diff.events_equal)

    def test_build_run_diff_detects_invariant_outcome_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_a = self._materialize_demo_run(root, sampler_lag=0, max_off_policy_steps=0)
            run_b = self._materialize_demo_run(root, sampler_lag=2, max_off_policy_steps=0)

            diff = build_run_diff(
                runs_dir=root,
                graph_id="async_rl_demo",
                run_a=run_a,
                run_b=run_b,
                invariant="staleness_invariant",
            )

            self.assertTrue(diff.differs)
            self.assertEqual(len(diff.invariant_diffs), 1)
            invariant_diff = diff.invariant_diffs[0]
            self.assertEqual(invariant_diff.node_id, "staleness_invariant")
            self.assertTrue(invariant_diff.outcome_run_a)
            self.assertFalse(invariant_diff.outcome_run_b)
            self.assertEqual(invariant_diff.severity_run_a, "error")
            self.assertEqual(invariant_diff.severity_run_b, "error")

    def test_run_summary_persists_resolved_otel_config(self) -> None:
        module = importlib.import_module("mentalmodel.examples.async_rl.demo")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with patch.dict(
                os.environ,
                {
                    "MENTALMODEL_OTEL_MODE": "console",
                    "MENTALMODEL_OTEL_SERVICE_NAME": "mentalmodel-console",
                },
                clear=True,
            ):
                report = run_verification(build_program(), module=module, runs_dir=root)
            self.assertTrue(report.success)
            summary = resolve_run_summary(runs_dir=root, graph_id="async_rl_demo")
            self.assertEqual(summary.trace_mode, "console")
            self.assertEqual(summary.trace_service_name, "mentalmodel-console")
            self.assertTrue(summary.trace_sink_configured)
            self.assertTrue(summary.trace_mirror_to_disk)


if __name__ == "__main__":
    unittest.main()
