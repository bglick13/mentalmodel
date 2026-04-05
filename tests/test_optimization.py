from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mentalmodel.examples.autoresearch_sorting import SORT_CANDIDATES, build_objective, run_search
from mentalmodel.integrations.autoresearch import write_autoresearch_bundle
from mentalmodel.optimization import evaluate_objective


class OptimizationTest(unittest.TestCase):
    def test_evaluate_objective_returns_metric_signal_for_candidate(self) -> None:
        result = evaluate_objective(build_objective(), "insertion")
        self.assertTrue(result.verification_success)
        self.assertTrue(result.success)
        self.assertEqual(result.signal.metric_name, "mentalmodel.demo.sorting.comparison_count")
        self.assertGreater(result.score, 0)
        self.assertEqual(result.metric_values, (result.score,))

    def test_search_objective_selects_best_successful_candidate(self) -> None:
        search = run_search()
        self.assertEqual(search.objective_name, "sorting_efficiency")
        self.assertIn(search.best_candidate, SORT_CANDIDATES)
        self.assertTrue(search.best_result.success)
        successful_scores = [result.score for result in search.results if result.success]
        self.assertEqual(search.best_result.score, min(successful_scores))

    def test_write_autoresearch_bundle_writes_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle = write_autoresearch_bundle(Path(tmpdir))
            self.assertTrue(bundle.program_path.exists())
            self.assertTrue(bundle.objective_path.exists())
            self.assertTrue(bundle.candidates_path.exists())
            program_text = bundle.program_path.read_text(encoding="utf-8")
            self.assertIn("mentalmodel autoresearch sorting demo", program_text)
            self.assertIn("comparison_count", program_text)


if __name__ == "__main__":
    unittest.main()
