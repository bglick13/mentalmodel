from __future__ import annotations

import unittest

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.ir.lowering import lower_program
from mentalmodel.testing import (
    BoundaryObservation,
    aligned_key_sets,
    assert_aligned_key_sets,
    assert_causal_order,
    assert_monotonic_non_decreasing,
    assert_runtime_boundary_crossings,
    collect_runtime_boundary_observations,
    invariant_fail,
    invariant_pass,
    is_monotonic_non_decreasing,
)


class VerificationHelpersTest(unittest.TestCase):
    def test_aligned_key_sets_returns_shared_key_set(self) -> None:
        shared = aligned_key_sets({"a": 1, "b": 2}, {"b": 3, "a": 4})
        self.assertEqual(shared, frozenset({"a", "b"}))

    def test_aligned_key_sets_returns_none_for_mismatch(self) -> None:
        shared = aligned_key_sets({"a": 1}, {"b": 2})
        self.assertIsNone(shared)

    def test_assert_aligned_key_sets_raises_for_mismatch(self) -> None:
        with self.assertRaisesRegex(AssertionError, "expected aligned key sets"):
            assert_aligned_key_sets(
                {"a": 1},
                {"b": 2},
                labels=("pangram", "quality"),
            )

    def test_monotonic_helpers_detect_non_decreasing_sequence(self) -> None:
        self.assertTrue(is_monotonic_non_decreasing([1, 1, 2, 3]))
        assert_monotonic_non_decreasing([1, 1, 2, 3], label="policy_versions")

    def test_assert_monotonic_non_decreasing_raises_for_drop(self) -> None:
        with self.assertRaisesRegex(AssertionError, "policy_versions"):
            assert_monotonic_non_decreasing([1, 3, 2], label="policy_versions")

    def test_assert_causal_order_accepts_valid_observation(self) -> None:
        assert_causal_order(3, 4, max_lag=1)

    def test_assert_causal_order_rejects_invalid_lag(self) -> None:
        with self.assertRaisesRegex(AssertionError, "max_lag"):
            assert_causal_order(1, 4, max_lag=2)

    def test_collect_runtime_boundary_observations_from_demo_graph(self) -> None:
        graph = lower_program(build_program())
        observations = collect_runtime_boundary_observations(graph)
        self.assertGreaterEqual(len(observations), 1)
        pairs = {
            (
                observation.producer_node_id,
                observation.consumer_node_id,
                observation.producer_runtime,
                observation.consumer_runtime,
            )
            for observation in observations
        }
        self.assertIn(
            ("batch_source", "sample_policy", "local", "sandbox"),
            pairs,
        )
        self.assertIn(
            ("sample_policy", "pangram_reward", "sandbox", "local"),
            pairs,
        )

    def test_assert_runtime_boundary_crossings_accepts_allowed_pairs(self) -> None:
        observations = (
            BoundaryObservation(
                producer_node_id="sample_policy",
                consumer_node_id="pangram_reward",
                producer_runtime="sandbox",
                consumer_runtime="local",
            ),
            BoundaryObservation(
                producer_node_id="batch_source",
                consumer_node_id="sample_policy",
                producer_runtime="local",
                consumer_runtime="sandbox",
            ),
        )
        assert_runtime_boundary_crossings(
            observations,
            allowed={("sandbox", "local"), ("local", "sandbox")},
        )

    def test_assert_runtime_boundary_crossings_rejects_unexpected_pair(self) -> None:
        observations = (
            BoundaryObservation(
                producer_node_id="sample_policy",
                consumer_node_id="pangram_reward",
                producer_runtime="sandbox",
                consumer_runtime="local",
            ),
        )
        with self.assertRaisesRegex(AssertionError, "unexpected runtime-context crossings"):
            assert_runtime_boundary_crossings(observations, allowed={("local", "sandbox")})

    def test_invariant_pass_and_fail_preserve_details(self) -> None:
        passing = invariant_pass(details={"current_policy_version": 3})
        failing = invariant_fail(details={"current_policy_version": 5})
        self.assertTrue(passing.passed)
        self.assertFalse(failing.passed)
        self.assertEqual(dict(passing.details), {"current_policy_version": 3})
        self.assertEqual(dict(failing.details), {"current_policy_version": 5})


if __name__ == "__main__":
    unittest.main()
