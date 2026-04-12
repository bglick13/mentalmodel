"""Testing and verification exports."""

from mentalmodel.testing.harness import (
    RuntimeInvariantFailure,
    RuntimeVerificationResult,
    VerificationReport,
    execute_program,
    run_verification,
)
from mentalmodel.runtime.managed import (
    ManagedExecutionOptions,
    ManagedExecutionResult,
    ManagedInvariantFailure,
    ManagedRunTarget,
    run_managed,
)
from mentalmodel.testing.invariants import (
    PropertyCheck,
    PropertyCheckResult,
    discover_property_checks,
    hypothesis_property_check,
    property_check,
    run_property_checks,
)
from mentalmodel.testing.verification_helpers import (
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

__all__ = [
    "BoundaryObservation",
    "PropertyCheck",
    "PropertyCheckResult",
    "RuntimeInvariantFailure",
    "RuntimeVerificationResult",
    "VerificationReport",
    "aligned_key_sets",
    "assert_aligned_key_sets",
    "assert_causal_order",
    "assert_monotonic_non_decreasing",
    "assert_runtime_boundary_crossings",
    "collect_runtime_boundary_observations",
    "discover_property_checks",
    "execute_program",
    "ManagedExecutionOptions",
    "ManagedExecutionResult",
    "ManagedInvariantFailure",
    "ManagedRunTarget",
    "hypothesis_property_check",
    "invariant_fail",
    "invariant_pass",
    "is_monotonic_non_decreasing",
    "property_check",
    "run_managed",
    "run_property_checks",
    "run_verification",
]
