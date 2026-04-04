"""Runtime-specific error types."""

from mentalmodel.errors import MentalModelError


class ExecutionError(MentalModelError):
    """Raised when runtime execution fails."""


class InvariantViolationError(ExecutionError):
    """Raised when an invariant does not hold at runtime."""
