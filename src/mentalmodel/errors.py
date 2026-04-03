"""Shared error types for the mentalmodel package."""


class MentalModelError(Exception):
    """Base package error."""


class EntrypointLoadError(MentalModelError):
    """Raised when a CLI entrypoint cannot be loaded or executed."""


class LoweringError(MentalModelError):
    """Raised when an authoring model cannot be lowered into IR."""
