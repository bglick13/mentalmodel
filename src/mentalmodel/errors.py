"""Shared error types for the mentalmodel package."""


class MentalModelError(Exception):
    """Base package error."""


class EntrypointLoadError(MentalModelError):
    """Raised when a CLI entrypoint cannot be loaded or executed."""


class LoweringError(MentalModelError):
    """Raised when an authoring model cannot be lowered into IR."""


class VerificationError(MentalModelError):
    """Raised when verification setup or execution fails."""


class SkillInstallError(MentalModelError):
    """Raised when packaged skill installation cannot be completed."""
