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


class RunInspectionError(MentalModelError):
    """Raised when persisted run artifacts cannot be located or parsed."""


class TracingConfigError(MentalModelError):
    """Raised when tracing configuration is invalid or unsupported."""


class ObjectiveEvaluationError(MentalModelError):
    """Raised when a verifiable objective cannot be evaluated deterministically."""


class OptionalDependencyError(MentalModelError):
    """Raised when an optional integration requires dependencies that are absent."""
