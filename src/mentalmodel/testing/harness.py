from __future__ import annotations

from dataclasses import dataclass
from types import ModuleType

from mentalmodel.analysis import AnalysisReport, run_analysis
from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.workflow import Workflow
from mentalmodel.ir.lowering import lower_program
from mentalmodel.runtime import AsyncExecutor, ExecutionResult
from mentalmodel.testing.invariants import PropertyCheckResult, run_property_checks


@dataclass(slots=True, frozen=True)
class RuntimeVerificationResult:
    """Runtime verification outcome for a workflow execution."""

    success: bool
    record_count: int
    output_count: int
    state_count: int
    error: str | None = None


@dataclass(slots=True, frozen=True)
class VerificationReport:
    """Combined static, runtime, and property-check verification report."""

    analysis: AnalysisReport
    runtime: RuntimeVerificationResult
    property_checks: tuple[PropertyCheckResult, ...]

    @property
    def success(self) -> bool:
        """Return whether every verification layer succeeded."""

        return (
            not self.analysis.has_errors
            and self.runtime.success
            and all(result.success for result in self.property_checks)
        )

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-safe projection of the report."""

        return {
            "graph_id": self.analysis.graph.graph_id,
            "analysis": {
                "error_count": self.analysis.error_count,
                "warning_count": self.analysis.warning_count,
                "findings": [
                    {
                        "code": finding.code,
                        "severity": finding.severity,
                        "message": finding.message,
                        "node_id": finding.node_id,
                    }
                    for finding in self.analysis.findings
                ],
            },
            "runtime": {
                "success": self.runtime.success,
                "record_count": self.runtime.record_count,
                "output_count": self.runtime.output_count,
                "state_count": self.runtime.state_count,
                "error": self.runtime.error,
            },
            "property_checks": [
                {
                    "name": result.name,
                    "success": result.success,
                    "hypothesis_backed": result.hypothesis_backed,
                    "error": result.error,
                }
                for result in self.property_checks
            ],
            "success": self.success,
        }


def execute_program(program: Workflow[NamedPrimitive]) -> ExecutionResult:
    """Run one workflow through the deterministic async executor."""

    import asyncio

    return asyncio.run(AsyncExecutor().run(program))


def run_verification(
    program: Workflow[NamedPrimitive],
    *,
    module: ModuleType | None = None,
) -> VerificationReport:
    """Run static analysis, runtime execution, and property checks."""

    graph = lower_program(program)
    analysis = run_analysis(graph)
    runtime = _run_runtime(program)
    property_checks = (
        run_property_checks(module, program)
        if module is not None
        else tuple[PropertyCheckResult, ...]()
    )
    return VerificationReport(
        analysis=analysis,
        runtime=runtime,
        property_checks=property_checks,
    )


def _run_runtime(program: Workflow[NamedPrimitive]) -> RuntimeVerificationResult:
    try:
        result = execute_program(program)
    except Exception as exc:
        return RuntimeVerificationResult(
            success=False,
            record_count=0,
            output_count=0,
            state_count=0,
            error=f"{type(exc).__name__}: {exc}",
        )
    return RuntimeVerificationResult(
        success=True,
        record_count=len(result.records),
        output_count=len(result.outputs),
        state_count=len(result.state),
    )
