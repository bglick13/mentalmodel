from __future__ import annotations

from dataclasses import dataclass

from mentalmodel.analysis.findings import Finding
from mentalmodel.analysis.graph_checks import run_graph_checks
from mentalmodel.analysis.semantic_checks import run_semantic_checks
from mentalmodel.ir.graph import IRGraph


@dataclass(slots=True, frozen=True)
class AnalysisReport:
    """Aggregated analysis result for one lowered graph."""

    graph: IRGraph
    findings: tuple[Finding, ...]
    error_count: int
    warning_count: int

    @property
    def has_errors(self) -> bool:
        """Return whether any error-level finding was produced."""

        return self.error_count > 0


def run_analysis(graph: IRGraph) -> AnalysisReport:
    """Run all current structural and semantic analysis passes."""

    findings = tuple([*run_graph_checks(graph), *run_semantic_checks(graph)])
    error_count = sum(1 for finding in findings if finding.severity == "error")
    warning_count = sum(1 for finding in findings if finding.severity == "warning")
    return AnalysisReport(
        graph=graph,
        findings=findings,
        error_count=error_count,
        warning_count=warning_count,
    )
