"""mentalmodel package."""

from mentalmodel.core import (
    Actor,
    Effect,
    Invariant,
    Join,
    MetricContext,
    MetricDefinition,
    MetricExtractor,
    MetricKind,
    MetricObservation,
    OutputMetricSpec,
    Parallel,
    Ref,
    Workflow,
    extract_output_metrics,
    infer_output_metrics,
)
from mentalmodel.integrations.autoresearch import AutoResearch
from mentalmodel.optimization import (
    ObjectiveAggregation,
    ObjectiveDirection,
    ObjectiveResult,
    ObjectiveSignal,
    SearchResult,
    VerifiableObjective,
    evaluate_objective,
    search_objective,
)
from mentalmodel.plugins.runtime_context import RuntimeContext

from .version import __version__

__all__ = [
    "__version__",
    "Actor",
    "AutoResearch",
    "Effect",
    "MetricContext",
    "MetricDefinition",
    "MetricExtractor",
    "MetricKind",
    "MetricObservation",
    "OutputMetricSpec",
    "Invariant",
    "Join",
    "Parallel",
    "ObjectiveAggregation",
    "ObjectiveDirection",
    "ObjectiveResult",
    "ObjectiveSignal",
    "Ref",
    "RuntimeContext",
    "SearchResult",
    "VerifiableObjective",
    "Workflow",
    "evaluate_objective",
    "extract_output_metrics",
    "infer_output_metrics",
    "search_objective",
]
