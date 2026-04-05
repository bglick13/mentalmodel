from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Generic, TypeVar

from mentalmodel.analysis import run_analysis
from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.workflow import Workflow
from mentalmodel.errors import ObjectiveEvaluationError
from mentalmodel.ir.lowering import lower_program
from mentalmodel.observability.metrics import MetricObservation, RecordingMetricEmitter
from mentalmodel.runtime import AsyncExecutor

CandidateT = TypeVar("CandidateT")


class ObjectiveDirection(StrEnum):
    """Optimization direction for one objective signal."""

    MAXIMIZE = "maximize"
    MINIMIZE = "minimize"


class ObjectiveAggregation(StrEnum):
    """Reduction strategy when a metric is observed multiple times in one run."""

    LAST = "last"
    MAX = "max"
    MIN = "min"
    MEAN = "mean"
    SUM = "sum"


@dataclass(slots=True, frozen=True)
class ObjectiveSignal:
    """Named metric signal used to score one candidate workflow run."""

    metric_name: str
    direction: ObjectiveDirection
    aggregation: ObjectiveAggregation = ObjectiveAggregation.LAST
    success_threshold: float | None = None

    def resolve(self, observations: Sequence[MetricObservation]) -> tuple[float, tuple[float, ...]]:
        values = tuple(
            float(observation.value)
            for observation in observations
            if observation.definition.name == self.metric_name
        )
        if not values:
            raise ObjectiveEvaluationError(
                f"Metric signal {self.metric_name!r} was not emitted during objective evaluation."
            )
        if self.aggregation is ObjectiveAggregation.LAST:
            return values[-1], values
        if self.aggregation is ObjectiveAggregation.MAX:
            return max(values), values
        if self.aggregation is ObjectiveAggregation.MIN:
            return min(values), values
        if self.aggregation is ObjectiveAggregation.SUM:
            return sum(values), values
        return sum(values) / len(values), values

    def threshold_passes(self, score: float) -> bool:
        if self.success_threshold is None:
            return True
        if self.direction is ObjectiveDirection.MAXIMIZE:
            return score >= self.success_threshold
        return score <= self.success_threshold

    def sort_key(self, score: float) -> float:
        if self.direction is ObjectiveDirection.MAXIMIZE:
            return -score
        return score


@dataclass(slots=True, frozen=True)
class ObjectiveResult:
    """Deterministic evaluation result for one objective candidate."""

    objective_name: str
    candidate_label: str
    signal: ObjectiveSignal
    score: float
    metric_values: tuple[float, ...]
    verification_success: bool
    success: bool
    graph_id: str
    run_id: str | None
    error: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "objective_name": self.objective_name,
            "candidate_label": self.candidate_label,
            "metric_name": self.signal.metric_name,
            "direction": self.signal.direction.value,
            "aggregation": self.signal.aggregation.value,
            "score": self.score,
            "metric_values": list(self.metric_values),
            "verification_success": self.verification_success,
            "success": self.success,
            "graph_id": self.graph_id,
            "run_id": self.run_id,
            "error": self.error,
        }


@dataclass(slots=True, frozen=True)
class SearchResult(Generic[CandidateT]):
    """Comparison result over a bounded set of objective candidates."""

    objective_name: str
    best_candidate: CandidateT
    best_result: ObjectiveResult
    results: tuple[ObjectiveResult, ...]


@dataclass(slots=True, frozen=True)
class VerifiableObjective(Generic[CandidateT]):
    """Lightweight objective wrapper around candidate-authored workflows."""

    name: str
    signal: ObjectiveSignal
    build_candidate: Callable[[CandidateT], Workflow[NamedPrimitive]]
    describe_candidate: Callable[[CandidateT], str] = str


def evaluate_objective(
    objective: VerifiableObjective[CandidateT],
    candidate: CandidateT,
    *,
    runs_dir: Path | None = None,
) -> ObjectiveResult:
    """Build and evaluate one candidate with local metric capture."""

    return asyncio.run(evaluate_objective_async(objective, candidate, runs_dir=runs_dir))


async def evaluate_objective_async(
    objective: VerifiableObjective[CandidateT],
    candidate: CandidateT,
    *,
    runs_dir: Path | None = None,
) -> ObjectiveResult:
    """Build and evaluate one candidate inside an existing event loop."""

    del runs_dir
    program = objective.build_candidate(candidate)
    graph = lower_program(program)
    analysis = run_analysis(graph)
    metric_emitter = RecordingMetricEmitter()
    executor = AsyncExecutor(metrics=metric_emitter)
    try:
        result = await executor.run(program)
    except Exception as exc:
        partial_observations = metric_emitter.snapshot()
        score, metric_values = _resolve_metric_or_default(
            signal=objective.signal,
            observations=partial_observations,
        )
        return ObjectiveResult(
            objective_name=objective.name,
            candidate_label=objective.describe_candidate(candidate),
            signal=objective.signal,
            score=score,
            metric_values=metric_values,
            verification_success=False,
            success=False,
            graph_id=graph.graph_id,
            run_id=executor.recorder.last_run_id,
            error=f"{type(exc).__name__}: {exc}",
        )
    score, metric_values = objective.signal.resolve(metric_emitter.snapshot())
    verification_success = not analysis.has_errors
    success = verification_success and objective.signal.threshold_passes(score)
    return ObjectiveResult(
        objective_name=objective.name,
        candidate_label=objective.describe_candidate(candidate),
        signal=objective.signal,
        score=score,
        metric_values=metric_values,
        verification_success=verification_success,
        success=success,
        graph_id=graph.graph_id,
        run_id=result.run_id,
    )


def search_objective(
    objective: VerifiableObjective[CandidateT],
    candidates: Sequence[CandidateT],
    *,
    runs_dir: Path | None = None,
) -> SearchResult[CandidateT]:
    """Evaluate a bounded candidate set and return the best successful result."""

    return asyncio.run(search_objective_async(objective, candidates, runs_dir=runs_dir))


async def search_objective_async(
    objective: VerifiableObjective[CandidateT],
    candidates: Sequence[CandidateT],
    *,
    runs_dir: Path | None = None,
) -> SearchResult[CandidateT]:
    """Evaluate a bounded candidate set and return the best successful result."""

    if not candidates:
        raise ObjectiveEvaluationError("Objective search requires at least one candidate.")
    collected_results: list[ObjectiveResult] = []
    for candidate in candidates:
        collected_results.append(
            await evaluate_objective_async(objective, candidate, runs_dir=runs_dir)
        )
    results = tuple(collected_results)
    successful = [
        (candidate, result)
        for candidate, result in zip(candidates, results, strict=True)
        if result.success
    ]
    if not successful:
        raise ObjectiveEvaluationError(
            f"Objective {objective.name!r} produced no successful candidates."
        )
    best_candidate, best_result = min(
        successful,
        key=lambda item: objective.signal.sort_key(item[1].score),
    )
    return SearchResult(
        objective_name=objective.name,
        best_candidate=best_candidate,
        best_result=best_result,
        results=results,
    )


def _resolve_metric_or_default(
    *,
    signal: ObjectiveSignal,
    observations: Sequence[MetricObservation],
) -> tuple[float, tuple[float, ...]]:
    try:
        return signal.resolve(observations)
    except ObjectiveEvaluationError:
        return 0.0, tuple()
