from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias, TypedDict

from mentalmodel.core import (
    Effect,
    EffectHandler,
    Invariant,
    InvariantChecker,
    InvariantResult,
    Ref,
    Workflow,
    project_flat_metric_map,
)
from mentalmodel.integrations.autoresearch.plugin import AutoResearch, AutoResearchOutput
from mentalmodel.optimization import (
    ObjectiveAggregation,
    ObjectiveDirection,
    ObjectiveSignal,
    SearchResult,
    VerifiableObjective,
    search_objective,
)
from mentalmodel.plugins.runtime_context import RuntimeContext
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.testing import invariant_fail, invariant_pass

SortAlgorithm = Literal["insertion", "selection", "merge"]
SORT_CANDIDATES: tuple[SortAlgorithm, ...] = ("insertion", "selection", "merge")

DATASET: tuple[tuple[int, ...], ...] = (
    (5, 3, 1, 4, 2),
    (9, 7, 8, 6),
    (4, 1, 3, 2, 0),
)


class SortInvariantInputs(TypedDict):
    autoresearch_sorting: AutoResearchOutput


class SortExecutionOutput(TypedDict):
    algorithm: SortAlgorithm
    sorted_arrays: list[list[int]]
    comparison_count: int
    success_score: float


class CandidateSortInputs(TypedDict):
    pass


class CandidateSortInvariantInputs(TypedDict):
    sort_arrays: SortExecutionOutput


SortingDemoNode: TypeAlias = (
    RuntimeContext | AutoResearch[SortAlgorithm] | Invariant[SortInvariantInputs, float]
)
CandidateSortingNode: TypeAlias = (
    RuntimeContext
    | Effect[CandidateSortInputs, SortExecutionOutput]
    | Invariant[CandidateSortInvariantInputs, int]
)


class SearchResultInvariant(InvariantChecker[SortInvariantInputs, float]):
    async def check(
        self,
        inputs: SortInvariantInputs,
        ctx: ExecutionContext,
    ) -> InvariantResult[float]:
        del ctx
        summary = inputs["autoresearch_sorting"]
        if summary["best_candidate"] != "merge":
            return invariant_fail(details={"best_score": float(summary["best_score"])})
        return invariant_pass(details={"best_score": float(summary["best_score"])})


def build_program() -> Workflow[SortingDemoNode]:
    return Workflow(
        name="autoresearch_sorting_demo",
        description="Runtime-executable AutoResearch demo over bounded sorting candidates.",
        children=[
            RuntimeContext(
                name="local_control_plane",
                runtime="local",
                children=[
                    AutoResearch[SortAlgorithm](
                        "autoresearch_sorting",
                        objective=build_objective(),
                        candidates=SORT_CANDIDATES,
                        metrics=[
                            project_flat_metric_map(
                                prefix="mentalmodel.demo.autoresearch",
                                fields=("best_score", "successful_candidate_count"),
                                accessor=autoresearch_metric_map,
                            )
                        ],
                    ),
                    Invariant[SortInvariantInputs, float](
                        "search_result_invariant",
                        checker=SearchResultInvariant(),
                        inputs=[Ref("autoresearch_sorting")],
                    ),
                ],
            )
        ],
    )


def build_objective() -> VerifiableObjective[SortAlgorithm]:
    return VerifiableObjective(
        name="sorting_efficiency",
        signal=ObjectiveSignal(
            metric_name="mentalmodel.demo.sorting.comparison_count",
            direction=ObjectiveDirection.MINIMIZE,
            aggregation=ObjectiveAggregation.LAST,
        ),
        build_candidate=build_candidate_program,
        describe_candidate=lambda candidate: candidate,
    )


def run_search() -> SearchResult[SortAlgorithm]:
    return search_objective(build_objective(), SORT_CANDIDATES)


def autoresearch_metric_map(output: AutoResearchOutput) -> dict[str, float]:
    return {
        "best_score": float(output["best_score"]),
        "successful_candidate_count": float(output["successful_candidate_count"]),
    }


def sorting_metric_map(output: SortExecutionOutput) -> dict[str, float]:
    return {
        "comparison_count": float(output["comparison_count"]),
        "success_score": float(output["success_score"]),
    }


@dataclass(slots=True)
class SortArrays(EffectHandler[CandidateSortInputs, SortExecutionOutput]):
    algorithm: SortAlgorithm

    async def invoke(
        self,
        inputs: CandidateSortInputs,
        context: ExecutionContext,
    ) -> SortExecutionOutput:
        del inputs, context
        sorted_arrays: list[list[int]] = []
        comparison_count = 0
        for array in DATASET:
            sorted_array, comparisons = sort_array(list(array), self.algorithm)
            sorted_arrays.append(sorted_array)
            comparison_count += comparisons
        success_score = 1.0 if is_sorted_dataset(sorted_arrays) else 0.0
        return {
            "algorithm": self.algorithm,
            "sorted_arrays": sorted_arrays,
            "comparison_count": comparison_count,
            "success_score": success_score,
        }


class CandidateSortingInvariant(InvariantChecker[CandidateSortInvariantInputs, int]):
    async def check(
        self,
        inputs: CandidateSortInvariantInputs,
        context: ExecutionContext,
    ) -> InvariantResult[int]:
        del context
        sort_output = inputs["sort_arrays"]
        if not is_sorted_dataset(sort_output["sorted_arrays"]):
            return invariant_fail(details={"comparison_count": sort_output["comparison_count"]})
        return invariant_pass(details={"comparison_count": sort_output["comparison_count"]})


def build_candidate_program(algorithm: SortAlgorithm) -> Workflow[CandidateSortingNode]:
    return Workflow(
        name=f"sorting_candidate_{algorithm}",
        description="Bounded sorting candidate program evaluated by AutoResearch.",
        children=[
            RuntimeContext(
                name="sandbox_sorting",
                runtime="sandbox",
                children=[
                    Effect[CandidateSortInputs, SortExecutionOutput](
                        "sort_arrays",
                        handler=SortArrays(algorithm=algorithm),
                        metrics=[
                            project_flat_metric_map(
                                prefix="mentalmodel.demo.sorting",
                                fields=("comparison_count", "success_score"),
                                accessor=sorting_metric_map,
                            )
                        ],
                    ),
                    Invariant[CandidateSortInvariantInputs, int](
                        "sorting_invariant",
                        checker=CandidateSortingInvariant(),
                        inputs=[Ref("sort_arrays")],
                    ),
                ],
            )
        ],
    )


def sort_array(values: list[int], algorithm: SortAlgorithm) -> tuple[list[int], int]:
    if algorithm == "insertion":
        return insertion_sort(values)
    if algorithm == "selection":
        return selection_sort(values)
    return merge_sort(values)


def insertion_sort(values: list[int]) -> tuple[list[int], int]:
    items = list(values)
    comparisons = 0
    for index in range(1, len(items)):
        current = items[index]
        cursor = index - 1
        while cursor >= 0:
            comparisons += 1
            if items[cursor] <= current:
                break
            items[cursor + 1] = items[cursor]
            cursor -= 1
        items[cursor + 1] = current
    return items, comparisons


def selection_sort(values: list[int]) -> tuple[list[int], int]:
    items = list(values)
    comparisons = 0
    for index in range(len(items)):
        best_index = index
        for cursor in range(index + 1, len(items)):
            comparisons += 1
            if items[cursor] < items[best_index]:
                best_index = cursor
        items[index], items[best_index] = items[best_index], items[index]
    return items, comparisons


def merge_sort(values: list[int]) -> tuple[list[int], int]:
    if len(values) <= 1:
        return list(values), 0
    middle = len(values) // 2
    left, left_comparisons = merge_sort(values[:middle])
    right, right_comparisons = merge_sort(values[middle:])
    merged: list[int] = []
    comparisons = left_comparisons + right_comparisons
    left_index = 0
    right_index = 0
    while left_index < len(left) and right_index < len(right):
        comparisons += 1
        if left[left_index] <= right[right_index]:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
    merged.extend(left[left_index:])
    merged.extend(right[right_index:])
    return merged, comparisons


def is_sorted_dataset(sorted_arrays: list[list[int]]) -> bool:
    return all(array == sorted(array) for array in sorted_arrays)
