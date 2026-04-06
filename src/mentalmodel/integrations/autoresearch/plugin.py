from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypedDict, TypeVar, cast

from mentalmodel.core.interfaces import RuntimeValue
from mentalmodel.core.refs import InputRef
from mentalmodel.ir.graph import IRFragment, IRNode
from mentalmodel.observability.metrics import (
    OutputMetricSpec,
    cast_metric_specs,
)
from mentalmodel.optimization import (
    SearchResult,
    VerifiableObjective,
    search_objective_async,
)
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.execution import (
    CompiledPluginNode,
    ExecutionNodeMetadata,
    MappingInputAdapter,
    PluginExecutionHandler,
)

if TYPE_CHECKING:
    from mentalmodel.ir.lowering import LoweringContext

CandidateT = TypeVar("CandidateT")


class CandidateResultPayload(TypedDict):
    """JSON-safe candidate result payload."""

    candidate_label: str
    score: float
    success: bool
    verification_success: bool
    metric_values: list[float]


class AutoResearchOutput(TypedDict):
    """JSON-safe search summary payload."""

    objective_name: str
    metric_name: str
    best_candidate: str
    best_score: float
    evaluated_candidate_count: int
    successful_candidate_count: int
    candidate_results: list[CandidateResultPayload]


@dataclass(slots=True)
class AutoResearch(Generic[CandidateT]):
    """Plugin-authored primitive that runs bounded candidate search at runtime."""

    name: str
    objective: VerifiableObjective[CandidateT]
    candidates: Sequence[CandidateT]
    inputs: list[InputRef] = field(default_factory=list)
    metrics: list[OutputMetricSpec[AutoResearchOutput]] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class AutoResearchExecutor(
    Generic[CandidateT], PluginExecutionHandler[Mapping[str, RuntimeValue], AutoResearchOutput]
):
    primitive: AutoResearch[CandidateT]

    async def execute(
        self,
        inputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> AutoResearchOutput:
        del inputs, context
        search = await search_objective_async(
            self.primitive.objective,
            tuple(self.primitive.candidates),
        )
        return build_search_output(search)


class AutoResearchPlugin:
    """Lowering and runtime compilation for the `AutoResearch` primitive."""

    kind: str = "autoresearch"
    origin: str = "mentalmodel.integrations.autoresearch"
    version: str | None = "0.1.0"

    def supports(self, primitive: object) -> bool:
        return isinstance(primitive, AutoResearch)

    def lower(self, primitive: object, ctx: LoweringContext) -> IRFragment:
        autoresearch = cast(AutoResearch[object], primitive)
        metadata = dict(autoresearch.metadata)
        metadata["objective_name"] = autoresearch.objective.name
        metadata["objective_metric"] = autoresearch.objective.signal.metric_name
        metadata["candidate_count"] = str(len(autoresearch.candidates))
        node = IRNode(
            node_id=autoresearch.name,
            kind="autoresearch",
            label=autoresearch.name,
            metadata=metadata,
        )
        return ctx.lower_leaf(primitive=autoresearch, node=node, inputs=autoresearch.inputs)

    def compile(
        self,
        *,
        primitive: object,
        metadata: ExecutionNodeMetadata,
        input_adapter: MappingInputAdapter[object],
    ) -> CompiledPluginNode[Mapping[str, RuntimeValue], AutoResearchOutput]:
        autoresearch = cast(AutoResearch[object], primitive)
        return CompiledPluginNode(
            metadata=metadata,
            handler=AutoResearchExecutor(primitive=autoresearch),
            input_adapter=cast(MappingInputAdapter[Mapping[str, RuntimeValue]], input_adapter),
            metrics=cast_metric_specs(autoresearch.metrics),
        )


def build_search_output(search: SearchResult[CandidateT]) -> AutoResearchOutput:
    results: list[CandidateResultPayload] = []
    successful_candidate_count = 0
    for result in search.results:
        if result.success:
            successful_candidate_count += 1
        results.append(
            {
                "candidate_label": result.candidate_label,
                "score": result.score,
                "success": result.success,
                "verification_success": result.verification_success,
                "metric_values": list(result.metric_values),
            }
        )
    return {
        "objective_name": search.objective_name,
        "metric_name": search.best_result.signal.metric_name,
        "best_candidate": str(search.best_candidate),
        "best_score": search.best_result.score,
        "evaluated_candidate_count": len(search.results),
        "successful_candidate_count": successful_candidate_count,
        "candidate_results": results,
    }
