from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias, TypedDict

from hypothesis import strategies as st

from mentalmodel.core import (
    Actor,
    ActorHandler,
    Effect,
    EffectHandler,
    Invariant,
    InvariantChecker,
    InvariantResult,
    Join,
    JoinReducer,
    MetricContext,
    MetricExtractor,
    MetricObservation,
    Parallel,
    Ref,
    Workflow,
    extract_output_metrics,
)
from mentalmodel.core.models import ActorResult
from mentalmodel.ir.lowering import lower_program
from mentalmodel.observability.metrics import MetricDefinition, MetricKind
from mentalmodel.plugins.runtime_context import RuntimeContext
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.testing import (
    assert_monotonic_non_decreasing,
    assert_runtime_boundary_crossings,
    collect_runtime_boundary_observations,
    hypothesis_property_check,
    invariant_fail,
    invariant_pass,
    property_check,
)

PlanName = Literal["starter", "pro", "enterprise"]
DiscountName = Literal["none", "annual", "edu"]

PLAN_PRICING: dict[PlanName, float] = {
    "starter": 12.0,
    "pro": 20.0,
    "enterprise": 35.0,
}
PRIORITY_SUPPORT: dict[PlanName, bool] = {
    "starter": False,
    "pro": True,
    "enterprise": True,
}
DISCOUNTS: dict[DiscountName, float] = {
    "none": 0.0,
    "annual": 0.15,
    "edu": 0.30,
}


class BillingTask(TypedDict):
    task_id: str
    question: str
    plan_name: PlanName
    seats: int
    discount_name: DiscountName
    expected_total_monthly_cost: float
    expected_priority_support: bool


DEFAULT_TASKS: tuple[BillingTask, ...] = (
    {
        "task_id": "task-pro-annual-3",
        "question": "What is the monthly cost for 3 Pro seats on annual billing?",
        "plan_name": "pro",
        "seats": 3,
        "discount_name": "annual",
        "expected_total_monthly_cost": 51.0,
        "expected_priority_support": True,
    },
    {
        "task_id": "task-enterprise-edu-2",
        "question": "What is the monthly cost for 2 Enterprise seats on education pricing?",
        "plan_name": "enterprise",
        "seats": 2,
        "discount_name": "edu",
        "expected_total_monthly_cost": 49.0,
        "expected_priority_support": True,
    },
    {
        "task_id": "task-starter-none-4",
        "question": "What is the monthly cost for 4 Starter seats with no discount?",
        "plan_name": "starter",
        "seats": 4,
        "discount_name": "none",
        "expected_total_monthly_cost": 48.0,
        "expected_priority_support": False,
    },
)


def make_task(
    *,
    task_id: str,
    plan_name: PlanName,
    seats: int,
    discount_name: DiscountName,
) -> BillingTask:
    base_price = PLAN_PRICING[plan_name]
    discount_rate = DISCOUNTS[discount_name]
    total = round(base_price * seats * (1.0 - discount_rate), 2)
    return {
        "task_id": task_id,
        "question": (
            f"What is the monthly cost for {seats} {plan_name.title()} seats "
            f"with {discount_name} pricing?"
        ),
        "plan_name": plan_name,
        "seats": seats,
        "discount_name": discount_name,
        "expected_total_monthly_cost": total,
        "expected_priority_support": PRIORITY_SUPPORT[plan_name],
    }


class TaskSourceInputs(TypedDict):
    pass


class PlanningInputs(TypedDict):
    task_source: BillingTask


class ToolPlan(TypedDict):
    task_id: str
    plan_name: PlanName
    seats: int
    discount_name: DiscountName


class PlanLookupOutput(TypedDict):
    monthly_per_seat: float


class DiscountLookupOutput(TypedDict):
    discount_rate: float


class SupportLookupOutput(TypedDict):
    priority_support: bool


class ToolJoinInputs(TypedDict):
    task_source: BillingTask
    plan_task: ToolPlan
    plan_lookup: PlanLookupOutput
    discount_lookup: DiscountLookupOutput
    support_lookup: SupportLookupOutput


class ToolJoinOutput(TypedDict):
    task_id: str
    seats: int
    monthly_per_seat: float
    discount_rate: float
    priority_support: bool


class CalculatorInputs(TypedDict):
    tool_results_join: ToolJoinOutput


class CalculatorOutput(TypedDict):
    undiscounted_monthly_cost: float
    total_monthly_cost: float


class AnswerInputs(TypedDict):
    task_source: BillingTask
    tool_results_join: ToolJoinOutput
    cost_calculator: CalculatorOutput


class AnswerOutput(TypedDict):
    answer_text: str
    total_monthly_cost: float
    priority_support: bool
    tool_call_count: int
    success_score: float


class AnswerInvariantInputs(TypedDict):
    task_source: BillingTask
    answer_synthesizer: AnswerOutput


@dataclass(slots=True, frozen=True)
class AnswerMetricsExtractor(MetricExtractor[AnswerOutput]):
    prefix: str

    def extract(
        self,
        output: AnswerOutput,
        context: MetricContext,
    ) -> tuple[MetricObservation, ...]:
        base_attributes = context.default_attributes()
        return (
            MetricObservation(
                definition=MetricDefinition(
                    name=f"{self.prefix}.total_monthly_cost",
                    kind=MetricKind.HISTOGRAM,
                    description="Total computed monthly cost for one task.",
                ),
                value=output["total_monthly_cost"],
                attributes=dict(base_attributes),
            ),
            MetricObservation(
                definition=MetricDefinition(
                    name=f"{self.prefix}.tool_call_count",
                    kind=MetricKind.HISTOGRAM,
                    description="Number of tool effects used to answer one task.",
                ),
                value=output["tool_call_count"],
                attributes=dict(base_attributes),
            ),
            MetricObservation(
                definition=MetricDefinition(
                    name=f"{self.prefix}.success_score",
                    kind=MetricKind.HISTOGRAM,
                    description="Structured task success score for one answer.",
                ),
                value=output["success_score"],
                attributes=dict(base_attributes),
            ),
        )


@dataclass(slots=True)
class TaskSource(ActorHandler[TaskSourceInputs, object, BillingTask]):
    task: BillingTask

    async def handle(
        self,
        inputs: TaskSourceInputs,
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[BillingTask, object]:
        del inputs, state, ctx
        return ActorResult(output=self.task)


class Planner(ActorHandler[PlanningInputs, object, ToolPlan]):
    async def handle(
        self,
        inputs: PlanningInputs,
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[ToolPlan, object]:
        del state, ctx
        task = inputs["task_source"]
        return ActorResult(
            output={
                "task_id": task["task_id"],
                "plan_name": task["plan_name"],
                "seats": task["seats"],
                "discount_name": task["discount_name"],
            }
        )


class PlanLookup(EffectHandler[PlanningInputs, PlanLookupOutput]):
    async def invoke(
        self,
        inputs: PlanningInputs,
        ctx: ExecutionContext,
    ) -> PlanLookupOutput:
        del ctx
        task = inputs["task_source"]
        return {"monthly_per_seat": PLAN_PRICING[task["plan_name"]]}


class DiscountLookup(EffectHandler[PlanningInputs, DiscountLookupOutput]):
    async def invoke(
        self,
        inputs: PlanningInputs,
        ctx: ExecutionContext,
    ) -> DiscountLookupOutput:
        del ctx
        task = inputs["task_source"]
        return {"discount_rate": DISCOUNTS[task["discount_name"]]}


class SupportLookup(EffectHandler[PlanningInputs, SupportLookupOutput]):
    async def invoke(
        self,
        inputs: PlanningInputs,
        ctx: ExecutionContext,
    ) -> SupportLookupOutput:
        del ctx
        task = inputs["task_source"]
        return {"priority_support": PRIORITY_SUPPORT[task["plan_name"]]}


class ToolResultsJoin(JoinReducer[ToolJoinInputs, ToolJoinOutput]):
    async def reduce(
        self,
        inputs: ToolJoinInputs,
        ctx: ExecutionContext,
    ) -> ToolJoinOutput:
        del ctx
        task = inputs["task_source"]
        return {
            "task_id": task["task_id"],
            "seats": inputs["plan_task"]["seats"],
            "monthly_per_seat": inputs["plan_lookup"]["monthly_per_seat"],
            "discount_rate": inputs["discount_lookup"]["discount_rate"],
            "priority_support": inputs["support_lookup"]["priority_support"],
        }


class CostCalculator(EffectHandler[CalculatorInputs, CalculatorOutput]):
    async def invoke(
        self,
        inputs: CalculatorInputs,
        ctx: ExecutionContext,
    ) -> CalculatorOutput:
        del ctx
        tool_payload = inputs["tool_results_join"]
        undiscounted = round(
            tool_payload["monthly_per_seat"] * tool_payload["seats"],
            2,
        )
        total = round(
            undiscounted * (1.0 - tool_payload["discount_rate"]),
            2,
        )
        return {
            "undiscounted_monthly_cost": undiscounted,
            "total_monthly_cost": total,
        }


class AnswerSynthesizer(ActorHandler[AnswerInputs, object, AnswerOutput]):
    async def handle(
        self,
        inputs: AnswerInputs,
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[AnswerOutput, object]:
        del state, ctx
        task = inputs["task_source"]
        tools = inputs["tool_results_join"]
        total = inputs["cost_calculator"]["total_monthly_cost"]
        priority_support = tools["priority_support"]
        success_score = (
            1.0
            if total == task["expected_total_monthly_cost"]
            and priority_support is task["expected_priority_support"]
            else 0.0
        )
        return ActorResult(
            output={
                "answer_text": (
                    f"{task['plan_name'].title()} costs ${total:.2f}/month for "
                    f"{task['seats']} seats; priority support is "
                    f"{'included' if priority_support else 'not included'}."
                ),
                "total_monthly_cost": total,
                "priority_support": priority_support,
                "tool_call_count": 4,
                "success_score": success_score,
            }
        )


class AnswerInvariant(InvariantChecker[AnswerInvariantInputs, float | bool]):
    async def check(
        self,
        inputs: AnswerInvariantInputs,
        ctx: ExecutionContext,
    ) -> InvariantResult[float | bool]:
        del ctx
        task = inputs["task_source"]
        answer = inputs["answer_synthesizer"]
        details: dict[str, float | bool] = {
            "expected_total_monthly_cost": task["expected_total_monthly_cost"],
            "actual_total_monthly_cost": answer["total_monthly_cost"],
            "expected_priority_support": task["expected_priority_support"],
            "actual_priority_support": answer["priority_support"],
        }
        if (
            answer["total_monthly_cost"] != task["expected_total_monthly_cost"]
            or answer["priority_support"] is not task["expected_priority_support"]
        ):
            return invariant_fail(details=details)
        return invariant_pass(details=details)


AgentToolUseNode: TypeAlias = (
    RuntimeContext
    | Actor[TaskSourceInputs, BillingTask, object]
    | Actor[PlanningInputs, ToolPlan, object]
    | Parallel[
        Effect[PlanningInputs, PlanLookupOutput]
        | Effect[PlanningInputs, DiscountLookupOutput]
        | Effect[PlanningInputs, SupportLookupOutput]
    ]
    | Join[ToolJoinInputs, ToolJoinOutput]
    | Effect[CalculatorInputs, CalculatorOutput]
    | Actor[AnswerInputs, AnswerOutput, object]
    | Invariant[AnswerInvariantInputs, float | bool]
)


def build_program(task: BillingTask | None = None) -> Workflow[AgentToolUseNode]:
    resolved_task = DEFAULT_TASKS[0] if task is None else task
    return Workflow(
        name="agent_tool_use_demo",
        description="Agent tool-use workflow with local planning and sandboxed tools.",
        children=[
            RuntimeContext(
                name="local_control_plane",
                runtime="local",
                children=[
                    Actor[TaskSourceInputs, BillingTask, object](
                        "task_source",
                        handler=TaskSource(task=resolved_task),
                    ),
                    Actor[PlanningInputs, ToolPlan, object](
                        "plan_task",
                        handler=Planner(),
                        inputs=[Ref("task_source")],
                    ),
                    RuntimeContext(
                        name="sandbox_tools",
                        runtime="sandbox",
                        children=[
                            Parallel[
                                Effect[PlanningInputs, PlanLookupOutput]
                                | Effect[PlanningInputs, DiscountLookupOutput]
                                | Effect[PlanningInputs, SupportLookupOutput]
                            ](
                                name="tool_fanout",
                                children=[
                                    Effect[PlanningInputs, PlanLookupOutput](
                                        "plan_lookup",
                                        handler=PlanLookup(),
                                        inputs=[Ref("task_source")],
                                    ),
                                    Effect[PlanningInputs, DiscountLookupOutput](
                                        "discount_lookup",
                                        handler=DiscountLookup(),
                                        inputs=[Ref("task_source")],
                                    ),
                                    Effect[PlanningInputs, SupportLookupOutput](
                                        "support_lookup",
                                        handler=SupportLookup(),
                                        inputs=[Ref("task_source")],
                                    ),
                                ],
                            ),
                            Join[ToolJoinInputs, ToolJoinOutput](
                                "tool_results_join",
                                inputs=[
                                    Ref("task_source"),
                                    Ref("plan_task"),
                                    Ref("plan_lookup"),
                                    Ref("discount_lookup"),
                                    Ref("support_lookup"),
                                ],
                                reducer=ToolResultsJoin(),
                            ),
                            Effect[CalculatorInputs, CalculatorOutput](
                                "cost_calculator",
                                handler=CostCalculator(),
                                inputs=[Ref("tool_results_join")],
                            ),
                        ],
                    ),
                    Actor[AnswerInputs, AnswerOutput, object](
                        "answer_synthesizer",
                        handler=AnswerSynthesizer(),
                        inputs=[
                            Ref("task_source"),
                            Ref("tool_results_join"),
                            Ref("cost_calculator"),
                        ],
                        metrics=[
                            extract_output_metrics(
                                AnswerMetricsExtractor(
                                    prefix="mentalmodel.demo.agent_tool_use.answer"
                                )
                            )
                        ],
                    ),
                    Invariant[AnswerInvariantInputs, float | bool](
                        "answer_invariant",
                        checker=AnswerInvariant(),
                        inputs=[Ref("task_source"), Ref("answer_synthesizer")],
                    ),
                ],
            )
        ],
    )


@hypothesis_property_check(
    "seat increases preserve non-decreasing monthly cost",
    seats=st.integers(min_value=1, max_value=6),
)
def property_total_cost_monotonic(
    program: Workflow[AgentToolUseNode],
    seats: int,
) -> None:
    del program
    values: list[float] = []
    for seat_count in range(1, seats + 1):
        result = build_program(
            make_task(
                task_id=f"seat-check-{seat_count}",
                plan_name="pro",
                seats=seat_count,
                discount_name="annual",
            )
        )
        from mentalmodel.testing import execute_program

        execution = execute_program(result)
        answer = execution.outputs["answer_synthesizer"]
        assert isinstance(answer, dict)
        values.append(float(answer["total_monthly_cost"]))
    assert_monotonic_non_decreasing(values, label="monthly cost by seat count")


@property_check("runtime-context boundary crossings are explicitly declared")
def property_runtime_boundaries_declared(program: Workflow[AgentToolUseNode]) -> None:
    observations = collect_runtime_boundary_observations(lower_program(program))
    assert_runtime_boundary_crossings(
        observations,
        allowed={("local", "sandbox"), ("sandbox", "local")},
    )
