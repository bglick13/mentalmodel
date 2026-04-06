from __future__ import annotations

from typing import cast

from mentalmodel import (
    BlockDefaults,
    Effect,
    Invariant,
    Join,
    LoopCarry,
    LoopItemRef,
    LoopStateRef,
    LoopSummary,
    Ref,
    StepLoop,
    Use,
    Workflow,
)
from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.environment import ResourceKey, RuntimeEnvironment, RuntimeProfile
from mentalmodel.examples.review_workflow.blocks import review_step_block
from mentalmodel.examples.review_workflow.handlers import (
    QueueSummaryInvariant,
    QueueSummaryReducer,
    TicketBatchSource,
)
from mentalmodel.examples.review_workflow.resources import (
    REVIEW_POLICY_RESOURCE,
    ReviewPolicy,
)


def build_environment() -> RuntimeEnvironment:
    return RuntimeEnvironment(
        profiles={
            "fixture_review": RuntimeProfile(
                name="fixture_review",
                resources=cast(
                    dict[ResourceKey[object], object],
                    {REVIEW_POLICY_RESOURCE: ReviewPolicy(escalation_keywords=tuple())},
                ),
            ),
            "strict_review": RuntimeProfile(
                name="strict_review",
                resources=cast(
                    dict[ResourceKey[object], object],
                    {
                        REVIEW_POLICY_RESOURCE: ReviewPolicy(
                            escalation_keywords=("refund", "chargeback"),
                        )
                    },
                ),
            ),
        }
    )


def build_program(*, review_runtime: str = "fixture_review") -> Workflow[NamedPrimitive]:
    return Workflow(
        "review_workflow",
        description=(
            "Serious reference workflow showing Block + StepLoop + RuntimeEnvironment "
            "on a ticket review queue."
        ),
        children=(
            Effect(
                "tickets",
                handler=TicketBatchSource(),
                metadata={"runtime_context": review_runtime},
            ),
            StepLoop(
                "ticket_review_loop",
                for_each=Ref("tickets"),
                body=Use(
                    "review_step",
                    block=review_step_block,
                    bind={
                        "ticket": LoopItemRef(),
                        "review_state": LoopStateRef("review_state"),
                    },
                    defaults=BlockDefaults(runtime_context=review_runtime),
                ),
                carry=LoopCarry(
                    state_name="review_state",
                    initial={"processed": 0, "escalations": 0},
                    next_state_output="next_state",
                ),
                summary=LoopSummary(
                    final_outputs=("next_state",),
                    history_outputs=("review_audit",),
                ),
            ),
            Join(
                "queue_summary",
                reducer=QueueSummaryReducer(),
                inputs=[Ref("ticket_review_loop")],
            ),
            Invariant(
                "queue_summary_consistent",
                checker=QueueSummaryInvariant(),
                inputs=[Ref("queue_summary")],
                metadata={"runtime_context": review_runtime},
            ),
        ),
    )
