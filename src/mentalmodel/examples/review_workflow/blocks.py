from __future__ import annotations

from typing import cast

from mentalmodel import (
    Block,
    BlockDefaults,
    BlockInput,
    BlockOutput,
    BlockRef,
    Effect,
    Invariant,
    Join,
    Ref,
)
from mentalmodel.environment import ResourceKey
from mentalmodel.examples.review_workflow.handlers import (
    NormalizeTicket,
    ReviewAuditReducer,
    ReviewDecisionInvariant,
    ReviewStateReducer,
    ReviewTicket,
)
from mentalmodel.examples.review_workflow.resources import REVIEW_POLICY_RESOURCE

review_step_block = Block(
    "review_step",
    inputs={
        "ticket": BlockInput[object](),
        "review_state": BlockInput[object](),
    },
    outputs={
        "next_state": BlockOutput[object]("next_state"),
        "review_audit": BlockOutput[object]("review_audit"),
    },
    defaults=BlockDefaults(
        resources=cast(
            tuple[ResourceKey[object], ...],
            (REVIEW_POLICY_RESOURCE,),
        )
    ),
    children=(
        Effect(
            "normalize_ticket",
            handler=NormalizeTicket(),
            inputs=[BlockRef("ticket")],
        ),
        Effect(
            "review_ticket",
            handler=ReviewTicket(),
            inputs=[Ref("normalize_ticket")],
        ),
        Join(
            "next_state",
            reducer=ReviewStateReducer(),
            inputs=[Ref("review_ticket"), BlockRef("review_state")],
        ),
        Join(
            "review_audit",
            reducer=ReviewAuditReducer(),
            inputs=[Ref("review_ticket"), BlockRef("review_state")],
        ),
        Invariant(
            "review_decision_consistent",
            checker=ReviewDecisionInvariant(),
            inputs=[Ref("review_ticket"), Ref("review_audit")],
            severity="warning",
        ),
    ),
    description=(
        "Reusable review step that normalizes a ticket, routes it, "
        "and updates queue state."
    ),
)
