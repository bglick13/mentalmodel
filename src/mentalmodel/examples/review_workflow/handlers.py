from __future__ import annotations

from typing import cast

from mentalmodel.core.interfaces import (
    EffectHandler,
    InvariantChecker,
    JoinReducer,
    JsonValue,
)
from mentalmodel.core.loop import StepLoopResult
from mentalmodel.core.models import InvariantResult
from mentalmodel.examples.review_workflow.resources import REVIEW_POLICY_RESOURCE
from mentalmodel.examples.review_workflow.types import (
    NormalizedTicket,
    QueueSummary,
    ReviewAudit,
    ReviewDecision,
    ReviewState,
    Ticket,
)
from mentalmodel.runtime.context import ExecutionContext


class TicketBatchSource(EffectHandler[dict[str, object], list[Ticket]]):
    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> list[Ticket]:
        del inputs, ctx
        return [
            {
                "ticket_id": "ticket-001",
                "text": "Publish the approved spring announcement.",
                "contains_pii": False,
            },
            {
                "ticket_id": "ticket-002",
                "text": "Customer is asking for a refund on a delayed shipment.",
                "contains_pii": False,
            },
            {
                "ticket_id": "ticket-003",
                "text": "Customer included account number 1234 in the request.",
                "contains_pii": True,
            },
        ]


class NormalizeTicket(EffectHandler[dict[str, object], NormalizedTicket]):
    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> NormalizedTicket:
        del ctx
        ticket = cast(Ticket, inputs["ticket"])
        return {
            "ticket_id": ticket["ticket_id"],
            "normalized_text": ticket["text"].strip().lower(),
            "contains_pii": ticket["contains_pii"],
        }


class ReviewTicket(EffectHandler[dict[str, object], ReviewDecision]):
    async def invoke(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> ReviewDecision:
        ticket = cast(NormalizedTicket, inputs["normalize_ticket"])
        policy = ctx.resources.require(REVIEW_POLICY_RESOURCE)
        escalated = ticket["contains_pii"] or any(
            keyword in ticket["normalized_text"]
            for keyword in policy.escalation_keywords
        )
        return {
            "ticket_id": ticket["ticket_id"],
            "route": "human_review" if escalated else "auto_publish",
            "confidence": 0.91 if escalated else 0.62,
            "escalated": escalated,
        }


class ReviewStateReducer(JoinReducer[dict[str, object], ReviewState]):
    async def reduce(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> ReviewState:
        del ctx
        prior = cast(ReviewState, inputs["review_state"])
        decision = cast(ReviewDecision, inputs["review_ticket"])
        return {
            "processed": prior["processed"] + 1,
            "escalations": prior["escalations"] + (1 if decision["escalated"] else 0),
        }


class ReviewAuditReducer(JoinReducer[dict[str, object], ReviewAudit]):
    async def reduce(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> ReviewAudit:
        del ctx
        prior = cast(ReviewState, inputs["review_state"])
        decision = cast(ReviewDecision, inputs["review_ticket"])
        return {
            "ticket_id": decision["ticket_id"],
            "route": decision["route"],
            "escalated": decision["escalated"],
            "processed_after": prior["processed"] + 1,
            "escalations_after": prior["escalations"] + (1 if decision["escalated"] else 0),
        }


class ReviewDecisionInvariant(InvariantChecker[dict[str, object], JsonValue]):
    async def check(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> InvariantResult[JsonValue]:
        del ctx
        decision = cast(ReviewDecision, inputs["review_ticket"])
        audit = cast(ReviewAudit, inputs["review_audit"])
        passed = (
            audit["escalated"] == decision["escalated"]
            and (
                (decision["route"] == "human_review") == decision["escalated"]
            )
        )
        return InvariantResult(
            passed=passed,
            details={
                "route": decision["route"],
                "escalated": decision["escalated"],
                "audit_escalated": audit["escalated"],
            },
        )


class QueueSummaryReducer(JoinReducer[dict[str, object], QueueSummary]):
    async def reduce(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> QueueSummary:
        del ctx
        step_loop = inputs["ticket_review_loop"]
        if isinstance(step_loop, StepLoopResult):
            final_outputs = step_loop.final_outputs
            history_outputs = step_loop.history_outputs
        else:
            mapping = cast(dict[str, object], step_loop)
            final_outputs = cast(dict[str, object], mapping["final_outputs"])
            history_outputs = cast(dict[str, list[object]], mapping["history_outputs"])
        final_state = cast(ReviewState, final_outputs["next_state"])
        audits = cast(list[ReviewAudit], history_outputs["review_audit"])
        return {
            "processed": final_state["processed"],
            "escalations": final_state["escalations"],
            "auto_publish": sum(1 for audit in audits if audit["route"] == "auto_publish"),
        }


class QueueSummaryInvariant(InvariantChecker[dict[str, object], JsonValue]):
    async def check(
        self,
        inputs: dict[str, object],
        ctx: ExecutionContext,
    ) -> InvariantResult[JsonValue]:
        del ctx
        summary = cast(QueueSummary, inputs["queue_summary"])
        passed = summary["processed"] == summary["escalations"] + summary["auto_publish"]
        return InvariantResult(
            passed=passed,
            details={
                "processed": summary["processed"],
                "escalations": summary["escalations"],
                "auto_publish": summary["auto_publish"],
            },
        )
