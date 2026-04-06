from __future__ import annotations

from typing import Literal, TypedDict


class Ticket(TypedDict):
    ticket_id: str
    text: str
    contains_pii: bool


class NormalizedTicket(TypedDict):
    ticket_id: str
    normalized_text: str
    contains_pii: bool


ReviewRoute = Literal["auto_publish", "human_review"]


class ReviewDecision(TypedDict):
    ticket_id: str
    route: ReviewRoute
    confidence: float
    escalated: bool


class ReviewState(TypedDict):
    processed: int
    escalations: int


class ReviewAudit(TypedDict):
    ticket_id: str
    route: ReviewRoute
    escalated: bool
    processed_after: int
    escalations_after: int


class QueueSummary(TypedDict):
    processed: int
    escalations: int
    auto_publish: int
