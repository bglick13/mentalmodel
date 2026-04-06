from __future__ import annotations

from dataclasses import dataclass

from mentalmodel import ResourceKey


@dataclass(slots=True, frozen=True)
class ReviewPolicy:
    escalation_keywords: tuple[str, ...]


REVIEW_POLICY_RESOURCE = ResourceKey("review_policy", ReviewPolicy)
