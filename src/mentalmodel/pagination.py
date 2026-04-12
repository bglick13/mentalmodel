from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(slots=True, frozen=True)
class PageSlice(Generic[T]):
    """One descending sequence page returned by backend and UI services."""

    items: tuple[T, ...]
    next_cursor: str | None
    total_count: int

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None


def decode_sequence_cursor(cursor: str | None) -> int | None:
    if cursor is None or cursor == "":
        return None
    try:
        value = int(cursor)
    except ValueError as exc:
        raise ValueError("Cursor must be an integer sequence value.") from exc
    if value < 0:
        raise ValueError("Cursor must be non-negative.")
    return value


def encode_sequence_cursor(sequence: int | None) -> str | None:
    if sequence is None:
        return None
    return str(sequence)


def paginate_descending_sequence(
    items: Sequence[T],
    *,
    sequence_for: Callable[[T], int],
    cursor: str | None,
    limit: int,
) -> PageSlice[T]:
    if limit < 1:
        raise ValueError("Page size limit must be positive.")
    before = decode_sequence_cursor(cursor)
    ordered = sorted(items, key=sequence_for, reverse=True)
    if before is not None:
        ordered = [item for item in ordered if sequence_for(item) < before]
    page_items = tuple(ordered[:limit])
    next_cursor = None
    if len(ordered) > limit and page_items:
        next_cursor = encode_sequence_cursor(sequence_for(page_items[-1]))
    return PageSlice(
        items=page_items,
        next_cursor=next_cursor,
        total_count=len(items),
    )
