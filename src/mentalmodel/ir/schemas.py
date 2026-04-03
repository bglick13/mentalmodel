from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class EntryPointSpec:
    """Parsed `module:function` style entrypoint spec."""

    module_name: str
    attribute_name: str
