"""Autoresearch-style bundle generation and runtime plugin exports."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mentalmodel.integrations.autoresearch.adapter import (
        AutoresearchBundle,
        build_sorting_demo_bundle,
        write_autoresearch_bundle,
    )
    from mentalmodel.integrations.autoresearch.plugin import AutoResearch, AutoResearchPlugin

__all__ = [
    "AutoResearch",
    "AutoResearchPlugin",
    "AutoresearchBundle",
    "build_sorting_demo_bundle",
    "write_autoresearch_bundle",
]


def __getattr__(name: str) -> object:
    if name in {"AutoResearch", "AutoResearchPlugin"}:
        from mentalmodel.integrations.autoresearch.plugin import AutoResearch, AutoResearchPlugin

        if name == "AutoResearch":
            return AutoResearch
        return AutoResearchPlugin
    if name in {
        "AutoresearchBundle",
        "build_sorting_demo_bundle",
        "write_autoresearch_bundle",
    }:
        from mentalmodel.integrations.autoresearch.adapter import (
            AutoresearchBundle,
            build_sorting_demo_bundle,
            write_autoresearch_bundle,
        )

        if name == "AutoresearchBundle":
            return AutoresearchBundle
        if name == "build_sorting_demo_bundle":
            return build_sorting_demo_bundle
        return write_autoresearch_bundle
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
