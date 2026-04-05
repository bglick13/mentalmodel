from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

from mentalmodel.plugins.base import ExecutablePrimitivePlugin, PrimitivePlugin


@dataclass(slots=True)
class PluginRegistry:
    """Lookup table for extension primitives."""

    plugins: list[PrimitivePlugin] = field(default_factory=list)

    def register(self, plugin: PrimitivePlugin) -> None:
        if any(existing.kind == plugin.kind for existing in self.plugins):
            raise ValueError(f"Plugin kind already registered: {plugin.kind!r}")
        self.plugins.append(plugin)

    def find_plugin(self, primitive: object) -> PrimitivePlugin | None:
        for plugin in self.plugins:
            if plugin.supports(primitive):
                return plugin
        return None

    def find_executable_plugin(self, primitive: object) -> ExecutablePrimitivePlugin | None:
        plugin = self.find_plugin(primitive)
        if plugin is None or not hasattr(plugin, "compile"):
            return None
        return cast(ExecutablePrimitivePlugin, plugin)


def default_registry() -> PluginRegistry:
    """Create the default registry used by lowering and CLI tools."""

    from mentalmodel.integrations.autoresearch.plugin import AutoResearchPlugin
    from mentalmodel.plugins.runtime_context import RuntimeContextPlugin

    registry = PluginRegistry()
    registry.register(RuntimeContextPlugin())
    registry.register(AutoResearchPlugin())
    return registry
