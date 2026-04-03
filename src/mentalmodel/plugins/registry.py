from __future__ import annotations

from dataclasses import dataclass, field

from mentalmodel.plugins.base import PrimitivePlugin


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


def default_registry() -> PluginRegistry:
    """Create the default registry used by lowering and CLI tools."""

    from mentalmodel.plugins.runtime_context import RuntimeContextPlugin

    registry = PluginRegistry()
    registry.register(RuntimeContextPlugin())
    return registry
