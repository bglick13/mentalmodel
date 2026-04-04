from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mentalmodel.plugins.base import PrimitivePlugin

PLUGIN_KIND_METADATA_KEY = "plugin_kind"
PLUGIN_ORIGIN_METADATA_KEY = "plugin_origin"
PLUGIN_VERSION_METADATA_KEY = "plugin_version"

CORE_PLUGIN_KIND = "core"
CORE_PLUGIN_ORIGIN = "mentalmodel.core"


@dataclass(slots=True, frozen=True)
class NodeProvenance:
    """Stable provenance metadata stamped onto lowered IR nodes."""

    kind: str
    origin: str
    version: str | None = None

    @classmethod
    def core(cls) -> NodeProvenance:
        """Return the canonical provenance for core primitives."""

        return cls(kind=CORE_PLUGIN_KIND, origin=CORE_PLUGIN_ORIGIN)

    @classmethod
    def from_plugin(cls, plugin: PrimitivePlugin) -> NodeProvenance:
        """Construct provenance from a registered plugin."""

        return cls(kind=plugin.kind, origin=plugin.origin, version=plugin.version)

    def as_metadata(self) -> dict[str, str]:
        """Return the metadata projection stored on IR nodes."""

        metadata = {
            PLUGIN_KIND_METADATA_KEY: self.kind,
            PLUGIN_ORIGIN_METADATA_KEY: self.origin,
        }
        if self.version is not None:
            metadata[PLUGIN_VERSION_METADATA_KEY] = self.version
        return metadata
