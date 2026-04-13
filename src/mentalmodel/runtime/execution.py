from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Generic, Protocol, TypeVar, cast

from mentalmodel.core.bindings import InputBindingSource
from mentalmodel.core.interfaces import JsonValue, RuntimeValue
from mentalmodel.environment import ResourceKey
from mentalmodel.observability.export import serialize_runtime_value
from mentalmodel.observability.metrics import (
    OutputMetricSpec,
    derive_output_metrics,
    emit_metric_batch,
)
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.events import NODE_INPUTS_RESOLVED

InputT = TypeVar("InputT")
InputT_contra = TypeVar("InputT_contra", contravariant=True)
InputBoundT_co = TypeVar("InputBoundT_co", covariant=True)
OutputT = TypeVar("OutputT")
OutputT_co = TypeVar("OutputT_co", covariant=True)


@dataclass(slots=True, frozen=True)
class ExecutionNodeMetadata:
    """Executable node metadata derived from the canonical IR."""

    node_id: str
    kind: str
    label: str
    runtime_context: str | None
    metadata: dict[str, str]
    dependencies: tuple[str, ...]
    input_bindings: tuple[tuple[str, InputBindingSource], ...]
    resource_keys: tuple[ResourceKey[object], ...] = field(default_factory=tuple)


class InputAdapter(Protocol[InputBoundT_co]):
    """Converts resolved upstream runtime values into a handler input shape."""

    def bind(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> InputBoundT_co:
        """Bind raw upstream outputs into the typed handler input."""


@dataclass(slots=True, frozen=True)
class MappingInputAdapter(Generic[InputT]):
    """Default adapter that presents upstream outputs as a mapping."""

    bindings: tuple[tuple[str, InputBindingSource], ...]

    def bind(self, outputs: Mapping[str, RuntimeValue], context: ExecutionContext) -> InputT:
        bound = {}
        for alias, source in self.bindings:
            if source.kind == "node_output":
                bound[alias] = outputs[source.key]
                continue
            if source.kind == "loop_item":
                bound[alias] = context.loop_item_values[source.key]
                continue
            bound[alias] = context.loop_state_values[source.key]
        return cast(InputT, bound)


class CompiledExecutionNode(Protocol):
    """Executable runtime node compiled from a semantic primitive."""

    metadata: ExecutionNodeMetadata

    async def execute(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> RuntimeValue:
        """Execute the node against resolved upstream outputs."""


class PluginExecutionHandler(Protocol[InputT_contra, OutputT_co]):
    """Runtime contract for executable plugin node handlers."""

    async def execute(
        self,
        inputs: InputT_contra,
        context: ExecutionContext,
    ) -> OutputT_co:
        """Execute the plugin-owned node and return its output."""


@dataclass(slots=True, frozen=True)
class CompiledPluginNode(Generic[InputT, OutputT]):
    """Executable runtime node compiled from a plugin primitive."""

    metadata: ExecutionNodeMetadata
    handler: PluginExecutionHandler[InputT, OutputT]
    input_adapter: InputAdapter[InputT]
    metrics: tuple[OutputMetricSpec[object], ...] = ()

    async def execute(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> RuntimeValue:
        context.require_resources(self.metadata.resource_keys)
        typed_inputs = self.input_adapter.bind(outputs, context)
        record_resolved_inputs(context=context, metadata=self.metadata, inputs=typed_inputs)
        output = await self.handler.execute(typed_inputs, context)
        emit_metric_batch(
            context.metrics,
            derive_output_metrics(
                output=output,
                context=context.metric_context(),
                specs=self.metrics,
            ),
        )
        return cast(RuntimeValue, output)


def summarize_runtime_value(value: RuntimeValue) -> dict[str, JsonValue]:
    """Summarize runtime values into recorder-safe JSON payloads."""

    if value is None:
        return {"type": "None"}
    if isinstance(value, dict):
        return {"type": "dict", "keys": [str(key) for key in sorted(value.keys())]}
    if isinstance(value, list):
        return {"type": "list", "length": len(value)}
    return {"type": type(value).__name__}


def runtime_value_payload(
    *,
    value: RuntimeValue,
    metadata: ExecutionNodeMetadata,
) -> JsonValue:
    mode = metadata.metadata.get("record_payload_mode", "full").strip().lower()
    if mode == "summary":
        return summarize_runtime_value(value)
    if mode == "none":
        return {"type": "suppressed"}
    return serialize_runtime_value(value)


def capture_framed_output(metadata: ExecutionNodeMetadata) -> bool:
    raw_value = metadata.metadata.get("capture_framed_output", "true").strip().lower()
    return raw_value not in {"false", "0", "no"}


def record_resolved_inputs(
    *,
    context: ExecutionContext,
    metadata: ExecutionNodeMetadata,
    inputs: object,
) -> None:
    """Record the concrete input payload bound for one executable node."""

    context.recorder.record(
        run_id=context.run_id,
        node_id=metadata.node_id,
        event_type=NODE_INPUTS_RESOLVED,
        timestamp_ms=context.clock.now_ms(),
        frame=context.frame,
        payload={
            "input_keys": [alias for alias, _ in metadata.input_bindings],
            "inputs": runtime_value_payload(
                value=cast(RuntimeValue, inputs),
                metadata=metadata,
            ),
        },
    )
