from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Generic, TypeAlias, TypeVar, cast

from mentalmodel.core import Actor, Effect, Invariant, Join, StepLoop, Workflow
from mentalmodel.core.bindings import InputBindingSource
from mentalmodel.core.interfaces import (
    ActorHandler,
    InvariantChecker,
    JoinReducer,
    JsonValue,
    NamedPrimitive,
    RuntimeValue,
)
from mentalmodel.core.loop import StepLoopResult
from mentalmodel.environment import ResourceKey
from mentalmodel.errors import LoweringError
from mentalmodel.ir.graph import IRGraph, IRNode
from mentalmodel.ir.lowering import lower_program_with_bindings
from mentalmodel.observability.metrics import (
    OutputMetricSpec,
    cast_metric_specs,
    derive_output_metrics,
    emit_metric_batch,
    invariant_failure_observation,
    node_duration_observation,
    node_execution_observation,
)
from mentalmodel.observability.serialization import serialize_runtime_value
from mentalmodel.plugins.registry import PluginRegistry, default_registry
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.errors import InvariantViolationError
from mentalmodel.runtime.events import (
    EFFECT_COMPLETED,
    EFFECT_INVOKED,
    INVARIANT_CHECKED,
    JOIN_RESOLVED,
    NODE_FAILED,
    NODE_STARTED,
    NODE_SUCCEEDED,
    STATE_READ,
    STATE_TRANSITION,
)
from mentalmodel.runtime.execution import (
    CompiledPluginNode,
    ExecutionNodeMetadata,
    InputAdapter,
    MappingInputAdapter,
    record_resolved_inputs,
    summarize_runtime_value,
)
from mentalmodel.runtime.frame import FramedNodeValue, FramedStateValue
from mentalmodel.runtime.scheduler import execute_plan_nodes

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
StateT = TypeVar("StateT")
DetailT = TypeVar("DetailT", bound=JsonValue)


@dataclass(slots=True, frozen=True)
class CompiledActorNode(Generic[InputT, OutputT, StateT]):
    metadata: ExecutionNodeMetadata
    handler: ActorHandler[InputT, StateT, OutputT]
    input_adapter: InputAdapter[InputT]
    state_key: str
    metrics: tuple[OutputMetricSpec[object], ...] = ()

    async def execute(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> RuntimeValue:
        context.require_resources(self.metadata.resource_keys)
        typed_inputs = self.input_adapter.bind(outputs, context)
        record_resolved_inputs(context=context, metadata=self.metadata, inputs=typed_inputs)
        previous_state = context.state_store.get(self.state_key)
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=STATE_READ,
            timestamp_ms=context.clock.now_ms(),
            frame=context.frame,
            payload={"had_state": previous_state is not None},
        )
        result = await self.handler.handle(
            typed_inputs,
            cast(StateT | None, previous_state),
            context,
        )
        if result.next_state is not None or previous_state is not None:
            context.state_store[self.state_key] = result.next_state
            context.framed_state.append(
                FramedStateValue(
                    state_key=self.state_key,
                    frame=context.frame,
                    value=cast(RuntimeValue, result.next_state),
                )
            )
            context.recorder.record(
                run_id=context.run_id,
                node_id=self.metadata.node_id,
                event_type=STATE_TRANSITION,
                timestamp_ms=context.clock.now_ms(),
                frame=context.frame,
                payload={
                    "from_state": summarize_runtime_value(previous_state),
                    "to_state": summarize_runtime_value(cast(RuntimeValue, result.next_state)),
                },
            )
        emit_metric_batch(
            context.metrics,
            derive_output_metrics(
                output=result.output,
                context=context.metric_context(),
                specs=self.metrics,
            ),
        )
        return cast(RuntimeValue, result.output)


@dataclass(slots=True, frozen=True)
class CompiledEffectNode(Generic[InputT, OutputT]):
    metadata: ExecutionNodeMetadata
    handler: Effect[InputT, OutputT]
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
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=EFFECT_INVOKED,
            timestamp_ms=context.clock.now_ms(),
            frame=context.frame,
            payload={
                "input_keys": [alias for alias, _ in self.metadata.input_bindings]
            },
        )
        output = await self.handler.handler.invoke(typed_inputs, context)
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=EFFECT_COMPLETED,
            timestamp_ms=context.clock.now_ms(),
            frame=context.frame,
            payload={"output_type": type(output).__name__},
        )
        emit_metric_batch(
            context.metrics,
            derive_output_metrics(
                output=output,
                context=context.metric_context(),
                specs=self.metrics,
            ),
        )
        return cast(RuntimeValue, output)


@dataclass(slots=True, frozen=True)
class CompiledJoinNode(Generic[InputT, OutputT]):
    metadata: ExecutionNodeMetadata
    reducer: JoinReducer[InputT, OutputT] | None
    input_adapter: InputAdapter[InputT]

    async def execute(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> RuntimeValue:
        context.require_resources(self.metadata.resource_keys)
        typed_inputs = self.input_adapter.bind(outputs, context)
        record_resolved_inputs(context=context, metadata=self.metadata, inputs=typed_inputs)
        if self.reducer is None:
            output = cast(RuntimeValue, typed_inputs)
        else:
            output = cast(RuntimeValue, await self.reducer.reduce(typed_inputs, context))
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=JOIN_RESOLVED,
            timestamp_ms=context.clock.now_ms(),
            frame=context.frame,
            payload={
                "input_keys": [alias for alias, _ in self.metadata.input_bindings]
            },
        )
        return output


@dataclass(slots=True, frozen=True)
class CompiledInvariantNode(Generic[InputT, DetailT]):
    metadata: ExecutionNodeMetadata
    checker: InvariantChecker[InputT, DetailT]
    severity: str
    input_adapter: InputAdapter[InputT]

    async def execute(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> RuntimeValue:
        context.require_resources(self.metadata.resource_keys)
        typed_inputs = self.input_adapter.bind(outputs, context)
        record_resolved_inputs(context=context, metadata=self.metadata, inputs=typed_inputs)
        result = await self.checker.check(typed_inputs, context)
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=INVARIANT_CHECKED,
            timestamp_ms=context.clock.now_ms(),
            frame=context.frame,
            payload={"passed": result.passed, "severity": self.severity},
        )
        if not result.passed:
            emit_metric_batch(
                context.metrics,
                [
                    invariant_failure_observation(
                        context.metric_context(),
                        severity=self.severity,
                    )
                ],
            )
            if self.severity == "warning":
                return cast(RuntimeValue, result)
            raise InvariantViolationError(
                f"Invariant {self.metadata.node_id!r} failed: {dict(result.details)!r}"
            )
        return cast(RuntimeValue, result)


@dataclass(slots=True, frozen=True)
class CompiledStepLoopNode:
    metadata: ExecutionNodeMetadata
    primitive: StepLoop
    input_adapter: InputAdapter[object]
    body_plan: ExecutionPlan
    body_output_nodes: dict[str, str]

    async def execute(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> RuntimeValue:
        context.require_resources(self.metadata.resource_keys)
        typed_inputs = self.input_adapter.bind(outputs, context)
        record_resolved_inputs(context=context, metadata=self.metadata, inputs=typed_inputs)
        if not isinstance(typed_inputs, Mapping):
            raise LoweringError("StepLoop inputs must bind to a mapping.")
        if "for_each" not in typed_inputs:
            raise LoweringError(
                f"StepLoop {self.metadata.node_id!r} is missing the bound for_each input."
            )
        raw_items = typed_inputs["for_each"]
        if not isinstance(raw_items, list):
            raise LoweringError(
                f"StepLoop {self.metadata.node_id!r} expected for_each to be a list."
            )

        carry_state = self.primitive.carry.initial if self.primitive.carry is not None else None
        history_outputs: dict[str, list[object]] = {
            name: [] for name in self.primitive.summary.history_outputs
        } if self.primitive.summary is not None else {}
        final_outputs: dict[str, object] = {}

        max_iterations = self.primitive.max_iterations
        iteration_items = raw_items if max_iterations is None else raw_items[:max_iterations]
        for iteration_index, item in enumerate(iteration_items):
            iteration_frame = context.frame.child(
                loop_node_id=self.metadata.node_id,
                iteration_index=iteration_index,
            )
            iteration_ctx = context.with_frame(iteration_frame).with_loop_bindings(
                item_values={"item": item},
                state_values=(
                    {}
                    if self.primitive.carry is None
                    else {self.primitive.carry.state_name: carry_state}
                ),
            )
            iteration_outputs = await execute_plan_nodes(
                plan=self.body_plan,
                context=iteration_ctx,
                run_node=_LoopNodeRunner(),
                max_concurrency=1,
                initial_outputs=context.outputs,
            )
            final_output_names = (
                self.primitive.summary.final_outputs
                if self.primitive.summary is not None
                else ()
            )
            history_output_names = (
                self.primitive.summary.history_outputs
                if self.primitive.summary is not None
                else ()
            )
            for logical_name in final_output_names:
                final_outputs[logical_name] = iteration_outputs[
                    self.body_output_nodes[logical_name]
                ]
            for logical_name in history_output_names:
                history_outputs[logical_name].append(
                    iteration_outputs[self.body_output_nodes[logical_name]]
                )
            if self.primitive.carry is not None:
                carry_source = self.body_output_nodes[self.primitive.carry.next_state_output]
                carry_state = iteration_outputs[carry_source]

        return cast(
            RuntimeValue,
            StepLoopResult(
                iteration_count=len(iteration_items),
                final_outputs=final_outputs,
                history_outputs=history_outputs,
                final_carry_state=carry_state,
            ),
        )


PlanNode: TypeAlias = (
    CompiledActorNode[object, object, object]
    | CompiledEffectNode[object, object]
    | CompiledJoinNode[object, object]
    | CompiledInvariantNode[object, JsonValue]
    | CompiledStepLoopNode
    | CompiledPluginNode[object, object]
)


@dataclass(slots=True, frozen=True)
class ExecutionPlan:
    """Typed compiled execution plan for a lowered graph."""

    nodes: dict[str, PlanNode]

    def dependencies_for(self, node_id: str) -> tuple[str, ...]:
        return self.nodes[node_id].metadata.dependencies


@dataclass(slots=True, frozen=True)
class CompiledProgram:
    """Executable projection of an authored program."""

    program: Workflow[NamedPrimitive]
    graph: IRGraph
    plan: ExecutionPlan
    resource_bindings: Mapping[str, tuple[ResourceKey[object], ...]]


def compile_program(
    program: Workflow[NamedPrimitive],
    *,
    registry: PluginRegistry | None = None,
) -> CompiledProgram:
    """Compile authored primitives into a graph plus a typed execution plan."""

    resolved_registry = registry or default_registry()
    graph, primitives, resource_bindings = lower_program_with_bindings(
        program,
        registry=resolved_registry,
    )
    plan = build_execution_plan(
        graph=graph,
        primitives=primitives,
        resource_bindings=resource_bindings,
        registry=resolved_registry,
    )
    return CompiledProgram(
        program=program,
        graph=graph,
        plan=plan,
        resource_bindings=resource_bindings,
    )


def build_execution_plan(
    *,
    graph: IRGraph,
    primitives: Mapping[str, NamedPrimitive],
    resource_bindings: Mapping[str, tuple[ResourceKey[object], ...]],
    registry: PluginRegistry,
    node_filter: set[str] | None = None,
    include_loop_owned: bool = False,
    local_dependencies_only: bool = False,
) -> ExecutionPlan:
    """Build executable runtime nodes from a lowered graph and primitive index."""

    allowed_node_ids = (
        {node.node_id for node in graph.nodes}
        if node_filter is None
        else set(node_filter)
    )
    dependencies = build_data_dependencies(
        graph,
        allowed_node_ids=allowed_node_ids,
        local_only=local_dependencies_only,
    )
    input_bindings = build_input_bindings(graph, allowed_node_ids=allowed_node_ids)
    compiled_nodes: dict[str, PlanNode] = {}

    for node in graph.nodes:
        if node.node_id not in allowed_node_ids:
            continue
        if not include_loop_owned and "loop_owner" in node.metadata:
            continue
        primitive = primitives.get(node.node_id)
        if primitive is None:
            continue
        if (
            node.kind not in {"actor", "effect", "join", "invariant", "step_loop"}
            and registry.find_executable_plugin(primitive) is None
        ):
            continue
        metadata = ExecutionNodeMetadata(
            node_id=node.node_id,
            kind=node.kind,
            label=node.label,
            runtime_context=node.metadata.get("runtime_context"),
            metadata=dict(node.metadata),
            dependencies=tuple(sorted(dependencies[node.node_id])),
            input_bindings=tuple(sorted(input_bindings[node.node_id].items())),
            resource_keys=resource_bindings.get(node.node_id, tuple()),
        )
        adapter = MappingInputAdapter[object](metadata.input_bindings)
        compiled_nodes[node.node_id] = compile_execution_node(
            metadata=metadata,
            primitive=primitive,
            input_adapter=adapter,
            registry=registry,
            graph=graph,
            primitives=primitives,
            resource_bindings=resource_bindings,
        )

    return ExecutionPlan(nodes=compiled_nodes)


def compile_execution_node(
    *,
    metadata: ExecutionNodeMetadata,
    primitive: NamedPrimitive,
    input_adapter: MappingInputAdapter[object],
    registry: PluginRegistry | None = None,
    graph: IRGraph | None = None,
    primitives: Mapping[str, NamedPrimitive] | None = None,
    resource_bindings: Mapping[str, tuple[ResourceKey[object], ...]] | None = None,
) -> PlanNode:
    """Compile one authored primitive into an executable runtime node."""

    resolved_registry = registry or default_registry()
    if metadata.kind == "actor" and isinstance(primitive, Actor):
        return cast(
            PlanNode,
            CompiledActorNode(
                metadata=metadata,
                handler=primitive.handler,
                input_adapter=input_adapter,
                state_key=metadata.node_id,
                metrics=cast_metric_specs(primitive.metrics),
            ),
        )
    if metadata.kind == "effect" and isinstance(primitive, Effect):
        return cast(
            PlanNode,
            CompiledEffectNode(
                metadata=metadata,
                handler=primitive,
                input_adapter=input_adapter,
                metrics=cast_metric_specs(primitive.metrics),
            ),
        )
    if metadata.kind == "join" and isinstance(primitive, Join):
        return cast(
            PlanNode,
            CompiledJoinNode(
                metadata=metadata,
                reducer=primitive.reducer,
                input_adapter=input_adapter,
            ),
        )
    if metadata.kind == "invariant" and isinstance(primitive, Invariant):
        return cast(
            PlanNode,
            CompiledInvariantNode(
                metadata=metadata,
                checker=primitive.checker,
                severity=primitive.severity,
                input_adapter=input_adapter,
            ),
        )
    if metadata.kind == "step_loop" and isinstance(primitive, StepLoop):
        if graph is None or primitives is None:
            raise LoweringError("StepLoop compilation requires graph and primitive bindings.")
        body_node_ids = {
            node.node_id
            for node in graph.nodes
            if node.metadata.get("loop_owner") == metadata.node_id
        }
        body_plan = build_execution_plan(
            graph=graph,
            primitives=primitives,
            resource_bindings=resource_bindings or {},
            registry=resolved_registry,
            node_filter=body_node_ids,
            include_loop_owned=True,
            local_dependencies_only=True,
        )
        body_output_nodes = {
            logical_name: f"{metadata.node_id}.{primitive.body.name}.{output.source_node_id}"
            for logical_name, output in primitive.body.block.outputs.items()
        }
        return cast(
            PlanNode,
            CompiledStepLoopNode(
                metadata=metadata,
                primitive=primitive,
                input_adapter=input_adapter,
                body_plan=body_plan,
                body_output_nodes=body_output_nodes,
            ),
        )
    plugin = resolved_registry.find_executable_plugin(primitive)
    if plugin is not None:
        return cast(
            PlanNode,
            plugin.compile(
                primitive=primitive,
                metadata=metadata,
                input_adapter=input_adapter,
            ),
        )
    raise LoweringError(
        "Primitive kind mismatch during execution-plan compilation: "
        f"node={metadata.node_id!r} kind={metadata.kind!r} primitive={type(primitive).__name__!r}"
    )

def build_data_dependencies(
    graph: IRGraph,
    *,
    allowed_node_ids: set[str],
    local_only: bool,
) -> dict[str, set[str]]:
    """Build a data-dependency map for executable graph nodes."""

    dependencies: dict[str, set[str]] = defaultdict(set)
    for node in graph.nodes:
        if node.node_id not in allowed_node_ids:
            continue
        dependencies[node.node_id]
    for edge in graph.edges:
        if edge.kind != "data":
            continue
        if edge.target_node_id not in allowed_node_ids:
            continue
        if local_only and edge.source_node_id not in allowed_node_ids:
            continue
        dependencies[edge.target_node_id].add(edge.source_node_id)
    return dependencies


def build_input_bindings(
    graph: IRGraph,
    *,
    allowed_node_ids: set[str],
) -> dict[str, dict[str, InputBindingSource]]:
    """Build an input-alias map from data edges for each node."""

    bindings: dict[str, dict[str, InputBindingSource]] = {
        node.node_id: {} for node in graph.nodes if node.node_id in allowed_node_ids
    }
    for edge in graph.edges:
        if edge.kind != "data":
            continue
        if edge.target_node_id not in allowed_node_ids:
            continue
        bindings[edge.target_node_id][edge.target_port] = InputBindingSource.node_output(
            edge.source_node_id
        )
    for node in graph.nodes:
        if node.node_id not in allowed_node_ids:
            continue
        bindings[node.node_id].update(_synthetic_loop_bindings(node.metadata))
    return bindings


def _synthetic_loop_bindings(
    metadata: Mapping[str, str],
) -> dict[str, InputBindingSource]:
    bindings: dict[str, InputBindingSource] = {}
    for alias, logical_name in _parse_binding_metadata(
        metadata.get("loop_item_bindings")
    ).items():
        bindings[alias] = InputBindingSource.loop_item(logical_name)
    for alias, logical_name in _parse_binding_metadata(
        metadata.get("loop_state_bindings")
    ).items():
        bindings[alias] = InputBindingSource.loop_state(logical_name)
    return bindings


def _node_started_payload(
    *,
    node_ctx: ExecutionContext,
    node: PlanNode,
) -> dict[str, JsonValue]:
    payload: dict[str, JsonValue] = {
        "kind": node.metadata.kind,
        "input_count": len(node.metadata.dependencies),
    }
    if node_ctx.runtime_profile is not None:
        payload["runtime_profile"] = node_ctx.runtime_profile
    return payload


def _parse_binding_metadata(value: str | None) -> dict[str, str]:
    if value is None or not value:
        return {}
    bindings: dict[str, str] = {}
    for part in value.split(","):
        alias, logical_name = part.split("=", 1)
        bindings[alias] = logical_name
    return bindings


class _LoopNodeRunner:
    async def __call__(
        self,
        *,
        node: PlanNode,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, RuntimeValue]:
        async with semaphore:
            node_ctx = context.for_node(_loop_ir_node(node))
            emit_metric_batch(
                node_ctx.metrics,
                [node_execution_observation(node_ctx.metric_context())],
            )
            node_ctx.recorder.record(
                run_id=node_ctx.run_id,
                node_id=node.metadata.node_id,
                event_type=NODE_STARTED,
                timestamp_ms=node_ctx.clock.now_ms(),
                frame=node_ctx.frame,
                payload=_node_started_payload(node_ctx=node_ctx, node=node),
            )
            start_time_ms = node_ctx.clock.now_ms()
            node_success = False
            try:
                with node_ctx.tracing.start_span(
                    f"{node.metadata.kind}:{node.metadata.node_id}",
                    attributes=node_ctx.span_attributes(),
                ):
                    output = await node.execute(outputs, node_ctx)
                node_success = True
            except Exception as exc:
                node_ctx.recorder.record(
                    run_id=node_ctx.run_id,
                    node_id=node.metadata.node_id,
                    event_type=NODE_FAILED,
                    timestamp_ms=node_ctx.clock.now_ms(),
                    frame=node_ctx.frame,
                    payload={"error": type(exc).__name__, "message": str(exc)},
                )
                raise
            finally:
                duration_ms = max(0.0, node_ctx.clock.now_ms() - start_time_ms)
                emit_metric_batch(
                    node_ctx.metrics,
                    [
                        node_duration_observation(
                            node_ctx.metric_context(),
                            duration_ms=duration_ms,
                            success=node_success,
                        )
                    ],
                )
            node_ctx.recorder.record(
                run_id=node_ctx.run_id,
                node_id=node.metadata.node_id,
                event_type=NODE_SUCCEEDED,
                timestamp_ms=node_ctx.clock.now_ms(),
                frame=node_ctx.frame,
                payload={
                    "kind": node.metadata.kind,
                    "output_type": type(output).__name__,
                    "output": serialize_runtime_value(output),
                },
            )
            node_ctx.framed_outputs.append(
                FramedNodeValue(
                    node_id=node.metadata.node_id,
                    frame=node_ctx.frame,
                    value=output,
                )
            )
            if node.metadata.kind == "invariant":
                return node.metadata.node_id, output
            return node.metadata.node_id, output


def _loop_ir_node(node: PlanNode) -> IRNode:
    metadata = (
        {"runtime_context": node.metadata.runtime_context}
        if node.metadata.runtime_context is not None
        else {}
    )
    return IRNode(
        node_id=node.metadata.node_id,
        kind=node.metadata.kind,
        label=node.metadata.label,
        metadata=metadata,
    )


__all__ = [
    "CompiledActorNode",
    "CompiledEffectNode",
    "CompiledInvariantNode",
    "CompiledJoinNode",
    "CompiledProgram",
    "ExecutionNodeMetadata",
    "ExecutionPlan",
    "MappingInputAdapter",
    "PlanNode",
    "build_execution_plan",
    "compile_execution_node",
    "compile_program",
]
