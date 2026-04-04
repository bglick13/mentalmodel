from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Generic, Protocol, TypeAlias, TypeVar, cast

from mentalmodel.core import Actor, Effect, Invariant, Join, Workflow
from mentalmodel.core.interfaces import (
    ActorHandler,
    InvariantChecker,
    JoinReducer,
    JsonValue,
    NamedPrimitive,
    RuntimeValue,
)
from mentalmodel.errors import LoweringError
from mentalmodel.ir.graph import IRGraph
from mentalmodel.ir.lowering import lower_program
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.errors import InvariantViolationError
from mentalmodel.runtime.events import (
    EFFECT_COMPLETED,
    EFFECT_INVOKED,
    INVARIANT_CHECKED,
    JOIN_RESOLVED,
    STATE_READ,
    STATE_TRANSITION,
)

InputT = TypeVar("InputT")
InputBoundT_co = TypeVar("InputBoundT_co", covariant=True)
OutputT = TypeVar("OutputT")
StateT = TypeVar("StateT")
DetailT = TypeVar("DetailT", bound=JsonValue)


@dataclass(slots=True, frozen=True)
class ExecutionNodeMetadata:
    """Executable node metadata derived from the canonical IR."""

    node_id: str
    kind: str
    label: str
    runtime_context: str | None
    dependencies: tuple[str, ...]


class InputAdapter(Protocol[InputBoundT_co]):
    """Converts resolved upstream runtime values into a handler input shape."""

    def bind(self, outputs: Mapping[str, RuntimeValue]) -> InputBoundT_co:
        """Bind raw upstream outputs into the typed handler input."""


@dataclass(slots=True, frozen=True)
class MappingInputAdapter(Generic[InputT]):
    """Default adapter that presents upstream outputs as a mapping."""

    dependencies: tuple[str, ...]

    def bind(self, outputs: Mapping[str, RuntimeValue]) -> InputT:
        bound = {dependency: outputs[dependency] for dependency in self.dependencies}
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


@dataclass(slots=True, frozen=True)
class CompiledActorNode(Generic[InputT, OutputT, StateT]):
    metadata: ExecutionNodeMetadata
    handler: ActorHandler[InputT, StateT, OutputT]
    input_adapter: InputAdapter[InputT]
    state_key: str

    async def execute(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> RuntimeValue:
        typed_inputs = self.input_adapter.bind(outputs)
        previous_state = context.state_store.get(self.state_key)
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=STATE_READ,
            timestamp_ms=context.clock.now_ms(),
            payload={"had_state": previous_state is not None},
        )
        result = await self.handler.handle(
            typed_inputs,
            cast(StateT | None, previous_state),
            context,
        )
        if result.next_state is not None or previous_state is not None:
            context.state_store[self.state_key] = result.next_state
            context.recorder.record(
                run_id=context.run_id,
                node_id=self.metadata.node_id,
                event_type=STATE_TRANSITION,
                timestamp_ms=context.clock.now_ms(),
                payload={
                    "from_state": summarize_runtime_value(previous_state),
                    "to_state": summarize_runtime_value(cast(RuntimeValue, result.next_state)),
                },
            )
        return cast(RuntimeValue, result.output)


@dataclass(slots=True, frozen=True)
class CompiledEffectNode(Generic[InputT, OutputT]):
    metadata: ExecutionNodeMetadata
    handler: Effect[InputT, OutputT]
    input_adapter: InputAdapter[InputT]

    async def execute(
        self,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
    ) -> RuntimeValue:
        typed_inputs = self.input_adapter.bind(outputs)
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=EFFECT_INVOKED,
            timestamp_ms=context.clock.now_ms(),
            payload={"input_keys": [key for key in self.metadata.dependencies]},
        )
        output = await self.handler.handler.invoke(typed_inputs, context)
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=EFFECT_COMPLETED,
            timestamp_ms=context.clock.now_ms(),
            payload={"output_type": type(output).__name__},
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
        typed_inputs = self.input_adapter.bind(outputs)
        if self.reducer is None:
            output = cast(RuntimeValue, typed_inputs)
        else:
            output = cast(RuntimeValue, await self.reducer.reduce(typed_inputs, context))
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=JOIN_RESOLVED,
            timestamp_ms=context.clock.now_ms(),
            payload={"input_keys": [key for key in self.metadata.dependencies]},
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
        typed_inputs = self.input_adapter.bind(outputs)
        result = await self.checker.check(typed_inputs, context)
        context.recorder.record(
            run_id=context.run_id,
            node_id=self.metadata.node_id,
            event_type=INVARIANT_CHECKED,
            timestamp_ms=context.clock.now_ms(),
            payload={"passed": result.passed, "severity": self.severity},
        )
        if not result.passed:
            raise InvariantViolationError(
                f"Invariant {self.metadata.node_id!r} failed: {dict(result.details)!r}"
            )
        return cast(RuntimeValue, result)


PlanNode: TypeAlias = (
    CompiledActorNode[object, object, object]
    | CompiledEffectNode[object, object]
    | CompiledJoinNode[object, object]
    | CompiledInvariantNode[object, JsonValue]
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


def compile_program(program: Workflow[NamedPrimitive]) -> CompiledProgram:
    """Compile authored primitives into a graph plus a typed execution plan."""

    graph = lower_program(program)
    primitives = index_primitives(program)
    plan = build_execution_plan(graph=graph, primitives=primitives)
    return CompiledProgram(program=program, graph=graph, plan=plan)


def build_execution_plan(
    *,
    graph: IRGraph,
    primitives: Mapping[str, NamedPrimitive],
) -> ExecutionPlan:
    """Build executable runtime nodes from a lowered graph and primitive index."""

    dependencies = build_data_dependencies(graph)
    compiled_nodes: dict[str, PlanNode] = {}

    for node in graph.nodes:
        if node.kind not in {"actor", "effect", "join", "invariant"}:
            continue
        primitive = primitives.get(node.node_id)
        if primitive is None:
            raise LoweringError(f"Missing primitive binding for node {node.node_id!r}.")
        metadata = ExecutionNodeMetadata(
            node_id=node.node_id,
            kind=node.kind,
            label=node.label,
            runtime_context=node.metadata.get("runtime_context"),
            dependencies=tuple(sorted(dependencies[node.node_id])),
        )
        adapter = MappingInputAdapter[object](metadata.dependencies)
        compiled_nodes[node.node_id] = compile_execution_node(
            metadata=metadata,
            primitive=primitive,
            input_adapter=adapter,
        )

    return ExecutionPlan(nodes=compiled_nodes)


def compile_execution_node(
    *,
    metadata: ExecutionNodeMetadata,
    primitive: NamedPrimitive,
    input_adapter: MappingInputAdapter[object],
) -> PlanNode:
    """Compile one authored primitive into an executable runtime node."""

    if metadata.kind == "actor" and isinstance(primitive, Actor):
        return cast(
            PlanNode,
            CompiledActorNode(
                metadata=metadata,
                handler=primitive.handler,
                input_adapter=input_adapter,
                state_key=primitive.name,
            ),
        )
    if metadata.kind == "effect" and isinstance(primitive, Effect):
        return cast(
            PlanNode,
            CompiledEffectNode(
                metadata=metadata,
                handler=primitive,
                input_adapter=input_adapter,
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
    raise LoweringError(
        "Primitive kind mismatch during execution-plan compilation: "
        f"node={metadata.node_id!r} kind={metadata.kind!r} primitive={type(primitive).__name__!r}"
    )


def index_primitives(program: Workflow[NamedPrimitive]) -> dict[str, NamedPrimitive]:
    """Index authored primitives by stable node name."""

    indexed: dict[str, NamedPrimitive] = {}

    def visit(primitive: NamedPrimitive) -> None:
        if primitive.name in indexed:
            raise LoweringError(f"Duplicate primitive name during indexing: {primitive.name!r}")
        indexed[primitive.name] = primitive
        children = getattr(primitive, "children", None)
        if isinstance(children, (tuple, list)):
            for child in children:
                visit(cast(NamedPrimitive, child))

    visit(program)
    return indexed


def build_data_dependencies(graph: IRGraph) -> dict[str, set[str]]:
    """Build a data-dependency map for executable graph nodes."""

    dependencies: dict[str, set[str]] = defaultdict(set)
    for node in graph.nodes:
        dependencies[node.node_id]
    for edge in graph.edges:
        if edge.kind != "data":
            continue
        dependencies[edge.target_node_id].add(edge.source_node_id)
    return dependencies


def summarize_runtime_value(value: RuntimeValue) -> dict[str, JsonValue]:
    """Summarize runtime values into recorder-safe JSON payloads."""

    if value is None:
        return {"type": "None"}
    if isinstance(value, dict):
        return {"type": "dict", "keys": [str(key) for key in sorted(value.keys())]}
    if isinstance(value, list):
        return {"type": "list", "length": len(value)}
    return {"type": type(value).__name__}
