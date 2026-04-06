from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

from mentalmodel.core.bindings import InputBindingSource
from mentalmodel.core.refs import LoopItemRef, LoopStateRef, Ref
from mentalmodel.core.use import Use
from mentalmodel.errors import LoweringError
from mentalmodel.ir.graph import IRFragment, IRNode

if TYPE_CHECKING:
    from mentalmodel.ir.lowering import LoweringContext

StateT = TypeVar("StateT")


@dataclass(slots=True, frozen=True)
class LoopCarry(Generic[StateT]):
    """Loop-carried state declaration for a StepLoop."""

    state_name: str
    initial: StateT
    next_state_output: str


@dataclass(slots=True, frozen=True)
class LoopSummary:
    """Select which loop-body outputs should be summarized."""

    final_outputs: Sequence[str] = field(default_factory=tuple)
    history_outputs: Sequence[str] = field(default_factory=tuple)


@dataclass(slots=True, frozen=True)
class StepLoopResult:
    """Structured result returned by a StepLoop node."""

    iteration_count: int
    final_outputs: dict[str, object] = field(default_factory=dict)
    history_outputs: dict[str, list[object]] = field(default_factory=dict)
    final_carry_state: object | None = None


@dataclass(slots=True)
class StepLoop:
    """Sequential loop over a block-instantiated workflow body."""

    name: str
    body: Use
    for_each: Ref
    carry: LoopCarry[object] | None = None
    summary: LoopSummary | None = None
    max_iterations: int | None = None
    description: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def lower(self, ctx: LoweringContext) -> IRFragment:
        _validate_step_loop(self)

        metadata = dict(self.metadata)
        metadata["loop_body_name"] = self.body.name
        if self.description is not None:
            metadata["description"] = self.description
        if self.max_iterations is not None:
            metadata["max_iterations"] = str(self.max_iterations)
        if self.carry is not None:
            metadata["carry_state_name"] = self.carry.state_name
            metadata["carry_next_state_output"] = self.carry.next_state_output
        if self.summary is not None:
            metadata["summary_final_outputs"] = ",".join(sorted(self.summary.final_outputs))
            metadata["summary_history_outputs"] = ",".join(
                sorted(self.summary.history_outputs)
            )

        fragment = IRFragment()
        lowered_loop = ctx.register_container_node(
            node=IRNode(
                node_id=self.name,
                kind="step_loop",
                label=self.name,
                metadata=metadata,
            ),
            primitive=self,
        )
        fragment.nodes.append(lowered_loop)
        for_each_ref = ctx.resolve_external_ref(self.for_each)
        fragment.edges.append(
            ctx.make_edge(
                source_node_id=for_each_ref.target,
                source_port=for_each_ref.port,
                target_node_id=lowered_loop.node_id,
                target_port="for_each",
            )
        )

        body_bindings: dict[str, InputBindingSource] = {}
        for logical_name, binding in self.body.bind.items():
            if isinstance(binding, LoopItemRef):
                body_bindings[logical_name] = InputBindingSource.loop_item(
                    binding.logical_name
                )
            elif isinstance(binding, LoopStateRef):
                body_bindings[logical_name] = InputBindingSource.loop_state(
                    binding.logical_name
                )
            else:
                body_bindings[logical_name] = ctx.resolve_input_source(binding)
        for logical_name, source in sorted(body_bindings.items()):
            if source.kind != "node_output":
                continue
            fragment.edges.append(
                ctx.make_edge(
                    source_node_id=source.key,
                    source_port="default",
                    target_node_id=lowered_loop.node_id,
                    target_port=f"body.{logical_name}",
                )
            )

        body_metadata = {"loop_owner": lowered_loop.node_id}
        child_ctx = ctx.child_context(
            metadata=body_metadata,
            namespace_suffix=self.name,
            input_bindings={},
        )
        body_fragment = child_ctx.lower(self.body)
        fragment.extend(body_fragment)
        for child_root in child_ctx.fragment_roots(body_fragment):
            fragment.edges.append(
                ctx.make_edge(
                    source_node_id=lowered_loop.node_id,
                    source_port="contains",
                    target_node_id=child_root,
                    target_port="contained",
                    kind="contains",
                )
            )
        return fragment


def _validate_step_loop(loop: StepLoop) -> None:
    if loop.max_iterations is not None and loop.max_iterations < 0:
        raise LoweringError("StepLoop max_iterations must be non-negative when provided.")

    body_inputs = set(loop.body.block.inputs)
    for logical_name in loop.body.bind:
        if logical_name not in body_inputs:
            raise LoweringError(
                f"StepLoop {loop.name!r} binds unknown body input {logical_name!r}."
            )
    if loop.carry is None:
        missing = [
            logical_name
            for logical_name, binding in loop.body.bind.items()
            if isinstance(binding, LoopStateRef)
        ]
        if missing:
            raise LoweringError(
                f"StepLoop {loop.name!r} uses LoopStateRef without LoopCarry: {missing!r}"
            )
    if loop.carry is not None and loop.carry.next_state_output not in loop.body.block.outputs:
        raise LoweringError(
            f"StepLoop {loop.name!r} carry output {loop.carry.next_state_output!r} "
            "is not a declared body output."
        )
    if loop.carry is not None:
        invalid_state_refs = [
            binding.logical_name
            for binding in loop.body.bind.values()
            if isinstance(binding, LoopStateRef)
            and binding.logical_name != loop.carry.state_name
        ]
        if invalid_state_refs:
            raise LoweringError(
                f"StepLoop {loop.name!r} uses LoopStateRef names that do not match "
                f"carry state {loop.carry.state_name!r}: {sorted(invalid_state_refs)!r}"
            )
    if loop.summary is not None:
        declared_outputs = set(loop.body.block.outputs)
        missing = sorted(
            (
                set(loop.summary.final_outputs)
                | set(loop.summary.history_outputs)
            )
            - declared_outputs
        )
        if missing:
            raise LoweringError(
                f"StepLoop {loop.name!r} references unknown body outputs in summary: {missing!r}"
            )
