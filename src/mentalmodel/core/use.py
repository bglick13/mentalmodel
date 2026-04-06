from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from mentalmodel.core.block import Block, BlockDefaults
from mentalmodel.core.refs import InputRef, Ref
from mentalmodel.environment import ResourceKey, merge_resource_keys
from mentalmodel.errors import LoweringError
from mentalmodel.ir.graph import IRFragment, IRNode

if TYPE_CHECKING:
    from mentalmodel.core.interfaces import NamedPrimitive
    from mentalmodel.ir.lowering import LoweringContext


@dataclass(slots=True)
class Use:
    """Instantiate a reusable block under a namespace."""

    name: str
    block: Block[NamedPrimitive]
    bind: Mapping[str, InputRef] = field(default_factory=dict)
    defaults: BlockDefaults | None = None
    description: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    def output_ref(self, logical_output: str, *, port: str = "default") -> Ref:
        """Return a concrete ref to one declared logical block output."""

        output = self.block.outputs.get(logical_output)
        if output is None:
            raise LoweringError(
                f"Use {self.name!r} does not declare block output {logical_output!r}."
            )
        return Ref(target=f"{self.name}.{output.source_node_id}", port=port)

    def lower(self, ctx: LoweringContext) -> IRFragment:
        _validate_use_bindings(self)
        _validate_block_outputs(self.block)

        use_node_id = ctx.namespaced_name(self.name)
        resolved_bindings = {
            logical_name: ctx.resolve_input_source(binding)
            for logical_name, binding in self.bind.items()
        }

        metadata = dict(self.metadata)
        metadata["block_name"] = self.block.name
        metadata["block_inputs"] = ",".join(sorted(self.block.inputs))
        metadata["block_outputs"] = ",".join(
            f"{name}={output.source_node_id}"
            for name, output in sorted(self.block.outputs.items())
        )
        if self.description is not None:
            metadata["description"] = self.description
        node = IRNode(
            node_id=self.name,
            kind="use",
            label=self.name,
            metadata=metadata,
        )
        fragment = IRFragment()
        lowered_use = ctx.register_container_node(node=node, primitive=self)
        fragment.nodes.append(lowered_use)

        for logical_name, binding in sorted(resolved_bindings.items()):
            if binding.kind != "node_output":
                continue
            fragment.edges.append(
                ctx.make_edge(
                    source_node_id=binding.key,
                    source_port="default",
                    target_node_id=lowered_use.node_id,
                    target_port=logical_name,
                    kind="bind",
                )
            )

        child_metadata = _merged_block_metadata(
            block_defaults=self.block.defaults,
            use_defaults=self.defaults,
        )
        child_resources = _merged_block_resources(
            block_defaults=self.block.defaults,
            use_defaults=self.defaults,
        )
        child_metadata["block_name"] = self.block.name
        child_metadata["use_name"] = use_node_id
        child_ctx = ctx.child_context(
            metadata=child_metadata,
            namespace_suffix=self.name,
            input_bindings=resolved_bindings,
            inherited_resources=child_resources,
        )
        for child in self.block.children:
            child_fragment = child_ctx.lower(child)
            fragment.extend(child_fragment)
            for child_root in child_ctx.fragment_roots(child_fragment):
                fragment.edges.append(
                    ctx.make_edge(
                        source_node_id=lowered_use.node_id,
                        source_port="contains",
                        target_node_id=child_root,
                        target_port="contained",
                        kind="contains",
                    )
                )
        return fragment


def _validate_use_bindings(use: Use) -> None:
    declared_inputs = set(use.block.inputs)
    unknown = sorted(set(use.bind) - declared_inputs)
    if unknown:
        raise LoweringError(
            f"Use {use.name!r} binds unknown block inputs: {unknown!r}"
        )
    missing = sorted(
        input_name
        for input_name, declaration in use.block.inputs.items()
        if declaration.required and input_name not in use.bind
    )
    if missing:
        raise LoweringError(
            f"Use {use.name!r} is missing required block inputs: {missing!r}"
        )


def _validate_block_outputs(block: Block[NamedPrimitive]) -> None:
    logical_node_ids = _collect_logical_node_ids(block.children)
    missing = sorted(
        output.source_node_id
        for output in block.outputs.values()
        if output.source_node_id not in logical_node_ids
    )
    if missing:
        raise LoweringError(
            f"Block {block.name!r} declares outputs from unknown logical nodes: {missing!r}"
        )


def _collect_logical_node_ids(
    children: Sequence[NamedPrimitive],
    *,
    prefix: str = "",
) -> set[str]:
    node_ids: set[str] = set()
    for child in children:
        logical_name = f"{prefix}{child.name}"
        node_ids.add(logical_name)
        if isinstance(child, Use):
            nested_prefix = f"{logical_name}."
            node_ids.update(
                _collect_logical_node_ids(
                    list(child.block.children),
                    prefix=nested_prefix,
                )
            )
            continue
        nested_children = getattr(child, "children", None)
        if isinstance(nested_children, (tuple, list)):
            node_ids.update(_collect_logical_node_ids(list(nested_children), prefix=prefix))
    return node_ids


def _merged_block_metadata(
    *,
    block_defaults: BlockDefaults | None,
    use_defaults: BlockDefaults | None,
) -> dict[str, str]:
    metadata: dict[str, str] = {}
    runtime_context = None
    if block_defaults is not None:
        metadata.update(dict(block_defaults.metadata))
        runtime_context = block_defaults.runtime_context
    if use_defaults is not None:
        metadata.update(dict(use_defaults.metadata))
        if use_defaults.runtime_context is not None:
            runtime_context = use_defaults.runtime_context
    if runtime_context is not None:
        metadata["runtime_context"] = runtime_context
    return metadata


def _merged_block_resources(
    *,
    block_defaults: BlockDefaults | None,
    use_defaults: BlockDefaults | None,
) -> tuple[ResourceKey[object], ...]:
    block_resources = (
        tuple(block_defaults.resources) if block_defaults is not None else tuple()
    )
    use_resources = tuple(use_defaults.resources) if use_defaults is not None else tuple()
    return merge_resource_keys(block_resources, use_resources)
