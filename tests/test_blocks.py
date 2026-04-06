from __future__ import annotations

import asyncio
import unittest

from mentalmodel import Actor, Block, BlockInput, BlockOutput, BlockRef, Ref, Use, Workflow
from mentalmodel.core import ActorHandler, ActorResult, NamedPrimitive
from mentalmodel.docs import build_node_inventory, render_markdown_artifacts, render_mermaid
from mentalmodel.errors import LoweringError
from mentalmodel.examples.reusable_blocks.demo import build_program, number_block
from mentalmodel.ir.lowering import lower_program
from mentalmodel.runtime import AsyncExecutor
from mentalmodel.runtime.context import ExecutionContext


class SourceHandler(ActorHandler[dict[str, object], object, int]):
    async def handle(
        self,
        inputs: dict[str, object],
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[int, object]:
        del inputs, state, ctx
        return ActorResult(output=1)


class CounterHandler(ActorHandler[dict[str, int], int, int]):
    async def handle(
        self,
        inputs: dict[str, int],
        state: int | None,
        ctx: ExecutionContext,
    ) -> ActorResult[int, int]:
        del ctx
        next_state = (state or 0) + inputs["upstream"]
        return ActorResult(output=next_state, next_state=next_state)


class BlockTest(unittest.TestCase):
    def test_use_instantiates_block_with_automatic_namespacing(self) -> None:
        graph = lower_program(build_program())
        node_ids = {node.node_id for node in graph.nodes}
        self.assertIn("first", node_ids)
        self.assertIn("first.double", node_ids)
        self.assertIn("first.positive", node_ids)
        self.assertIn("second", node_ids)
        self.assertIn("second.double", node_ids)
        self.assertIn("second.positive", node_ids)
        edge_pairs = {(edge.source_node_id, edge.target_node_id, edge.kind) for edge in graph.edges}
        self.assertIn(("source", "first", "bind"), edge_pairs)
        self.assertIn(("source", "first.double", "data"), edge_pairs)
        self.assertIn(("first.double", "second", "bind"), edge_pairs)
        self.assertIn(("first.double", "second.double", "data"), edge_pairs)

    def test_use_output_ref_points_at_instantiated_output_node(self) -> None:
        first = Use("first", block=number_block, bind={"upstream": Ref("source")})
        self.assertEqual(first.output_ref("result").target, "first.double")
        self.assertEqual(first.output_ref("check").target, "first.positive")

    def test_use_output_ref_rejects_unknown_logical_output(self) -> None:
        first = Use("first", block=number_block, bind={"upstream": Ref("source")})
        with self.assertRaises(LoweringError):
            first.output_ref("missing")

    def test_use_requires_required_block_bindings(self) -> None:
        program = Workflow(
            name="missing_binding",
            children=(Use("broken", block=number_block, bind={}),),
        )
        with self.assertRaises(LoweringError):
            lower_program(program)

    def test_use_rejects_unknown_bindings(self) -> None:
        program = Workflow(
            name="unknown_binding",
            children=(Use("broken", block=number_block, bind={"missing": Ref("source")}),),
        )
        with self.assertRaises(LoweringError):
            lower_program(program)

    def test_block_output_must_reference_known_logical_node(self) -> None:
        broken_block: Block[NamedPrimitive] = Block(
            name="broken",
            inputs={"upstream": BlockInput[object]()},
            outputs={"result": BlockOutput[object]("missing_node")},
            children=(),
        )
        program = Workflow(
            name="broken_output",
            children=(Use("broken", block=broken_block, bind={"upstream": Ref("source")}),),
        )
        with self.assertRaises(LoweringError):
            lower_program(program)

    def test_block_ref_requires_binding(self) -> None:
        orphan_block: Block[NamedPrimitive] = Block(
            name="orphan",
            inputs={"upstream": BlockInput[object](required=False)},
            outputs={},
            children=(),
        )
        program = Workflow(
            name="orphan_program",
            children=(Use("orphan_use", block=orphan_block, bind={}),),
        )
        graph = lower_program(program)
        self.assertEqual(graph.graph_id, "orphan_program")

        broken_ref_block: Block[NamedPrimitive] = Block(
            name="broken_ref",
            inputs={"upstream": BlockInput[object](required=False)},
            outputs={},
            children=(
                Actor[dict[str, object], int, object](
                    "sink",
                    handler=SourceHandler(),
                    inputs=[BlockRef("upstream")],
                ),
            ),
        )
        broken_program = Workflow(
            name="broken_ref_program",
            children=(Use("broken", block=broken_ref_block, bind={}),),
        )
        with self.assertRaises(LoweringError):
            lower_program(broken_program)

    def test_async_executor_runs_reused_block_workflow(self) -> None:
        result = asyncio.run(AsyncExecutor().run(build_program()))
        self.assertEqual(result.outputs["first.double"], {"value": 6})
        self.assertEqual(result.outputs["second.double"], {"value": 12})
        self.assertEqual(
            result.outputs["pair_join"],
            {
                "first.double": {"value": 6},
                "second.double": {"value": 12},
            },
        )

    def test_reused_stateful_block_instances_keep_distinct_state_keys(self) -> None:
        stateful_block: Block[NamedPrimitive] = Block(
            name="stateful_counter",
            inputs={"upstream": BlockInput[object]()},
            outputs={"result": BlockOutput[object]("counter")},
            children=(
                Actor[dict[str, int], int, int](
                    "counter",
                    handler=CounterHandler(),
                    inputs=[BlockRef("upstream")],
                ),
            ),
        )
        program = Workflow(
            name="stateful_block_demo",
            children=(
                Actor[dict[str, object], int, object](
                    "source",
                    handler=SourceHandler(),
                ),
                Use("first", block=stateful_block, bind={"upstream": Ref("source")}),
                Use("second", block=stateful_block, bind={"upstream": Ref("source")}),
            ),
        )
        result = asyncio.run(AsyncExecutor().run(program))
        self.assertEqual(result.state["first.counter"], 1)
        self.assertEqual(result.state["second.counter"], 1)

    def test_docs_surface_use_node_block_metadata(self) -> None:
        graph = lower_program(build_program())
        inventory = build_node_inventory(graph)
        first_use = next(entry for entry in inventory if entry.node_id == "first")
        self.assertEqual(first_use.kind, "use")
        self.assertEqual(first_use.block_name, "number_processing")
        self.assertEqual(first_use.block_inputs, ("upstream",))
        self.assertEqual(first_use.block_outputs, ("check=positive", "result=double"))
        self.assertEqual(first_use.bind_dependencies, ("source",))

        mermaid = render_mermaid(graph)
        self.assertIn('source -->|"bind"| first', mermaid)
        self.assertIn('first -->|"contains"| first_double', mermaid)

        artifacts = render_markdown_artifacts(graph)
        self.assertIn("## `first`", artifacts.node_inventory)
        self.assertIn("- Block Name: `number_processing`", artifacts.node_inventory)
        self.assertIn("- Block Inputs: `upstream`", artifacts.node_inventory)


if __name__ == "__main__":
    unittest.main()
