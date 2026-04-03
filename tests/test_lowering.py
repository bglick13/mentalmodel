from __future__ import annotations

import unittest

from mentalmodel.core import Actor, ActorHandler, ActorResult, Ref, Workflow
from mentalmodel.ir.lowering import lower_program
from mentalmodel.plugins.registry import default_registry
from mentalmodel.plugins.runtime_context import RuntimeContext
from mentalmodel.runtime.context import ExecutionContext


class NoOpHandler(ActorHandler[dict[str, object], object, object]):
    async def handle(
        self,
        inputs: dict[str, object],
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[object, object]:
        del inputs, state, ctx
        return ActorResult(output="ok")


class LoweringTest(unittest.TestCase):
    def test_runtime_context_plugin_registry_is_available(self) -> None:
        registry = default_registry()
        plugin = registry.find_plugin(RuntimeContext(name="ctx", runtime="local"))
        self.assertIsNotNone(plugin)
        assert plugin is not None
        self.assertEqual(plugin.kind, "runtime_context")

    def test_lower_program_preserves_runtime_context_metadata(self) -> None:
        program: Workflow[RuntimeContext] = Workflow(
            name="demo",
            children=[
                RuntimeContext(
                    name="ctx",
                    runtime="sandbox",
                    children=[Actor(name="worker", handler=NoOpHandler(), inputs=[])],
                )
            ],
        )
        graph = lower_program(program)
        worker = next(node for node in graph.nodes if node.node_id == "worker")
        self.assertEqual(worker.metadata["runtime_context"], "sandbox")

    def test_lower_program_creates_data_edge_from_ref(self) -> None:
        program: Workflow[Actor[dict[str, object], object, object]] = Workflow(
            name="demo",
            children=[
                Actor(name="source", handler=NoOpHandler(), inputs=[]),
                Actor(name="sink", handler=NoOpHandler(), inputs=[Ref("source")]),
            ],
        )
        graph = lower_program(program)
        edge_pairs = {(edge.source_node_id, edge.target_node_id, edge.kind) for edge in graph.edges}
        self.assertIn(("source", "sink", "data"), edge_pairs)


if __name__ == "__main__":
    unittest.main()
