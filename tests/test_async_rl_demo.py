from __future__ import annotations

import unittest

from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.ir.lowering import lower_program


class AsyncRlDemoTest(unittest.TestCase):
    def test_build_program_lowers_to_expected_graph_shape(self) -> None:
        program = build_program()
        graph = lower_program(program)
        self.assertEqual(graph.graph_id, "async_rl_demo")
        node_ids = {node.node_id for node in graph.nodes}
        self.assertIn("sample_policy", node_ids)
        self.assertIn("rollout_join", node_ids)
        self.assertIn("remote_sampling", node_ids)
        self.assertGreaterEqual(len(graph.edges), 1)


if __name__ == "__main__":
    unittest.main()
