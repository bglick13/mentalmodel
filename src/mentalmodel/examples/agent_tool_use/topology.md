# agent_tool_use_demo Topology

- Nodes: 13
- Edges: 27
- Findings: 0

## Node Kinds

- `actor`: 3
- `effect`: 4
- `invariant`: 1
- `join`: 1
- `parallel`: 1
- `runtime_context`: 2
- `workflow`: 1

## Edges

- `agent_tool_use_demo` -> `local_control_plane` (`contains`)
- `local_control_plane` -> `answer_invariant` (`contains`)
- `local_control_plane` -> `answer_synthesizer` (`contains`)
- `local_control_plane` -> `plan_task` (`contains`)
- `local_control_plane` -> `sandbox_tools` (`contains`)
- `local_control_plane` -> `task_source` (`contains`)
- `sandbox_tools` -> `cost_calculator` (`contains`)
- `sandbox_tools` -> `tool_fanout` (`contains`)
- `sandbox_tools` -> `tool_results_join` (`contains`)
- `tool_fanout` -> `discount_lookup` (`contains`)
- `tool_fanout` -> `plan_lookup` (`contains`)
- `tool_fanout` -> `support_lookup` (`contains`)
- `answer_synthesizer` -> `answer_invariant` (`data`)
- `cost_calculator` -> `answer_synthesizer` (`data`)
- `discount_lookup` -> `tool_results_join` (`data`)
- `plan_lookup` -> `tool_results_join` (`data`)
- `plan_task` -> `tool_results_join` (`data`)
- `support_lookup` -> `tool_results_join` (`data`)
- `task_source` -> `answer_invariant` (`data`)
- `task_source` -> `answer_synthesizer` (`data`)
- `task_source` -> `discount_lookup` (`data`)
- `task_source` -> `plan_lookup` (`data`)
- `task_source` -> `plan_task` (`data`)
- `task_source` -> `support_lookup` (`data`)
- `task_source` -> `tool_results_join` (`data`)
- `tool_results_join` -> `answer_synthesizer` (`data`)
- `tool_results_join` -> `cost_calculator` (`data`)

## Findings

- No findings.
