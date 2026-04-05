# agent_tool_use_demo Runtime Contexts

## `local`

- `answer_synthesizer` (`actor`, provenance=`core`)
- `plan_task` (`actor`, provenance=`core`)
- `task_source` (`actor`, provenance=`core`)
- `answer_invariant` (`invariant`, provenance=`core`)
- `local_control_plane` (`runtime_context`, provenance=`runtime_context`)

## `sandbox`

- `cost_calculator` (`effect`, provenance=`core`)
- `discount_lookup` (`effect`, provenance=`core`)
- `plan_lookup` (`effect`, provenance=`core`)
- `support_lookup` (`effect`, provenance=`core`)
- `tool_results_join` (`join`, provenance=`core`)
- `tool_fanout` (`parallel`, provenance=`core`)
- `sandbox_tools` (`runtime_context`, provenance=`runtime_context`)
