# agent_tool_use_demo Node Inventory

## `answer_synthesizer`

- Kind: `actor`
- Label: `answer_synthesizer`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `cost_calculator`, `task_source`, `tool_results_join`
- Data Dependents: `answer_invariant`
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `handler`: `AnswerSynthesizer`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`
  - `stateful`: `true`

## `plan_task`

- Kind: `actor`
- Label: `plan_task`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `task_source`
- Data Dependents: `tool_results_join`
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `handler`: `Planner`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`
  - `stateful`: `true`

## `task_source`

- Kind: `actor`
- Label: `task_source`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: none
- Data Dependents: `answer_invariant`, `answer_synthesizer`, `discount_lookup`, `plan_lookup`, `plan_task`, `support_lookup`, `tool_results_join`
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `handler`: `TaskSource`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`
  - `stateful`: `true`

## `cost_calculator`

- Kind: `effect`
- Label: `cost_calculator`
- Runtime Context: `sandbox`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `tool_results_join`
- Data Dependents: `answer_synthesizer`
- Container Parent: `sandbox_tools`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `CostCalculator`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `sandbox`

## `discount_lookup`

- Kind: `effect`
- Label: `discount_lookup`
- Runtime Context: `sandbox`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `task_source`
- Data Dependents: `tool_results_join`
- Container Parent: `tool_fanout`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `DiscountLookup`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `sandbox`

## `plan_lookup`

- Kind: `effect`
- Label: `plan_lookup`
- Runtime Context: `sandbox`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `task_source`
- Data Dependents: `tool_results_join`
- Container Parent: `tool_fanout`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `PlanLookup`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `sandbox`

## `support_lookup`

- Kind: `effect`
- Label: `support_lookup`
- Runtime Context: `sandbox`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `task_source`
- Data Dependents: `tool_results_join`
- Container Parent: `tool_fanout`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `SupportLookup`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `sandbox`

## `answer_invariant`

- Kind: `invariant`
- Label: `answer_invariant`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `answer_synthesizer`, `task_source`
- Data Dependents: none
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `checker`: `AnswerInvariant`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`
  - `severity`: `error`

## `tool_results_join`

- Kind: `join`
- Label: `tool_results_join`
- Runtime Context: `sandbox`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `discount_lookup`, `plan_lookup`, `plan_task`, `support_lookup`, `task_source`
- Data Dependents: `answer_synthesizer`, `cost_calculator`
- Container Parent: `sandbox_tools`
- Contained Children: none
- Metadata:

  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `reducer`: `ToolResultsJoin`
  - `runtime_context`: `sandbox`

## `tool_fanout`

- Kind: `parallel`
- Label: `tool_fanout`
- Runtime Context: `sandbox`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: none
- Data Dependents: none
- Container Parent: `sandbox_tools`
- Contained Children: `discount_lookup`, `plan_lookup`, `support_lookup`
- Metadata:

  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `sandbox`

## `local_control_plane`

- Kind: `runtime_context`
- Label: `local_control_plane`
- Runtime Context: `local`
- Plugin Kind: `runtime_context`
- Plugin Origin: `mentalmodel.plugins.runtime_context`
- Plugin Version: `0.1.0`
- Data Dependencies: none
- Data Dependents: none
- Container Parent: `agent_tool_use_demo`
- Contained Children: `answer_invariant`, `answer_synthesizer`, `plan_task`, `sandbox_tools`, `task_source`
- Metadata:

  - `plugin_kind`: `runtime_context`
  - `plugin_origin`: `mentalmodel.plugins.runtime_context`
  - `plugin_version`: `0.1.0`
  - `runtime_context`: `local`

## `sandbox_tools`

- Kind: `runtime_context`
- Label: `sandbox_tools`
- Runtime Context: `sandbox`
- Plugin Kind: `runtime_context`
- Plugin Origin: `mentalmodel.plugins.runtime_context`
- Plugin Version: `0.1.0`
- Data Dependencies: none
- Data Dependents: none
- Container Parent: `local_control_plane`
- Contained Children: `cost_calculator`, `tool_fanout`, `tool_results_join`
- Metadata:

  - `plugin_kind`: `runtime_context`
  - `plugin_origin`: `mentalmodel.plugins.runtime_context`
  - `plugin_version`: `0.1.0`
  - `runtime_context`: `sandbox`

## `agent_tool_use_demo`

- Kind: `workflow`
- Label: `agent_tool_use_demo`
- Runtime Context: none
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: none
- Data Dependents: none
- Container Parent: none
- Contained Children: `local_control_plane`
- Metadata:

  - `description`: `Agent tool-use workflow with local planning and sandboxed tools.`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
