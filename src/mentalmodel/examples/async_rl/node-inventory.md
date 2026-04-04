# async_rl_demo Node Inventory

## `batch_source`

- Kind: `actor`
- Label: `batch_source`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: none
- Data Dependents: `sample_policy`
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `handler`: `BatchSource`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`
  - `stateful`: `true`

## `learner_update`

- Kind: `actor`
- Label: `learner_update`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `rollout_join`
- Data Dependents: `refresh_sampler`
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `handler`: `LearnerUpdate`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`
  - `stateful`: `true`

## `policy_snapshot`

- Kind: `actor`
- Label: `policy_snapshot`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: none
- Data Dependents: `rollout_join`, `sample_policy`
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `handler`: `PolicySnapshot`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`
  - `stateful`: `true`

## `kl_prefetch`

- Kind: `effect`
- Label: `kl_prefetch`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `sample_policy`
- Data Dependents: `rollout_join`
- Container Parent: `reward_fanout`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `KLPrefetch`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`

## `pangram_reward`

- Kind: `effect`
- Label: `pangram_reward`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `sample_policy`
- Data Dependents: `rollout_join`
- Container Parent: `reward_fanout`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `PangramScorer`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`

## `quality_reward`

- Kind: `effect`
- Label: `quality_reward`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `sample_policy`
- Data Dependents: `rollout_join`
- Container Parent: `reward_fanout`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `QualityScorer`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`

## `refresh_sampler`

- Kind: `effect`
- Label: `refresh_sampler`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `learner_update`
- Data Dependents: none
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `RefreshSampler`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`

## `sample_policy`

- Kind: `effect`
- Label: `sample_policy`
- Runtime Context: `sandbox`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `batch_source`, `policy_snapshot`
- Data Dependents: `kl_prefetch`, `pangram_reward`, `quality_reward`, `rollout_join`
- Container Parent: `remote_sampling`
- Contained Children: none
- Metadata:

  - `effectful`: `true`
  - `handler`: `PolicySampler`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `sandbox`

## `staleness_invariant`

- Kind: `invariant`
- Label: `staleness_invariant`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `rollout_join`
- Data Dependents: none
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `checker`: `PolicyStalenessChecker`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`
  - `severity`: `error`

## `rollout_join`

- Kind: `join`
- Label: `rollout_join`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: `kl_prefetch`, `pangram_reward`, `policy_snapshot`, `quality_reward`, `sample_policy`
- Data Dependents: `learner_update`, `staleness_invariant`
- Container Parent: `local_control_plane`
- Contained Children: none
- Metadata:

  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `reducer`: `RolloutJoinReducer`
  - `runtime_context`: `local`

## `reward_fanout`

- Kind: `parallel`
- Label: `reward_fanout`
- Runtime Context: `local`
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: none
- Data Dependents: none
- Container Parent: `local_control_plane`
- Contained Children: `kl_prefetch`, `pangram_reward`, `quality_reward`
- Metadata:

  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
  - `runtime_context`: `local`

## `local_control_plane`

- Kind: `runtime_context`
- Label: `local_control_plane`
- Runtime Context: `local`
- Plugin Kind: `runtime_context`
- Plugin Origin: `mentalmodel.plugins.runtime_context`
- Plugin Version: `0.1.0`
- Data Dependencies: none
- Data Dependents: none
- Container Parent: `async_rl_demo`
- Contained Children: `batch_source`, `learner_update`, `policy_snapshot`, `refresh_sampler`, `remote_sampling`, `reward_fanout`, `rollout_join`, `staleness_invariant`
- Metadata:

  - `plugin_kind`: `runtime_context`
  - `plugin_origin`: `mentalmodel.plugins.runtime_context`
  - `plugin_version`: `0.1.0`
  - `runtime_context`: `local`

## `remote_sampling`

- Kind: `runtime_context`
- Label: `remote_sampling`
- Runtime Context: `sandbox`
- Plugin Kind: `runtime_context`
- Plugin Origin: `mentalmodel.plugins.runtime_context`
- Plugin Version: `0.1.0`
- Data Dependencies: none
- Data Dependents: none
- Container Parent: `local_control_plane`
- Contained Children: `sample_policy`
- Metadata:

  - `plugin_kind`: `runtime_context`
  - `plugin_origin`: `mentalmodel.plugins.runtime_context`
  - `plugin_version`: `0.1.0`
  - `runtime_context`: `sandbox`

## `async_rl_demo`

- Kind: `workflow`
- Label: `async_rl_demo`
- Runtime Context: none
- Plugin Kind: `core`
- Plugin Origin: `mentalmodel.core`
- Plugin Version: none
- Data Dependencies: none
- Data Dependents: none
- Container Parent: none
- Contained Children: `local_control_plane`
- Metadata:

  - `description`: `Milestone 2 async RL demo authored in semantic primitives.`
  - `plugin_kind`: `core`
  - `plugin_origin`: `mentalmodel.core`
