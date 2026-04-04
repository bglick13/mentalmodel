# async_rl_demo Topology

- Nodes: 14
- Edges: 26
- Findings: 0

## Node Kinds

- `actor`: 3
- `effect`: 5
- `invariant`: 1
- `join`: 1
- `parallel`: 1
- `runtime_context`: 2
- `workflow`: 1

## Edges

- `async_rl_demo` -> `local_control_plane` (`contains`)
- `local_control_plane` -> `batch_source` (`contains`)
- `local_control_plane` -> `learner_update` (`contains`)
- `local_control_plane` -> `policy_snapshot` (`contains`)
- `local_control_plane` -> `refresh_sampler` (`contains`)
- `local_control_plane` -> `remote_sampling` (`contains`)
- `local_control_plane` -> `reward_fanout` (`contains`)
- `local_control_plane` -> `rollout_join` (`contains`)
- `local_control_plane` -> `staleness_invariant` (`contains`)
- `remote_sampling` -> `sample_policy` (`contains`)
- `reward_fanout` -> `kl_prefetch` (`contains`)
- `reward_fanout` -> `pangram_reward` (`contains`)
- `reward_fanout` -> `quality_reward` (`contains`)
- `batch_source` -> `sample_policy` (`data`)
- `kl_prefetch` -> `rollout_join` (`data`)
- `learner_update` -> `refresh_sampler` (`data`)
- `pangram_reward` -> `rollout_join` (`data`)
- `policy_snapshot` -> `rollout_join` (`data`)
- `policy_snapshot` -> `sample_policy` (`data`)
- `quality_reward` -> `rollout_join` (`data`)
- `rollout_join` -> `learner_update` (`data`)
- `rollout_join` -> `staleness_invariant` (`data`)
- `sample_policy` -> `kl_prefetch` (`data`)
- `sample_policy` -> `pangram_reward` (`data`)
- `sample_policy` -> `quality_reward` (`data`)
- `sample_policy` -> `rollout_join` (`data`)

## Findings

- No findings.
