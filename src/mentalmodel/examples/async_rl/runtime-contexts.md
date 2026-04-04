# async_rl_demo Runtime Contexts

## `local`

- `batch_source` (`actor`, provenance=`core`)
- `learner_update` (`actor`, provenance=`core`)
- `policy_snapshot` (`actor`, provenance=`core`)
- `kl_prefetch` (`effect`, provenance=`core`)
- `pangram_reward` (`effect`, provenance=`core`)
- `quality_reward` (`effect`, provenance=`core`)
- `refresh_sampler` (`effect`, provenance=`core`)
- `staleness_invariant` (`invariant`, provenance=`core`)
- `rollout_join` (`join`, provenance=`core`)
- `reward_fanout` (`parallel`, provenance=`core`)
- `local_control_plane` (`runtime_context`, provenance=`runtime_context`)

## `sandbox`

- `sample_policy` (`effect`, provenance=`core`)
- `remote_sampling` (`runtime_context`, provenance=`runtime_context`)
