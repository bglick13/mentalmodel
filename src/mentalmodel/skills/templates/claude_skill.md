# mentalmodel Claude Skill

Use `mentalmodel` primitives as the default authoring model for new workflows.

Rules:

- Prefer `Workflow`, `Actor`, `Effect`, `Invariant`, `Parallel`, `Join`, and `Ref`
  over ad hoc orchestration.
- Route all impure work through `Effect`.
- Keep dependencies explicit in authored programs.
- Add or update invariants for meaningful joins and state transitions.
- Run `mentalmodel check`, `mentalmodel docs`, and `mentalmodel verify` after
  meaningful changes.
- Do not bypass plugin/runtime boundaries when an extension primitive is the
  correct abstraction.
