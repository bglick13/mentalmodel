# mentalmodel

`mentalmodel` is a Python package skeleton for building programs whose topology,
causality, state transitions, effects, and invariants are mechanically
recoverable by humans and AI coding agents.

The package is intended to provide:

- A small set of semantic primitives such as `Workflow`, `Actor`, `Effect`,
  `Invariant`, `Parallel`, `Join`, and `Ref`
- A canonical intermediate representation (IR) that all primitives lower into
- An async runtime with opinionated OpenTelemetry export
- Static analysis, generated docs, and generated diagrams from the same IR
- A CLI for scaffolding, validation, graph/doc generation, and skill
  installation for coding agents
- An extension model so domain-specific primitives such as `RuntimeContext` can
  be added without changing the base package

Primary design boundaries:

- The core package is a programming model, not a domain-specific RL library
- OpenTelemetry is an export/backend layer, not the source of truth
- Domain-specific behavior should be implemented as extensions that lower into
  the core IR

Verification:

- Python module syntax can be validated with
  `python3 -m compileall /Users/ben/repos/mentalmodel/src`

See [PLAN.md](/Users/ben/repos/mentalmodel/PLAN.md) for the concrete package
plan, module layout, core interfaces, CLI surface, and a minimal async RL demo.

## Tooling

The project uses a single `pyproject.toml` as the source of truth for Python
tooling.

Development setup:

```bash
uv sync
```

Common commands:

```bash
uv run ruff check --fix .
uv run ruff format .
uv run mypy
uv run pytest
```

Import sorting is handled by Ruff's `I` rules rather than a separate `isort`
configuration.

Optional pre-commit setup:

```bash
uv run pre-commit install
```
