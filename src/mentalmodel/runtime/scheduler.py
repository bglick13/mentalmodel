from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

from mentalmodel.core.interfaces import RuntimeValue
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.errors import ExecutionError

if TYPE_CHECKING:
    from mentalmodel.runtime.plan import ExecutionPlan, PlanNode


def build_dependents(plan: ExecutionPlan) -> dict[str, set[str]]:
    """Build reverse dependency edges from a compiled plan."""

    dependents: dict[str, set[str]] = defaultdict(set)
    for node in plan.nodes.values():
        for dependency in node.metadata.dependencies:
            dependents[dependency].add(node.metadata.node_id)
    return dependents


async def execute_plan_nodes(
    *,
    plan: ExecutionPlan,
    context: ExecutionContext,
    run_node: NodeRunner,
    max_concurrency: int,
    initial_outputs: Mapping[str, RuntimeValue] | None = None,
) -> dict[str, RuntimeValue]:
    """Execute one compiled execution plan to completion."""

    dependents = build_dependents(plan)
    ready = sorted(
        node_id for node_id, node in plan.nodes.items() if not node.metadata.dependencies
    )
    running: dict[asyncio.Task[tuple[str, RuntimeValue]], str] = {}
    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    outputs = dict(initial_outputs or {})

    while ready or running:
        while ready and len(running) < max(1, max_concurrency):
            node_id = ready.pop(0)
            node = plan.nodes[node_id]
            task = asyncio.create_task(
                run_node(
                    node=node,
                    outputs=outputs,
                    context=context,
                    semaphore=semaphore,
                )
            )
            running[task] = node_id

        if not running:
            break

        done, _ = await asyncio.wait(
            list(running.keys()),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            node_id = running.pop(task)
            completed_node_id, output = task.result()
            outputs[completed_node_id] = output
            for dependent in sorted(dependents.get(node_id, set())):
                dependency_ids = set(plan.dependencies_for(dependent))
                if dependency_ids.issubset(outputs.keys()) and dependent not in ready:
                    ready.append(dependent)
            ready.sort()

    unresolved = [
        node_id
        for node_id in plan.nodes
        if node_id not in outputs and plan.nodes[node_id].metadata.kind != "invariant"
    ]
    if unresolved:
        raise ExecutionError(
            f"Execution finished with unresolved executable nodes: {sorted(unresolved)!r}"
        )
    return outputs


class NodeRunner(Protocol):
    async def __call__(
        self,
        *,
        node: PlanNode,
        outputs: Mapping[str, RuntimeValue],
        context: ExecutionContext,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, RuntimeValue]:
        ...
