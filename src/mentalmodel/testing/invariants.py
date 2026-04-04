from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType

from hypothesis import given, settings
from hypothesis.strategies import SearchStrategy

from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.workflow import Workflow

PROPERTY_CHECK_ATTR = "__mentalmodel_property_check__"


PropertyCheckRunner = Callable[..., None]


@dataclass(slots=True, frozen=True)
class PropertyCheck:
    """Named property or invariant check attached to a module."""

    name: str
    runner: PropertyCheckRunner
    hypothesis_backed: bool = False


@dataclass(slots=True, frozen=True)
class PropertyCheckResult:
    """Result from executing one property check."""

    name: str
    success: bool
    hypothesis_backed: bool
    error: str | None = None


def property_check(name: str) -> Callable[[PropertyCheckRunner], PropertyCheckRunner]:
    """Register a plain deterministic property check."""

    def decorator(func: PropertyCheckRunner) -> PropertyCheckRunner:
        setattr(
            func,
            PROPERTY_CHECK_ATTR,
            PropertyCheck(name=name, runner=func, hypothesis_backed=False),
        )
        return func

    return decorator


def hypothesis_property_check(
    name: str,
    /,
    **strategies: SearchStrategy[object],
) -> Callable[[PropertyCheckRunner], PropertyCheckRunner]:
    """Register a Hypothesis-backed property check."""

    def decorator(func: PropertyCheckRunner) -> PropertyCheckRunner:
        wrapped = given(**strategies)(func)
        configured = settings(
            deadline=None,
            derandomize=True,
            max_examples=10,
        )(wrapped)
        setattr(
            configured,
            PROPERTY_CHECK_ATTR,
            PropertyCheck(name=name, runner=configured, hypothesis_backed=True),
        )
        return configured

    return decorator


def discover_property_checks(module: ModuleType) -> tuple[PropertyCheck, ...]:
    """Discover registered property checks from one module."""

    discovered: list[PropertyCheck] = []
    for value in module.__dict__.values():
        check = getattr(value, PROPERTY_CHECK_ATTR, None)
        if isinstance(check, PropertyCheck):
            discovered.append(check)
    return tuple(sorted(discovered, key=lambda item: item.name))


def run_property_checks(
    module: ModuleType,
    program: Workflow[NamedPrimitive],
) -> tuple[PropertyCheckResult, ...]:
    """Run all property checks registered on the module."""

    results: list[PropertyCheckResult] = []
    for check in discover_property_checks(module):
        try:
            check.runner(program)
        except Exception as exc:
            results.append(
                PropertyCheckResult(
                    name=check.name,
                    success=False,
                    hypothesis_backed=check.hypothesis_backed,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
            continue
        results.append(
            PropertyCheckResult(
                name=check.name,
                success=True,
                hypothesis_backed=check.hypothesis_backed,
            )
        )
    return tuple(results)
