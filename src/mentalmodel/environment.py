from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Generic, TypeVar, cast

from mentalmodel.errors import MentalModelError

ResourceT = TypeVar("ResourceT")
FinalizerResult = object | Awaitable[object]
RuntimeEnvironmentFinalizer = Callable[[], FinalizerResult]


class RuntimeProfileError(MentalModelError):
    """Raised when runtime profile resolution fails."""


class MissingRuntimeProfileError(RuntimeProfileError):
    """Raised when a node requires a runtime profile that is unavailable."""


class MissingRuntimeResourceError(RuntimeProfileError):
    """Raised when a runtime profile does not provide one required resource."""


@dataclass(slots=True, frozen=True)
class ResourceKey(Generic[ResourceT]):
    """Typed identifier for one shared runtime resource."""

    name: str
    type_: type[ResourceT]


@dataclass(slots=True, frozen=True)
class RuntimeProfile:
    """Named set of resources available during execution."""

    name: str
    resources: Mapping[ResourceKey[object], object]
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for key, value in self.resources.items():
            if not isinstance(value, key.type_):
                raise TypeError(
                    f"Runtime profile {self.name!r} binds resource {key.name!r} "
                    f"to {type(value).__name__!r}, expected {key.type_.__name__!r}."
                )

    def get(self, key: ResourceKey[ResourceT]) -> ResourceT | None:
        value = self.resources.get(cast(ResourceKey[object], key))
        if value is None:
            return None
        return cast(ResourceT, value)

    def require(self, key: ResourceKey[ResourceT]) -> ResourceT:
        value = self.get(key)
        if value is None:
            raise MissingRuntimeResourceError(
                f"Runtime profile {self.name!r} does not provide resource {key.name!r}."
            )
        return value

    def resource_names(self) -> tuple[str, ...]:
        return tuple(sorted(key.name for key in self.resources))


@dataclass(slots=True, frozen=True)
class RuntimeEnvironment:
    """Collection of named runtime profiles plus one optional default."""

    profiles: Mapping[str, RuntimeProfile]
    default_profile_name: str | None = None
    finalizers: tuple[RuntimeEnvironmentFinalizer, ...] = ()

    def __post_init__(self) -> None:
        for profile_name, profile in self.profiles.items():
            if profile.name != profile_name:
                raise TypeError(
                    f"RuntimeEnvironment profile key {profile_name!r} does not match "
                    f"RuntimeProfile.name {profile.name!r}."
                )
        if (
            self.default_profile_name is not None
            and self.default_profile_name not in self.profiles
        ):
            raise TypeError(
                f"RuntimeEnvironment default profile {self.default_profile_name!r} "
                "is not present in profiles."
            )

    def resolve_profile_name(self, runtime_context: str | None) -> str | None:
        if runtime_context is not None:
            return runtime_context
        return self.default_profile_name

    def profile_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.profiles))

    def get_profile(self, profile_name: str | None) -> RuntimeProfile | None:
        if profile_name is None:
            return None
        return self.profiles.get(profile_name)

    def require_profile(self, profile_name: str | None) -> RuntimeProfile:
        if profile_name is None:
            raise MissingRuntimeProfileError(
                "No active runtime profile is configured for this node."
            )
        profile = self.profiles.get(profile_name)
        if profile is None:
            raise MissingRuntimeProfileError(
                f"Runtime profile {profile_name!r} is not configured in the environment."
            )
        return profile

    async def finalize(self) -> None:
        """Run registered environment finalizers in reverse registration order."""

        for finalizer in reversed(self.finalizers):
            result = finalizer()
            if inspect.isawaitable(result):
                await cast(Awaitable[object], result)


@dataclass(slots=True, frozen=True)
class ResourceResolver:
    """Node-scoped typed resource access against one runtime environment."""

    environment: RuntimeEnvironment
    active_profile_name: str | None
    node_id: str | None = None

    @property
    def active_profile(self) -> RuntimeProfile | None:
        return self.environment.get_profile(self.active_profile_name)

    def get(self, key: ResourceKey[ResourceT]) -> ResourceT | None:
        profile = self.active_profile
        if profile is None:
            return None
        return profile.get(key)

    def require(self, key: ResourceKey[ResourceT]) -> ResourceT:
        profile = self.environment.require_profile(self.active_profile_name)
        try:
            return profile.require(key)
        except MissingRuntimeResourceError as exc:
            if self.node_id is None:
                raise
            raise MissingRuntimeResourceError(
                f"Node {self.node_id!r} requires resource {key.name!r} "
                f"from runtime profile {profile.name!r}."
            ) from exc


def merge_resource_keys(
    *resource_groups: Sequence[ResourceKey[object]],
) -> tuple[ResourceKey[object], ...]:
    """Merge resource-key groups while preserving first-seen order."""

    merged: dict[ResourceKey[object], None] = {}
    for group in resource_groups:
        for key in group:
            merged.setdefault(key, None)
    return tuple(merged)


EMPTY_RUNTIME_ENVIRONMENT = RuntimeEnvironment(profiles={})
