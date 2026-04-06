from __future__ import annotations

import importlib
import inspect
import json
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import TypeVar, cast

from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.core.workflow import Workflow
from mentalmodel.environment import RuntimeEnvironment
from mentalmodel.errors import EntrypointLoadError
from mentalmodel.ir.schemas import EntryPointSpec

LoadedT = TypeVar("LoadedT")


@dataclass(slots=True, frozen=True)
class InvocationFactorySpec:
    entrypoint: str
    params: Mapping[str, object] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class VerifyInvocationSpec:
    program: InvocationFactorySpec
    environment: InvocationFactorySpec | None = None
    invocation_name: str | None = None
    runs_dir: Path | None = None


def parse_entrypoint(raw: str) -> EntryPointSpec:
    if ":" not in raw:
        raise EntrypointLoadError(
            "Entrypoint must be in the format 'module.submodule:function_name'."
        )
    module_name, attribute_name = raw.split(":", 1)
    if not module_name or not attribute_name:
        raise EntrypointLoadError("Entrypoint must include both a module and an attribute name.")
    return EntryPointSpec(module_name=module_name, attribute_name=attribute_name)


def load_json_object(
    *,
    raw_json: str | None = None,
    file_path: Path | None = None,
    subject: str,
) -> dict[str, object] | None:
    if raw_json is not None and file_path is not None:
        raise EntrypointLoadError(f"Use only one of {subject} JSON text or file.")
    if raw_json is None and file_path is None:
        return None
    if raw_json is not None:
        raw_payload = raw_json
    else:
        assert file_path is not None
        try:
            raw_payload = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise EntrypointLoadError(
                f"Failed to read {subject} file {str(file_path)!r}: {exc}"
            ) from exc
    try:
        decoded = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise EntrypointLoadError(f"Failed to parse {subject}: {exc.msg}.") from exc
    if not isinstance(decoded, dict):
        raise EntrypointLoadError(f"{subject.capitalize()} must decode to a JSON object.")
    if not all(isinstance(key, str) for key in decoded):
        raise EntrypointLoadError(f"{subject.capitalize()} keys must be strings.")
    return cast(dict[str, object], decoded)


def load_workflow_subject(
    spec: InvocationFactorySpec,
) -> tuple[ModuleType, Workflow[NamedPrimitive]]:
    module, loaded = load_invocation_subject(
        spec,
        expected_type=Workflow,
        subject="workflow entrypoint",
    )
    return module, cast(Workflow[NamedPrimitive], loaded)


def load_runtime_environment_subject(
    spec: InvocationFactorySpec,
) -> tuple[ModuleType, RuntimeEnvironment]:
    module, loaded = load_invocation_subject(
        spec,
        expected_type=RuntimeEnvironment,
        subject="runtime environment entrypoint",
    )
    return module, loaded


def load_invocation_subject(
    spec: InvocationFactorySpec,
    *,
    expected_type: type[LoadedT],
    subject: str,
) -> tuple[ModuleType, LoadedT]:
    entrypoint = parse_entrypoint(spec.entrypoint)
    try:
        module = importlib.import_module(entrypoint.module_name)
    except Exception as exc:
        raise EntrypointLoadError(
            f"Failed to import module {entrypoint.module_name!r}: {exc}"
        ) from exc
    try:
        attribute = getattr(module, entrypoint.attribute_name)
    except AttributeError as exc:
        raise EntrypointLoadError(
            f"Module {entrypoint.module_name!r} does not define {entrypoint.attribute_name!r}."
        ) from exc
    if callable(attribute):
        loaded = _invoke_entrypoint_callable(
            spec.entrypoint,
            cast(Callable[..., object], attribute),
            params=spec.params,
        )
    else:
        if spec.params:
            raise EntrypointLoadError(
                f"Entrypoint {spec.entrypoint!r} resolved to a non-callable "
                f"{expected_type.__name__}; parameters require a callable entrypoint."
            )
        loaded = attribute
    if not isinstance(loaded, expected_type):
        raise EntrypointLoadError(
            f"Entrypoint {spec.entrypoint!r} must resolve to a {expected_type.__name__}, "
            f"got {type(loaded).__name__}."
        )
    return module, loaded


def read_verify_invocation_spec(path: Path) -> VerifyInvocationSpec:
    try:
        raw_payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise EntrypointLoadError(f"Failed to read verify spec {str(path)!r}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise EntrypointLoadError(f"Failed to parse verify spec {str(path)!r}: {exc}") from exc
    if not isinstance(raw_payload, dict):
        raise EntrypointLoadError("Verify spec must decode to a TOML table.")
    payload = cast(dict[str, object], raw_payload)
    base_dir = path.parent
    program_section = _require_table(payload, "program")
    environment_section = _optional_table(payload, "environment")
    runtime_section = _optional_table(payload, "runtime")
    program = _parse_factory_section(program_section, base_dir=base_dir, subject="program")
    environment = (
        None
        if environment_section is None
        else _parse_factory_section(
            environment_section,
            base_dir=base_dir,
            subject="environment",
        )
    )
    invocation_name = (
        None
        if runtime_section is None
        else _optional_str(runtime_section, "invocation_name")
    )
    runs_dir = None
    if runtime_section is not None:
        runs_dir_value = runtime_section.get("runs_dir")
        if runs_dir_value is not None:
            if not isinstance(runs_dir_value, str):
                raise EntrypointLoadError("runtime.runs_dir must be a string path.")
            runs_dir = (base_dir / runs_dir_value).resolve()
    return VerifyInvocationSpec(
        program=program,
        environment=environment,
        invocation_name=invocation_name,
        runs_dir=runs_dir,
    )


def _parse_factory_section(
    section: Mapping[str, object],
    *,
    base_dir: Path,
    subject: str,
) -> InvocationFactorySpec:
    entrypoint = _require_str(section, "entrypoint", subject=subject)
    params = _parse_section_params(section, base_dir=base_dir, subject=subject)
    return InvocationFactorySpec(entrypoint=entrypoint, params=params)


def _parse_section_params(
    section: Mapping[str, object],
    *,
    base_dir: Path,
    subject: str,
) -> dict[str, object]:
    params_inline = section.get("params")
    params_json = section.get("params_json")
    params_file = section.get("params_file")
    provided_count = sum(
        value is not None for value in (params_inline, params_json, params_file)
    )
    if provided_count > 1:
        raise EntrypointLoadError(
            f"{subject.capitalize()} spec may define only one of params, "
            "params_json, or params_file."
        )
    if params_inline is not None:
        if not isinstance(params_inline, Mapping):
            raise EntrypointLoadError(f"{subject}.params must be a TOML table.")
        return {str(key): value for key, value in params_inline.items()}
    if params_json is not None:
        if not isinstance(params_json, str):
            raise EntrypointLoadError(f"{subject}.params_json must be a string.")
        return load_json_object(raw_json=params_json, subject=f"{subject} params") or {}
    if params_file is not None:
        if not isinstance(params_file, str):
            raise EntrypointLoadError(f"{subject}.params_file must be a string path.")
        return load_json_object(
            file_path=(base_dir / params_file).resolve(),
            subject=f"{subject} params",
        ) or {}
    return {}


def _invoke_entrypoint_callable(
    raw_entrypoint: str,
    attribute: Callable[..., object],
    *,
    params: Mapping[str, object],
) -> object:
    try:
        signature = inspect.signature(attribute)
    except (TypeError, ValueError):
        signature = None
    if signature is not None:
        try:
            signature.bind(**params)
        except TypeError as exc:
            raise EntrypointLoadError(
                f"Invalid parameters for entrypoint {raw_entrypoint!r}: {exc}"
            ) from exc
    try:
        return attribute(**params)
    except TypeError as exc:
        raise EntrypointLoadError(
            f"Failed to invoke entrypoint {raw_entrypoint!r} with parameters: {exc}"
        ) from exc


def _require_table(payload: Mapping[str, object], key: str) -> dict[str, object]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise EntrypointLoadError(f"Verify spec requires a [{key}] table.")
    return {str(inner_key): inner_value for inner_key, inner_value in value.items()}


def _optional_table(payload: Mapping[str, object], key: str) -> dict[str, object] | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise EntrypointLoadError(f"Verify spec [{key}] section must be a table.")
    return {str(inner_key): inner_value for inner_key, inner_value in value.items()}


def _require_str(payload: Mapping[str, object], key: str, *, subject: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise EntrypointLoadError(f"{subject}.{key} must be a non-empty string.")
    return value


def _optional_str(payload: Mapping[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise EntrypointLoadError(f"{key} must be a non-empty string when provided.")
    return value
