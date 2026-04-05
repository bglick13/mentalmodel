from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from urllib.parse import urlparse, urlunparse

from mentalmodel.errors import TracingConfigError


class TracingMode(StrEnum):
    """Supported runtime tracing export modes."""

    DISK = "disk"
    OTLP_HTTP = "otlp_http"
    OTLP_GRPC = "otlp_grpc"
    CONSOLE = "console"
    DISABLED = "disabled"


@dataclass(slots=True, frozen=True)
class TracingConfig:
    """Resolved tracing configuration for one runtime."""

    service_name: str
    service_namespace: str | None = None
    service_version: str | None = None
    mode: TracingMode = TracingMode.DISK
    otlp_endpoint: str | None = None
    otlp_headers: dict[str, str] = field(default_factory=dict)
    otlp_insecure: bool = False
    mirror_to_disk: bool = True
    capture_local_spans: bool = True

    @property
    def external_sink_configured(self) -> bool:
        return self.mode in {
            TracingMode.OTLP_HTTP,
            TracingMode.OTLP_GRPC,
            TracingMode.CONSOLE,
        }

    def summary(self) -> dict[str, str | bool | None]:
        return {
            "trace_mode": self.mode.value,
            "trace_otlp_endpoint": self.otlp_endpoint,
            "trace_mirror_to_disk": self.mirror_to_disk,
            "trace_capture_local_spans": self.capture_local_spans,
            "trace_sink_configured": self.external_sink_configured,
            "trace_service_name": self.service_name,
            "trace_service_namespace": self.service_namespace,
            "trace_service_version": self.service_version,
        }


def load_tracing_config(
    *,
    service_name: str = "mentalmodel",
    environ: Mapping[str, str] | None = None,
) -> TracingConfig:
    """Resolve tracing configuration from explicit defaults and environment."""

    env = os.environ if environ is None else environ
    resolved_service_name = env.get("MENTALMODEL_OTEL_SERVICE_NAME", service_name)
    service_namespace = _optional_str(env, "MENTALMODEL_OTEL_SERVICE_NAMESPACE")
    service_version = _optional_str(env, "MENTALMODEL_OTEL_SERVICE_VERSION")
    mode = _resolve_mode(env)
    endpoint = _resolve_endpoint(env, mode)
    headers = _parse_headers(
        env.get("OTEL_EXPORTER_OTLP_TRACES_HEADERS")
        or env.get("OTEL_EXPORTER_OTLP_HEADERS")
        or ""
    )
    insecure = _parse_bool(
        env.get("OTEL_EXPORTER_OTLP_TRACES_INSECURE")
        or env.get("OTEL_EXPORTER_OTLP_INSECURE"),
        default=False,
    )
    mirror_to_disk = _parse_bool(env.get("MENTALMODEL_OTEL_MIRROR_TO_DISK"), default=True)
    capture_local_spans = _parse_bool(
        env.get("MENTALMODEL_OTEL_CAPTURE_LOCAL_SPANS"),
        default=mode is not TracingMode.DISABLED,
    )
    if mode is TracingMode.DISABLED:
        mirror_to_disk = False
        capture_local_spans = False
    return TracingConfig(
        service_name=resolved_service_name,
        service_namespace=service_namespace,
        service_version=service_version,
        mode=mode,
        otlp_endpoint=endpoint,
        otlp_headers=headers,
        otlp_insecure=insecure,
        mirror_to_disk=mirror_to_disk,
        capture_local_spans=capture_local_spans,
    )


def _resolve_mode(env: Mapping[str, str]) -> TracingMode:
    explicit_mode = env.get("MENTALMODEL_OTEL_MODE")
    if explicit_mode is not None:
        return _parse_mode(explicit_mode)

    traces_exporter = env.get("OTEL_TRACES_EXPORTER")
    if traces_exporter == "none":
        return TracingMode.DISABLED
    if traces_exporter == "console":
        return TracingMode.CONSOLE
    if _has_any(
        env,
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ) or traces_exporter == "otlp":
        protocol = (
            env.get("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            or env.get("OTEL_EXPORTER_OTLP_PROTOCOL")
            or "http/protobuf"
        )
        return _mode_for_protocol(protocol)
    return TracingMode.DISK


def _resolve_endpoint(env: Mapping[str, str], mode: TracingMode) -> str | None:
    endpoint = env.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or env.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT"
    )
    if mode in {TracingMode.OTLP_HTTP, TracingMode.OTLP_GRPC}:
        if endpoint is None:
            raise TracingConfigError(
                f"Tracing mode {mode.value!r} requires an OTLP endpoint."
            )
        if mode is TracingMode.OTLP_HTTP:
            return _normalize_otlp_http_traces_endpoint(endpoint)
        return endpoint
    return endpoint


def _mode_for_protocol(protocol: str) -> TracingMode:
    normalized = protocol.strip().lower()
    if normalized == "grpc":
        return TracingMode.OTLP_GRPC
    if normalized in {"http/protobuf", "http"}:
        return TracingMode.OTLP_HTTP
    raise TracingConfigError(
        "Unsupported OTLP protocol. Expected 'grpc' or 'http/protobuf'."
    )


def _parse_mode(raw: str) -> TracingMode:
    normalized = raw.strip().lower()
    try:
        return TracingMode(normalized)
    except ValueError as exc:
        raise TracingConfigError(
            "Unsupported tracing mode. Expected one of: "
            "'disk', 'otlp_http', 'otlp_grpc', 'console', 'disabled'."
        ) from exc


def _optional_str(env: Mapping[str, str], key: str) -> str | None:
    value = env.get(key)
    return value if value else None


def _parse_headers(raw: str) -> dict[str, str]:
    if not raw:
        return {}
    headers: dict[str, str] = {}
    for item in raw.split(","):
        part = item.strip()
        if not part:
            continue
        if "=" not in part:
            raise TracingConfigError(
                "OTLP headers must be a comma-separated list of key=value entries."
            )
        key, value = part.split("=", 1)
        header_key = key.strip()
        header_value = value.strip()
        if not header_key or not header_value:
            raise TracingConfigError(
                "OTLP headers must include non-empty keys and values."
            )
        headers[header_key] = header_value
    return headers


def _parse_bool(raw: str | None, *, default: bool) -> bool:
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise TracingConfigError(f"Expected a boolean value, got {raw!r}.")


def _has_any(env: Mapping[str, str], *keys: str) -> bool:
    return any(env.get(key) for key in keys)


def _normalize_otlp_http_traces_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    path = parsed.path.rstrip("/")
    if path.endswith("/v1/traces"):
        return endpoint
    if not path:
        path = "/v1/traces"
    else:
        path = f"{path}/v1/traces"
    return urlunparse(parsed._replace(path=path))
