from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final
from urllib import error, request

from mentalmodel.remote.contracts import RemoteContractError


class RemoteFailureCategory(StrEnum):
    """Stable classification for producer-side remote request failures."""

    AUTH = "auth"
    CLIENT = "client"
    SERVER = "server"
    RATE_LIMIT = "rate_limit"
    NETWORK = "network"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


@dataclass(slots=True, frozen=True)
class RemoteRetryPolicy:
    """Deterministic retry policy for remote service requests."""

    max_attempts: int = 4
    base_delay_seconds: float = 0.25
    max_delay_seconds: float = 2.0
    timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise RemoteContractError("RemoteRetryPolicy.max_attempts must be at least 1.")
        if self.base_delay_seconds <= 0:
            raise RemoteContractError(
                "RemoteRetryPolicy.base_delay_seconds must be positive."
            )
        if self.max_delay_seconds < self.base_delay_seconds:
            raise RemoteContractError(
                "RemoteRetryPolicy.max_delay_seconds must be at least base_delay_seconds."
            )
        if self.timeout_seconds <= 0:
            raise RemoteContractError("RemoteRetryPolicy.timeout_seconds must be positive.")

    def delay_for_attempt(self, attempt: int) -> float:
        bounded_attempt = max(1, attempt)
        delay = self.base_delay_seconds * (2 ** (bounded_attempt - 1))
        return float(min(delay, self.max_delay_seconds))


DEFAULT_REMOTE_RETRY_POLICY: Final = RemoteRetryPolicy()


class RemoteRequestError(RemoteContractError):
    """Structured producer-side error after exhausting remote request attempts."""

    def __init__(
        self,
        *,
        message: str,
        category: RemoteFailureCategory,
        retryable: bool,
        attempt_count: int,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.retryable = retryable
        self.attempt_count = attempt_count
        self.status_code = status_code


@dataclass(slots=True, frozen=True)
class RemoteJsonResponse:
    """Decoded JSON payload plus request attempt metadata."""

    payload: dict[str, object]
    attempt_count: int


def request_json_with_retry(
    *,
    url: str,
    method: str,
    payload: dict[str, object] | None,
    api_key: str | None,
    retry_policy: RemoteRetryPolicy = DEFAULT_REMOTE_RETRY_POLICY,
    sleep: Callable[[float], None] = time.sleep,
) -> RemoteJsonResponse:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if api_key is not None:
        headers["Authorization"] = f"Bearer {api_key}"
    if body is not None:
        headers["Content-Type"] = "application/json"

    last_error: RemoteRequestError | None = None
    for attempt in range(1, retry_policy.max_attempts + 1):
        req = request.Request(url, data=body, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=retry_policy.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
            decoded = json.loads(raw)
            if not isinstance(decoded, dict):
                raise RemoteRequestError(
                    message="Remote response must be a JSON object.",
                    category=RemoteFailureCategory.UNKNOWN,
                    retryable=False,
                    attempt_count=attempt,
                )
            return RemoteJsonResponse(payload=decoded, attempt_count=attempt)
        except Exception as exc:  # pragma: no cover - exercised via helpers/tests
            classified = _classify_remote_error(exc, attempt_count=attempt)
            last_error = classified
            if not classified.retryable or attempt >= retry_policy.max_attempts:
                raise classified from exc
            sleep(retry_policy.delay_for_attempt(attempt))
    if last_error is None:  # pragma: no cover - defensive
        raise RemoteRequestError(
            message="Remote request failed for an unknown reason.",
            category=RemoteFailureCategory.UNKNOWN,
            retryable=False,
            attempt_count=retry_policy.max_attempts,
        )
    raise last_error


def _classify_remote_error(
    exc: Exception,
    *,
    attempt_count: int,
) -> RemoteRequestError:
    if isinstance(exc, RemoteRequestError):
        return RemoteRequestError(
            message=str(exc),
            category=exc.category,
            retryable=exc.retryable,
            attempt_count=attempt_count,
            status_code=exc.status_code,
        )
    if isinstance(exc, error.HTTPError):
        status_code = exc.code
        body_text = ""
        try:
            body_text = exc.read().decode("utf-8").strip()
        except Exception:
            body_text = ""
        if status_code in {401, 403}:
            return RemoteRequestError(
                message=_http_error_message("authorization failed", status_code, body_text),
                category=RemoteFailureCategory.AUTH,
                retryable=False,
                attempt_count=attempt_count,
                status_code=status_code,
            )
        if status_code == 429:
            return RemoteRequestError(
                message=_http_error_message("rate limited", status_code, body_text),
                category=RemoteFailureCategory.RATE_LIMIT,
                retryable=True,
                attempt_count=attempt_count,
                status_code=status_code,
            )
        if status_code >= 500:
            return RemoteRequestError(
                message=_http_error_message("server error", status_code, body_text),
                category=RemoteFailureCategory.SERVER,
                retryable=True,
                attempt_count=attempt_count,
                status_code=status_code,
            )
        return RemoteRequestError(
            message=_http_error_message("client error", status_code, body_text),
            category=RemoteFailureCategory.CLIENT,
            retryable=False,
            attempt_count=attempt_count,
            status_code=status_code,
        )
    if isinstance(exc, error.URLError):
        reason = exc.reason
        if isinstance(reason, TimeoutError):
            return RemoteRequestError(
                message=f"Remote request timed out: {reason}",
                category=RemoteFailureCategory.TIMEOUT,
                retryable=True,
                attempt_count=attempt_count,
            )
        return RemoteRequestError(
            message=f"Remote network request failed: {reason}",
            category=RemoteFailureCategory.NETWORK,
            retryable=True,
            attempt_count=attempt_count,
        )
    if isinstance(exc, TimeoutError):
        return RemoteRequestError(
            message=f"Remote request timed out: {exc}",
            category=RemoteFailureCategory.TIMEOUT,
            retryable=True,
            attempt_count=attempt_count,
        )
    return RemoteRequestError(
        message=f"Remote request failed: {exc}",
        category=RemoteFailureCategory.UNKNOWN,
        retryable=False,
        attempt_count=attempt_count,
    )


def _http_error_message(prefix: str, status_code: int, body_text: str) -> str:
    suffix = "" if body_text == "" else f" ({body_text})"
    return f"Remote request {prefix} with HTTP {status_code}{suffix}."
