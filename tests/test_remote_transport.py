from __future__ import annotations

import io
import unittest
from email.message import Message
from unittest.mock import patch
from urllib import error

from mentalmodel.remote.transport import (
    RemoteFailureCategory,
    RemoteRequestError,
    request_json_with_retry,
)


class _FakeResponse:
    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        del exc_type, exc, tb
        return None

    def read(self) -> bytes:
        return self._raw


class RemoteTransportTest(unittest.TestCase):
    def test_request_json_retries_transient_http_errors(self) -> None:
        attempts: list[int] = []

        def fake_urlopen(_request, timeout):  # type: ignore[no-untyped-def]
            attempts.append(int(timeout))
            if len(attempts) < 3:
                raise error.HTTPError(
                    url="http://example.test",
                    code=503,
                    msg="Service Unavailable",
                    hdrs=Message(),
                    fp=io.BytesIO(b'{"detail":"try again"}'),
                )
            return _FakeResponse(b'{"ok": true}')

        sleep_calls: list[float] = []
        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            response = request_json_with_retry(
                url="http://example.test",
                method="GET",
                payload=None,
                api_key="token",
                sleep=sleep_calls.append,
            )
        self.assertEqual(response.payload, {"ok": True})
        self.assertEqual(response.attempt_count, 3)
        self.assertEqual(len(sleep_calls), 2)

    def test_request_json_does_not_retry_auth_errors(self) -> None:
        with patch(
            "urllib.request.urlopen",
            side_effect=error.HTTPError(
                url="http://example.test",
                code=401,
                msg="Unauthorized",
                hdrs=Message(),
                fp=io.BytesIO(b""),
            ),
        ):
            with self.assertRaises(RemoteRequestError) as ctx:
                request_json_with_retry(
                    url="http://example.test",
                    method="GET",
                    payload=None,
                    api_key="token",
                )
        self.assertEqual(ctx.exception.category, RemoteFailureCategory.AUTH)
        self.assertFalse(ctx.exception.retryable)
        self.assertEqual(ctx.exception.attempt_count, 1)


if __name__ == "__main__":
    unittest.main()
