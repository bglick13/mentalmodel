from __future__ import annotations

import unittest
from types import TracebackType
from typing import cast
from unittest.mock import patch

from mentalmodel.remote.backend import (
    _POSTGRES_POOLS,
    _REMOTE_LIVE_SPAN_INSERT_SQL,
    PostgresLiveSessionIndex,
    _shared_connection_pool,
)
from mentalmodel.remote.contracts import (
    RemoteLiveSessionRecord,
    RemoteLiveSessionStatus,
    RemoteLiveSessionUpdateRequest,
)


class _FakeCursorResult:
    def fetchone(self) -> None:
        return None


class _FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.commit_count = 0

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        del exc_type, exc, tb

    def execute(
        self,
        query: str,
        params: tuple[object, ...] | list[object] = (),
    ) -> _FakeCursorResult:
        self.executed.append((query, tuple(params)))
        return _FakeCursorResult()

    def commit(self) -> None:
        self.commit_count += 1


class _TestPostgresLiveSessionIndex(PostgresLiveSessionIndex):
    def __init__(self) -> None:
        super().__init__("postgresql://example.invalid/test")
        self._schema_ready = True
        self._session = RemoteLiveSessionRecord(
            graph_id="graph",
            run_id="run",
            started_at_ms=1000,
            updated_at_ms=1000,
            status=RemoteLiveSessionStatus.RUNNING,
            graph={"graph_id": "graph", "nodes": [], "edges": [], "metadata": {}},
            analysis={"error_count": 0, "warning_count": 0, "findings": []},
        )

    def _ensure_schema(self) -> None:
        return

    def get_session(
        self,
        *,
        graph_id: str,
        run_id: str,
        include_payloads: bool = True,
    ) -> RemoteLiveSessionRecord:
        assert graph_id == "graph"
        assert run_id == "run"
        del include_payloads
        return self._session


class _FakePool:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def connection(self) -> _FakeConnection:
        return self._connection


class RemoteBackendSqlTest(unittest.TestCase):
    def test_shared_connection_pool_uses_autocommit_for_pooled_connections(self) -> None:
        _POSTGRES_POOLS.clear()
        with patch("mentalmodel.remote.backend.ConnectionPool") as pool_cls:
            sentinel = object()
            pool_cls.return_value = sentinel
            returned = _shared_connection_pool("postgresql://example.invalid/test")

        self.assertIs(returned, sentinel)
        pool_cls.assert_called_once()
        _, kwargs = pool_cls.call_args
        self.assertEqual(kwargs["kwargs"], {"autocommit": True})

    def test_remote_live_span_insert_sql_matches_span_payload_shape(self) -> None:
        self.assertEqual(_REMOTE_LIVE_SPAN_INSERT_SQL.count("%s"), 11)

    def test_apply_session_update_inserts_live_spans_with_matching_parameter_count(self) -> None:
        index = _TestPostgresLiveSessionIndex()
        connection = _FakeConnection()
        payload = RemoteLiveSessionUpdateRequest(
            graph_id="graph",
            run_id="run",
            updated_at_ms=1100,
            spans=(
                {
                    "span_id": "span-1",
                    "sequence": 1,
                    "name": "effect:build_variant_artifact",
                    "start_time_ns": 123,
                    "end_time_ns": 456,
                    "duration_ns": 333,
                    "attributes": {
                        "mentalmodel.node.id": "build_variant_artifact",
                        "mentalmodel.frame.id": "root",
                        "mentalmodel.runtime.profile": "local.control_plane",
                    },
                    "frame_id": "root",
                    "loop_node_id": None,
                    "iteration_index": None,
                    "error_type": None,
                    "error_message": None,
                },
            ),
        )

        index._pool = _FakePool(connection)  # type: ignore[assignment]
        result = index.apply_session_update(payload)

        span_insert = next(
            query_and_params
            for query_and_params in connection.executed
            if "insert into remote_live_spans" in query_and_params[0]
        )
        query, params = span_insert
        self.assertEqual(query.count("%s"), len(params))
        self.assertEqual(len(params), 11)
        self.assertEqual(cast(str, params[0]), "graph")
        self.assertEqual(cast(str, params[1]), "run")
        self.assertEqual(cast(str, params[2]), "span-1")
        self.assertEqual(cast(str, params[9]), "local.control_plane")
        self.assertEqual(connection.commit_count, 1)
        self.assertEqual(result.status, RemoteLiveSessionStatus.RUNNING)


if __name__ == "__main__":
    unittest.main()
