from __future__ import annotations

import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from mentalmodel.analysis import AnalysisReport
from mentalmodel.core import Actor, ActorHandler, ActorResult, Workflow
from mentalmodel.core.interfaces import NamedPrimitive
from mentalmodel.environment import EMPTY_RUNTIME_ENVIRONMENT
from mentalmodel.observability.live import AsyncLiveExporter, LiveIngestionConfig
from mentalmodel.observability.metrics import MetricObservation
from mentalmodel.observability.tracing import RecordedSpan
from mentalmodel.remote import (
    CatalogSource,
    CompletedRunPublishResult,
    LiveExecutionPublishResult,
)
from mentalmodel.remote.contracts import RunManifest
from mentalmodel.remote.sync import RemoteServiceCompletedRunSink
from mentalmodel.runtime import (
    ManagedExecutionOptions,
    ManagedRunTarget,
    run_managed,
)
from mentalmodel.runtime.context import ExecutionContext
from mentalmodel.runtime.executor import ExecutionResult
from mentalmodel.runtime.managed import resolve_managed_execution


class _NoOpHandler(ActorHandler[dict[str, object], object, str]):
    async def handle(
        self,
        inputs: dict[str, object],
        state: object | None,
        ctx: ExecutionContext,
    ) -> ActorResult[str, object]:
        del inputs, state, ctx
        return ActorResult(output="ok")


class _RecordingCompletedRunSink:
    def __init__(self) -> None:
        self.calls: list[tuple[str | None, Path]] = []

    def publish(
        self,
        *,
        manifest: RunManifest,
        run_dir: Path,
    ) -> CompletedRunPublishResult:
        self.calls.append((manifest.project_id, run_dir))
        return CompletedRunPublishResult(
            transport="recording",
            success=True,
            graph_id=manifest.graph_id,
            run_id=manifest.run_id,
            project_id=manifest.project_id,
            remote_run_dir=f"/remote/{manifest.graph_id}/{manifest.run_id}",
            uploaded_at_ms=123,
        )


class _RecordingLiveExecutionSink:
    def __init__(self) -> None:
        self.started = 0
        self.records = 0
        self.spans = 0
        self.metrics = 0
        self.completed: list[tuple[bool, str | None]] = []

    def start(self, *, graph: object, analysis: AnalysisReport) -> None:
        del graph, analysis
        self.started += 1

    def emit_record(self, record: object) -> None:
        del record
        self.records += 1

    def emit_span(self, span: RecordedSpan) -> None:
        del span
        self.spans += 1

    def emit_metrics(self, observations: Sequence[MetricObservation]) -> None:
        self.metrics += len(observations)

    def complete(self, *, success: bool, error: str | None = None) -> None:
        self.completed.append((success, error))

    def runtime_tracing_config(self) -> None:
        return None

    def delivery_result(self) -> LiveExecutionPublishResult:
        return LiveExecutionPublishResult(
            transport="recording-live",
            delivery_mode="recording",
            success=True,
            graph_id="managed_graph",
            run_id="run-managed",
            project_id="managed-project",
            required=False,
            accepted_log_count=self.records,
            accepted_span_count=self.spans,
            accepted_metric_count=self.metrics,
            exported_log_count=self.records,
            exported_span_count=self.spans,
            exported_metric_count=self.metrics,
        )


def _build_program() -> Workflow[NamedPrimitive]:
    return Workflow(
        name="managed_graph",
        children=[Actor(name="source", handler=_NoOpHandler())],
    )


class ManagedExecutionTest(unittest.TestCase):
    def test_run_managed_persists_and_emits_remote_surfaces(self) -> None:
        completed_sink = _RecordingCompletedRunSink()
        live_sink = _RecordingLiveExecutionSink()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_managed(
                _build_program(),
                invocation_name="managed_smoke",
                options=ManagedExecutionOptions(
                    target=ManagedRunTarget(
                        runs_dir=Path(tmpdir),
                        project_id="managed-project",
                        project_label="Managed Project",
                        environment_name="dev",
                        catalog_entry_id="catalog-node",
                        catalog_source=CatalogSource.BUILTIN,
                    ),
                    completed_run_sink=completed_sink,
                    live_execution_sink=live_sink,
                ),
            )

            self.assertTrue(result.success)
            self.assertIsInstance(result.execution, ExecutionResult)
            self.assertIsNotNone(result.run_artifacts)
            self.assertIsNotNone(result.completed_run_upload)
            self.assertIsNotNone(result.live_execution_delivery)
            self.assertEqual(completed_sink.calls[0][0], "managed-project")
            self.assertEqual(live_sink.started, 1)
            self.assertEqual(live_sink.completed, [(True, None)])
            self.assertGreater(live_sink.spans, 0)
            self.assertGreater(live_sink.metrics, 0)
            assert result.run_artifacts is not None
            self.assertTrue(result.run_artifacts.run_dir.exists())
            self.assertEqual(result.run_artifacts.manifest.project_id, "managed-project")
            self.assertEqual(result.run_artifacts.manifest.catalog_entry_id, "catalog-node")

    def test_resolve_managed_execution_discovers_linked_project_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            default_runs_dir = repo_root / "custom-runs"
            (repo_root / "mentalmodel.toml").write_text(
                "\n".join(
                    [
                        "[project]",
                        'project_id = "managed-project"',
                        'label = "Managed Project"',
                        "",
                        "[remote]",
                        'server_url = "http://127.0.0.1:8765"',
                        'api_key_env = "MENTALMODEL_REMOTE_API_KEY"',
                        'default_environment = "staging"',
                        "",
                        "[catalog]",
                        'provider = "mentalmodel.examples.async_rl.demo:build_program"',
                        "",
                        "[runs]",
                        'default_runs_dir = "custom-runs"',
                    ]
                ),
                encoding="utf-8",
            )

            resolution = resolve_managed_execution(
                options=ManagedExecutionOptions(
                    config_search_start=repo_root,
                ),
                run_id="run-123",
                invocation_name="managed_invocation",
                environment=EMPTY_RUNTIME_ENVIRONMENT,
            )

        self.assertIsNotNone(resolution.linked_project_config)
        self.assertEqual(resolution.target.project_id, "managed-project")
        self.assertEqual(resolution.target.project_label, "Managed Project")
        self.assertEqual(resolution.target.environment_name, "staging")
        self.assertEqual(resolution.target.runs_dir, default_runs_dir.resolve())
        self.assertIsInstance(resolution.completed_run_sink, RemoteServiceCompletedRunSink)
        self.assertIsNone(resolution.live_execution_sink)

    def test_resolve_managed_execution_uses_explicit_live_ingestion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            resolution = resolve_managed_execution(
                options=ManagedExecutionOptions(
                    live_ingestion=LiveIngestionConfig(
                        otlp_endpoint="http://127.0.0.1:4318",
                        outbox_dir=Path(tmpdir) / "outbox",
                    )
                ),
                run_id="run-123",
                invocation_name="managed_invocation",
                environment=EMPTY_RUNTIME_ENVIRONMENT,
            )
            assert resolution.live_execution_sink is not None
            resolution.live_execution_sink.complete(success=True)

        self.assertIsInstance(resolution.live_execution_sink, AsyncLiveExporter)

    def test_run_managed_fails_open_when_live_delivery_is_optional(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_managed(
                _build_program(),
                options=ManagedExecutionOptions(
                    target=ManagedRunTarget(runs_dir=Path(tmpdir)),
                    live_ingestion=LiveIngestionConfig(
                        otlp_endpoint="http://127.0.0.1:4318",
                        outbox_dir=Path(tmpdir) / "outbox",
                        max_outbox_bytes=1,
                        require_live_delivery=False,
                    ),
                ),
            )

        self.assertTrue(result.success)
        assert result.live_execution_delivery is not None
        self.assertFalse(result.live_execution_delivery.success)
        self.assertTrue(result.live_execution_delivery.failed_open)

    def test_run_managed_raises_when_required_live_delivery_cannot_buffer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(Exception, "hard cap"):
                run_managed(
                    _build_program(),
                    options=ManagedExecutionOptions(
                        target=ManagedRunTarget(runs_dir=Path(tmpdir)),
                        live_ingestion=LiveIngestionConfig(
                            otlp_endpoint="http://127.0.0.1:4318",
                            outbox_dir=Path(tmpdir) / "outbox",
                            max_outbox_bytes=1,
                            require_live_delivery=True,
                        ),
                    ),
                )


if __name__ == "__main__":
    unittest.main()
