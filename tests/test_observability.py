from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mentalmodel.cli import main
from mentalmodel.errors import TracingConfigError
from mentalmodel.examples.async_rl.demo import build_program
from mentalmodel.observability import load_tracing_config, write_otel_demo
from mentalmodel.observability.config import TracingMode
from mentalmodel.runtime.runs import resolve_run_summary
from mentalmodel.testing import run_verification


class ObservabilityTest(unittest.TestCase):
    def test_load_tracing_config_defaults_to_disk_mode(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            config = load_tracing_config()
        self.assertEqual(config.mode, TracingMode.DISK)
        self.assertTrue(config.mirror_to_disk)
        self.assertTrue(config.capture_local_spans)
        self.assertFalse(config.external_sink_configured)

    def test_load_tracing_config_resolves_otlp_http_from_env(self) -> None:
        with patch.dict(
            os.environ,
            {
                "MENTALMODEL_OTEL_MODE": "otlp_http",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
                "MENTALMODEL_OTEL_SERVICE_NAME": "mentalmodel-demo",
            },
            clear=True,
        ):
            config = load_tracing_config()
        self.assertEqual(config.mode, TracingMode.OTLP_HTTP)
        self.assertEqual(config.otlp_endpoint, "http://localhost:4318/v1/traces")
        self.assertEqual(config.service_name, "mentalmodel-demo")
        self.assertTrue(config.external_sink_configured)

    def test_load_tracing_config_preserves_explicit_http_traces_path(self) -> None:
        with patch.dict(
            os.environ,
            {
                "MENTALMODEL_OTEL_MODE": "otlp_http",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318/v1/traces",
            },
            clear=True,
        ):
            config = load_tracing_config()
        self.assertEqual(config.otlp_endpoint, "http://localhost:4318/v1/traces")

    def test_load_tracing_config_rejects_missing_otlp_endpoint(self) -> None:
        with patch.dict(
            os.environ,
            {"MENTALMODEL_OTEL_MODE": "otlp_http"},
            clear=True,
        ):
            with self.assertRaises(TracingConfigError):
                load_tracing_config()

    def test_load_tracing_config_rejects_invalid_mode(self) -> None:
        with patch.dict(
            os.environ,
            {"MENTALMODEL_OTEL_MODE": "not-real"},
            clear=True,
        ):
            with self.assertRaises(TracingConfigError):
                load_tracing_config()

    def test_write_otel_demo_writes_expected_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            written = write_otel_demo(output_dir=Path(tmpdir), stack="lgtm")
            names = {path.name for path in written}
            self.assertEqual(
                names,
                {
                    "docker-compose.otel-lgtm.yml",
                    "mentalmodel.otel.env",
                    "OTEL-DEMO.md",
                },
            )
            env_contents = (Path(tmpdir) / "mentalmodel.otel.env").read_text(encoding="utf-8")
            self.assertIn(
                "OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces",
                env_contents,
            )

    def test_otel_show_config_command_outputs_json(self) -> None:
        stdout = io.StringIO()
        with patch.dict(
            os.environ,
            {
                "MENTALMODEL_OTEL_MODE": "console",
                "MENTALMODEL_OTEL_SERVICE_NAME": "mentalmodel-cli",
            },
            clear=True,
        ):
            with contextlib.redirect_stdout(stdout):
                exit_code = main(["otel", "show-config", "--json"])
        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn('"mode": "console"', rendered)
        self.assertIn('"service_name": "mentalmodel-cli"', rendered)

    def test_otel_write_demo_command_writes_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "otel",
                        "write-demo",
                        "--stack",
                        "jaeger",
                        "--output-dir",
                        tmpdir,
                        "--json",
                    ]
                )
            self.assertEqual(exit_code, 0)
            self.assertTrue((Path(tmpdir) / "docker-compose.otel-jaeger.yml").exists())
            self.assertTrue((Path(tmpdir) / "mentalmodel.otel.jaeger.env").exists())

    def test_disabled_tracing_mode_skips_span_file_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                os.environ,
                {"MENTALMODEL_OTEL_MODE": "disabled"},
                clear=True,
            ):
                report = run_verification(build_program(), runs_dir=Path(tmpdir))
            self.assertTrue(report.success)
            summary = resolve_run_summary(runs_dir=Path(tmpdir), graph_id="async_rl_demo")
            self.assertEqual(summary.trace_mode, "disabled")
            self.assertFalse(summary.trace_mirror_to_disk)
            self.assertFalse(summary.trace_capture_local_spans)
            run_dir = Path(report.runtime.run_artifacts_dir or "")
            self.assertFalse((run_dir / "otel-spans.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
