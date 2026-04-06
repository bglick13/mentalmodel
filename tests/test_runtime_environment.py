from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import cast

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.environment import (
    MissingRuntimeProfileError,
    ResourceKey,
    RuntimeEnvironment,
    RuntimeProfile,
)
from mentalmodel.examples.runtime_environment.demo import (
    MULTIPLIER_RESOURCE,
    Multiplier,
    build_environment,
    build_program,
)
from mentalmodel.runtime import AsyncExecutor
from mentalmodel.runtime.runs import load_run_records, resolve_run_summary
from mentalmodel.testing import run_verification


class RuntimeEnvironmentTest(unittest.TestCase):
    def test_program_can_bind_two_runtime_profiles_in_one_run(self) -> None:
        result = asyncio.run(
            AsyncExecutor(environment=build_environment()).run(build_program())
        )

        self.assertEqual(
            result.outputs["comparison"],
            {"fixture_scaled": 14, "real_scaled": 35},
        )

        started_records = {
            record.node_id: record.payload.get("runtime_profile")
            for record in result.records
            if record.event_type == "node.started"
            and record.node_id in {"fixture_scaling.scale", "real_scaling.scale"}
        }
        self.assertEqual(
            started_records,
            {
                "fixture_scaling.scale": "fixture",
                "real_scaling.scale": "real",
            },
        )

    def test_verification_persists_runtime_profile_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report = run_verification(
                build_program(),
                runs_dir=root,
                environment=build_environment(),
            )

            self.assertTrue(report.success)
            summary = resolve_run_summary(
                runs_dir=root,
                graph_id="runtime_environment_demo",
            )
            self.assertIsNone(summary.runtime_default_profile_name)
            self.assertEqual(summary.runtime_profile_names, ("fixture", "real"))

            records = load_run_records(
                runs_dir=root,
                graph_id="runtime_environment_demo",
                node_id="fixture_scaling.scale",
                event_type="node.started",
            )
            payload = cast(dict[str, JsonValue], records[-1]["payload"])
            self.assertEqual(payload["runtime_profile"], "fixture")

    def test_missing_runtime_profile_fails_for_declared_resources(self) -> None:
        environment = RuntimeEnvironment(
            profiles={
                "fixture": RuntimeProfile(
                    name="fixture",
                    resources=cast(
                        dict[ResourceKey[object], object],
                        {MULTIPLIER_RESOURCE: Multiplier(2)},
                    ),
                )
            }
        )

        with self.assertRaises(MissingRuntimeProfileError):
            asyncio.run(AsyncExecutor(environment=environment).run(build_program()))
