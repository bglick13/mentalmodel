from __future__ import annotations

import contextlib
import io
import unittest

from mentalmodel.cli import build_parser, main


class CliTest(unittest.TestCase):
    def test_demo_command_parses(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["demo", "async-rl"])
        self.assertEqual(args.command, "demo")
        self.assertEqual(args.name, "async-rl")

    def test_check_command_succeeds_for_demo_entrypoint(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "check",
                    "--entrypoint",
                    "mentalmodel.examples.async_rl.demo:build_program",
                ]
            )
        self.assertEqual(exit_code, 0)
        rendered = stdout.getvalue()
        self.assertIn("check summary:", rendered)
        self.assertIn("async_rl_demo", rendered)


if __name__ == "__main__":
    unittest.main()
