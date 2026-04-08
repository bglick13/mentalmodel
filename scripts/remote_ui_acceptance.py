#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path
from urllib import parse, request


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Launch one catalog entry from the dashboard UI with agent-browser and "
            "assert the run appears in /api/runs."
        )
    )
    parser.add_argument("--server-url", default="http://127.0.0.1:8765")
    parser.add_argument("--spec-id", required=True)
    parser.add_argument("--project-id")
    parser.add_argument("--graph-id", required=True)
    parser.add_argument("--invocation-name", required=True)
    parser.add_argument("--session-name", default="mentalmodel-e2e")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()

    projects_payload = _fetch_json(f"{args.server_url.rstrip('/')}/api/projects")
    if args.project_id is not None:
        _require_project_runs_dir(projects_payload, args.project_id)

    baseline = _run_ids(
        _fetch_json(
            _runs_url(
                server_url=args.server_url,
                graph_id=args.graph_id,
                invocation_name=args.invocation_name,
            )
        )
    )

    open_url = (
        f"{args.server_url.rstrip('/')}/?spec={parse.quote(args.spec_id, safe='')}#launch"
    )
    browser_prefix = ["agent-browser", "--session-name", args.session_name]
    if args.headed:
        browser_prefix.insert(1, "--headed")
    _run_browser(browser_prefix + ["open", open_url])
    _run_browser(browser_prefix + ["wait", "--load", "networkidle"])
    snapshot = _snapshot(browser_prefix)
    launch_ref = _find_ref(snapshot, role="button", name="Run again (selected catalog)")
    if launch_ref is None:
        raise RuntimeError("Could not find the Run again (selected catalog) button in Launch.")
    _run_browser(browser_prefix + ["click", f"@{launch_ref}"])
    _run_browser(browser_prefix + ["wait", "750"])

    deadline = time.time() + args.timeout_seconds
    while time.time() < deadline:
        payload = _fetch_json(
            _runs_url(
                server_url=args.server_url,
                graph_id=args.graph_id,
                invocation_name=args.invocation_name,
            )
        )
        current = _run_ids(payload)
        new_run_ids = [run_id for run_id in current if run_id not in baseline]
        if new_run_ids:
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "spec_id": args.spec_id,
                        "graph_id": args.graph_id,
                        "invocation_name": args.invocation_name,
                        "new_run_id": new_run_ids[0],
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        time.sleep(args.poll_seconds)

    screenshot_path = Path(tempfile.gettempdir()) / (
        f"mentalmodel-ui-acceptance-{int(time.time())}.png"
    )
    _run_browser(browser_prefix + ["screenshot", str(screenshot_path)])
    latest_payload = _fetch_json(
        _runs_url(
            server_url=args.server_url,
            graph_id=args.graph_id,
            invocation_name=args.invocation_name,
        )
    )
    print(
        json.dumps(
            {
                "status": "timeout",
                "spec_id": args.spec_id,
                "graph_id": args.graph_id,
                "invocation_name": args.invocation_name,
                "baseline_run_ids": sorted(baseline),
                "current_runs": latest_payload.get("runs", []),
                "screenshot": str(screenshot_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1


def _run_ids(payload: dict[str, object]) -> set[str]:
    runs = payload.get("runs", [])
    if not isinstance(runs, list):
        return set()
    run_ids: set[str] = set()
    for item in runs:
        if not isinstance(item, dict):
            continue
        run_id = item.get("run_id")
        if isinstance(run_id, str):
            run_ids.add(run_id)
    return run_ids


def _runs_url(*, server_url: str, graph_id: str, invocation_name: str) -> str:
    query = parse.urlencode(
        {
            "graph_id": graph_id,
            "invocation_name": invocation_name,
        }
    )
    return f"{server_url.rstrip('/')}/api/runs?{query}"


def _require_project_runs_dir(payload: dict[str, object], project_id: str) -> None:
    projects = payload.get("projects", [])
    if not isinstance(projects, list):
        raise RuntimeError("/api/projects returned an unexpected payload.")
    for item in projects:
        if not isinstance(item, dict):
            continue
        if item.get("project_id") != project_id:
            continue
        if item.get("runs_dir") in (None, ""):
            raise RuntimeError(
                f"Project {project_id!r} has no runs_dir in /api/projects. "
                "Fix the workspace routing before relying on UI launches."
            )
        return
    raise RuntimeError(f"Project {project_id!r} not found in /api/projects.")


def _snapshot(browser_prefix: list[str]) -> dict[str, object]:
    completed = subprocess.run(
        browser_prefix + ["snapshot", "-i", "--json"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    decoded = json.loads(completed.stdout)
    if not isinstance(decoded, dict):
        raise RuntimeError("agent-browser snapshot did not return a JSON object.")
    data = decoded.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("agent-browser snapshot payload was missing data.")
    return data


def _find_ref(
    snapshot: dict[str, object],
    *,
    role: str,
    name: str,
) -> str | None:
    refs = snapshot.get("refs")
    if not isinstance(refs, dict):
        return None
    for ref, payload in refs.items():
        if not isinstance(ref, str) or not isinstance(payload, dict):
            continue
        if payload.get("role") != role:
            continue
        if payload.get("name") != name:
            continue
        return ref
    return None


def _run_browser(command: list[str]) -> None:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            completed.stderr.strip()
            or completed.stdout.strip()
            or f"agent-browser failed: {' '.join(command)}"
        )


def _fetch_json(url: str) -> dict[str, object]:
    with request.urlopen(url) as response:
        decoded = json.loads(response.read().decode("utf-8"))
    if not isinstance(decoded, dict):
        raise RuntimeError(f"Expected JSON object from {url}.")
    return decoded


if __name__ == "__main__":
    raise SystemExit(main())
