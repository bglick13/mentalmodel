from __future__ import annotations

import json
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, TextIO, cast

from mentalmodel.core.interfaces import JsonValue
from mentalmodel.remote.workspace import ProjectRunTarget
from mentalmodel.ui.catalog import DashboardCatalogError

WORKER_EVENT_PREFIX = "MM_EVENT:"

WorkerEventKind = Literal["completion", "lifecycle", "message", "record"]


@dataclass(slots=True, frozen=True)
class WorkerExecutionEvent:
    """One typed event emitted by a project execution worker."""

    kind: WorkerEventKind
    payload: dict[str, JsonValue]


@dataclass(slots=True, frozen=True)
class WorkerExecutionResult:
    """Final completion payload returned by a project execution worker."""

    payload: dict[str, JsonValue]


class ProjectExecutionWorker(Protocol):
    """Execution boundary for runs launched in an owning project environment."""

    def execute(
        self,
        *,
        spec_path: Path,
        root_dir: Path,
        run_target: ProjectRunTarget,
        on_event: Callable[[WorkerExecutionEvent], None],
    ) -> WorkerExecutionResult:
        """Execute one verification and stream typed worker events."""


class SubprocessProjectExecutionWorker:
    """Run verification in the external project environment via ``uv run``."""

    def execute(
        self,
        *,
        spec_path: Path,
        root_dir: Path,
        run_target: ProjectRunTarget,
        on_event: Callable[[WorkerExecutionEvent], None],
    ) -> WorkerExecutionResult:
        runs_dir_arg = (
            "-"
            if run_target.runs_dir is None
            else str(run_target.runs_dir.expanduser().resolve())
        )
        command = [
            "uv",
            "run",
            "--directory",
            str(root_dir),
            "python",
            "-c",
            _EXTERNAL_VERIFY_WORKER_SCRIPT,
            str(spec_path),
            runs_dir_arg,
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        state = _WorkerState()
        stdout_thread = threading.Thread(
            target=_drain_stdout,
            args=(process.stdout, on_event, state),
            daemon=True,
            name=f"mentalmodel-worker-stdout-{spec_path.stem}",
        )
        stderr_thread = threading.Thread(
            target=_drain_stderr,
            args=(process.stderr, on_event, state),
            daemon=True,
            name=f"mentalmodel-worker-stderr-{spec_path.stem}",
        )
        stdout_thread.start()
        stderr_thread.start()
        returncode = process.wait()
        stdout_thread.join()
        stderr_thread.join()
        if returncode != 0:
            raise DashboardCatalogError(
                state.failure_message()
                or f"External project command failed with exit code {returncode}."
            )
        completion = state.completion_payload
        if completion is None:
            raise DashboardCatalogError(
                "External verification completed without a completion event."
            )
        return WorkerExecutionResult(payload=completion)


@dataclass(slots=True)
class _WorkerState:
    completion_payload: dict[str, JsonValue] | None = None
    stdout_lines: list[str] = field(default_factory=list)
    stderr_lines: list[str] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def append_stdout(self, line: str) -> None:
        with self._lock:
            self.stdout_lines.append(line)

    def append_stderr(self, line: str) -> None:
        with self._lock:
            self.stderr_lines.append(line)

    def set_completion(self, payload: dict[str, JsonValue]) -> None:
        with self._lock:
            self.completion_payload = payload

    def failure_message(self) -> str:
        with self._lock:
            return (
                "\n".join(self.stderr_lines[-10:])
                or "\n".join(self.stdout_lines[-10:])
            )


def _drain_stdout(
    pipe: object,
    on_event: Callable[[WorkerExecutionEvent], None],
    state: _WorkerState,
) -> None:
    stream = cast(TextIO | None, pipe)
    if stream is None:
        return
    try:
        for raw_line in stream:
            line = raw_line.rstrip()
            if not line:
                continue
            event = _decode_worker_event(line)
            if event is None:
                state.append_stdout(line)
                on_event(
                    WorkerExecutionEvent(
                        kind="message",
                        payload={
                            "level": "info",
                            "message": line,
                            "source": "external-process",
                        },
                    )
                )
                continue
            if event.kind == "completion":
                state.set_completion(event.payload)
            else:
                if event.kind == "message":
                    message = event.payload.get("message")
                    if isinstance(message, str):
                        state.append_stdout(message)
                on_event(event)
    finally:
        stream.close()


def _drain_stderr(
    pipe: object,
    on_event: Callable[[WorkerExecutionEvent], None],
    state: _WorkerState,
) -> None:
    stream = cast(TextIO | None, pipe)
    if stream is None:
        return
    try:
        for raw_line in stream:
            line = raw_line.rstrip()
            if not line:
                continue
            state.append_stderr(line)
            on_event(
                WorkerExecutionEvent(
                    kind="message",
                    payload={
                        "level": "error",
                        "message": line,
                        "source": "external-process",
                    },
                )
            )
    finally:
        stream.close()


def _decode_worker_event(line: str) -> WorkerExecutionEvent | None:
    if not line.startswith(WORKER_EVENT_PREFIX):
        return None
    raw_payload = line[len(WORKER_EVENT_PREFIX) :]
    try:
        decoded = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise DashboardCatalogError(
            f"Failed to decode external worker event: {exc.msg}"
        ) from exc
    if not isinstance(decoded, dict):
        raise DashboardCatalogError("External worker event must be a JSON object.")
    kind = decoded.get("kind")
    payload = decoded.get("payload")
    if kind not in {"completion", "lifecycle", "message", "record"}:
        raise DashboardCatalogError(f"Unsupported external worker event kind {kind!r}.")
    if not isinstance(payload, dict):
        raise DashboardCatalogError("External worker event payload must be a JSON object.")
    return WorkerExecutionEvent(
        kind=cast(WorkerEventKind, kind),
        payload=_coerce_json_object(payload),
    )


def _coerce_json_object(value: dict[object, object]) -> dict[str, JsonValue]:
    coerced: dict[str, JsonValue] = {}
    for key, item in value.items():
        coerced[str(key)] = _coerce_json_value(item)
    return coerced


def _coerce_json_value(value: object) -> JsonValue:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_coerce_json_value(item) for item in value]
    if isinstance(value, dict):
        return _coerce_json_object(value)
    raise DashboardCatalogError(
        f"Unsupported external worker JSON value type {type(value).__name__}."
    )


_EXTERNAL_VERIFY_WORKER_SCRIPT = """
import json
import sys
from pathlib import Path

from mentalmodel.invocation import (
    load_runtime_environment_subject,
    load_workflow_subject,
    read_verify_invocation_spec,
)
from mentalmodel.observability.export import execution_record_to_json
from mentalmodel.testing import run_verification

EVENT_PREFIX = "MM_EVENT:"


def emit(kind: str, payload: object) -> None:
    print(EVENT_PREFIX + json.dumps({"kind": kind, "payload": payload}), flush=True)


spec_path = Path(sys.argv[1])
runs_dir_arg = sys.argv[2]
runs_dir = None if runs_dir_arg == "-" else Path(runs_dir_arg)
emit(
    "lifecycle",
    {
        "status": "starting",
        "spec_path": str(spec_path),
        "runs_dir": None if runs_dir is None else str(runs_dir),
    },
)
invocation = read_verify_invocation_spec(spec_path)
module, program = load_workflow_subject(invocation.program)
environment = None
if invocation.environment is not None:
    _, environment = load_runtime_environment_subject(invocation.environment)


def on_record(record: object) -> None:
    emit("record", execution_record_to_json(record))


report = run_verification(
    program,
    module=module,
    runs_dir=runs_dir or invocation.runs_dir,
    environment=environment,
    invocation_name=invocation.invocation_name,
    record_listeners=(on_record,),
)
emit("completion", report.as_dict())
""".strip()
