"""Runtime exports."""

from mentalmodel.runtime.errors import ExecutionError, InvariantViolationError
from mentalmodel.runtime.execution import ExecutionNodeMetadata
from mentalmodel.runtime.executor import AsyncExecutor, ExecutionResult
from mentalmodel.runtime.frame import ExecutionFrame, ExecutionFrameSegment
from mentalmodel.runtime.plan import (
    CompiledProgram,
    ExecutionPlan,
    compile_program,
)
from mentalmodel.runtime.recorder import ExecutionRecorder
from mentalmodel.runtime.replay import (
    InvariantDiff,
    NodeDiff,
    ReplayEvent,
    ReplayNodeSummary,
    ReplayReport,
    RunDiff,
    build_replay_report,
    build_run_diff,
)

__all__ = [
    "AsyncExecutor",
    "CompiledProgram",
    "ExecutionError",
    "ExecutionFrame",
    "ExecutionFrameSegment",
    "ExecutionNodeMetadata",
    "ExecutionPlan",
    "ExecutionRecorder",
    "ExecutionResult",
    "InvariantDiff",
    "InvariantViolationError",
    "NodeDiff",
    "ReplayEvent",
    "ReplayNodeSummary",
    "ReplayReport",
    "RunDiff",
    "build_replay_report",
    "build_run_diff",
    "compile_program",
]
