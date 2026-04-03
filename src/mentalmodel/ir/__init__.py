"""IR exports."""

from mentalmodel.ir.graph import IREdge, IRFragment, IRGraph, IRNode
from mentalmodel.ir.lowering import LoweringContext, lower_program

__all__ = [
    "IREdge",
    "IRFragment",
    "IRGraph",
    "IRNode",
    "LoweringContext",
    "lower_program",
]
