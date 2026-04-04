"""IR exports."""

from mentalmodel.ir.graph import IREdge, IRFragment, IRGraph, IRNode
from mentalmodel.ir.lowering import LoweringContext, lower_program
from mentalmodel.ir.provenance import NodeProvenance

__all__ = [
    "IREdge",
    "IRFragment",
    "IRGraph",
    "IRNode",
    "LoweringContext",
    "NodeProvenance",
    "lower_program",
]
