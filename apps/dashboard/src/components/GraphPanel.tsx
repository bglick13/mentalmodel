import { useMemo } from "react";
import dagre from "dagre";
import {
  Background,
  Controls,
  Edge,
  MarkerType,
  Node,
  ReactFlow,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import type { GraphPayload, ReplayNodeSummary } from "../types";

type GraphPanelProps = {
  graph: GraphPayload;
  nodeSummaries: ReplayNodeSummary[];
  selectedNodeId: string | null;
  onSelectNode: (nodeId: string) => void;
};

type NodeStatus = "idle" | "pass" | "warning" | "fail" | "running";

function statusForNode(nodeId: string, summaries: ReplayNodeSummary[]): NodeStatus {
  const matching = summaries.filter((summary) => summary.node_id === nodeId);
  if (matching.length === 0) {
    return "idle";
  }
  if (matching.some((summary) => summary.failed)) {
    return "fail";
  }
  if (
    matching.some(
      (summary) =>
        summary.invariant_status === "fail" && summary.invariant_severity === "warning",
    )
  ) {
    return "warning";
  }
  if (
    matching.some(
      (summary) => summary.last_event_type === "node.started" && !summary.succeeded,
    )
  ) {
    return "running";
  }
  if (matching.some((summary) => summary.invariant_status === "pass")) {
    return "pass";
  }
  return "pass";
}

function layoutGraph(graph: GraphPayload, summaries: ReplayNodeSummary[]) {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({
    rankdir: "LR",
    ranksep: 80,
    nodesep: 40,
  });
  graph.nodes.forEach((node) => {
    dagreGraph.setNode(node.node_id, { width: 210, height: 84 });
  });
  graph.edges
    .filter((edge) => edge.kind !== "contains")
    .forEach((edge) => {
      dagreGraph.setEdge(edge.source_node_id, edge.target_node_id);
    });
  dagre.layout(dagreGraph);

  const nodes: Node[] = graph.nodes.map((node) => {
    const positioned = dagreGraph.node(node.node_id);
    const status = statusForNode(node.node_id, summaries);
    const runtimeContext = node.metadata.runtime_context;
    const tone =
      status === "fail"
        ? "#ffb4ab"
        : status === "warning"
          ? "#ffcf86"
          : status === "running"
            ? "#8ed5ff"
            : status === "pass"
              ? "#56e5a9"
              : "#8d9ab9";
    return {
      id: node.node_id,
      type: "default",
      position: {
        x: positioned?.x ?? 0,
        y: positioned?.y ?? 0,
      },
      data: {
        label: (
          <div style={{ display: "grid", gap: 6 }}>
            <div style={{ fontWeight: 700 }}>{node.label}</div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              <span className="chip">{node.kind}</span>
              {runtimeContext ? <span className="chip">{runtimeContext}</span> : null}
              <span className="chip" style={{ borderColor: tone, color: tone }}>
                {status}
              </span>
            </div>
          </div>
        ),
      },
      style: {
        width: 210,
        borderRadius: 8,
        border: "none",
        padding: 12,
        background:
          "linear-gradient(180deg, rgba(45,52,73,0.68) 0%, rgba(23,31,51,0.94) 100%)",
        color: "#dae2fd",
        boxShadow: `inset 0 0 0 1px ${tone}33, 0 8px 32px rgba(6, 14, 32, 0.42)`,
      },
    };
  });

  const edges: Edge[] = graph.edges
    .filter((edge) => edge.kind !== "contains")
    .map((edge) => ({
      id: edge.edge_id,
      source: edge.source_node_id,
      target: edge.target_node_id,
      label: edge.kind === "data" ? "" : edge.kind,
      type: "smoothstep",
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 18,
        height: 18,
        color: "#475569",
      },
      style: {
        stroke: "#475569",
        strokeWidth: 1.6,
      },
    }));

  return { nodes, edges };
}

export function GraphPanel({
  graph,
  nodeSummaries,
  selectedNodeId,
  onSelectNode,
}: GraphPanelProps) {
  const { nodes, edges } = useMemo(
    () => layoutGraph(graph, nodeSummaries),
    [graph, nodeSummaries],
  );

  return (
    <div className="panel graph-panel">
      <div className="panel-header">
        <div>
          <div className="panel-title">Graph Explorer</div>
          <div className="panel-subtitle">
            Click any node to inspect its inputs, outputs, and invariant trace.
          </div>
        </div>
      </div>
      <div className="graph-canvas">
        <ReactFlow
          nodes={nodes.map((node) => ({
            ...node,
            selected: node.id === selectedNodeId,
          }))}
          edges={edges}
          fitView
          fitViewOptions={{ padding: 0.14 }}
          onNodeClick={(_, node) => onSelectNode(node.id)}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable
        >
          <Background color="#1e293b" gap={18} />
          <Controls />
        </ReactFlow>
      </div>
    </div>
  );
}
