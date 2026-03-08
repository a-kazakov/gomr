import { useState } from 'react';
import ReactFlow, { Controls, Node, EdgeTypes } from 'reactflow';
import { useRealtimeGraph } from './hooks/useRealtimeGraph';
import PipelineNode from './components/PipelineNode';
import CollectionNode from './components/CollectionNode';
import ValueNode from './components/ValueNode';
import DagreEdge from './components/DagreEdge';
import InfoPanel from './components/InfoPanel';
import { NodeData } from './types/pipeline';
import 'reactflow/dist/style.css';
import './App.css';

// Define outside component to prevent re-creation on every render
const nodeTypes = {
  custom: PipelineNode,
  collection: CollectionNode,
  value: ValueNode,
};

const edgeTypes: EdgeTypes = {
  dagre: DagreEdge,
};

/**
 * Main application component.
 * This is now a clean controller that just connects the wires:
 * - Uses the smart hook for data management
 * - Manages local UI state (selection)
 * - Renders the graph and panel
 */
export default function App() {
  // Extract jobId from URL (e.g., /job/01KFV7MMJ2HN0F4D1YNEWCN9QY)
  // For now, default to empty string - in production, extract from route
  const jobId = window.location.pathname.split('/').pop() || '';
  
  // Use our smart hook for data management
  const { nodes, edges, onNodesChange, onEdgesChange, operations, collections, values } = useRealtimeGraph(jobId);
  
  // Local UI state (selection)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  
  const selectedNode: Node<NodeData> | null = selectedNodeId 
    ? (nodes.find((n) => n.id === selectedNodeId) ?? null)
    : null;

  return (
    <div className="app-container">
      <div className="graph-panel">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={(_, node) => setSelectedNodeId(node.id)}
          onPaneClick={() => setSelectedNodeId(null)}
          nodesDraggable={false}
          fitView
        >
          <Controls />
        </ReactFlow>
      </div>

      <InfoPanel selectedNode={selectedNode} allOperations={operations} allCollections={collections} allValues={values} />
    </div>
  );
}
