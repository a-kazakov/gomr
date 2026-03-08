import dagre from 'dagre';
import { Node, Edge } from 'reactflow';

/**
 * Calculates node positions and edge waypoints using dagre layout algorithm.
 * 
 * @param nodes - Array of React Flow nodes (with IDs and temporary positions)
 * @param edges - Array of React Flow edges (for connection topology)
 * @param direction - Layout direction: 'TB' (top-bottom), 'LR' (left-right), etc.
 * @returns Object with layouted nodes and edges (edges include waypoints and label positions)
 */
export const getLayoutedElements = (
  nodes: Node[], 
  edges: Edge[], 
  direction: 'TB' | 'LR' | 'BT' | 'RL' = 'TB'
): { nodes: Node[]; edges: Edge[] } => {
  const dagreGraph = new dagre.graphlib.Graph();
  
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  dagreGraph.setGraph({ 
    rankdir: direction, 
    nodesep: 50,  // Horizontal spacing between nodes
    ranksep: 25, // Vertical spacing between ranks
  });

  // Register all nodes with their dimensions
  // Operation nodes: 180px width, 80px height
  // Collection nodes: 107px width, 60px height
  // Value nodes: 120px width, 50px height
  nodes.forEach((node) => {
    if (node.type === 'collection') {
      dagreGraph.setNode(node.id, { width: 107, height: 60 });
    } else if (node.type === 'value') {
      dagreGraph.setNode(node.id, { width: 120, height: 50 });
    } else {
      dagreGraph.setNode(node.id, { width: 180, height: 80 });
    }
  });

  // Add edges directly (no dummy nodes)
  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  // Calculate layout
  dagre.layout(dagreGraph);

  // Map dagre positions back to React Flow nodes
  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    const isCollection = node.type === 'collection';
    const isValue = node.type === 'value';
    return {
      ...node,
      position: {
        // Shift x/y to center the node based on its width/height
        // dagre gives us the center position, so we subtract half the dimensions
        x: nodeWithPosition.x - (isCollection ? 53.5 : isValue ? 60 : 90),  // 107/2, 120/2, or 180/2
        y: nodeWithPosition.y - (isCollection ? 30 : isValue ? 25 : 40),  // 60/2, 50/2, or 80/2
      },
    };
  });

  // Reconstruct edges with waypoints and label positions
  const layoutedEdges = edges.map((edge) => {
    try {
      const dagreEdge = dagreGraph.edge(edge.source, edge.target);
      const points = dagreEdge.points || [];
      
      // Calculate label position (middle of the edge)
      let labelX: number;
      let labelY: number;
      if (points.length > 0) {
        const midIndex = Math.floor(points.length / 2);
        labelX = points[midIndex].x;
        labelY = points[midIndex].y;
      } else {
        // Fallback: use midpoint between source and target node centers
        const sourceNode = dagreGraph.node(edge.source);
        const targetNode = dagreGraph.node(edge.target);
        labelX = (sourceNode.x + targetNode.x) / 2;
        labelY = (sourceNode.y + targetNode.y) / 2;
      }

      return {
        ...edge,
        type: 'dagre',
        data: {
          ...(edge.data || {}),
          points,
          labelX,
          labelY,
        },
      };
    } catch (error) {
      // Fallback: return original edge with empty points
      return {
        ...edge,
        type: 'dagre',
        data: {
          ...(edge.data || {}),
          points: [],
        },
      };
    }
  });

  return { nodes: layoutedNodes, edges: layoutedEdges };
};
