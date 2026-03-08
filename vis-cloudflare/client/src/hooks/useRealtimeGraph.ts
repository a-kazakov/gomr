import { useState, useEffect, useRef } from 'react';
import { Node, Edge, useNodesState, useEdgesState, MarkerType } from 'reactflow';
import { getLayoutedElements } from '../utils/layout';
import { ServerResponse, ServerOperation, ServerCollection, ServerValue, CollectionNodeData, ValueNodeData, CustomNodeData } from '../types/pipeline';

/**
 * Custom hook that fetches live pipeline metrics from the backend and maps them
 * to React Flow nodes/edges without causing UI flickering.
 * 
 * @param jobId - The job ID to fetch metrics for
 * @returns React Flow nodes, edges, and change handlers
 */
export function useRealtimeGraph(jobId: string) {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [isLayoutReady, setIsLayoutReady] = useState(false);
  const [operations, setOperations] = useState<Record<string, ServerOperation>>({});
  const [collections, setCollections] = useState<Record<string, ServerCollection>>({});
  const [values, setValues] = useState<Record<string, ServerValue>>({});
  // Ref to track current nodes for edge layout recalculation
  const nodesRef = useRef<Node[]>([]);

  /**
   * Fetches pipeline metrics from the backend
   */
  const fetchMetrics = async (): Promise<ServerResponse | null> => {
    try {
      const response = await fetch(`/job/${jobId}`);
      if (!response.ok) {
        console.warn(`Failed to fetch metrics: ${response.status} ${response.statusText}`);
        return null;
      }
      const data: ServerResponse = await response.json();
      return data;
    } catch (error) {
      console.warn('Error fetching metrics:', error);
      return null;
    }
  };

  /**
   * Transforms server operations to React Flow nodes
   */
  const transformOperationsToNodes = (
    operations: Record<string, ServerOperation>,
    values: Record<string, ServerValue> = {}
  ): Node[] => {
    return Object.values(operations).map((op) => {
      const nodeData: CustomNodeData = { ...op };
      
      // For map_value operations, compute resolved input values count
      if (op.kind === 'map_value' && op.input_values) {
        const resolvedCount = op.input_values.filter((inputVal) => {
          const value = values[inputVal.id];
          return value?.is_resolved || false;
        }).length;
        nodeData.input_values_resolved_count = resolvedCount;
      }
      
      return {
        id: op.id,
        type: 'custom' as const,
        data: nodeData,
        position: { x: 0, y: 0 }, // Temporary, will be set by layout
      };
    });
  };

  /**
   * Transforms server collections to React Flow nodes
   */
  const transformCollectionsToNodes = (collections: Record<string, ServerCollection>): Node[] => {
    return Object.values(collections).map((coll) => ({
      id: coll.id,
      type: 'collection' as const,
      data: coll as CollectionNodeData,
      position: { x: 0, y: 0 }, // Temporary, will be set by layout
    }));
  };

  /**
   * Transforms server values to React Flow nodes
   */
  const transformValuesToNodes = (values: Record<string, ServerValue>): Node[] => {
    return Object.values(values).map((value) => ({
      id: value.id,
      type: 'value' as const,
      data: value as ValueNodeData,
      position: { x: 0, y: 0 }, // Temporary, will be set by layout
    }));
  };

  /**
   * Infers edges from operations, collections, and values.
   * 
   * With collections and values as first-class citizens:
   * 1. Create edges from operations to their output collections
   * 2. Create edges from collections to operations that consume them
   * 3. Create edges from operations to their output values
   * 4. Create edges from values to operations that consume them
   */
  const inferEdges = (
    operations: Record<string, ServerOperation>,
    collections: Record<string, ServerCollection>,
    values: Record<string, ServerValue>
  ): Edge[] => {
    const edges: Edge[] = [];

    // Create edges: operation -> collection (for outputs)
    Object.values(operations).forEach((op) => {
      op.output_collections.forEach((coll) => {
        const collection = collections[coll.id];
        if (collection) {
          const isHighPressure = collection.pressure > (collection.capacity || 1000) * 0.8;
          const edgeColor = isHighPressure ? 'var(--edge-highlight)' : 'var(--edge-default)';

          edges.push({
            id: `op-${op.id}-coll-${coll.id}`,
            source: op.id,
            target: coll.id,
            type: 'dagre',
            label: '',
            labelStyle: {
              fill: 'var(--bg-elevated)',
            },
            labelShowBg: true,
            labelBgStyle: {
              fill: edgeColor,
              stroke: edgeColor,
              strokeWidth: '0.1rem',
            },
            labelBgPadding: [4, 6],
            style: {
              strokeWidth: '0.1rem',
              stroke: edgeColor,
            },
            data: {},
          });
        }
      });
    });

    // Create edges: collection -> operation (for inputs)
    Object.values(operations).forEach((op) => {
      op.input_collections.forEach((coll) => {
        const collection = collections[coll.id];
        if (collection) {
          const isHighPressure = collection.pressure > (collection.capacity || 1000) * 0.8;
          const edgeColor = isHighPressure ? 'var(--edge-highlight)' : 'var(--edge-default)';

          edges.push({
            id: `coll-${coll.id}-op-${op.id}`,
            source: coll.id,
            target: op.id,
            type: 'dagre',
            label: '',
            labelStyle: {
              fill: 'var(--bg-elevated)',
            },
            labelShowBg: true,
            labelBgStyle: {
              fill: edgeColor,
              stroke: edgeColor,
              strokeWidth: '0.1rem',
            },
            labelBgPadding: [4, 6],
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: edgeColor,
            },
            style: {
              strokeWidth: '0.1rem',
              stroke: edgeColor,
            },
            data: {},
          });
        }
      });
    });

    // Create edges: operation -> value (for outputs)
    Object.values(operations).forEach((op) => {
      op.output_values.forEach((val) => {
        const value = values[val.id];
        if (value) {
          edges.push({
            id: `op-${op.id}-val-${val.id}`,
            source: op.id,
            target: val.id,
            type: 'dagre',
            label: '',
            style: {
              strokeWidth: '0.1rem',
              stroke: 'var(--edge-default)',
              strokeDasharray: '5,5',
            },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: 'var(--edge-default)',
            },
            data: {},
          });
        }
      });
    });

    // Create edges: value -> operation (for inputs)
    Object.values(operations).forEach((op) => {
      op.input_values.forEach((val) => {
        const value = values[val.id];
        if (value) {
          const isResolved = value.is_resolved;
          const edgeColor = isResolved ? 'var(--status-completed)' : 'var(--edge-default)';
          
          edges.push({
            id: `val-${val.id}-op-${op.id}`,
            source: val.id,
            target: op.id,
            type: 'dagre',
            label: '',
            style: {
              strokeWidth: '0.1rem',
              stroke: edgeColor,
              strokeDasharray: '5,5',
            },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: edgeColor,
            },
            data: {},
          });
        }
      });
    });

    return edges;
  };

  // Phase 1: Initial Load (The "Structure" Phase)
  // Run this ONCE when component mounts or jobId changes to set up the graph structure
  useEffect(() => {
    if (!jobId) return;

    let isMounted = true;

    const initializeGraph = async () => {
      const data = await fetchMetrics();
      if (!data || !isMounted) return;

      // Transform server data to React Flow format
            const operationNodes = transformOperationsToNodes(data.operations, data.values || {});
            const collectionNodes = transformCollectionsToNodes(data.collections);
            const valueNodes = transformValuesToNodes(data.values || {});
            const flowNodes = [...operationNodes, ...collectionNodes, ...valueNodes];
            const flowEdges = inferEdges(data.operations, data.collections, data.values || {});

      // Calculate layout (this is expensive, so we only do it once)
      const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(flowNodes, flowEdges);

      setNodes(layoutedNodes);
      setEdges(layoutedEdges);
      nodesRef.current = layoutedNodes; // Update ref
      setOperations(data.operations);
      setCollections(data.collections);
      setValues(data.values || {});
      setIsLayoutReady(true);
    };

    initializeGraph();

    return () => {
      isMounted = false;
    };
  }, [jobId, setNodes, setEdges]);

  // Phase 2: The Live Update Loop (The "Telemetry" Phase)
  // This runs every second and handles both data updates and structural changes
  useEffect(() => {
    if (!isLayoutReady || !jobId) return;

    const interval = setInterval(async () => {
      const data = await fetchMetrics();
      if (!data) return;

      // Update operations, collections, and values maps
      setOperations(data.operations);
      setCollections(data.collections);
      setValues(data.values || {});

      // Check if structure changed by comparing current state with fresh data
      // We need to check this before updating state to decide how to handle updates
      let structureChanged = false;
      let edgesToUpdate: Edge[] | null = null;

      setNodes((currentNodes) => {
        const currentNodeIds = new Set(currentNodes.map(n => n.id));
        const freshOperationIds = new Set(Object.keys(data.operations));
        const freshCollectionIds = new Set(Object.keys(data.collections));
        const freshValueIds = new Set(Object.keys(data.values || {}));
        const freshNodeIds = new Set([...freshOperationIds, ...freshCollectionIds, ...freshValueIds]);
        
        structureChanged = 
          currentNodeIds.size !== freshNodeIds.size ||
          Array.from(currentNodeIds).some(id => !freshNodeIds.has(id)) ||
          Array.from(freshNodeIds).some(id => !currentNodeIds.has(id));

        // If structure changed, rebuild nodes and recalculate layout
        if (structureChanged) {
              const operationNodes = transformOperationsToNodes(data.operations, data.values || {});
              const collectionNodes = transformCollectionsToNodes(data.collections);
              const valueNodes = transformValuesToNodes(data.values || {});
              const newNodes = [...operationNodes, ...collectionNodes, ...valueNodes];
              const newEdges = inferEdges(data.operations, data.collections, data.values || {});
          const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(newNodes, newEdges);
          
          // Store edges to update after nodes (to avoid nested state updates)
          edgesToUpdate = layoutedEdges;
          // Update ref
          nodesRef.current = layoutedNodes;
          
          return layoutedNodes;
        }

        // Otherwise, just update data for existing nodes
        const updatedNodes = currentNodes.map((node) => {
          // Check if it's an operation node
          const freshOperation = data.operations[node.id];
          if (freshOperation) {
            // For map_value operations, compute resolved input values count
            const nodeData: CustomNodeData = { ...freshOperation };
            if (freshOperation.kind === 'map_value' && freshOperation.input_values) {
              const resolvedCount = freshOperation.input_values.filter((inputVal) => {
                const value = (data.values || {})[inputVal.id];
                return value?.is_resolved || false;
              }).length;
              nodeData.input_values_resolved_count = resolvedCount;
            }
            
            return {
              ...node,
              data: nodeData,
            };
          }

          // Check if it's a collection node
          const freshCollection = data.collections[node.id];
          if (freshCollection) {
            return {
              ...node,
              data: { ...freshCollection },
            };
          }

          // Check if it's a value node
          const freshValue = data.values?.[node.id];
          if (freshValue) {
            return {
              ...node,
              data: { ...freshValue },
            };
          }

          // Node not found in fresh data, keep old state
          return node;
        });
        // Update ref
        nodesRef.current = updatedNodes;
        return updatedNodes;
      });

      // Update edges based on whether structure changed
      if (structureChanged && edgesToUpdate) {
        // Structure changed - use the edges we calculated above (already have dagre layout)
        setEdges(edgesToUpdate);
      } else {
        // Structure unchanged - check for edge-only changes and update data
        setEdges((currentEdges) => {
          const expectedEdges = inferEdges(data.operations, data.collections, data.values || {});
          const currentEdgeIds = new Set(currentEdges.map(e => e.id));
          const expectedEdgeIds = new Set(expectedEdges.map(e => e.id));
          
          const edgesChanged = 
            currentEdgeIds.size !== expectedEdgeIds.size ||
            Array.from(currentEdgeIds).some(id => !expectedEdgeIds.has(id)) ||
            Array.from(expectedEdgeIds).some(id => !currentEdgeIds.has(id));

          // If edges structure changed (but nodes didn't), rebuild them with layout
          if (edgesChanged) {
            // Recalculate layout for edges using current nodes from ref
            // Make sure we have valid nodes
            if (nodesRef.current.length === 0) {
              // Fallback: if ref is empty, return expected edges (will be fixed on next update)
              return expectedEdges;
            }
            const { edges: layoutedEdges } = getLayoutedElements(nodesRef.current, expectedEdges);
            // The layoutedEdges already have all properties from expectedEdges (via ...edge spread in layout.ts)
            // and the layout waypoints in data
            return layoutedEdges;
          }

          // Otherwise, just update existing edges with fresh data
          // Preserve all dagre properties (type, points, labelX, labelY)
          return currentEdges.map((edge) => {
            // Check if this is a value edge (dotted line)
            const sourceValue = data.values?.[edge.source];
            const targetValue = data.values?.[edge.target];
            const isValueEdge = sourceValue || targetValue;
            
            if (isValueEdge) {
              // Value edges: use green if source value is resolved, otherwise default
              const isResolved = sourceValue?.is_resolved || false;
              const edgeColor = isResolved ? 'var(--status-completed)' : 'var(--edge-default)';
              
              return {
                ...edge,
                type: edge.type || 'dagre',
                style: {
                  ...(edge.style || {}),
                  strokeWidth: '0.1rem',
                  stroke: edgeColor,
                  strokeDasharray: '5,5', // Dotted line
                },
                markerEnd: {
                  type: MarkerType.ArrowClosed,
                  color: edgeColor,
                },
                data: {
                  ...(edge.data || {}),
                },
              };
            }

            // Collection edges: update based on pressure
            const collectionId = data.collections[edge.source] ? edge.source : 
                                 data.collections[edge.target] ? edge.target : null;
            const freshCollection = collectionId ? data.collections[collectionId] : null;

            // If collection missing, keep old state
            if (!freshCollection) return edge;

            // Determine edge color based on pressure
            const isHighPressure = freshCollection.pressure > (freshCollection.capacity || 1000) * 0.8;
            const edgeColor = isHighPressure ? 'var(--edge-highlight)' : 'var(--edge-default)';

            // Update label and color, but preserve all dagre properties (type, points, labelX, labelY)
            return {
              ...edge,
              type: edge.type || 'dagre', // Preserve dagre type
              label: edge.label || '', // Preserve label
              labelStyle: {
                ...(edge.labelStyle || {}),
                fill: 'var(--bg-elevated)', // Text color - use background color for contrast
              },
              labelShowBg: edge.labelShowBg !== undefined ? edge.labelShowBg : true, // Preserve labelShowBg
              labelBgStyle: {
                ...(edge.labelBgStyle || {}),
                fill: edgeColor,
                stroke: edgeColor,
                strokeWidth: '0.1rem',
              },
              labelBgPadding: edge.labelBgPadding || [4, 6], // Preserve labelBgPadding
              // Remove arrowhead for edges leading into collections
              markerEnd: data.collections[edge.target] ? undefined : {
                type: MarkerType.ArrowClosed,
                color: edgeColor,
              },
              style: {
                ...(edge.style || {}),
                strokeWidth: '0.1rem',
                stroke: edgeColor,
              },
              data: {
                ...(edge.data || {}), // Preserve existing dagre data (points, labelX, labelY)
              },
            };
          });
        });
      }
    }, 1000);

    return () => clearInterval(interval);
  }, [isLayoutReady, jobId, setNodes, setEdges]);

  return { nodes, edges, onNodesChange, onEdgesChange, operations, collections, values };
}
