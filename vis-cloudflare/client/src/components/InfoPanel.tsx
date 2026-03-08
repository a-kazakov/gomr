import { useState } from 'react';
import { Node } from 'reactflow';
import { CustomNodeData, CollectionNodeData, ValueNodeData, NodeData, ServerOperation, ServerCollection, ServerValue } from '../types/pipeline';
import '../App.css';

interface InfoPanelProps {
  selectedNode: Node<NodeData> | null;
  allOperations: Record<string, ServerOperation>;
  allCollections?: Record<string, ServerCollection>; // Optional, not currently used but available for future use
  allValues?: Record<string, ServerValue>; // Optional, not currently used but available for future use
}

/**
 * Formats a number with commas (e.g., 1234567 -> "1,234,567")
 */
function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
}

/**
 * Formats bytes to KB/MB/GB (e.g., 1024 -> "1.00 KB", 1048576 -> "1.00 MB")
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  } else if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(2)} KB`;
  } else if (bytes < 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  } else {
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  }
}

/**
 * Formats speed/rate (e.g., 120 -> "120/s")
 */
function formatSpeed(num: number): string {
  return `${formatNumber(Math.round(num))}/s`;
}

/**
 * Formats a value with its speed in parentheses (e.g., 1050, 45 -> "1,050 (45/s)")
 */
function formatValueWithSpeed(value: number, speed: number | undefined): string {
  const formattedValue = formatNumber(value);
  if (speed !== undefined && speed > 0) {
    return `${formattedValue} (${formatSpeed(speed)})`;
  }
  return formattedValue;
}

/**
 * Gets the status display text and class
 */
function getStatusInfo(phase: string): { text: string; class: string } {
  if (!phase || phase === '') {
    return { text: 'Pending', class: 'status-pending' };
  }
  return {
    text: phase.charAt(0).toUpperCase() + phase.slice(1),
    class: `status-${phase}`,
  };
}

/**
 * Finds the operation name for a collection ID.
 * For input collections: finds the operation that outputs to this collection.
 * For output collections: finds the operation that inputs from this collection.
 */
function getOperationNameForCollection(
  collectionId: string,
  allOperations: Record<string, ServerOperation>,
  isInput: boolean
): string {
  for (const op of Object.values(allOperations)) {
    if (isInput) {
      // For inputs, find the operation that outputs to this collection
      if (op.output_collections.some(c => c.id === collectionId)) {
        return op.name;
      }
    } else {
      // For outputs, find the operation that inputs from this collection
      if (op.input_collections.some(c => c.id === collectionId)) {
        return op.name;
      }
    }
  }
  // Fallback: return collection ID if no operation found (shouldn't happen in valid pipelines)
  return collectionId;
}

/**
 * Professional side panel that displays detailed information about the selected pipeline node.
 * Google Dataflow-style layout with clear visual hierarchy.
 */
export default function InfoPanel({ selectedNode, allOperations, allCollections: _allCollections, allValues: _allValues }: InfoPanelProps) {
  const [isCollapsed, setIsCollapsed] = useState(false);

  return (
    <div className={`info-panel ${isCollapsed ? 'collapsed' : ''}`}>
      <button 
        className="collapse-button"
        onClick={() => setIsCollapsed(!isCollapsed)}
        aria-label={isCollapsed ? 'Expand panel' : 'Collapse panel'}
      >
        {isCollapsed ? '◀' : '▶'}
      </button>
      
      {!isCollapsed && (
        <div className="info-content">
          {selectedNode ? (
            selectedNode.type === 'collection' ? (
              <CollectionDetailsPanel 
                nodeData={selectedNode.data as CollectionNodeData} 
                allOperations={allOperations}
              />
            ) : selectedNode.type === 'value' ? (
              <ValueDetailsPanel 
                nodeData={selectedNode.data as ValueNodeData} 
                allOperations={allOperations}
              />
            ) : (
              <NodeDetailsPanel 
                nodeData={selectedNode.data as CustomNodeData} 
                allOperations={allOperations} 
              />
            )
          ) : (
            <div className="pipeline-info">
              <h2>Pipeline Information</h2>
              <div className="info-section">
                <p className="placeholder">Select a node to view detailed information</p>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * Detailed view for a selected node
 */
function NodeDetailsPanel({ 
  nodeData, 
  allOperations 
}: { 
  nodeData: CustomNodeData;
  allOperations: Record<string, ServerOperation>;
}) {
  const statusInfo = getStatusInfo(nodeData.phase || '');

  return (
    <div className="node-info">
      {/* Header Section */}
      <div className="info-header">
        <h2 className="info-title">{nodeData.name}</h2>
        <div className="info-header-badges">
          <span className={`info-badge info-badge-type`}>{nodeData.kind.toUpperCase()}</span>
          <span className={`info-badge info-badge-status ${statusInfo.class}`}>
            {statusInfo.text}
          </span>
        </div>
      </div>

      {/* Inputs Section */}
      {nodeData.input_collections && nodeData.input_collections.length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">Inputs</h3>
          {nodeData.input_collections.map((collection, index) => {
            const elements = collection.elements_consumed;
            const elementsSpeed = nodeData.elements_consumed_speed?.[index];
            const batches = collection.batches_consumed;
            const batchesSpeed = nodeData.batches_consumed_speed?.[index];
            const operationName = getOperationNameForCollection(collection.id, allOperations, true);

            return (
              <div key={collection.id} className="info-collection">
                <h4 className="info-collection-name">{operationName}</h4>
                <div className="info-metrics-grid">
                  <div className="info-metric">
                    <span className="info-metric-label">Elements</span>
                    <span className="info-metric-value">
                      {formatValueWithSpeed(elements, elementsSpeed)}
                    </span>
                  </div>
                  <div className="info-metric">
                    <span className="info-metric-label">Batches</span>
                    <span className="info-metric-value">
                      {formatValueWithSpeed(batches, batchesSpeed)}
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Outputs Section */}
      {nodeData.output_collections && nodeData.output_collections.length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">Outputs</h3>
          {nodeData.output_collections.map((collection, index) => {
            const elements = collection.elements_produced;
            const elementsSpeed = nodeData.elements_produced_speed?.[index];
            const batches = collection.batches_produced;
            const batchesSpeed = nodeData.batches_produced_speed?.[index];
            const operationName = getOperationNameForCollection(collection.id, allOperations, false);

            return (
              <div key={collection.id} className="info-collection">
                <h4 className="info-collection-name">{operationName}</h4>
                <div className="info-metrics-grid">
                  <div className="info-metric">
                    <span className="info-metric-label">Elements</span>
                    <span className="info-metric-value">
                      {formatValueWithSpeed(elements, elementsSpeed)}
                    </span>
                  </div>
                  <div className="info-metric">
                    <span className="info-metric-label">Batches</span>
                    <span className="info-metric-value">
                      {formatValueWithSpeed(batches, batchesSpeed)}
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Shuffle Diagnostics Section (conditional) */}
      {nodeData.shuffle && (nodeData.shuffle.disk_usage > 0 || nodeData.shuffle.spills_count > 0) && (
        <div className="info-section">
          <h3 className="info-section-title">Shuffle Diagnostics</h3>
          <div className="info-metrics-grid">
            <div className="info-metric">
              <span className="info-metric-label">Disk Usage</span>
              <span className="info-metric-value">
                {formatBytes(nodeData.shuffle.disk_usage)}
                {nodeData.shuffle_disk_usage_speed !== undefined && nodeData.shuffle_disk_usage_speed > 0 && (
                  <span className="info-metric-speed">
                    {' '}({formatSpeed(nodeData.shuffle_disk_usage_speed)})
                  </span>
                )}
              </span>
            </div>
            <div className="info-metric">
              <span className="info-metric-label">Spills</span>
              <span className="info-metric-value">
                {formatValueWithSpeed(nodeData.shuffle.spills_count, nodeData.shuffle_spills_count_speed)}
              </span>
            </div>
            <div className="info-metric">
              <span className="info-metric-label">Elements Gathered</span>
              <span className="info-metric-value">
                {formatValueWithSpeed(nodeData.shuffle.elements_gathered, nodeData.elements_gathered_speed)}
              </span>
            </div>
            <div className="info-metric">
              <span className="info-metric-label">Groups Gathered</span>
              <span className="info-metric-value">
                {formatValueWithSpeed(nodeData.shuffle.groups_gathered, nodeData.groups_gathered_speed)}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* User Counters Section */}
      {nodeData.user_counters && Object.keys(nodeData.user_counters).length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">User Counters</h3>
          <div className="info-counters-list">
            {Object.entries(nodeData.user_counters)
              .sort(([keyA], [keyB]) => keyA.localeCompare(keyB))
              .map(([key, value]) => (
                <div key={key} className="info-counter">
                  <span className="info-counter-key">{key}</span>
                  <span className="info-counter-value">{formatNumber(value)}</span>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Phase History Section */}
      {nodeData.phases && nodeData.phases.length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">Phase History</h3>
          <div className="info-phase-history">
            {nodeData.phases.map((phaseEntry, index) => {
              const timestamp = new Date(phaseEntry.switch_timestamp);
              return (
                <div key={index} className="info-phase-entry">
                  <div className="info-phase-name">
                    {phaseEntry.phase_name || 'pending'}
                  </div>
                  <div className="info-phase-timestamp">
                    {timestamp.toLocaleString()}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Additional Info */}
      <div className="info-section">
        <h3 className="info-section-title">Configuration</h3>
        <div className="info-metrics-grid">
          <div className="info-metric">
            <span className="info-metric-label">Parallelism</span>
            <span className="info-metric-value">{nodeData.parallelism}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * Detailed view for a selected collection node
 */
function CollectionDetailsPanel({ 
  nodeData, 
  allOperations 
}: { 
  nodeData: CollectionNodeData;
  allOperations: Record<string, ServerOperation>;
}) {
  const pressurePercent = nodeData.capacity > 0 
    ? Math.min(100, (nodeData.pressure / nodeData.capacity) * 100) 
    : 0;
  const isHighPressure = pressurePercent > 80;
  const isCompleted = nodeData.completed && nodeData.pressure === 0;

  // Determine status badge
  let statusBadge: { text: string; class: string };
  if (isCompleted) {
    statusBadge = { text: 'COMPLETED', class: 'completed' };
  } else if (isHighPressure) {
    statusBadge = { text: 'HIGH PRESSURE', class: 'high-pressure' };
  } else {
    statusBadge = { text: 'LOW PRESSURE', class: 'low-pressure' };
  }

  // Find operations that produce to this collection
  const producerOps = Object.values(allOperations).filter(op =>
    op.output_collections.some(coll => coll.id === nodeData.id)
  );

  // Find operations that consume from this collection
  const consumerOps = Object.values(allOperations).filter(op =>
    op.input_collections.some(coll => coll.id === nodeData.id)
  );

  return (
    <div className="node-info">
      {/* Header Section */}
      <div className="info-header">
        <h2 className="info-title">{nodeData.name}</h2>
        <div className="info-header-badges">
          <span className={`info-badge info-badge-type`}>COLLECTION</span>
          <span className={`info-badge ${statusBadge.class}`}>
            {statusBadge.text}
          </span>
        </div>
      </div>

      {/* Pressure Section */}
      <div className="info-section">
        <h3 className="info-section-title">Pressure</h3>
        <div className="info-metrics-grid">
          <div className="info-metric">
            <span className="info-metric-label">Current</span>
            <span className="info-metric-value">{formatNumber(nodeData.pressure)}</span>
          </div>
          <div className="info-metric">
            <span className="info-metric-label">Capacity</span>
            <span className="info-metric-value">{formatNumber(nodeData.capacity)}</span>
          </div>
        </div>
        <div style={{ marginTop: '12px' }}>
          <div className="collection-node-pressure-bar-container" style={{ height: '12px' }}>
            <div 
              className="collection-node-pressure-bar"
              style={{
                width: `${pressurePercent}%`,
                backgroundColor: isHighPressure ? 'var(--edge-highlight)' : 'var(--edge-default)',
                height: '100%',
              }}
            />
          </div>
          <div style={{ marginTop: '4px', fontSize: '11px', color: 'var(--text-secondary)' }}>
            {pressurePercent.toFixed(1)}% full
          </div>
        </div>
      </div>

      {/* Configuration Section */}
      <div className="info-section">
        <h3 className="info-section-title">Configuration</h3>
        <div className="info-metrics-grid">
          <div className="info-metric">
            <span className="info-metric-label">Batch Size</span>
            <span className="info-metric-value">{formatNumber(nodeData.batch_size)}</span>
          </div>
        </div>
      </div>

      {/* User Counters Section */}
      {nodeData.user_counters && Object.keys(nodeData.user_counters).length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">User Counters</h3>
          <div className="info-counters-list">
            {Object.entries(nodeData.user_counters)
              .sort(([keyA], [keyB]) => keyA.localeCompare(keyB))
              .map(([key, value]) => (
                <div key={key} className="info-counter">
                  <span className="info-counter-key">{key}</span>
                  <span className="info-counter-value">{formatNumber(value)}</span>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Producers Section */}
      {producerOps.length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">Producers</h3>
          {producerOps.map((op) => (
            <div key={op.id} className="info-collection">
              <h4 className="info-collection-name">{op.name}</h4>
              <div className="info-metrics-grid">
                <div className="info-metric">
                  <span className="info-metric-label">Kind</span>
                  <span className="info-metric-value">{op.kind.toUpperCase()}</span>
                </div>
                <div className="info-metric">
                  <span className="info-metric-label">Phase</span>
                  <span className="info-metric-value">{op.phase || 'pending'}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Consumers Section */}
      {consumerOps.length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">Consumers</h3>
          {consumerOps.map((op) => (
            <div key={op.id} className="info-collection">
              <h4 className="info-collection-name">{op.name}</h4>
              <div className="info-metrics-grid">
                <div className="info-metric">
                  <span className="info-metric-label">Kind</span>
                  <span className="info-metric-value">{op.kind.toUpperCase()}</span>
                </div>
                <div className="info-metric">
                  <span className="info-metric-label">Phase</span>
                  <span className="info-metric-value">{op.phase || 'pending'}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * Detailed view for a selected value node
 */
function ValueDetailsPanel({ 
  nodeData, 
  allOperations 
}: { 
  nodeData: ValueNodeData;
  allOperations: Record<string, ServerOperation>;
}) {
  // Find operations that produce this value
  const producerOps = Object.values(allOperations).filter(op =>
    op.output_values.some(val => val.id === nodeData.id)
  );

  // Find operations that consume this value
  const consumerOps = Object.values(allOperations).filter(op =>
    op.input_values.some(val => val.id === nodeData.id)
  );

  return (
    <div className="node-info">
      {/* Header Section */}
      <div className="info-header">
        <h2 className="info-title">{nodeData.name}</h2>
        <div className="info-header-badges">
          <span className={`info-badge info-badge-type`}>VALUE</span>
          <span className={`info-badge ${nodeData.is_resolved ? 'resolved' : 'pending'}`}>
            {nodeData.is_resolved ? 'RESOLVED' : 'PENDING'}
          </span>
        </div>
      </div>

      {/* Status Section */}
      <div className="info-section">
        <h3 className="info-section-title">Status</h3>
        <div className="info-metrics-grid">
          <div className="info-metric">
            <span className="info-metric-label">State</span>
            <span className={`info-metric-value ${nodeData.is_resolved ? 'resolved' : 'pending'}`}>
              {nodeData.is_resolved ? 'RESOLVED' : 'PENDING'}
            </span>
          </div>
        </div>
      </div>

      {/* User Counters Section */}
      {nodeData.user_counters && Object.keys(nodeData.user_counters).length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">User Counters</h3>
          <div className="info-counters-list">
            {Object.entries(nodeData.user_counters)
              .sort(([keyA], [keyB]) => keyA.localeCompare(keyB))
              .map(([key, value]) => (
                <div key={key} className="info-counter">
                  <span className="info-counter-key">{key}</span>
                  <span className="info-counter-value">{formatNumber(value)}</span>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Producers Section */}
      {producerOps.length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">Producers</h3>
          {producerOps.map((op) => (
            <div key={op.id} className="info-collection">
              <h4 className="info-collection-name">{op.name}</h4>
              <div className="info-metrics-grid">
                <div className="info-metric">
                  <span className="info-metric-label">Kind</span>
                  <span className="info-metric-value">{op.kind.toUpperCase()}</span>
                </div>
                <div className="info-metric">
                  <span className="info-metric-label">Phase</span>
                  <span className="info-metric-value">{op.phase || 'pending'}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Consumers Section */}
      {consumerOps.length > 0 && (
        <div className="info-section">
          <h3 className="info-section-title">Consumers</h3>
          {consumerOps.map((op) => (
            <div key={op.id} className="info-collection">
              <h4 className="info-collection-name">{op.name}</h4>
              <div className="info-metrics-grid">
                <div className="info-metric">
                  <span className="info-metric-label">Kind</span>
                  <span className="info-metric-value">{op.kind.toUpperCase()}</span>
                </div>
                <div className="info-metric">
                  <span className="info-metric-label">Phase</span>
                  <span className="info-metric-value">{op.phase || 'pending'}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
