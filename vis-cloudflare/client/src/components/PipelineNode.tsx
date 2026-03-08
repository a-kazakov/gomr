import { Handle, Position, NodeProps } from 'reactflow';
import { CustomNodeData } from '../types/pipeline';
import '../App.css';

/**
 * Formats a number with appropriate suffix (K/M/B) or no suffix.
 * Examples: 123 -> "123", 123.45 -> "123.45", 1234 -> "1.23K", 1234567 -> "1.23M", 1234567890 -> "1.23B"
 * For values < 1K, shows up to 2 decimal places if needed, otherwise shows as integer.
 */
function formatNumber(value: number): string {
  if (value < 1000) {
    // For values < 1K, show up to 2 decimal places if not a whole number
    if (Number.isInteger(value)) {
      return value.toString();
    } else {
      // Round to 2 decimal places and remove trailing zeros
      return parseFloat(value.toFixed(2)).toString();
    }
  } else if (value < 1_000_000) {
    return `${(value / 1000).toFixed(2)}K`;
  } else if (value < 1_000_000_000) {
    return `${(value / 1_000_000).toFixed(2)}M`;
  } else {
    return `${(value / 1_000_000_000).toFixed(2)}B`;
  }
}

/**
 * Custom node component for pipeline visualization.
 * Renders a node with title, operation type badge, and status-based styling.
 */
export default function PipelineNode({ data }: NodeProps<CustomNodeData>) {
  // Normalize phase: empty string from Go backend represents "pending"
  const phaseClass = data.phase || 'pending';
  
  // Check if there are inbound/outbound connections
  const hasInputs = data.input_collections && data.input_collections.length > 0;
  const hasOutputs = data.output_collections && data.output_collections.length > 0;
  const isMapValue = data.kind === 'map_value';
  const hasInputValues = isMapValue && data.input_values && data.input_values.length > 0;
  
  // Sum elements consumed and produced across all collections
  const totalConsumed = data.input_collections?.reduce((sum: number, coll) => sum + coll.elements_consumed, 0) || 0;
  const totalProduced = data.output_collections?.reduce((sum: number, coll) => sum + coll.elements_produced, 0) || 0;
  
  return (
    <div className={`custom-node status-${phaseClass}`}>
      {/* Invisible handles for React Flow to calculate edge connection points */}
      <Handle type="target" position={Position.Top} style={{ opacity: 0 }} />
      
      <div className="custom-node-header">
        <div className="custom-node-title">{data.name}</div>
        <div className="custom-node-type">{data.kind.replace(/_/g, ' ')}</div>
      </div>
      
      {(hasInputs || hasOutputs || hasInputValues) && (
        <div className="custom-node-content">
          {hasInputValues && (
            <div className="custom-node-metric-row">
              <span className="custom-node-metric-label">IN:</span>
              <span className="custom-node-metric-value">
                {data.input_values_resolved_count ?? 0}/{data.input_values?.length ?? 0}
              </span>
            </div>
          )}
          {hasInputs && (
            <div className="custom-node-metric-row">
              <span className="custom-node-metric-label">IN:</span>
              <span className="custom-node-metric-value">{formatNumber(totalConsumed)}</span>
              <span className="custom-node-metric-speed">
                {data.elements_consumed_speed
                  ? `${formatNumber(data.elements_consumed_speed.reduce((sum: number, val: number) => sum + val, 0))}/s`
                  : '0/s'}
              </span>
            </div>
          )}
          {hasOutputs && (
            <div className="custom-node-metric-row">
              <span className="custom-node-metric-label">OUT:</span>
              <span className="custom-node-metric-value">{formatNumber(totalProduced)}</span>
              <span className="custom-node-metric-speed">
                {data.elements_produced_speed
                  ? `${formatNumber(data.elements_produced_speed.reduce((sum: number, val: number) => sum + val, 0))}/s`
                  : '0/s'}
              </span>
            </div>
          )}
        </div>
      )}
      
      <Handle type="source" position={Position.Bottom} style={{ opacity: 0 }} />
    </div>
  );
}
