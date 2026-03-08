import { Handle, Position, NodeProps } from 'reactflow';
import { CollectionNodeData } from '../types/pipeline';
import '../App.css';

/**
 * Formats a number with appropriate suffix (K/M/B) or no suffix.
 */
function formatNumber(value: number): string {
  if (value < 1000) {
    return value.toString();
  } else if (value < 1_000_000) {
    return `${(value / 1000).toFixed(2)}K`;
  } else if (value < 1_000_000_000) {
    return `${(value / 1_000_000).toFixed(2)}M`;
  } else {
    return `${(value / 1_000_000_000).toFixed(2)}B`;
  }
}

/**
 * Collection node component for pipeline visualization.
 * Displays collection name on first line, and pressure bar with value on second line.
 */
export default function CollectionNode({ data }: NodeProps<CollectionNodeData>) {
  const pressurePercent = data.capacity > 0 
    ? Math.min(100, (data.pressure / data.capacity) * 100) 
    : 0;
  const isHighPressure = pressurePercent > 80;
  // Collection is truly completed only if marked as completed AND pressure is zero
  const isCompleted = data.completed && data.pressure === 0;

  return (
    <div className={`collection-node ${isCompleted ? 'completed' : ''} ${isHighPressure ? 'high-pressure' : ''}`}>
      {/* Invisible handles for React Flow to calculate edge connection points */}
      <Handle type="target" position={Position.Left} style={{ opacity: 0 }} />
      
      <div className="collection-node-content">
        {/* First line: Collection name */}
        <div className="collection-node-name">{data.name}</div>
        
        {/* Second line: Pressure bar (full width) */}
        <div className="collection-node-pressure-container">
          <div className="collection-node-pressure-bar-container">
            <div 
              className="collection-node-pressure-bar"
              style={{
                width: isCompleted ? '100%' : `${pressurePercent}%`,
                backgroundColor: isCompleted 
                  ? 'var(--status-completed)' 
                  : (isHighPressure ? 'var(--edge-highlight)' : 'var(--edge-default)'),
              }}
            />
          </div>
          <div className={`collection-node-pressure-value ${isCompleted ? 'completed' : ''}`}>
            {isCompleted ? 'COMPLETED' : `${formatNumber(data.pressure)}/${formatNumber(data.capacity)}`}
          </div>
        </div>
      </div>
      
      <Handle type="source" position={Position.Right} style={{ opacity: 0 }} />
    </div>
  );
}
