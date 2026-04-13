import { Handle, Position, type NodeProps, type Node } from '@xyflow/react';
import { ValueNodeData } from '../types/pipeline';
import '../App.css';

type ValueNodeComponentProps = NodeProps<Node<ValueNodeData>>;

export default function ValueNode({ data }: ValueNodeComponentProps) {
  const isResolved = data.is_resolved;

  return (
    <div className={`value-node ${isResolved ? 'resolved' : 'pending'}`}>
      {/* Invisible handles for React Flow to calculate edge connection points */}
      <Handle type="target" position={Position.Left} style={{ opacity: 0 }} />
      
      <div className="value-node-content">
        {/* Value name */}
        <div className="value-node-name">{data.name}</div>
        
        {/* Status */}
        <div className={`value-node-status ${isResolved ? 'resolved' : 'pending'}`}>
          {isResolved ? 'RESOLVED' : 'PENDING'}
        </div>
      </div>
      
      <Handle type="source" position={Position.Right} style={{ opacity: 0 }} />
    </div>
  );
}
