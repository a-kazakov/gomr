import { BaseEdge, EdgeLabelRenderer, type EdgeProps, type Edge } from '@xyflow/react';

interface DagreEdgeData extends Record<string, unknown> {
  points?: Array<{ x: number; y: number }>;
  labelX?: number;
  labelY?: number;
}

type DagreEdgeProps = EdgeProps<Edge<DagreEdgeData>>;

/**
 * Returns a smooth SVG path string from an array of points using quadratic Bezier curves.
 * Creates fluid curves instead of jagged straight lines.
 */
function getSmoothPath(points: { x: number; y: number }[]) {
  if (!points || points.length === 0) return '';

  // 1. Move to start
  let path = `M ${points[0].x} ${points[0].y}`;

  // 2. Draw a curve through the points using a simple smoothing strategy
  // (Draws a quadratic curve to the midpoint of the next segment)
  for (let i = 0; i < points.length - 1; i++) {
    const current = points[i];
    const next = points[i + 1];

    // Calculate the midpoint between this point and the next
    const midX = (current.x + next.x) / 2;
    const midY = (current.y + next.y) / 2;

    // If it's the first point, just line to the first midpoint to start the curve
    if (i === 0) {
       path += ` L ${midX} ${midY}`; 
    } else {
       // Smooth curve: use the current point as the Control Point, 
       // and the midpoint as the Endpoint.
       path += ` Q ${current.x} ${current.y} ${midX} ${midY}`;
    }
  }

  // 3. Connect to the final point
  const last = points[points.length - 1];
  path += ` L ${last.x} ${last.y}`;

  return path;
}

/**
 * Custom edge component that uses Dagre-calculated waypoints to route edges around nodes.
 * Constructs a smooth SVG path from the waypoints using quadratic Bezier curves.
 */
export default function DagreEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  style,
  markerEnd,
  label,
  labelStyle,
  labelShowBg,
  labelBgStyle,
  labelBgPadding,
  data,
}: DagreEdgeProps) {
  const points = data?.points || [];

  // Construct smooth SVG path from waypoints
  const path = points.length > 0 
    ? getSmoothPath(points)
    : `M ${sourceX} ${sourceY} L ${targetX} ${targetY}`; // Fallback: straight line

  // Calculate label position: use explicit labelX/labelY if provided, otherwise fallback to middle point
  const labelX = data?.labelX !== undefined
    ? data.labelX
    : (points.length > 0 
        ? points[Math.floor(points.length / 2)].x 
        : (sourceX + targetX) / 2);
  const labelY = data?.labelY !== undefined
    ? data.labelY
    : (points.length > 0 
        ? points[Math.floor(points.length / 2)].y 
        : (sourceY + targetY) / 2);

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        style={style}
        markerEnd={markerEnd}
      />
      {label && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              pointerEvents: 'all',
            }}
            className="nodrag nopan edge-label-container"
          >
            {labelShowBg && (
              <div
                style={{
                  ...labelBgStyle,
                  backgroundColor: labelBgStyle?.fill || labelBgStyle?.stroke || 'var(--edge-default)',
                  padding: labelBgPadding ? `${labelBgPadding[0]}px ${labelBgPadding[1]}px` : '4px 6px',
                }}
                className="edge-label-bg"
              >
                <div style={labelStyle}>{label}</div>
              </div>
            )}
            {!labelShowBg && (
              <div style={labelStyle}>{label}</div>
            )}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}
