// Type definitions for the pipeline visualization
// These define the contract between the server and the UI

// Operation kinds from gomr/internal/core/pipeline.go
export type OperationKind = 'seed' | 'map' | 'map_value' | 'shuffle' | 'fork' | 'merge' | 'collect';

// Operation phases from gomr/internal/core/pipeline.go
// Note: empty string "" represents pending phase
export type OperationPhase = '' | 'running' | 'scattering' | 'flushing' | 'gathering' | 'pre-collect' | 'aggregate' | 'completed';

// Phase switch snapshot for timeline
export interface PhaseSwitchSnapshot {
  phase_name: string;
  switch_timestamp: string; // ISO 8601 timestamp
}

// Server response types matching the Go backend (PipelineMetricsSnapshot)
export interface ServerResponse {
  operations: Record<string, ServerOperation>;
  collections: Record<string, ServerCollection>;
  values?: Record<string, ServerValue>;
  user_counters?: Record<string, number>; // Global user counters
}

// Matches InputCollectionMetricsSnapshot from Go
export interface ServerInputCollection {
  id: string;
  elements_consumed: number; // int64
  batches_consumed: number; // int64
}

// Matches OutputCollectionMetricsSnapshot from Go
export interface ServerOutputCollection {
  id: string;
  elements_produced: number; // int64
  batches_produced: number; // int64
}

// Matches ShuffleMetricsSnapshot from Go
export interface ServerShuffle {
  spills_count: number; // int64
  disk_usage: number; // int64
  elements_gathered: number; // int64
  groups_gathered: number; // int64
}

// Matches InputValueMetricsSnapshot from Go
export interface ServerInputValue {
  id: string;
  is_consumed: boolean;
}

// Matches OutputValueMetricsSnapshot from Go
export interface ServerOutputValue {
  id: string;
  is_produced: boolean;
}

// Matches OperationMetricsSnapshot from Go
// Note: Speed fields are added by the server during enrichment
export interface ServerOperation {
  id: string;
  name: string;
  kind: OperationKind;
  parallelism: number;
  phase: OperationPhase; // Current phase name
  phases: PhaseSwitchSnapshot[]; // Phase timeline
  shuffle: ServerShuffle | null;
  input_collections: ServerInputCollection[];
  output_collections: ServerOutputCollection[];
  input_values: ServerInputValue[];
  output_values: ServerOutputValue[];
  user_counters?: Record<string, number>; // map[string]int64

  // Speed metrics (added by server during enrichment)
  elements_consumed_speed?: number[];
  batches_consumed_speed?: number[];
  elements_produced_speed?: number[];
  batches_produced_speed?: number[];
  shuffle_spills_count_speed?: number;
  shuffle_disk_usage_speed?: number;
  elements_gathered_speed?: number;
  groups_gathered_speed?: number;
}

// Matches CollectionMetricsSnapshot from Go
export interface ServerCollection {
  id: string;
  name: string;
  batch_size: number;
  pressure: number;
  capacity: number;
  completed: boolean;
  user_counters?: Record<string, number>; // map[string]int64
}

// Collection node data for React Flow
export interface CollectionNodeData extends ServerCollection {
  [key: string]: unknown;
}

// Matches ValueMetricsSnapshot from Go
export interface ServerValue {
  id: string;
  name: string;
  is_resolved: boolean;
  user_counters?: Record<string, number>; // map[string]int64
}

// What React Flow needs inside the "data" prop
// This is the ServerOperation directly from the backend
export interface CustomNodeData extends ServerOperation {
  // For map_value operations: number of resolved input values
  input_values_resolved_count?: number;
  [key: string]: unknown;
}

// Value node data for React Flow
export interface ValueNodeData extends ServerValue {
  [key: string]: unknown;
}

// Union type for all node data types
export type NodeData = CustomNodeData | CollectionNodeData | ValueNodeData;
