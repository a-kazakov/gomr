export type OperationKind = 'seed' | 'map' | 'shuffle' | 'fork' | 'merge' | 'collect';

export type OperationPhase = '' | 'running' | 'scattering' | 'flushing' | 'gathering' | 'pre-collect' | 'aggregate' | 'completed';

export interface PhaseSwitchSnapshot {
  phase_name: string;
  switch_timestamp: string;
}

export interface ServerResponse {
  operations: Record<string, ServerOperation>;
  collections: Record<string, ServerCollection>;
  values?: Record<string, ServerValue>;
  user_counters?: Record<string, number>;
}

export interface ServerInputCollection {
  id: string;
  elements_consumed: number;
  batches_consumed: number;
}

export interface ServerOutputCollection {
  id: string;
  elements_produced: number;
  batches_produced: number;
}

export interface ServerShuffle {
  spills_count: number;
  disk_usage: number;
  elements_gathered: number;
  groups_gathered: number;
}

export interface ServerInputValue {
  id: string;
  is_consumed: boolean;
}

export interface ServerOutputValue {
  id: string;
  is_produced: boolean;
}

export interface ServerOperation {
  id: string;
  name: string;
  kind: OperationKind;
  parallelism: number;
  phase: OperationPhase;
  phases: PhaseSwitchSnapshot[];
  shuffle: ServerShuffle | null;
  input_collections: ServerInputCollection[];
  output_collections: ServerOutputCollection[];
  input_values: ServerInputValue[];
  output_values: ServerOutputValue[];
  user_counters?: Record<string, number>;

  elements_consumed_speed?: number[];
  batches_consumed_speed?: number[];
  elements_produced_speed?: number[];
  batches_produced_speed?: number[];
  shuffle_spills_count_speed?: number;
  shuffle_disk_usage_speed?: number;
  elements_gathered_speed?: number;
  groups_gathered_speed?: number;
}

export interface ServerCollection {
  id: string;
  name: string;
  batch_size: number;
  pressure: number;
  capacity: number;
  completed: boolean;
  user_counters?: Record<string, number>;
}

export interface ServerValue {
  id: string;
  name: string;
  is_resolved: boolean;
  user_counters?: Record<string, number>;
}

export interface Env {
  GOMR_VIS: KVNamespace;
  PUSH_AUTH_TOKEN?: string;
  VIEW_BASIC_AUTH?: string; // "user:password"
}
