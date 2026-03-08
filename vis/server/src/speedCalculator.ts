/**
 * Speed calculation module for pipeline metrics.
 * Calculates rate per second (speed) for operation counters using a 5-second moving window.
 */

import { ServerResponse, ServerOperation } from './types/pipeline.js';

/**
 * History entry for a job snapshot
 */
interface HistoryEntry {
  timestamp: number; // Unix timestamp in milliseconds
  data: ServerResponse;
}

/**
 * History buffer for tracking job snapshots
 */
const jobHistory = new Map<string, HistoryEntry[]>();

/**
 * Maximum age of history entries (10 seconds in milliseconds)
 */
const MAX_HISTORY_AGE_MS = 10_000;

/**
 * Target window for speed calculation (5 seconds in milliseconds)
 */
const SPEED_WINDOW_MS = 5_000;

/**
 * Prunes old history entries for a job to prevent memory leaks.
 * Keeps only entries from the last MAX_HISTORY_AGE_MS.
 */
function pruneHistory(jobId: string, currentTime: number): void {
  const history = jobHistory.get(jobId);
  if (!history) return;

  const cutoffTime = currentTime - MAX_HISTORY_AGE_MS;
  const pruned = history.filter(entry => entry.timestamp >= cutoffTime);
  
  if (pruned.length !== history.length) {
    jobHistory.set(jobId, pruned);
  }
}

/**
 * Finds the reference snapshot for speed calculation.
 * Looks for the most recent record where (CurrentTime - RecordTime) >= 5000ms.
 * Falls back to the oldest available record if no record is older than 5s.
 */
function findReferenceSnapshot(
  history: HistoryEntry[],
  currentTime: number
): HistoryEntry | null {
  if (history.length === 0) return null;

  // Look backwards for a record at least 5 seconds old
  for (let i = history.length - 1; i >= 0; i--) {
    const entry = history[i];
    const age = currentTime - entry.timestamp;
    
    if (age >= SPEED_WINDOW_MS) {
      return entry;
    }
  }

  // Fallback: use the oldest record if no record is older than 5s
  return history[0];
}

/**
 * Calculates speed for a number value.
 * Returns 0 if timeDelta is 0 or invalid.
 */
function calculateNumberSpeed(
  currentValue: number,
  referenceValue: number,
  timeDeltaSeconds: number
): number {
  if (timeDeltaSeconds <= 0) return 0;
  return (currentValue - referenceValue) / timeDeltaSeconds;
}

/**
 * Calculates speed for an array of numbers.
 * Returns an array of speeds, one for each index.
 */
function calculateArraySpeed(
  currentArray: number[],
  referenceArray: number[],
  timeDeltaSeconds: number
): number[] {
  if (timeDeltaSeconds <= 0) {
    return currentArray.map(() => 0);
  }

  const maxLength = Math.max(currentArray.length, referenceArray.length);
  const speeds: number[] = [];

  for (let i = 0; i < maxLength; i++) {
    const current = currentArray[i] || 0;
    const reference = referenceArray[i] || 0;
    speeds.push((current - reference) / timeDeltaSeconds);
  }

  return speeds;
}

/**
 * Enriches an operation with speed metrics.
 */
function enrichOperationWithSpeed(
  currentOp: ServerOperation,
  referenceOp: ServerOperation | null,
  timeDeltaSeconds: number
): ServerOperation {
  // Extract arrays from nested structure
  const currentElementsConsumed = currentOp.input_collections.map(c => c.elements_consumed);
  const currentBatchesConsumed = currentOp.input_collections.map(c => c.batches_consumed);
  const currentElementsProduced = currentOp.output_collections.map(c => c.elements_produced);
  const currentBatchesProduced = currentOp.output_collections.map(c => c.batches_produced);
  
  const currentShuffle = currentOp.shuffle;
  
  // If no reference, all speeds are 0
  if (!referenceOp || timeDeltaSeconds <= 0) {
    const enriched: ServerOperation = {
      ...currentOp,
      elements_consumed_speed: currentElementsConsumed.map(() => 0),
      batches_consumed_speed: currentBatchesConsumed.map(() => 0),
      elements_produced_speed: currentElementsProduced.map(() => 0),
      batches_produced_speed: currentBatchesProduced.map(() => 0),
      shuffle_spills_count_speed: 0,
      shuffle_disk_usage_speed: 0,
      elements_gathered_speed: 0,
      groups_gathered_speed: 0,
    };
    
    return enriched;
  }

  // Extract arrays from reference structure
  const referenceElementsConsumed = referenceOp.input_collections.map(c => c.elements_consumed);
  const referenceBatchesConsumed = referenceOp.input_collections.map(c => c.batches_consumed);
  const referenceElementsProduced = referenceOp.output_collections.map(c => c.elements_produced);
  const referenceBatchesProduced = referenceOp.output_collections.map(c => c.batches_produced);
  
  const referenceShuffle = referenceOp.shuffle;

  const enriched: ServerOperation = {
    ...currentOp,
    elements_consumed_speed: calculateArraySpeed(
      currentElementsConsumed,
      referenceElementsConsumed,
      timeDeltaSeconds
    ),
    batches_consumed_speed: calculateArraySpeed(
      currentBatchesConsumed,
      referenceBatchesConsumed,
      timeDeltaSeconds
    ),
    elements_produced_speed: calculateArraySpeed(
      currentElementsProduced,
      referenceElementsProduced,
      timeDeltaSeconds
    ),
    batches_produced_speed: calculateArraySpeed(
      currentBatchesProduced,
      referenceBatchesProduced,
      timeDeltaSeconds
    ),
    shuffle_spills_count_speed: currentShuffle && referenceShuffle
      ? calculateNumberSpeed(
          currentShuffle.spills_count,
          referenceShuffle.spills_count,
          timeDeltaSeconds
        )
      : 0,
    shuffle_disk_usage_speed: currentShuffle && referenceShuffle
      ? calculateNumberSpeed(
          currentShuffle.disk_usage,
          referenceShuffle.disk_usage,
          timeDeltaSeconds
        )
      : 0,
    elements_gathered_speed: currentShuffle && referenceShuffle
      ? calculateNumberSpeed(
          currentShuffle.elements_gathered,
          referenceShuffle.elements_gathered,
          timeDeltaSeconds
        )
      : 0,
    groups_gathered_speed: currentShuffle && referenceShuffle
      ? calculateNumberSpeed(
          currentShuffle.groups_gathered,
          referenceShuffle.groups_gathered,
          timeDeltaSeconds
        )
      : 0,
  };
  
  return enriched;
}

/**
 * Enriches pipeline data with speed metrics and returns the enriched data.
 * 
 * @param jobId - The job identifier
 * @param currentData - The current snapshot data
 * @returns The enriched data with speed metrics added
 */
export function enrichWithSpeed(
  jobId: string,
  currentData: ServerResponse
): ServerResponse {
  const currentTime = Date.now();

  // Get or create history for this job
  let history = jobHistory.get(jobId);
  if (!history) {
    history = [];
    jobHistory.set(jobId, history);
  }

  // Prune old entries
  pruneHistory(jobId, currentTime);

  // Find reference snapshot
  const referenceEntry = findReferenceSnapshot(history, currentTime);
  
  // Calculate time delta in seconds
  let timeDeltaSeconds = 0;
  if (referenceEntry) {
    timeDeltaSeconds = (currentTime - referenceEntry.timestamp) / 1000;
  }

  // Enrich operations with speed metrics
  const enrichedOperations: Record<string, ServerOperation> = {};
  
  for (const [opId, currentOp] of Object.entries(currentData.operations)) {
    const referenceOp = referenceEntry?.data.operations[opId] || null;
    enrichedOperations[opId] = enrichOperationWithSpeed(
      currentOp,
      referenceOp,
      timeDeltaSeconds
    );
  }

  // Create enriched response (collections, values, and user_counters remain unchanged)
  const enrichedData: ServerResponse = {
    operations: enrichedOperations,
    collections: currentData.collections,
    values: currentData.values,
    user_counters: currentData.user_counters,
  };

  // Add current snapshot to history
  history.push({
    timestamp: currentTime,
    data: currentData, // Store original data, not enriched
  });

  return enrichedData;
}

/**
 * Cleans up history for a job (useful for testing or manual cleanup).
 */
export function clearHistory(jobId: string): void {
  jobHistory.delete(jobId);
}

/**
 * Clears all history (useful for testing).
 */
export function clearAllHistory(): void {
  jobHistory.clear();
}
