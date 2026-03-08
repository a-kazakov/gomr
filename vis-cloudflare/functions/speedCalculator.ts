import { ServerResponse, ServerOperation } from './types';

interface HistoryEntry {
  timestamp: number;
  data: ServerResponse;
}

const jobHistory = new Map<string, HistoryEntry[]>();

const MAX_HISTORY_AGE_MS = 10_000;
const SPEED_WINDOW_MS = 5_000;

function pruneHistory(jobId: string, currentTime: number): void {
  const history = jobHistory.get(jobId);
  if (!history) return;

  const cutoffTime = currentTime - MAX_HISTORY_AGE_MS;
  const pruned = history.filter(entry => entry.timestamp >= cutoffTime);

  if (pruned.length !== history.length) {
    jobHistory.set(jobId, pruned);
  }
}

function findReferenceSnapshot(
  history: HistoryEntry[],
  currentTime: number
): HistoryEntry | null {
  if (history.length === 0) return null;

  for (let i = history.length - 1; i >= 0; i--) {
    const entry = history[i];
    const age = currentTime - entry.timestamp;

    if (age >= SPEED_WINDOW_MS) {
      return entry;
    }
  }

  return history[0];
}

function calculateNumberSpeed(
  currentValue: number,
  referenceValue: number,
  timeDeltaSeconds: number
): number {
  if (timeDeltaSeconds <= 0) return 0;
  return (currentValue - referenceValue) / timeDeltaSeconds;
}

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

function enrichOperationWithSpeed(
  currentOp: ServerOperation,
  referenceOp: ServerOperation | null,
  timeDeltaSeconds: number
): ServerOperation {
  const currentElementsConsumed = currentOp.input_collections.map(c => c.elements_consumed);
  const currentBatchesConsumed = currentOp.input_collections.map(c => c.batches_consumed);
  const currentElementsProduced = currentOp.output_collections.map(c => c.elements_produced);
  const currentBatchesProduced = currentOp.output_collections.map(c => c.batches_produced);

  const currentShuffle = currentOp.shuffle;

  if (!referenceOp || timeDeltaSeconds <= 0) {
    return {
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
  }

  const referenceElementsConsumed = referenceOp.input_collections.map(c => c.elements_consumed);
  const referenceBatchesConsumed = referenceOp.input_collections.map(c => c.batches_consumed);
  const referenceElementsProduced = referenceOp.output_collections.map(c => c.elements_produced);
  const referenceBatchesProduced = referenceOp.output_collections.map(c => c.batches_produced);

  const referenceShuffle = referenceOp.shuffle;

  return {
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
}

export function enrichWithSpeed(
  jobId: string,
  currentData: ServerResponse
): ServerResponse {
  const currentTime = Date.now();

  let history = jobHistory.get(jobId);
  if (!history) {
    history = [];
    jobHistory.set(jobId, history);
  }

  pruneHistory(jobId, currentTime);

  const referenceEntry = findReferenceSnapshot(history, currentTime);

  let timeDeltaSeconds = 0;
  if (referenceEntry) {
    timeDeltaSeconds = (currentTime - referenceEntry.timestamp) / 1000;
  }

  const enrichedOperations: Record<string, ServerOperation> = {};

  for (const [opId, currentOp] of Object.entries(currentData.operations)) {
    const referenceOp = referenceEntry?.data.operations[opId] || null;
    enrichedOperations[opId] = enrichOperationWithSpeed(
      currentOp,
      referenceOp,
      timeDeltaSeconds
    );
  }

  const enrichedData: ServerResponse = {
    operations: enrichedOperations,
    collections: currentData.collections,
    values: currentData.values,
    user_counters: currentData.user_counters,
  };

  history.push({
    timestamp: currentTime,
    data: currentData,
  });

  return enrichedData;
}
