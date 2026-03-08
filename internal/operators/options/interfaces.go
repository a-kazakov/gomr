package options

// =============================================================================
// Option Interfaces
// =============================================================================

type SeedOption interface{ applyToSeed(*SeedOptions) }
type ForkOption interface{ applyToFork(*ForkOptions) }
type MergeOption interface{ applyToMerge(*MergeOptions) }
type MapOption interface{ applyToMap(*MapOptions) }
type ShuffleOption interface{ applyToShuffle(*ShuffleOptions) }
type CollectOption interface{ applyToCollect(*CollectOptions) }
type MapValueOption interface{ applyToMapValue(*MapValueOptions) }
type IgnoreOption interface{ applyToIgnore(*IgnoreOptions) }
type ToCollectionOption interface{ applyToToCollection(*ToCollectionOptions) }
type SpillBufferOption interface{ applyToSpillBuffer(*SpillBufferOptions) }
