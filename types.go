package gomr

import (
	"sync/atomic"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/parameters"
)

type Emitter[TOut any] = *primitives.Emitter[TOut]
type ShuffleReceiver[TIn any] = core.ShuffleReceiver[TIn]
type CollectionReceiver[TIn any] = core.CollectionReceiver[TIn]

// Re-export option types for public API
type (
	MapOption          = options.MapOption
	ShuffleOption      = options.ShuffleOption
	SeedOption         = options.SeedOption
	ForkOption         = options.ForkOption
	MergeOption        = options.MergeOption
	CollectOption      = options.CollectOption
	MapValueOption     = options.MapValueOption
	IgnoreOption       = options.IgnoreOption
	SpillBufferOption  = options.SpillBufferOption
	ToCollectionOption = options.ToCollectionOption
)

// Re-export With* functions for public API
var (
	// Shared options (work with multiple operators)
	WithOperationName      = options.WithOperationName
	WithOutCollectionNames = options.WithOutCollectionNames
	WithOutCollectionName  = options.WithOutCollectionName
	WithOutValueName       = options.WithOutValueName
	WithOutValueNames      = options.WithOutValueNames
	WithOutChannelCapacity = options.WithOutChannelCapacity
	WithOutBatchSize       = options.WithOutBatchSize
	WithParallelism        = options.WithParallelism

	// Shuffle-specific options
	WithNumShards                    = options.WithNumShards
	WithScatterParallelism           = options.WithScatterParallelism
	WithGatherParallelism            = options.WithGatherParallelism
	WithLocalShuffleBufferSize       = options.WithLocalShuffleBufferSize
	WithLocalShuffleBufferSizeJitter = options.WithLocalShuffleBufferSizeJitter
	WithShuffleReadBufferSize        = options.WithShuffleReadBufferSize
	WithShuffleWriteBufferSize       = options.WithShuffleWriteBufferSize
	WithFileMergeThreshold           = options.WithFileMergeThreshold
	WithScratchSpacePaths            = options.WithScratchSpacePaths
	WithTargetWriteLatency           = options.WithTargetWriteLatency

	// SpillBuffer-specific options
	WithSpillDirectories      = options.WithSpillDirectories
	WithMaxSpillFileSize      = options.WithMaxSpillFileSize
	WithSpillWriteBufferSize  = options.WithSpillWriteBufferSize
	WithSpillReadBufferSize   = options.WithSpillReadBufferSize
	WithSpillWriteParallelism = options.WithSpillWriteParallelism
	WithSpillReadParallelism  = options.WithSpillReadParallelism

	// User context
	WithUserOperatorContext = options.WithUserOperatorContext
)

// Re-export Optional for advanced use cases
type Optional[T any] = parameters.Optional[T]

// Some creates an Optional with a value.
func Some[T any](v T) Optional[T] {
	return parameters.Some(v)
}

// None creates an empty Optional.
func None[T any]() Optional[T] {
	return parameters.None[T]()
}

// UserCountersMap provides thread-safe access to user-defined counters
type UserCountersMap interface {
	GetCounter(name string) *atomic.Int64
}

const StopEmitting = core.StopEmitting
