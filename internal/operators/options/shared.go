package options

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/parameters"
)

// =============================================================================
// Shared Option Types
//
// Each option type is a thin wrapper around a value (string, int, etc.) that
// implements the applyTo* methods for every operator it applies to. When an
// option applies to multiple operators (e.g. WithOperationName works on all
// operators), its type implements all the corresponding interfaces. The
// With* constructor functions are the public API; the applyTo* methods and
// option types are unexported implementation details.
// =============================================================================

type operationNameOption string

func (o operationNameOption) applyToSeed(opts *SeedOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToFork(opts *ForkOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToMerge(opts *MergeOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToMap(opts *MapOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToShuffle(opts *ShuffleOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToCollect(opts *CollectOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToMapValue(opts *MapValueOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToIgnore(opts *IgnoreOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToToCollection(opts *ToCollectionOptions) {
	opts.OperationName = parameters.Some(string(o))
}
func (o operationNameOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.OperationName = parameters.Some(string(o))
}

func WithOperationName(name string) operationNameOption { return operationNameOption(name) }

type outCollectionNamesOption []string

func (o outCollectionNamesOption) applyToFork(opts *ForkOptions) {
	opts.OutCollectionNames = parameters.Some([]string(o))
}
func (o outCollectionNamesOption) applyToMap(opts *MapOptions) {
	opts.OutCollectionNames = parameters.Some([]string(o))
}
func (o outCollectionNamesOption) applyToShuffle(opts *ShuffleOptions) {
	opts.OutCollectionNames = parameters.Some([]string(o))
}

func WithOutCollectionNames(names ...string) outCollectionNamesOption {
	return outCollectionNamesOption(names)
}

type outCollectionNameOption string

func (o outCollectionNameOption) applyToSeed(opts *SeedOptions) {
	opts.OutCollectionName = parameters.Some(string(o))
}
func (o outCollectionNameOption) applyToMerge(opts *MergeOptions) {
	opts.OutCollectionName = parameters.Some(string(o))
}
func (o outCollectionNameOption) applyToToCollection(opts *ToCollectionOptions) {
	opts.OutCollectionName = parameters.Some(string(o))
}
func (o outCollectionNameOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.OutCollectionName = parameters.Some(string(o))
}

func WithOutCollectionName(name string) outCollectionNameOption { return outCollectionNameOption(name) }

type outValueNameOption string

func (o outValueNameOption) applyToCollect(opts *CollectOptions) {
	opts.OutValueName = parameters.Some(string(o))
}

func WithOutValueName(name string) outValueNameOption { return outValueNameOption(name) }

type outValueNamesOption []string

func (o outValueNamesOption) applyToMapValue(opts *MapValueOptions) {
	opts.OutValueNames = parameters.Some([]string(o))
}

func WithOutValueNames(names ...string) outValueNamesOption { return outValueNamesOption(names) }

type outChannelCapacityOption int

func (o outChannelCapacityOption) applyToSeed(opts *SeedOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}
func (o outChannelCapacityOption) applyToFork(opts *ForkOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}
func (o outChannelCapacityOption) applyToMerge(opts *MergeOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}
func (o outChannelCapacityOption) applyToMap(opts *MapOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}
func (o outChannelCapacityOption) applyToShuffle(opts *ShuffleOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}
func (o outChannelCapacityOption) applyToCollect(opts *CollectOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}
func (o outChannelCapacityOption) applyToMapValue(opts *MapValueOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}
func (o outChannelCapacityOption) applyToToCollection(opts *ToCollectionOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}
func (o outChannelCapacityOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.OutChannelCapacity = parameters.Some(int(o))
}

func WithOutChannelCapacity(capacity int) outChannelCapacityOption {
	return outChannelCapacityOption(capacity)
}

type outBatchSizeOption int

func (o outBatchSizeOption) applyToSeed(opts *SeedOptions) {
	opts.OutBatchSize = parameters.Some(int(o))
}
func (o outBatchSizeOption) applyToMap(opts *MapOptions) { opts.OutBatchSize = parameters.Some(int(o)) }
func (o outBatchSizeOption) applyToShuffle(opts *ShuffleOptions) {
	opts.OutBatchSize = parameters.Some(int(o))
}
func (o outBatchSizeOption) applyToToCollection(opts *ToCollectionOptions) {
	opts.OutBatchSize = parameters.Some(int(o))
}
func (o outBatchSizeOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.OutBatchSize = parameters.Some(int(o))
}

func WithOutBatchSize(size int) outBatchSizeOption { return outBatchSizeOption(size) }

type parallelismOption int

func (o parallelismOption) applyToMap(opts *MapOptions) { opts.Parallelism = parameters.Some(int(o)) }
func (o parallelismOption) applyToCollect(opts *CollectOptions) {
	opts.Parallelism = parameters.Some(int(o))
}

func WithParallelism(p int) parallelismOption { return parallelismOption(p) }

type userOperatorContextOption struct{ value any }

func WithUserOperatorContext(ctx any) userOperatorContextOption {
	return userOperatorContextOption{value: ctx}
}

func (o userOperatorContextOption) applyToSeed(opts *SeedOptions) {
	opts.UserOperatorContext = parameters.Some(o.value)
}
func (o userOperatorContextOption) applyToMap(opts *MapOptions) {
	opts.UserOperatorContext = parameters.Some(o.value)
}
func (o userOperatorContextOption) applyToShuffle(opts *ShuffleOptions) {
	opts.UserOperatorContext = parameters.Some(o.value)
}
func (o userOperatorContextOption) applyToCollect(opts *CollectOptions) {
	opts.UserOperatorContext = parameters.Some(o.value)
}
func (o userOperatorContextOption) applyToMapValue(opts *MapValueOptions) {
	opts.UserOperatorContext = parameters.Some(o.value)
}
func (o userOperatorContextOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.UserOperatorContext = parameters.Some(o.value)
}

func WithCompressionAlgorithm(algorithm core.CompressionAlgorithm) compressionAlgorithmOption {
	return compressionAlgorithmOption(algorithm)
}

type compressionAlgorithmOption int

func (o compressionAlgorithmOption) applyToShuffle(opts *ShuffleOptions) {
	opts.CompressionAlgorithm = parameters.Some(core.CompressionAlgorithm(o))
}

func (o compressionAlgorithmOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.CompressionAlgorithm = parameters.Some(core.CompressionAlgorithm(o))
}
