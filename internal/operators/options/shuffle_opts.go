package options

import (
	"time"

	"github.com/a-kazakov/gomr/parameters"
)

// =============================================================================
// Shuffle-specific options
// =============================================================================

type numShardsOption int32

func (o numShardsOption) applyToShuffle(opts *ShuffleOptions) {
	opts.NumShards = parameters.Some(int32(o))
}

// WithNumShards sets the number of shuffle shards.
func WithNumShards(n int32) numShardsOption { return numShardsOption(n) }

type scatterParallelismOption int

func (o scatterParallelismOption) applyToShuffle(opts *ShuffleOptions) {
	opts.ScatterParallelism = parameters.Some(int(o))
}

// WithScatterParallelism sets the scatter phase parallelism.
func WithScatterParallelism(p int) scatterParallelismOption { return scatterParallelismOption(p) }

type gatherParallelismOption int

func (o gatherParallelismOption) applyToShuffle(opts *ShuffleOptions) {
	opts.GatherParallelism = parameters.Some(int(o))
}

// WithGatherParallelism sets the gather phase parallelism.
func WithGatherParallelism(p int) gatherParallelismOption { return gatherParallelismOption(p) }

type localShuffleBufferSizeOption int64

func (o localShuffleBufferSizeOption) applyToShuffle(opts *ShuffleOptions) {
	opts.LocalShuffleBufferSize = parameters.Some(int64(o))
}

// WithLocalShuffleBufferSize sets the local shuffle buffer size in bytes.
func WithLocalShuffleBufferSize(size int64) localShuffleBufferSizeOption {
	return localShuffleBufferSizeOption(size)
}

type localShuffleBufferSizeJitterOption float64

func (o localShuffleBufferSizeJitterOption) applyToShuffle(opts *ShuffleOptions) {
	opts.LocalShuffleBufferSizeJitter = parameters.Some(float64(o))
}

// WithLocalShuffleBufferSizeJitter sets the jitter factor for buffer size (0.0-1.0).
func WithLocalShuffleBufferSizeJitter(jitter float64) localShuffleBufferSizeJitterOption {
	return localShuffleBufferSizeJitterOption(jitter)
}

type shuffleReadBufferSizeOption int

func (o shuffleReadBufferSizeOption) applyToShuffle(opts *ShuffleOptions) {
	opts.ReadBufferSize = parameters.Some(int(o))
}

// WithShuffleReadBufferSize sets the read buffer size for shuffle files.
func WithShuffleReadBufferSize(size int) shuffleReadBufferSizeOption {
	return shuffleReadBufferSizeOption(size)
}

type shuffleWriteBufferSizeOption int

func (o shuffleWriteBufferSizeOption) applyToShuffle(opts *ShuffleOptions) {
	opts.WriteBufferSize = parameters.Some(int(o))
}

// WithShuffleWriteBufferSize sets the write buffer size for shuffle files.
func WithShuffleWriteBufferSize(size int) shuffleWriteBufferSizeOption {
	return shuffleWriteBufferSizeOption(size)
}

type fileMergeThresholdOption int

func (o fileMergeThresholdOption) applyToShuffle(opts *ShuffleOptions) {
	opts.FileMergeThreshold = parameters.Some(int(o))
}

// WithFileMergeThreshold sets the number of files before triggering merge.
func WithFileMergeThreshold(threshold int) fileMergeThresholdOption {
	return fileMergeThresholdOption(threshold)
}

type scratchSpacePathsOption []string

func (o scratchSpacePathsOption) applyToShuffle(opts *ShuffleOptions) {
	opts.ScratchSpacePaths = parameters.Some([]string(o))
}

// WithScratchSpacePaths sets the scratch space paths for shuffle.
func WithScratchSpacePaths(paths ...string) scratchSpacePathsOption {
	return scratchSpacePathsOption(paths)
}

type targetWriteLatencyOption time.Duration

func (o targetWriteLatencyOption) applyToShuffle(opts *ShuffleOptions) {
	opts.TargetWriteLatency = parameters.Some(time.Duration(o))
}

// WithTargetWriteLatency sets the target write latency for disk operations during shuffle.
func WithTargetWriteLatency(d time.Duration) targetWriteLatencyOption {
	return targetWriteLatencyOption(d)
}
