package parameters

import (
	"runtime"

	"github.com/a-kazakov/gomr/internal/core"
)

// ShuffleParameters holds defaults for the shuffle operator (shard count, parallelism, buffers, compression).
type ShuffleParameters struct {
	DefaultNumShards                    *Parameter[int32]
	DefaultScatterParallelism           *Parameter[int]
	DefaultGatherParallelism            *Parameter[int]
	DefaultLocalShuffleBufferSize       *Parameter[int64]
	DefaultLocalShuffleBufferSizeJitter *Parameter[float64]
	DefaultFileMergeThreshold           *Parameter[int]
	DefaultReadBufferSize               *Parameter[int]
	DefaultWriteBufferSize              *Parameter[int]
	DefaultCompressionAlgorithm         *Parameter[core.CompressionAlgorithm]
}

func NewShuffleParameters() *ShuffleParameters {
	return &ShuffleParameters{
		DefaultNumShards: NewInt32ParamDynamic(
			"shuffle", "num_shards",
			"Default number of shuffle shards",
			func() int32 { return int32(4 * runtime.GOMAXPROCS(0)) },
			Positive[int32],
		),
		DefaultScatterParallelism: NewIntParamDynamic(
			"shuffle", "scatter_parallelism",
			"Default parallelism for scatter phase",
			func() int { return runtime.GOMAXPROCS(0) + 8 },
			Positive[int],
		),
		DefaultGatherParallelism: NewIntParamDynamic(
			"shuffle", "gather_parallelism",
			"Default parallelism for gather phase",
			func() int { return runtime.GOMAXPROCS(0) },
			Positive[int],
		),
		DefaultLocalShuffleBufferSize: NewByteSizeParam(
			"shuffle", "buffer_size",
			"Local shuffle buffer size (supports Ki, Mi, Gi suffixes)",
			512*1024*1024, // 512Mi
			Positive[int64],
		),
		DefaultLocalShuffleBufferSizeJitter: NewFloat64Param(
			"shuffle", "buffer_size_jitter",
			"Jitter factor for local shuffle buffer size (0.0-1.0)",
			0.2,
			InRange(0.0, 1.0),
		),
		DefaultFileMergeThreshold: NewIntParam(
			"shuffle", "file_merge_threshold",
			"Number of files before triggering merge",
			100,
			Positive[int],
		),
		DefaultReadBufferSize: NewByteSizeParamAsInt(
			"shuffle", "read_buffer_size",
			"Read buffer size for shuffle files",
			1024*1024, // 1Mi
			Positive[int],
		),
		DefaultWriteBufferSize: NewByteSizeParamAsInt(
			"shuffle", "write_buffer_size",
			"Write buffer size for shuffle files",
			8*1024*1024, // 8Mi
			Positive[int],
		),
		DefaultCompressionAlgorithm: NewCompressionAlgorithmParam(
			"shuffle", "compression_algorithm",
			"Compression algorithm for shuffle files",
			core.CompressionAlgorithmLz4,
		),
	}
}

func (sp *ShuffleParameters) RegisterFlags(f Flagger) {
	RegisterAllFlags(sp, f)
}

func (sp *ShuffleParameters) LoadFromSource(lookup Source) error {
	return LoadAllFromSource(sp, lookup)
}
