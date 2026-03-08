package parameters

import (
	"runtime"

	"github.com/a-kazakov/gomr/internal/core"
)

// SpillBufferParameters holds defaults for the spill buffer operator
// (element sizes, parallelism, file sizes, compression).
type SpillBufferParameters struct {
	MaxSerializedElementSize    *Parameter[int]
	DefaultWriteParallelism     *Parameter[int]
	DefaultReadParallelism      *Parameter[int]
	DefaultMaxSpillFileSize     *Parameter[int64]
	DefaultWriteBufferSize      *Parameter[int]
	DefaultReadBufferSize       *Parameter[int]
	DefaultCompressionAlgorithm *Parameter[core.CompressionAlgorithm]
}

func NewSpillBufferParameters() *SpillBufferParameters {
	return &SpillBufferParameters{
		MaxSerializedElementSize: NewByteSizeParamAsInt(
			"spill_buffer", "max_element_size",
			"Maximum size of a single serialized element",
			16*1024*1024, // 16Mi
			Positive[int],
		),
		DefaultWriteParallelism: NewIntParamDynamic(
			"spill_buffer", "write_parallelism",
			"Default parallelism for spill buffer writes",
			func() int { return runtime.GOMAXPROCS(0) },
			Positive[int],
		),
		DefaultReadParallelism: NewIntParamDynamic(
			"spill_buffer", "read_parallelism",
			"Default parallelism for spill buffer reads",
			func() int { return runtime.GOMAXPROCS(0) },
			Positive[int],
		),
		DefaultMaxSpillFileSize: NewByteSizeParam(
			"spill_buffer", "max_file_size",
			"Maximum size of a single spill file",
			64*1024*1024, // 64Mi
			Positive[int64],
		),
		DefaultWriteBufferSize: NewByteSizeParamAsInt(
			"spill_buffer", "write_buffer_size",
			"Write buffer size for spill files",
			256*1024, // 256Ki
			Positive[int],
		),
		DefaultReadBufferSize: NewByteSizeParamAsInt(
			"spill_buffer", "read_buffer_size",
			"Read buffer size for spill files",
			256*1024, // 256Ki
			Positive[int],
		),
		DefaultCompressionAlgorithm: NewCompressionAlgorithmParam(
			"spill_buffer", "compression_algorithm",
			"Compression algorithm for spill files",
			core.CompressionAlgorithmLz4,
		),
	}
}

func (sbp *SpillBufferParameters) RegisterFlags(f Flagger) {
	RegisterAllFlags(sbp, f)
}

func (sbp *SpillBufferParameters) LoadFromSource(lookup Source) error {
	return LoadAllFromSource(sbp, lookup)
}
