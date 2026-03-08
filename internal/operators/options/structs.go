// Package options provides functional-option types for configuring pipeline operators.
// Each operator has its own Options struct and Option interface.
package options

import (
	"time"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/parameters"
)

type SeedOptions struct {
	OperationName       parameters.Optional[string]
	OutCollectionName   parameters.Optional[string]
	OutBatchSize        parameters.Optional[int]
	OutChannelCapacity  parameters.Optional[int]
	UserOperatorContext parameters.Optional[any]
}

type ForkOptions struct {
	OperationName      parameters.Optional[string]
	OutCollectionNames parameters.Optional[[]string]
	OutChannelCapacity parameters.Optional[int]
}

type MergeOptions struct {
	OperationName      parameters.Optional[string]
	OutCollectionName  parameters.Optional[string]
	OutChannelCapacity parameters.Optional[int]
}

type MapOptions struct {
	OperationName       parameters.Optional[string]
	OutCollectionNames  parameters.Optional[[]string]
	Parallelism         parameters.Optional[int]
	OutBatchSize        parameters.Optional[int]
	OutChannelCapacity  parameters.Optional[int]
	UserOperatorContext parameters.Optional[any]
}

type ShuffleOptions struct {
	OperationName                parameters.Optional[string]
	OutCollectionNames           parameters.Optional[[]string]
	NumShards                    parameters.Optional[int32]
	ScatterParallelism           parameters.Optional[int]
	GatherParallelism            parameters.Optional[int]
	OutBatchSize                 parameters.Optional[int]
	LocalShuffleBufferSize       parameters.Optional[int64]
	LocalShuffleBufferSizeJitter parameters.Optional[float64]
	ReadBufferSize               parameters.Optional[int]
	WriteBufferSize              parameters.Optional[int]
	TargetWriteLatency           parameters.Optional[time.Duration]
	FileMergeThreshold           parameters.Optional[int]
	ScratchSpacePaths            parameters.Optional[[]string]
	CompressionAlgorithm         parameters.Optional[core.CompressionAlgorithm]
	OutChannelCapacity           parameters.Optional[int]
	UserOperatorContext          parameters.Optional[any]
}

type CollectOptions struct {
	OperationName       parameters.Optional[string]
	OutValueName        parameters.Optional[string]
	Parallelism         parameters.Optional[int]
	OutChannelCapacity  parameters.Optional[int]
	UserOperatorContext parameters.Optional[any]
}

type MapValueOptions struct {
	OperationName       parameters.Optional[string]
	OutValueNames       parameters.Optional[[]string]
	OutChannelCapacity  parameters.Optional[int]
	UserOperatorContext parameters.Optional[any]
}

type IgnoreOptions struct {
	OperationName parameters.Optional[string]
}

type ToCollectionOptions struct {
	OperationName      parameters.Optional[string]
	OutCollectionName  parameters.Optional[string]
	OutBatchSize       parameters.Optional[int]
	OutChannelCapacity parameters.Optional[int]
}

type SpillBufferOptions struct {
	OperationName        parameters.Optional[string]
	OutCollectionName    parameters.Optional[string]
	SpillDirectories     parameters.Optional[[]string]
	OutBatchSize         parameters.Optional[int]
	MaxSpillFileSize     parameters.Optional[int64]
	WriteBufferSize      parameters.Optional[int]
	ReadBufferSize       parameters.Optional[int]
	WriteParallelism     parameters.Optional[int]
	ReadParallelism      parameters.Optional[int]
	CompressionAlgorithm parameters.Optional[core.CompressionAlgorithm]
	OutChannelCapacity   parameters.Optional[int]
	UserOperatorContext  parameters.Optional[any]
}
