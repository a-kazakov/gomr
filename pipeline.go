// Package gomr provides a typed MapReduce-style pipeline framework for Go.
//
// A pipeline is a DAG of operators that process data through typed Collections
// (streams of batched elements) and Values (single computed results / futures).
// Operators run concurrently as goroutines; the framework manages lifecycle,
// backpressure (via channel capacities), and metrics collection.
//
// Basic usage:
//
//	p := gomr.NewPipeline()
//	seed := gomr.NewSeedCollection(p, func(emitter gomr.Emitter[int]) { ... })
//	mapped := gomr.Map(seed, func(ctx gomr.OperatorContext, r gomr.CollectionReceiver[int], emitter gomr.Emitter[string]) { ... })
//	result := gomr.Collect(mapped, preCollector, postCollector)
//	p.WaitForCompletion()
//
// All operator functions accept variadic functional options (e.g. WithParallelism,
// WithOperationName) for controlling parallelism, naming, batch sizes, and channel
// capacities. Generated NxM variants (MapTo2, ForkTo3, etc.) support multi-output
// operators.
//
// This package re-exports internal types and functions as a stable public API.
// The internal/ packages contain the actual implementations.
package gomr

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/parameters"
)

type Pipeline = *pipeline.Pipeline
type Collection[TOut any] = *pipeline.Collection[TOut]
type Value[T any] = *pipeline.Value[T]
type OperatorContext = *core.OperatorContext

func NewPipeline() *pipeline.Pipeline {
	return pipeline.NewPipeline()
}

func NewPipelineWithParameters(params *parameters.Parameters) *pipeline.Pipeline {
	return pipeline.NewPipelineWithParameters(params)
}

func NewSeedCollection[TOut any](p *pipeline.Pipeline, seed pipeline.Seed[TOut], opts ...options.SeedOption) *pipeline.Collection[TOut] {
	return pipeline.NewSeedCollection(p, seed, options.ApplySeedOptions(opts...))
}

const OPERATION_KIND_SEED = core.OPERATION_KIND_SEED
const OPERATION_KIND_MAP = core.OPERATION_KIND_MAP
const OPERATION_KIND_MAP_VALUE = core.OPERATION_KIND_MAP_VALUE
const OPERATION_KIND_SHUFFLE = core.OPERATION_KIND_SHUFFLE
const OPERATION_KIND_FORK = core.OPERATION_KIND_FORK
const OPERATION_KIND_MERGE = core.OPERATION_KIND_MERGE
const OPERATION_KIND_COLLECT = core.OPERATION_KIND_COLLECT
const OPERATION_KIND_TO_COLLECTION = core.OPERATION_KIND_TO_COLLECTION
const OPERATION_KIND_SPILL_BUFFER = core.OPERATION_KIND_SPILL_BUFFER

const OPERATION_PHASE_PENDING = core.OPERATION_PHASE_PENDING
const OPERATION_PHASE_RUNNING = core.OPERATION_PHASE_RUNNING
const OPERATION_PHASE_SCATTERING = core.OPERATION_PHASE_SCATTERING
const OPERATION_PHASE_GATHERING = core.OPERATION_PHASE_GATHERING
const OPERATION_PHASE_COMPLETED = core.OPERATION_PHASE_COMPLETED
