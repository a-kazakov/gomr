package pipeline

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

type Seed[TOut any] = func(ctx *core.OperatorContext, emitter *primitives.Emitter[TOut])

func NewSeedCollection[TOut any](pipeline *Pipeline, seed Seed[TOut], opts *options.SeedOptions) *Collection[TOut] {
	operationName := utils.OptStrOrDefault(opts.OperationName, "Seed")
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Seeded")
	outBatchSize := pipeline.Parameters.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	outChannelCapacity := pipeline.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outChannel := make(chan primitives.Batch[TOut], outChannelCapacity)
	collection := &Collection[TOut]{
		Pipeline: pipeline,
		Metrics: pipeline.Metrics.AddCollection(
			outCollectionName,
			outBatchSize,
			func() (int, int) { return len(outChannel), cap(outChannel) },
		),
		outChannel: outChannel,
		consumed:   false,
	}
	pipeline.RegisterCollection(collection)
	opMetrics := pipeline.Metrics.AddOperation(core.OPERATION_KIND_SEED, operationName)
	outMetrics := opMetrics.AddOutputCollection(collection.Metrics)
	emitter := primitives.NewEmitter(pipeline.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel, &outMetrics.ElementsProduced, &outMetrics.BatchesProduced)
	opMetrics.SetPhase(core.PHASE_RUNNING)
	opMetrics.Parallelism = 1
	seedOpContexts := pipeline.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{collection.Metrics}, nil, 1, opts.UserOperatorContext.GetOr(nil))
	pipeline.GoroutineDispatcher.StartTrackedGoroutine(func() {
		seed(seedOpContexts[0], emitter)
		emitter.Close()
		collection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return collection
}
