package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/utils"
)

func ToCollection[T any](input *pipeline.Value[T], opts *options.ToCollectionOptions) *pipeline.Collection[T] {
	p := input.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "To collection")
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Collection")
	outBatchSize := p.Parameters.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, outBatchSize, outChannelCapacity, input)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_TO_COLLECTION, operationName)
	inMetrics := opMetrics.AddInputValue(input.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	opMetrics.Parallelism = 1
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		emitter := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel, &outMetrics.ElementsProduced, &outMetrics.BatchesProduced)
		*emitter.GetEmitPointer() = input.Wait()
		inMetrics.IsConsumed = true
		emitter.Close()
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}
