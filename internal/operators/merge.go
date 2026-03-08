package operators

//go:generate go run codegen/merge/merge.go impl

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

func mergeFanIn[T any](collections []*pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collections[0].Pipeline
	batchSize := collections[0].Metrics.BatchSize
	for i := range collections {
		if collections[i].Metrics.BatchSize != batchSize {
			batchSize = -1
			break
		}
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, pipeline.CollectionsToParents(collections)...)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics := make([]*metrics.InputCollectionMetrics, 0, len(collections))
	for _, collection := range collections {
		inMetrics = append(inMetrics, opMetrics.AddInputCollection(collection.Metrics))
	}
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	inReceivers := make([]*primitives.ChannelReceiver[T], 0, len(collections))
	for idx := range collections {
		inReceivers = append(inReceivers, collections[idx].GetReceiver(&inMetrics[idx].ElementsConsumed, &inMetrics[idx].BatchesConsumed))
	}
	coroutineDispatcher := collections[0].GetPipeline().GoroutineDispatcher
	for _, receiver := range inReceivers {
		receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	}
	opMetrics.Parallelism = len(collections)
	coroutineDispatcher.StartParallelTrackedGoroutines(
		len(collections),
		func(shardIndex int) {
			inReceiver := inReceivers[shardIndex]
			for inValuesBatch := range inReceiver.IterBatchesNoRecycle() {
				outChannel <- inValuesBatch
				outMetrics.ElementsProduced.Add(int64(len(inValuesBatch.Values)))
				outMetrics.BatchesProduced.Add(1)
			}
		}, func() {
			outCollection.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		},
	)
	return outCollection
}
