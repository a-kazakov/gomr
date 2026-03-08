// Package operators implements all pipeline operators (Map, Fork, Merge, Collect, etc.).
// Each operator follows a common pattern: resolve options/parameters, register metrics,
// create output collections/values, obtain input receivers, and start tracked goroutines
// that consume inputs and produce outputs.
// Generated variants (*_gen.go) provide typed N-input × M-output overloads.
// Package operators implements all pipeline operators (Map, Fork, Merge, Collect,
// Shuffle, Ignore, ToCollection, SpillBuffer). Most operators have code-generated
// variants for different input/output arities in *_gen.go files.
//
// Each operator follows a common pattern:
//  1. Resolve options (names, parallelism, batch sizes) from opts + pipeline defaults
//  2. Create output collections/values and register metrics
//  3. Get a receiver from the input collection (marks it consumed)
//  4. Start tracked goroutines via the pipeline's GoroutineDispatcher
//  5. Process data, update metrics counters, close outputs when done
package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

func ForkToAny[T any](collection *pipeline.Collection[T], n int, opts *options.ForkOptions) []*pipeline.Collection[T] {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Fork")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_FORK, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	outCollections := make([]*pipeline.Collection[T], 0, n)
	outChannels := make([]chan primitives.Batch[T], 0, n)
	outMetrics := make([]*metrics.OutputCollectionMetrics, 0, n)
	for i := range n {
		outCollection, outChannel := pipeline.NewDerivedCollection[T](
			utils.OptGetOrDefault(opts.OutCollectionNames, i, collection.Metrics.Name),
			collection.Metrics.BatchSize,
			outChannelCapacity,
			collection,
		)
		outCollections = append(outCollections, outCollection)
		outChannels = append(outChannels, outChannel)
		outMetrics = append(outMetrics, opMetrics.AddOutputCollection(outCollection.Metrics))
	}
	inReceiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	inReceiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	opMetrics.Parallelism = 1
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		for inValuesBatch := range inReceiver.IterBatchesNoRecycle() {
			for i := range n {
				outMetrics[i].ElementsProduced.Add(int64(len(inValuesBatch.Values)))
				outMetrics[i].BatchesProduced.Add(1)
				outChannels[i] <- inValuesBatch.Fork()
			}
			inValuesBatch.Recycle()
		}
		for i := range n {
			outCollections[i].Close()
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollections
}

//go:generate go run ./codegen/fork/fork.go impl
