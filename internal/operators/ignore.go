package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/utils"
)

func Ignore[T any](collection *pipeline.Collection[T], opts *options.IgnoreOptions) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Ignore")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_COLLECT, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	inReceiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	inReceiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	opMetrics.Parallelism = 1
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		for range inReceiver.IterBatches() {
			// do nothing
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
}
