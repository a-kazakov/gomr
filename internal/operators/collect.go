package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

// PreCollector runs in parallel to reduce a collection shard into an intermediate value.
type PreCollector[TIn, TIntermediate any] = func(ctx *core.OperatorContext, receiver core.CollectionReceiver[TIn]) TIntermediate
// PostCollector runs single-threaded to aggregate all intermediate values into the final result.
type PostCollector[TIntermediate, TOut any] = func(ctx *core.OperatorContext, itermediates []TIntermediate) TOut
type PreCollectorWithSideValue[TIn, TIntermediate, TSideValue any] = func(ctx *core.OperatorContext, receiver core.CollectionReceiver[TIn], sideValue TSideValue) TIntermediate
type PostCollectorWithSideValue[TIntermediate, TOut, TSideValue any] = func(ctx *core.OperatorContext, itermediates []TIntermediate, sideValue TSideValue) TOut

func Collect[TIn, TIntermediate, TOut any](
	collection *pipeline.Collection[TIn],
	preCollector PreCollector[TIn, TIntermediate],
	postCollector PostCollector[TIntermediate, TOut],
	opts *options.CollectOptions,
) *pipeline.Value[TOut] {
	p := collection.Pipeline
	parallelism := p.Parameters.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	operationName := utils.OptStrOrDefault(opts.OperationName, "Collect")
	outValueName := utils.OptStrOrDefault(opts.OutValueName, "Collected value")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_COLLECT, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	outValue := pipeline.NewValue[TOut](p, outValueName)
	opMetrics.AddOutputValue(outValue.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_PRE_COLLECT) })
	opMetrics.Parallelism = parallelism
	preCollectOpContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{outValue.Metrics}, parallelism, opts.UserOperatorContext.GetOr(nil))
	postCollectOpContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{outValue.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	intermediateValues := make([]TIntermediate, parallelism)
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(shardIndex int) {
		intermediateValues[shardIndex] = preCollector(preCollectOpContexts[shardIndex], receiver)
	}, func() {
		opMetrics.Parallelism = 1
		opMetrics.SetPhase(core.PHASE_AGGREGATE)
		outValue.Resolve(postCollector(postCollectOpContexts[0], intermediateValues))
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outValue
}

func CollectWithSideValue[TIn, TIntermediate, TOut, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	preCollector PreCollectorWithSideValue[TIn, TIntermediate, TSideValue],
	postCollector PostCollectorWithSideValue[TIntermediate, TOut, TSideValue],
	opts *options.CollectOptions,
) *pipeline.Value[TOut] {
	p := collection.Pipeline
	parallelism := p.Parameters.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	operationName := utils.OptStrOrDefault(opts.OperationName, "Collect")
	outValueName := utils.OptStrOrDefault(opts.OutValueName, "Collected value")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_COLLECT, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outValue := pipeline.NewValue[TOut](p, outValueName)
	opMetrics.AddOutputValue(outValue.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_PRE_COLLECT) })
	inValueMetrics.IsConsumed = true
	opMetrics.Parallelism = parallelism
	preCollectOpContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{outValue.Metrics}, parallelism, opts.UserOperatorContext.GetOr(nil))
	postCollectOpContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{outValue.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	intermediateValues := make([]TIntermediate, parallelism)
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(shardIndex int) {
		sideValueResolved := sideValue.Wait()
		intermediateValues[shardIndex] = preCollector(preCollectOpContexts[shardIndex], receiver, sideValueResolved)
	}, func() {
		opMetrics.SetPhase(core.PHASE_AGGREGATE)
		sideValueResolved := sideValue.Wait()
		outValue.Resolve(postCollector(postCollectOpContexts[0], intermediateValues, sideValueResolved))
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outValue
}
