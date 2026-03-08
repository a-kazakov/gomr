package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

func MapTo1[TIn any, TOut0 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo1[TIn, TOut0],
	opts *options.MapOptions,
) *pipeline.Collection[TOut0] {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0)
	}, func() {
		outCollection0.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0
}

func MapTo1WithSideValue[TIn any, TOut0 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo1WithSideValue[TIn, TOut0, TSideValue],
	opts *options.MapOptions,
) *pipeline.Collection[TOut0] {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	inValueMetrics.IsConsumed = true
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, sideValueResolved)
	}, func() {
		outCollection0.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0
}

func MapTo2[TIn any, TOut0 any, TOut1 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo2[TIn, TOut0, TOut1],
	opts *options.MapOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Mapped value 1")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](output1Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
		defer emitter1.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, emitter1)
	}, func() {
		outCollection0.Close()
		outCollection1.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0, outCollection1
}

func MapTo2WithSideValue[TIn any, TOut0 any, TOut1 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo2WithSideValue[TIn, TOut0, TOut1, TSideValue],
	opts *options.MapOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Mapped value 1")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](output1Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	inValueMetrics.IsConsumed = true
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
		defer emitter1.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, emitter1, sideValueResolved)
	}, func() {
		outCollection0.Close()
		outCollection1.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0, outCollection1
}

func MapTo3[TIn any, TOut0 any, TOut1 any, TOut2 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo3[TIn, TOut0, TOut1, TOut2],
	opts *options.MapOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Mapped value 2")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](output1Name, outBatchSize, outChannelCapacity, collection)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](output2Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
		defer emitter1.Close()
		emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
		defer emitter2.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, emitter1, emitter2)
	}, func() {
		outCollection0.Close()
		outCollection1.Close()
		outCollection2.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0, outCollection1, outCollection2
}

func MapTo3WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo3WithSideValue[TIn, TOut0, TOut1, TOut2, TSideValue],
	opts *options.MapOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Mapped value 2")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](output1Name, outBatchSize, outChannelCapacity, collection)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](output2Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	inValueMetrics.IsConsumed = true
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
		defer emitter1.Close()
		emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
		defer emitter2.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, emitter1, emitter2, sideValueResolved)
	}, func() {
		outCollection0.Close()
		outCollection1.Close()
		outCollection2.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0, outCollection1, outCollection2
}

func MapTo4[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo4[TIn, TOut0, TOut1, TOut2, TOut3],
	opts *options.MapOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Mapped value 3")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](output1Name, outBatchSize, outChannelCapacity, collection)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](output2Name, outBatchSize, outChannelCapacity, collection)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](output3Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
		defer emitter1.Close()
		emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
		defer emitter2.Close()
		emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
		defer emitter3.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, emitter1, emitter2, emitter3)
	}, func() {
		outCollection0.Close()
		outCollection1.Close()
		outCollection2.Close()
		outCollection3.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0, outCollection1, outCollection2, outCollection3
}

func MapTo4WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo4WithSideValue[TIn, TOut0, TOut1, TOut2, TOut3, TSideValue],
	opts *options.MapOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Mapped value 3")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](output1Name, outBatchSize, outChannelCapacity, collection)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](output2Name, outBatchSize, outChannelCapacity, collection)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](output3Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	inValueMetrics.IsConsumed = true
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
		defer emitter1.Close()
		emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
		defer emitter2.Close()
		emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
		defer emitter3.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, emitter1, emitter2, emitter3, sideValueResolved)
	}, func() {
		outCollection0.Close()
		outCollection1.Close()
		outCollection2.Close()
		outCollection3.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0, outCollection1, outCollection2, outCollection3
}

func MapTo5[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo5[TIn, TOut0, TOut1, TOut2, TOut3, TOut4],
	opts *options.MapOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Mapped value 3")
	output4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Mapped value 4")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](output1Name, outBatchSize, outChannelCapacity, collection)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](output2Name, outBatchSize, outChannelCapacity, collection)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](output3Name, outBatchSize, outChannelCapacity, collection)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](output4Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
		defer emitter1.Close()
		emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
		defer emitter2.Close()
		emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
		defer emitter3.Close()
		emitter4 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel4, &outMetrics4.ElementsProduced, &outMetrics4.BatchesProduced)
		defer emitter4.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, emitter1, emitter2, emitter3, emitter4)
	}, func() {
		outCollection0.Close()
		outCollection1.Close()
		outCollection2.Close()
		outCollection3.Close()
		outCollection4.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func MapTo5WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo5WithSideValue[TIn, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue],
	opts *options.MapOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	output0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Mapped value 3")
	output4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Mapped value 4")
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](output0Name, outBatchSize, outChannelCapacity, collection)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](output1Name, outBatchSize, outChannelCapacity, collection)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](output2Name, outBatchSize, outChannelCapacity, collection)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](output3Name, outBatchSize, outChannelCapacity, collection)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](output4Name, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	inValueMetrics.IsConsumed = true
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
		defer emitter0.Close()
		emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
		defer emitter1.Close()
		emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
		defer emitter2.Close()
		emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
		defer emitter3.Close()
		emitter4 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel4, &outMetrics4.ElementsProduced, &outMetrics4.BatchesProduced)
		defer emitter4.Close()
		mapper(opContexts[goroutineIndex], receiver, emitter0, emitter1, emitter2, emitter3, emitter4, sideValueResolved)
	}, func() {
		outCollection0.Close()
		outCollection1.Close()
		outCollection2.Close()
		outCollection3.Close()
		outCollection4.Close()
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}
