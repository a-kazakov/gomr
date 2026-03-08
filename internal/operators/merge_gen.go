package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/utils"
)

func mergeFast2[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func mergeFast3[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], collection2 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection2.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil || inChannel2 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel2:
				if !ok {
					inChannel2 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics2.ElementsConsumed.Add(batchSize)
				inMetrics2.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func mergeFast4[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], collection2 *pipeline.Collection[T], collection3 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection2.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection3.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil || inChannel2 != nil || inChannel3 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel2:
				if !ok {
					inChannel2 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics2.ElementsConsumed.Add(batchSize)
				inMetrics2.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel3:
				if !ok {
					inChannel3 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics3.ElementsConsumed.Add(batchSize)
				inMetrics3.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func mergeFast5[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], collection2 *pipeline.Collection[T], collection3 *pipeline.Collection[T], collection4 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection2.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection3.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection4.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil || inChannel2 != nil || inChannel3 != nil || inChannel4 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel2:
				if !ok {
					inChannel2 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics2.ElementsConsumed.Add(batchSize)
				inMetrics2.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel3:
				if !ok {
					inChannel3 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics3.ElementsConsumed.Add(batchSize)
				inMetrics3.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel4:
				if !ok {
					inChannel4 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics4.ElementsConsumed.Add(batchSize)
				inMetrics4.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func mergeFast6[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], collection2 *pipeline.Collection[T], collection3 *pipeline.Collection[T], collection4 *pipeline.Collection[T], collection5 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection2.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection3.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection4.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection5.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4, collection5)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inMetrics5 := opMetrics.AddInputCollection(collection5.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	inChannel5 := collection5.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil || inChannel2 != nil || inChannel3 != nil || inChannel4 != nil || inChannel5 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel2:
				if !ok {
					inChannel2 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics2.ElementsConsumed.Add(batchSize)
				inMetrics2.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel3:
				if !ok {
					inChannel3 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics3.ElementsConsumed.Add(batchSize)
				inMetrics3.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel4:
				if !ok {
					inChannel4 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics4.ElementsConsumed.Add(batchSize)
				inMetrics4.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel5:
				if !ok {
					inChannel5 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics5.ElementsConsumed.Add(batchSize)
				inMetrics5.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func mergeFast7[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], collection2 *pipeline.Collection[T], collection3 *pipeline.Collection[T], collection4 *pipeline.Collection[T], collection5 *pipeline.Collection[T], collection6 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection2.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection3.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection4.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection5.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection6.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4, collection5, collection6)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inMetrics5 := opMetrics.AddInputCollection(collection5.Metrics)
	inMetrics6 := opMetrics.AddInputCollection(collection6.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	inChannel5 := collection5.GetRawChannel()
	inChannel6 := collection6.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil || inChannel2 != nil || inChannel3 != nil || inChannel4 != nil || inChannel5 != nil || inChannel6 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel2:
				if !ok {
					inChannel2 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics2.ElementsConsumed.Add(batchSize)
				inMetrics2.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel3:
				if !ok {
					inChannel3 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics3.ElementsConsumed.Add(batchSize)
				inMetrics3.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel4:
				if !ok {
					inChannel4 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics4.ElementsConsumed.Add(batchSize)
				inMetrics4.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel5:
				if !ok {
					inChannel5 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics5.ElementsConsumed.Add(batchSize)
				inMetrics5.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel6:
				if !ok {
					inChannel6 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics6.ElementsConsumed.Add(batchSize)
				inMetrics6.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func mergeFast8[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], collection2 *pipeline.Collection[T], collection3 *pipeline.Collection[T], collection4 *pipeline.Collection[T], collection5 *pipeline.Collection[T], collection6 *pipeline.Collection[T], collection7 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection2.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection3.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection4.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection5.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection6.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection7.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4, collection5, collection6, collection7)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inMetrics5 := opMetrics.AddInputCollection(collection5.Metrics)
	inMetrics6 := opMetrics.AddInputCollection(collection6.Metrics)
	inMetrics7 := opMetrics.AddInputCollection(collection7.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	inChannel5 := collection5.GetRawChannel()
	inChannel6 := collection6.GetRawChannel()
	inChannel7 := collection7.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil || inChannel2 != nil || inChannel3 != nil || inChannel4 != nil || inChannel5 != nil || inChannel6 != nil || inChannel7 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel2:
				if !ok {
					inChannel2 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics2.ElementsConsumed.Add(batchSize)
				inMetrics2.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel3:
				if !ok {
					inChannel3 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics3.ElementsConsumed.Add(batchSize)
				inMetrics3.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel4:
				if !ok {
					inChannel4 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics4.ElementsConsumed.Add(batchSize)
				inMetrics4.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel5:
				if !ok {
					inChannel5 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics5.ElementsConsumed.Add(batchSize)
				inMetrics5.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel6:
				if !ok {
					inChannel6 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics6.ElementsConsumed.Add(batchSize)
				inMetrics6.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel7:
				if !ok {
					inChannel7 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics7.ElementsConsumed.Add(batchSize)
				inMetrics7.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func mergeFast9[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], collection2 *pipeline.Collection[T], collection3 *pipeline.Collection[T], collection4 *pipeline.Collection[T], collection5 *pipeline.Collection[T], collection6 *pipeline.Collection[T], collection7 *pipeline.Collection[T], collection8 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection2.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection3.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection4.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection5.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection6.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection7.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection8.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4, collection5, collection6, collection7, collection8)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inMetrics5 := opMetrics.AddInputCollection(collection5.Metrics)
	inMetrics6 := opMetrics.AddInputCollection(collection6.Metrics)
	inMetrics7 := opMetrics.AddInputCollection(collection7.Metrics)
	inMetrics8 := opMetrics.AddInputCollection(collection8.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	inChannel5 := collection5.GetRawChannel()
	inChannel6 := collection6.GetRawChannel()
	inChannel7 := collection7.GetRawChannel()
	inChannel8 := collection8.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil || inChannel2 != nil || inChannel3 != nil || inChannel4 != nil || inChannel5 != nil || inChannel6 != nil || inChannel7 != nil || inChannel8 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel2:
				if !ok {
					inChannel2 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics2.ElementsConsumed.Add(batchSize)
				inMetrics2.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel3:
				if !ok {
					inChannel3 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics3.ElementsConsumed.Add(batchSize)
				inMetrics3.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel4:
				if !ok {
					inChannel4 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics4.ElementsConsumed.Add(batchSize)
				inMetrics4.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel5:
				if !ok {
					inChannel5 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics5.ElementsConsumed.Add(batchSize)
				inMetrics5.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel6:
				if !ok {
					inChannel6 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics6.ElementsConsumed.Add(batchSize)
				inMetrics6.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel7:
				if !ok {
					inChannel7 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics7.ElementsConsumed.Add(batchSize)
				inMetrics7.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel8:
				if !ok {
					inChannel8 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics8.ElementsConsumed.Add(batchSize)
				inMetrics8.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func mergeFast10[T any](collection0 *pipeline.Collection[T], collection1 *pipeline.Collection[T], collection2 *pipeline.Collection[T], collection3 *pipeline.Collection[T], collection4 *pipeline.Collection[T], collection5 *pipeline.Collection[T], collection6 *pipeline.Collection[T], collection7 *pipeline.Collection[T], collection8 *pipeline.Collection[T], collection9 *pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	if collection1.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection2.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection3.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection4.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection5.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection6.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection7.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection8.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	if collection9.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4, collection5, collection6, collection7, collection8, collection9)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inMetrics5 := opMetrics.AddInputCollection(collection5.Metrics)
	inMetrics6 := opMetrics.AddInputCollection(collection6.Metrics)
	inMetrics7 := opMetrics.AddInputCollection(collection7.Metrics)
	inMetrics8 := opMetrics.AddInputCollection(collection8.Metrics)
	inMetrics9 := opMetrics.AddInputCollection(collection9.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	inChannel5 := collection5.GetRawChannel()
	inChannel6 := collection6.GetRawChannel()
	inChannel7 := collection7.GetRawChannel()
	inChannel8 := collection8.GetRawChannel()
	inChannel9 := collection9.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for inChannel0 != nil || inChannel1 != nil || inChannel2 != nil || inChannel3 != nil || inChannel4 != nil || inChannel5 != nil || inChannel6 != nil || inChannel7 != nil || inChannel8 != nil || inChannel9 != nil {
			select {
			case batch, ok := <-inChannel0:
				if !ok {
					inChannel0 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics0.ElementsConsumed.Add(batchSize)
				inMetrics0.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel1:
				if !ok {
					inChannel1 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics1.ElementsConsumed.Add(batchSize)
				inMetrics1.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel2:
				if !ok {
					inChannel2 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics2.ElementsConsumed.Add(batchSize)
				inMetrics2.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel3:
				if !ok {
					inChannel3 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics3.ElementsConsumed.Add(batchSize)
				inMetrics3.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel4:
				if !ok {
					inChannel4 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics4.ElementsConsumed.Add(batchSize)
				inMetrics4.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel5:
				if !ok {
					inChannel5 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics5.ElementsConsumed.Add(batchSize)
				inMetrics5.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel6:
				if !ok {
					inChannel6 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics6.ElementsConsumed.Add(batchSize)
				inMetrics6.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel7:
				if !ok {
					inChannel7 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics7.ElementsConsumed.Add(batchSize)
				inMetrics7.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel8:
				if !ok {
					inChannel8 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics8.ElementsConsumed.Add(batchSize)
				inMetrics8.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			case batch, ok := <-inChannel9:
				if !ok {
					inChannel9 = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics9.ElementsConsumed.Add(batchSize)
				inMetrics9.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}

func Merge[T any](collections []*pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	switch len(collections) {
	case 2:
		return mergeFast2(collections[0], collections[1], opts)
	case 3:
		return mergeFast3(collections[0], collections[1], collections[2], opts)
	case 4:
		return mergeFast4(collections[0], collections[1], collections[2], collections[3], opts)
	case 5:
		return mergeFast5(collections[0], collections[1], collections[2], collections[3], collections[4], opts)
	case 6:
		return mergeFast6(collections[0], collections[1], collections[2], collections[3], collections[4], collections[5], opts)
	case 7:
		return mergeFast7(collections[0], collections[1], collections[2], collections[3], collections[4], collections[5], collections[6], opts)
	case 8:
		return mergeFast8(collections[0], collections[1], collections[2], collections[3], collections[4], collections[5], collections[6], collections[7], opts)
	case 9:
		return mergeFast9(collections[0], collections[1], collections[2], collections[3], collections[4], collections[5], collections[6], collections[7], collections[8], opts)
	case 10:
		return mergeFast10(collections[0], collections[1], collections[2], collections[3], collections[4], collections[5], collections[6], collections[7], collections[8], collections[9], opts)
	default:
		return mergeFanIn(collections, opts)
	}
}
