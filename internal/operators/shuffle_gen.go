package operators

import (
	"bytes"

	"github.com/a-kazakov/gomr/internal/capprobe"
	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
	"github.com/a-kazakov/gomr/internal/shuf"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

func Shuffle1To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To1[TIntermediate0, TOut0]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle1To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To1[TIntermediate0, TOut0]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle1To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To2[TIntermediate0, TOut0, TOut1]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle1To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To2[TIntermediate0, TOut0, TOut1]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle1To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To3[TIntermediate0, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle1To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To3[TIntermediate0, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle1To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To4[TIntermediate0, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle1To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To4[TIntermediate0, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle1To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To5[TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle1To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To5[TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 1
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				for key0 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					reducer.Reduce(smallestKey, &receiver0, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle2To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To1[TIntermediate0, TIntermediate1, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle2To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To1[TIntermediate0, TIntermediate1, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle2To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To2[TIntermediate0, TIntermediate1, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle2To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To2[TIntermediate0, TIntermediate1, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle2To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To3[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle2To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To3[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle2To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To4[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle2To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To4[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle2To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To5[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle2To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To5[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 2
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0, collection1)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				for key0 != nil || key1 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle3To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To1[TIntermediate0, TIntermediate1, TIntermediate2, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle3To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To1[TIntermediate0, TIntermediate1, TIntermediate2, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle3To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To2[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle3To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To2[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle3To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To3[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle3To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To3[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle3To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To4[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle3To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To4[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle3To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To5[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle3To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To5[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 3
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle4To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle4To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle4To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To2[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle4To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To2[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle4To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To3[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle4To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To3[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle4To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To4[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle4To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To4[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle4To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To5[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle4To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To5[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 4
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle5To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle5To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) *pipeline.Collection[TOut0] {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo1Teardown[TOut0](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0
}

func Shuffle5To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To2[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle5To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To2[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo2Teardown[TOut0, TOut1](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0, emitter1)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1
}

func Shuffle5To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To3[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle5To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To3[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo3Teardown[TOut0, TOut1, TOut2](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0, emitter1, emitter2)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2
}

func Shuffle5To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To4[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle5To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To4[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			emitter0 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel0, &outMetrics0.ElementsProduced, &outMetrics0.BatchesProduced)
			defer emitter0.Close()
			emitter1 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel1, &outMetrics1.ElementsProduced, &outMetrics1.BatchesProduced)
			defer emitter1.Close()
			emitter2 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel2, &outMetrics2.ElementsProduced, &outMetrics2.BatchesProduced)
			defer emitter2.Close()
			emitter3 := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel3, &outMetrics3.ElementsProduced, &outMetrics3.BatchesProduced)
			defer emitter3.Close()
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0, emitter1, emitter2, emitter3)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3
}

func Shuffle5To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To5[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex])
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex])
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetup(serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetup(serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetup(serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetup(serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetup(serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex])
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup(reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex])
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}

func Shuffle5To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To5[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts *options.ShuffleOptions,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	p := collection0.Pipeline
	const numInputs = 5
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	outCollection0Name := utils.OptGetOrDefault(opts.OutCollectionNames, 0, "Shuffled value 0")
	outCollection1Name := utils.OptGetOrDefault(opts.OutCollectionNames, 1, "Shuffled value 1")
	outCollection2Name := utils.OptGetOrDefault(opts.OutCollectionNames, 2, "Shuffled value 2")
	outCollection3Name := utils.OptGetOrDefault(opts.OutCollectionNames, 3, "Shuffled value 3")
	outCollection4Name := utils.OptGetOrDefault(opts.OutCollectionNames, 4, "Shuffled value 4")
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	outCollection0, outChannel0 := pipeline.NewDerivedCollection[TOut0](outCollection0Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection1, outChannel1 := pipeline.NewDerivedCollection[TOut1](outCollection1Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection2, outChannel2 := pipeline.NewDerivedCollection[TOut2](outCollection2Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection3, outChannel3 := pipeline.NewDerivedCollection[TOut3](outCollection3Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	outCollection4, outChannel4 := pipeline.NewDerivedCollection[TOut4](outCollection4Name, outBatchSize, outChannelCapacity, collection0, collection1, collection2, collection3, collection4)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	inMetrics0 := opMetrics.AddInputCollection(collection0.Metrics)
	inMetrics1 := opMetrics.AddInputCollection(collection1.Metrics)
	inMetrics2 := opMetrics.AddInputCollection(collection2.Metrics)
	inMetrics3 := opMetrics.AddInputCollection(collection3.Metrics)
	inMetrics4 := opMetrics.AddInputCollection(collection4.Metrics)
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	outMetrics0 := opMetrics.AddOutputCollection(outCollection0.Metrics)
	outMetrics1 := opMetrics.AddOutputCollection(outCollection1.Metrics)
	outMetrics2 := opMetrics.AddOutputCollection(outCollection2.Metrics)
	outMetrics3 := opMetrics.AddOutputCollection(outCollection3.Metrics)
	outMetrics4 := opMetrics.AddOutputCollection(outCollection4.Metrics)
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	inChannel0 := collection0.GetRawChannel()
	inChannel1 := collection1.GetRawChannel()
	inChannel2 := collection2.GetRawChannel()
	inChannel3 := collection3.GetRawChannel()
	inChannel4 := collection4.GetRawChannel()
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection0.Metrics, outCollection1.Metrics, outCollection2.Metrics, outCollection3.Metrics, outCollection4.Metrics}, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		var serializerState0 TSerializerState0
		var serializer0 TSerializer0 = &serializerState0
		serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
		if serializerSetup0 != nil {
			serializerSetup0.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState1 TSerializerState1
		var serializer1 TSerializer1 = &serializerState1
		serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
		if serializerSetup1 != nil {
			serializerSetup1.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState2 TSerializerState2
		var serializer2 TSerializer2 = &serializerState2
		serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
		if serializerSetup2 != nil {
			serializerSetup2.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState3 TSerializerState3
		var serializer3 TSerializer3 = &serializerState3
		serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
		if serializerSetup3 != nil {
			serializerSetup3.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		var serializerState4 TSerializerState4
		var serializer4 TSerializer4 = &serializerState4
		serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
		if serializerSetup4 != nil {
			serializerSetup4.Setup(scatterOpContexts[goroutineIndex], sideValueResolved)
		}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		localInChannel0 := inChannel0
		localInChannel1 := inChannel1
		localInChannel2 := inChannel2
		localInChannel3 := inChannel3
		localInChannel4 := inChannel4
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			case inValuesBatch, ok := <-localInChannel0:
				if !ok {
					localInChannel0 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer0, numInputs, 0)
				inMetrics0.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics0.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel1:
				if !ok {
					localInChannel1 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer1, numInputs, 1)
				inMetrics1.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics1.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel2:
				if !ok {
					localInChannel2 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer2, numInputs, 2)
				inMetrics2.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics2.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel3:
				if !ok {
					localInChannel3 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer3, numInputs, 3)
				inMetrics3.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics3.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			case inValuesBatch, ok := <-localInChannel4:
				if !ok {
					localInChannel4 = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer4, numInputs, 4)
				inMetrics4.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics4.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			sideValueResolved := sideValue.Wait()
			var serializerState0 TSerializerState0
			var serializer0 TSerializer0 = &serializerState0
			serializerSetup0 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer0)
			if serializerSetup0 != nil {
				serializerSetup0.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer0 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState1 TSerializerState1
			var serializer1 TSerializer1 = &serializerState1
			serializerSetup1 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer1)
			if serializerSetup1 != nil {
				serializerSetup1.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer1 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState2 TSerializerState2
			var serializer2 TSerializer2 = &serializerState2
			serializerSetup2 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer2)
			if serializerSetup2 != nil {
				serializerSetup2.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer2 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState3 TSerializerState3
			var serializer3 TSerializer3 = &serializerState3
			serializerSetup3 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer3)
			if serializerSetup3 != nil {
				serializerSetup3.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer3 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			var serializerState4 TSerializerState4
			var serializer4 TSerializer4 = &serializerState4
			serializerSetup4 := capprobe.GetShuffleSerializerSetupWithSideValue[TSideValue](serializer4)
			if serializerSetup4 != nil {
				serializerSetup4.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
			}
			bytesBuffer4 := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
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
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetupWithSideValue[TSideValue](reducer)
				reducerTeardown := capprobe.GetReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex], sideValueResolved)
				}
				shardReader0, shardReaderCloser0 := gatherer.GetShardReader(numInputs*logicalShardId+0, readBufferSize)
				key0 := shardReader0.PeekKey()
				shardReader1, shardReaderCloser1 := gatherer.GetShardReader(numInputs*logicalShardId+1, readBufferSize)
				key1 := shardReader1.PeekKey()
				shardReader2, shardReaderCloser2 := gatherer.GetShardReader(numInputs*logicalShardId+2, readBufferSize)
				key2 := shardReader2.PeekKey()
				shardReader3, shardReaderCloser3 := gatherer.GetShardReader(numInputs*logicalShardId+3, readBufferSize)
				key3 := shardReader3.PeekKey()
				shardReader4, shardReaderCloser4 := gatherer.GetShardReader(numInputs*logicalShardId+4, readBufferSize)
				key4 := shardReader4.PeekKey()
				for key0 != nil || key1 != nil || key2 != nil || key3 != nil || key4 != nil {
					var smallestKey []byte
					var includeMask uint64
					if key0 != nil {
						if smallestKey == nil {
							smallestKey = key0
							includeMask = 1
						} else {
							cmp := bytes.Compare(key0, smallestKey)
							if cmp < 0 {
								smallestKey = key0
								includeMask = 1
							} else if cmp == 0 {
								includeMask |= 1
							}
						}
					}
					if key1 != nil {
						if smallestKey == nil {
							smallestKey = key1
							includeMask = 2
						} else {
							cmp := bytes.Compare(key1, smallestKey)
							if cmp < 0 {
								smallestKey = key1
								includeMask = 2
							} else if cmp == 0 {
								includeMask |= 2
							}
						}
					}
					if key2 != nil {
						if smallestKey == nil {
							smallestKey = key2
							includeMask = 4
						} else {
							cmp := bytes.Compare(key2, smallestKey)
							if cmp < 0 {
								smallestKey = key2
								includeMask = 4
							} else if cmp == 0 {
								includeMask |= 4
							}
						}
					}
					if key3 != nil {
						if smallestKey == nil {
							smallestKey = key3
							includeMask = 8
						} else {
							cmp := bytes.Compare(key3, smallestKey)
							if cmp < 0 {
								smallestKey = key3
								includeMask = 8
							} else if cmp == 0 {
								includeMask |= 8
							}
						}
					}
					if key4 != nil {
						if smallestKey == nil {
							smallestKey = key4
							includeMask = 16
						} else {
							cmp := bytes.Compare(key4, smallestKey)
							if cmp < 0 {
								smallestKey = key4
								includeMask = 16
							} else if cmp == 0 {
								includeMask |= 16
							}
						}
					}
					var receiver0 primitives.IteratorReceiver[TIntermediate0]
					if includeMask&1 != 0 {
						keyReader0 := shardReader0.GetKeyReader()
						receiver0 = getReceiverFromKeyReader(serializer0, &keyReader0, smallestKey, bytesBuffer0, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver0 = getEmptyReceiver[TIntermediate0]()
					}
					var receiver1 primitives.IteratorReceiver[TIntermediate1]
					if includeMask&2 != 0 {
						keyReader1 := shardReader1.GetKeyReader()
						receiver1 = getReceiverFromKeyReader(serializer1, &keyReader1, smallestKey, bytesBuffer1, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver1 = getEmptyReceiver[TIntermediate1]()
					}
					var receiver2 primitives.IteratorReceiver[TIntermediate2]
					if includeMask&4 != 0 {
						keyReader2 := shardReader2.GetKeyReader()
						receiver2 = getReceiverFromKeyReader(serializer2, &keyReader2, smallestKey, bytesBuffer2, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver2 = getEmptyReceiver[TIntermediate2]()
					}
					var receiver3 primitives.IteratorReceiver[TIntermediate3]
					if includeMask&8 != 0 {
						keyReader3 := shardReader3.GetKeyReader()
						receiver3 = getReceiverFromKeyReader(serializer3, &keyReader3, smallestKey, bytesBuffer3, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver3 = getEmptyReceiver[TIntermediate3]()
					}
					var receiver4 primitives.IteratorReceiver[TIntermediate4]
					if includeMask&16 != 0 {
						keyReader4 := shardReader4.GetKeyReader()
						receiver4 = getReceiverFromKeyReader(serializer4, &keyReader4, smallestKey, bytesBuffer4, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
					} else {
						receiver4 = getEmptyReceiver[TIntermediate4]()
					}
					reducer.Reduce(smallestKey, &receiver0, &receiver1, &receiver2, &receiver3, &receiver4, emitter0, emitter1, emitter2, emitter3, emitter4)
					receiver0.EnsureUsed()
					key0 = shardReader0.PeekKey()
					receiver1.EnsureUsed()
					key1 = shardReader1.PeekKey()
					receiver2.EnsureUsed()
					key2 = shardReader2.PeekKey()
					receiver3.EnsureUsed()
					key3 = shardReader3.PeekKey()
					receiver4.EnsureUsed()
					key4 = shardReader4.PeekKey()
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown(emitter0, emitter1, emitter2, emitter3, emitter4)
				}
				shardReaderCloser0.Close()
				shardReaderCloser1.Close()
				shardReaderCloser2.Close()
				shardReaderCloser3.Close()
				shardReaderCloser4.Close()
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			outCollection0.Close()
			outCollection1.Close()
			outCollection2.Close()
			outCollection3.Close()
			outCollection4.Close()
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return outCollection0, outCollection1, outCollection2, outCollection3, outCollection4
}
