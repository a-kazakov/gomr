package operators

import (
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/rw/kv"
)

func getReceiverFromKeyReader[TIn any, TIntermediate any, TSerializer core.ShuffleSerializer[TIn, TIntermediate]](
	serializer TSerializer,
	keyReader *kv.KeyReader,
	key []byte,
	bytesBuffer []byte,
	elementsCounter *atomic.Int64,
	groupsCounter *atomic.Int64,
) primitives.IteratorReceiver[TIntermediate] {
	return primitives.NewIteratorReceiver(func(yield func(*TIntermediate) bool) {
		var value TIntermediate
		elementsCount := int64(0)
		defer func() {
			elementsCounter.Add(elementsCount)
			groupsCounter.Add(1)
		}()
		for {
			n, err := keyReader.ReadValue(bytesBuffer)
			if err != nil {
				if err == io.EOF {
					return
				}
				must.OK(err).Else("failed to read values from shuffle files")
			}
			elementsCount++
			serializer.UnmarshalValueFromBytes(key, bytesBuffer[:n], &value)
			if !yield(&value) {
				// Drain remaining values
				for {
					_, err := keyReader.ReadValue(bytesBuffer)
					if err != nil {
						if err == io.EOF {
							return
						}
						must.OK(err).Else("failed to read values from shuffle files")
					}
				}
			}
		}
	})
}

func getEmptyReceiver[TIn any]() primitives.IteratorReceiver[TIn] {
	return primitives.NewIteratorReceiver(func(yield func(*TIn) bool) {})
}

func extractShuffleParameters(pipeline *pipeline.Pipeline, opts *options.ShuffleOptions) (
	numShards int32,
	scatterParallelism int,
	gatherParallelism int,
	outBatchSize int,
	localShuffleBufferSize int64,
	localShuffleBufferSizeJitter float64,
	fileMergeThreshold int,
	readBufferSize int,
	writeBufferSize int,
	targetWriteLatency time.Duration,
	scratchSpacePaths []string,
) {
	params := pipeline.Parameters
	numShards = params.Shuffle.DefaultNumShards.Resolve(opts.NumShards)
	scatterParallelism = params.Shuffle.DefaultScatterParallelism.Resolve(opts.ScatterParallelism)
	gatherParallelism = params.Shuffle.DefaultGatherParallelism.Resolve(opts.GatherParallelism)
	outBatchSize = params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	localShuffleBufferSize = params.Shuffle.DefaultLocalShuffleBufferSize.Resolve(opts.LocalShuffleBufferSize)
	localShuffleBufferSizeJitter = params.Shuffle.DefaultLocalShuffleBufferSizeJitter.Resolve(opts.LocalShuffleBufferSizeJitter)
	fileMergeThreshold = params.Shuffle.DefaultFileMergeThreshold.Resolve(opts.FileMergeThreshold)
	readBufferSize = params.Shuffle.DefaultReadBufferSize.Resolve(opts.ReadBufferSize)
	writeBufferSize = params.Shuffle.DefaultWriteBufferSize.Resolve(opts.WriteBufferSize)
	targetWriteLatency = params.Disk.TargetWriteLatency.Resolve(opts.TargetWriteLatency)

	// Handle scratch space paths - use override if provided, otherwise use parameter default
	if opts.ScratchSpacePaths.IsSet() {
		scratchSpacePaths = opts.ScratchSpacePaths.Get()
	} else {
		scratchSpacePaths = params.Disk.GetScratchSpacePaths()
	}
	return numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths
}

func getShufflePath(scratchSpacePaths []string, jobId string, shuffleId string, goroutineIndex int) string {
	pathIndex := goroutineIndex % len(scratchSpacePaths)
	basePath := scratchSpacePaths[pathIndex]
	return filepath.Join(basePath, "gomr", jobId, "shuffles", shuffleId, fmt.Sprintf("%04d", goroutineIndex))
}

//go:generate go run ./codegen/shuffle/shuffle.go impl
