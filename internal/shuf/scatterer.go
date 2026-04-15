package shuf

import (
	"time"

	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/pagedbuffer"
	"github.com/a-kazakov/gomr/internal/rw/bigkvfile"
	"github.com/a-kazakov/gomr/internal/rw/kv"
	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
	"github.com/a-kazakov/gomr/metrics"
	"github.com/zeebo/xxh3"
)

type scatterConfig struct {
	numLogicalShards     int32
	numActualShards      int32
	workingDirectory     string
	writeBufferSize      int
	compressionAlgorithm core.CompressionAlgorithm
}

type Scatterer struct {
	config                  scatterConfig
	bigFile                 *bigkvfile.Builder
	pagedBufferOrchestrator *pagedbuffer.PagedBufferOrchestrator
	sortedBuffers           []SortedBuffer
	tempWriteBuffer         []byte
	tempFlushBuffer         []byte
	orchestrator            *pagedbuffer.PagedBufferOrchestrator
	metrics                 *metrics.ShuffleMetrics
}

type ScattererArguments struct {
	NumLogicalShards     int32
	NumInputs            int32
	WorkingDirectory     string
	MaxBufferSize        int64
	MaxBufferSizeJitter  float64
	FileMergeThreshold   int
	ReadBufferSize       int
	WriteBufferSize      int
	TargetWriteLatency   time.Duration
	CompressionAlgorithm core.CompressionAlgorithm
	Metrics              *metrics.ShuffleMetrics
}

func NewScatterer(args ScattererArguments) *Scatterer {
	numActualShards := args.NumLogicalShards * args.NumInputs
	sortedBuffers := make([]SortedBuffer, numActualShards)
	orchestrator := pagedbuffer.NewPagedBufferOrchestrator(args.MaxBufferSize, args.MaxBufferSizeJitter)
	result := Scatterer{
		config: scatterConfig{
			numLogicalShards:     args.NumLogicalShards,
			numActualShards:      numActualShards,
			workingDirectory:     args.WorkingDirectory,
			writeBufferSize:      args.WriteBufferSize,
			compressionAlgorithm: args.CompressionAlgorithm,
		},
		bigFile: bigkvfile.NewBuilder(
			args.NumLogicalShards*args.NumInputs,
			args.FileMergeThreshold,
			args.ReadBufferSize,
			args.WriteBufferSize,
			args.TargetWriteLatency,
		),
		pagedBufferOrchestrator: orchestrator,
		sortedBuffers:           sortedBuffers,
		tempWriteBuffer:         make([]byte, constants.MAX_SHUFFLE_KEY_SIZE+constants.MAX_SHUFFLE_VALUE_SIZE+8),
		tempFlushBuffer:         make([]byte, constants.MAX_SHUFFLE_KEY_SIZE+constants.MAX_SHUFFLE_VALUE_SIZE+8),
		orchestrator:            orchestrator,
		metrics:                 args.Metrics,
	}
	for i := range numActualShards {
		sortedBuffers[i] = NewSortedBuffer(orchestrator)
	}
	return &result
}

func (s *Scatterer) flush() {
	shardedFile := shardedfile.NewShardedFile(s.config.workingDirectory, s.config.compressionAlgorithm)
	shardedFileCreator := shardedFile.OpenCreator()
	somethingWritten := false
	for shardId := range s.config.numActualShards {
		sortedBuffer := &s.sortedBuffers[shardId]
		sortedBuffer.Sort(s.tempFlushBuffer)
		writerShardId, writer := shardedFileCreator.OpenShardWriter(s.config.writeBufferSize)
		if shardId != writerShardId {
			must.BeTrue(false, "Shard ID mismatch, expected %d, got %d", shardId, writerShardId)
		}
		kvWriter := kv.NewWriter(writer)
		somethingWritten = somethingWritten || sortedBuffer.Len() > 0
		sortedBuffer.FlushTo(kvWriter, s.tempFlushBuffer)
		writer.Close()
	}
	shardedFileCreator.Close()
	s.metrics.SpillsCount.Add(1)
	s.metrics.DiskUsage.Add(shardedFile.GetSize())
	s.bigFile.PushFile(shardedFile)
	s.orchestrator.ResetMaxPages()
}

func AddBatchToScatterer[TValue any, TIntermediate any, TSerializer core.ShuffleSerializer[TValue, TIntermediate]](s *Scatterer, elements []TValue, serializer TSerializer, numInputs int, inputIndex int) {
	for idx := range elements {
		element := &elements[idx]
		valueSize := serializer.MarshalValueToBytes(element, s.tempWriteBuffer)
		value := s.tempWriteBuffer[:valueSize]
		keyBuffer := s.tempWriteBuffer[valueSize:]
		var cursor int64
		var keySize int
		for {
			keySize, cursor = serializer.MarshalKeyToBytes(element, keyBuffer, cursor)
			key := keyBuffer[:keySize]
			logicalShardId := xxh3.Hash(key) % uint64(s.config.numLogicalShards)
			actualShardId := logicalShardId*uint64(numInputs) + uint64(inputIndex)
			shard := &s.sortedBuffers[actualShardId]
			if s.pagedBufferOrchestrator.IsFull() || shard.IsFull() {
				s.flush()
			}
			shard.PushBack(key, value)
			if cursor == core.StopEmitting {
				break
			}
		}
	}
}

func (s *Scatterer) Finalize() *shardedfile.ShardedFile {
	s.flush()
	return s.bigFile.Finalize()
}
