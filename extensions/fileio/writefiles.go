package fileio

import (
	"io"
	"path/filepath"

	"github.com/a-kazakov/gomr"
)

// FileSerializer defines how pipeline values map to file names and byte records.
type FileSerializer[TValue any] interface {
	MarshalFileName(value *TValue, dest []byte) int
	MarshalRecord(value *TValue, dest []byte) int
}

type writeContext[TSerializer FileSerializer[TValue], TValue any] struct {
	destPath   string
	serializer TSerializer
	makeWriter func(baseWriter io.Writer) io.WriteCloser
	backend    Backend
	bufferSize int
}

type writeSerializer[TSerializer FileSerializer[TValue], TValue any] struct {
	serializer TSerializer
}

func (s *writeSerializer[TSerializer, TValue]) Setup(ctx gomr.OperatorContext) {
	octx := ctx.UserOperatorContext.(*writeContext[TSerializer, TValue])
	s.serializer = octx.serializer
}

func (s *writeSerializer[TSerializer, TValue]) MarshalKeyToBytes(value *TValue, dest []byte, cursor int64) (int, int64) {
	return s.serializer.MarshalFileName(value, dest), gomr.StopEmitting
}

func (s *writeSerializer[TSerializer, TValue]) MarshalValueToBytes(value *TValue, dest []byte) int {
	return s.serializer.MarshalRecord(value, dest)
}

func (s *writeSerializer[TSerializer, TValue]) UnmarshalValueFromBytes(_ []byte, data []byte, dest *[]byte) {
	*dest = data
}

type writeReducer[TSerializer FileSerializer[TValue], TValue any] struct {
	destPath   string
	makeWriter func(baseWriter io.Writer) io.WriteCloser
	backend    Backend
	bufferSize int
}

func (r *writeReducer[TSerializer, TValue]) Setup(ctx gomr.OperatorContext) {
	octx := ctx.UserOperatorContext.(*writeContext[TSerializer, TValue])
	r.makeWriter = octx.makeWriter
	r.destPath = octx.destPath
	r.backend = octx.backend
	r.bufferSize = octx.bufferSize
}

func (r *writeReducer[TSerializer, TValue]) Reduce(key []byte, receiver gomr.ShuffleReceiver[[]byte], emitter gomr.Emitter[string]) {
	fullPath := filepath.Join(r.destPath, string(key))
	writer := MustCreate(fullPath, WithBackend(r.backend), WithBufferSize(r.bufferSize))
	defer writer.Close()
	recordWriter := r.makeWriter(writer)
	defer recordWriter.Close()
	for value := range receiver.IterValues() {
		recordWriter.Write(*value)
	}
}

// WriteFiles groups pipeline values by file name (via Shuffle) and writes each
// group using the provided makeWriter function. Returns a collection of produced
// file paths.
func WriteFiles[TValue any, TSerializer FileSerializer[TValue]](
	collection gomr.Collection[TValue],
	serializer TSerializer,
	destPath string,
	makeWriter func(baseWriter io.Writer) io.WriteCloser,
	opts ...WriteFilesOption,
) gomr.Collection[string] {
	cfg := &WriteFilesConfig{
		Backend:        LocalBackend,
		BufferSize:     16 * 1024 * 1024,
		OperationName:  "Write Files",
		CollectionName: "Produced Files",
		BatchSize:      1,
		ChannelCapacity: 100,
	}
	for _, o := range opts {
		o.ApplyWriteFilesConfig(cfg)
	}

	shuffleOpts := []gomr.ShuffleOption{
		gomr.WithUserOperatorContext(&writeContext[TSerializer, TValue]{
			destPath:   destPath,
			serializer: serializer,
			makeWriter: makeWriter,
			backend:    cfg.Backend,
			bufferSize: cfg.BufferSize,
		}),
		gomr.WithOperationName(cfg.OperationName),
		gomr.WithOutCollectionNames(cfg.CollectionName),
		gomr.WithOutBatchSize(cfg.BatchSize),
		gomr.WithOutChannelCapacity(cfg.ChannelCapacity),
	}
	if cfg.NumShards > 0 {
		shuffleOpts = append(shuffleOpts, gomr.WithNumShards(cfg.NumShards))
	}
	if cfg.ScatterParallelism > 0 {
		shuffleOpts = append(shuffleOpts, gomr.WithScatterParallelism(cfg.ScatterParallelism))
	}
	if cfg.GatherParallelism > 0 {
		shuffleOpts = append(shuffleOpts, gomr.WithGatherParallelism(cfg.GatherParallelism))
	}
	shuffleOpts = append(shuffleOpts, cfg.ExtraShuffleOpts...)

	return gomr.Shuffle[
		*writeSerializer[TSerializer, TValue],
		*writeReducer[TSerializer, TValue],
	](collection, shuffleOpts...)
}
