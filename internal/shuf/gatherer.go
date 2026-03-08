package shuf

import (
	"io"

	"github.com/a-kazakov/gomr/internal/rw/kv"
	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
	"github.com/a-kazakov/gomr/metrics"
)

type Gatherer struct {
	accessors []*shardedfile.Accessor
	metrics   *metrics.ShuffleMetrics
}

func NewGatherer(files []*shardedfile.ShardedFile, metrics *metrics.ShuffleMetrics) *Gatherer {
	accessors := make([]*shardedfile.Accessor, len(files))
	result := Gatherer{accessors: accessors, metrics: metrics}
	for idx := range len(files) {
		accessors[idx] = files[idx].OpenAccessor()
	}
	return &result
}

type closersWrapper struct {
	closers []io.Closer
}

func (c *closersWrapper) Close() error {
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Gatherer) GetShardReader(shardId int32, bufferSize int) (*kv.MultiReader, io.Closer) {
	readers := make([]io.Reader, len(e.accessors))
	closers := make([]io.Closer, len(e.accessors))
	for idx := range e.accessors {
		r := e.accessors[idx].GetShardReader(shardId, bufferSize)
		readers[idx] = r
		closers[idx] = r
	}
	return kv.NewMultiReader(readers), &closersWrapper{closers: closers}
}
