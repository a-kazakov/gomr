package pipeline

import (
	"sync/atomic"

	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/metrics"
)

// Collection is a typed data stream backed by a Go channel of batches.
// Each collection must be consumed by exactly one downstream operator.
type Collection[TOut any] struct {
	Pipeline   *Pipeline
	Metrics    *metrics.CollectionMetrics
	outChannel chan primitives.Batch[TOut]
	consumed   bool
}

func (s *Collection[TOut]) isConsumed() bool { return s.consumed }

func (s *Collection[TOut]) GetMetrics() *metrics.CollectionMetrics { return s.Metrics }

func (s *Collection[TOut]) GetPipeline() *Pipeline { return s.Pipeline }

// GetReceiver returns a ChannelReceiver for iterating over this collection.
// Panics if the collection has already been consumed.
func (s *Collection[TOut]) GetReceiver(elementsCounter *atomic.Int64, batchesCounter *atomic.Int64) *primitives.ChannelReceiver[TOut] {
	must.BeTrue(!s.consumed, "collection %q already consumed", s.Metrics.Name)
	s.consumed = true
	return primitives.NewChannelReceiver(s.outChannel, elementsCounter, batchesCounter)
}

// GetRawChannel returns the underlying channel. Panics if already consumed.
func (s *Collection[TOut]) GetRawChannel() chan primitives.Batch[TOut] {
	must.BeTrue(!s.consumed, "collection %q already consumed", s.Metrics.Name)
	s.consumed = true
	return s.outChannel
}

func (s *Collection[TOut]) Close() {
	close(s.outChannel)
	s.Metrics.Completed = true
}

type pipelineParent interface {
	GetPipeline() *Pipeline
}

func CollectionsToParents[T any](collections []*Collection[T]) []pipelineParent {
	result := make([]pipelineParent, 0, len(collections))
	for _, collection := range collections {
		result = append(result, collection)
	}
	return result
}

// NewDerivedCollection creates a new collection derived from parent collections.
// The outChannelCapacity should be the already-resolved capacity value.
func NewDerivedCollection[TNextOut any](name string, batchSize int, outChannelCapacity int, parents ...pipelineParent) (*Collection[TNextOut], chan primitives.Batch[TNextOut]) {
	pipeline := parents[0].GetPipeline()
	nextOutChannel := make(chan primitives.Batch[TNextOut], outChannelCapacity)
	for i := range parents {
		must.BeTrue(parents[i].GetPipeline() == pipeline, "all collections must belong to the same pipeline")
	}
	result := &Collection[TNextOut]{
		Pipeline: pipeline,
		Metrics: pipeline.Metrics.AddCollection(
			name,
			batchSize,
			func() (int, int) { return len(nextOutChannel), cap(nextOutChannel) },
		),
		outChannel: nextOutChannel,
		consumed:   false,
	}
	pipeline.RegisterCollection(result)
	return result, nextOutChannel
}
