package primitives

import (
	"iter"
	"sync/atomic"
)

// ChannelReceiver is the read side of a pipeline collection channel.
// Provides iterator-based access with batch recycling and metrics counters.
type ChannelReceiver[TIn any] struct {
	channel          chan Batch[TIn]
	elementsCounter  *atomic.Int64
	batchesCounter   *atomic.Int64
	firstBatchHook   func()
	firstBatchCalled atomic.Bool
}

func NewChannelReceiver[TIn any](channel chan Batch[TIn], elementsCounter *atomic.Int64, batchesCounter *atomic.Int64) *ChannelReceiver[TIn] {
	return &ChannelReceiver[TIn]{
		channel:         channel,
		elementsCounter: elementsCounter,
		batchesCounter:  batchesCounter,
	}
}

// SetFirstBatchHook sets a callback that will be called exactly once when the first batch is received.
// This is thread-safe and can be called from multiple goroutines - only the first invocation succeeds.
// The hook is called before the batch is yielded to the consumer.
func (r *ChannelReceiver[TIn]) SetFirstBatchHook(hook func()) {
	r.firstBatchHook = hook
}

func (r *ChannelReceiver[TIn]) callFirstBatchHookOnce() {
	if r.firstBatchHook != nil && r.firstBatchCalled.CompareAndSwap(false, true) {
		r.firstBatchHook()
	}
}

func (r *ChannelReceiver[TIn]) IterValues() iter.Seq[*TIn] {
	return func(yield func(*TIn) bool) {
		for batch := range r.channel {
			r.callFirstBatchHookOnce()
			values := batch.Values
			for idx := range values {
				if !yield(&values[idx]) {
					batch.Recycle()
					return
				}
			}
			r.elementsCounter.Add(int64(len(values)))
			r.batchesCounter.Add(1)
			batch.Recycle()
		}
	}
}

func (r *ChannelReceiver[TIn]) IterBatches() iter.Seq[[]TIn] {
	return func(yield func([]TIn) bool) {
		for batch := range r.channel {
			r.callFirstBatchHookOnce()
			if !yield(batch.Values) {
				batch.Recycle()
				return
			}
			r.elementsCounter.Add(int64(len(batch.Values)))
			r.batchesCounter.Add(1)
			batch.Recycle()
		}
	}
}

// IterBatchesNoRecycle yields raw Batch structs; caller is responsible for recycling.
func (r *ChannelReceiver[TIn]) IterBatchesNoRecycle() iter.Seq[Batch[TIn]] {
	return func(yield func(Batch[TIn]) bool) {
		for batch := range r.channel {
			r.callFirstBatchHookOnce()
			if !yield(batch) {
				return
			}
			r.elementsCounter.Add(int64(len(batch.Values)))
			r.batchesCounter.Add(1)
		}
	}
}

// IteratorReceiver wraps an iter.Seq[*TIn] to satisfy ShuffleReceiver.
// The iterator is consumed exactly once; subsequent calls return nil.
type IteratorReceiver[TIn any] struct {
	iterator iter.Seq[*TIn]
}

func NewIteratorReceiver[TIn any](iterator iter.Seq[*TIn]) IteratorReceiver[TIn] {
	return IteratorReceiver[TIn]{
		iterator: iterator,
	}
}

func (r *IteratorReceiver[TIn]) IterValues() iter.Seq[*TIn] {
	defer func() { r.iterator = nil }()
	return r.iterator
}

func (r *IteratorReceiver[TIn]) EnsureUsed() {
	if r.iterator != nil {
		for range r.iterator {
			break
		}
		r.iterator = nil
	}
}
