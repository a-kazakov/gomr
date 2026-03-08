// Package primitives provides the low-level building blocks for pipeline data flow:
// Batch, Emitter, and Receiver types.
package primitives

import (
	"sync"
	"sync/atomic"
)

var atomicsPool = sync.Pool{
	New: func() any {
		return new(atomic.Int64)
	},
}

// Batch wraps a []T slice with reference-counted recycling.
// When the last reference is recycled, the underlying slice is returned
// to the recycleChannel for reuse by the Emitter.
type Batch[T any] struct {
	Values         []T
	recycleChannel chan []T
	refCount       *atomic.Int64 // nil unless forked; tracks number of live references
}

// Fork creates a shallow copy of the batch sharing the same Values slice.
// The first call lazily initializes a ref count from atomicsPool.
// Must be called from a non-concurrent context on the first invocation.
func (b *Batch[T]) Fork() Batch[T] {
	if b.refCount == nil {
		b.refCount = atomicsPool.Get().(*atomic.Int64)
		b.refCount.Store(1)
	}
	b.refCount.Add(1)
	return *b
}

// Recycle decrements the reference count and returns the backing slice
// to the recycleChannel when the count reaches zero.
// If the channel is full, the slice is discarded.
func (b *Batch[T]) Recycle() {
	if b.refCount != nil {
		newCount := b.refCount.Add(-1)
		if newCount > 0 {
			return
		}
		atomicsPool.Put(b.refCount)
		b.refCount = nil
	}
	select {
	case b.recycleChannel <- b.Values:
	default:
	}
}
