package primitives

// Tests use package-internal access for: NewEmitter, NewChannelReceiver, NewIteratorReceiver constructors, SetFirstBatchHook.

import (
	"sync/atomic"
	"testing"
)

// =============================================================================
// Batch
// =============================================================================

func TestBatch(t *testing.T) {
	t.Run("recycle with no ref count", func(t *testing.T) {
		recycleCh := make(chan []int, 10)
		values := []int{1, 2, 3}
		b := Batch[int]{Values: values, recycleChannel: recycleCh}
		b.Recycle()
		select {
		case recycled := <-recycleCh:
			if len(recycled) != 3 {
				t.Errorf("recycled len = %d, want 3", len(recycled))
			}
		default:
			t.Error("expected values to be recycled")
		}
	})

	t.Run("recycle with ref count not zero", func(t *testing.T) {
		recycleCh := make(chan []int, 10)
		values := []int{1, 2, 3}
		b := Batch[int]{Values: values, recycleChannel: recycleCh}
		forked := b.Fork()
		// Now refCount = 2 (original got 1, fork added 1)
		forked.Recycle()
		select {
		case <-recycleCh:
			t.Error("should not recycle when refCount > 0")
		default:
		}
	})

	t.Run("recycle full channel", func(t *testing.T) {
		recycleCh := make(chan []int, 0) // zero capacity, cannot receive
		values := []int{1, 2, 3}
		b := Batch[int]{Values: values, recycleChannel: recycleCh}
		// Should not block; discards the batch
		b.Recycle()
	})

	t.Run("fork shares values", func(t *testing.T) {
		recycleCh := make(chan []int, 10)
		values := []int{1, 2, 3}
		b := Batch[int]{Values: values, recycleChannel: recycleCh}
		forked := b.Fork()
		// Both should share the same values
		if &forked.Values[0] != &b.Values[0] {
			t.Error("forked batch should share the same values slice")
		}
	})

	t.Run("fork multiple and recycle all", func(t *testing.T) {
		recycleCh := make(chan []int, 10)
		values := []int{10, 20}
		b := Batch[int]{Values: values, recycleChannel: recycleCh}
		f1 := b.Fork()
		f2 := b.Fork()
		// refCount should be 3 (original=1, +1 for f1, +1 for f2)
		f1.Recycle()
		select {
		case <-recycleCh:
			t.Error("should not recycle after first recycle")
		default:
		}
		f2.Recycle()
		select {
		case <-recycleCh:
			t.Error("should not recycle after second recycle")
		default:
		}
		// Original recycle should trigger actual recycling
		b.Recycle()
		select {
		case recycled := <-recycleCh:
			if len(recycled) != 2 {
				t.Errorf("recycled len = %d, want 2", len(recycled))
			}
		default:
			t.Error("expected final recycle to return values to channel")
		}
	})
}

// =============================================================================
// Emitter
// =============================================================================

func newTestEmitter(batchSize int) (*Emitter[int], chan Batch[int], *atomic.Int64, *atomic.Int64) {
	ch := make(chan Batch[int], 100)
	elemCounter := &atomic.Int64{}
	batchCounter := &atomic.Int64{}
	e := NewEmitter[int](1, batchSize, ch, elemCounter, batchCounter)
	return e, ch, elemCounter, batchCounter
}

func TestEmitter(t *testing.T) {
	t.Run("single batch flush on full", func(t *testing.T) {
		e, ch, _, _ := newTestEmitter(3)
		*e.GetEmitPointer() = 10
		*e.GetEmitPointer() = 20
		*e.GetEmitPointer() = 30
		// Buffer should be full now, next call should flush
		*e.GetEmitPointer() = 40
		select {
		case batch := <-ch:
			if len(batch.Values) != 3 {
				t.Errorf("batch len = %d, want 3", len(batch.Values))
			}
			if batch.Values[0] != 10 || batch.Values[1] != 20 || batch.Values[2] != 30 {
				t.Errorf("batch values = %v, want [10 20 30]", batch.Values)
			}
		default:
			t.Error("expected batch to be sent")
		}
	})

	t.Run("emit by pointer", func(t *testing.T) {
		e, ch, _, _ := newTestEmitter(2)
		v1, v2 := 42, 43
		e.Emit(&v1)
		e.Emit(&v2)
		// Buffer full, next emit flushes
		v3 := 44
		e.Emit(&v3)
		select {
		case batch := <-ch:
			if batch.Values[0] != 42 || batch.Values[1] != 43 {
				t.Errorf("batch values = %v, want [42 43]", batch.Values)
			}
		default:
			t.Error("expected batch")
		}
	})

	t.Run("close flushes partial batch", func(t *testing.T) {
		e, ch, elemCounter, batchCounter := newTestEmitter(10)
		*e.GetEmitPointer() = 1
		*e.GetEmitPointer() = 2
		e.Close()
		select {
		case batch := <-ch:
			if len(batch.Values) != 2 {
				t.Errorf("batch len = %d, want 2", len(batch.Values))
			}
		default:
			t.Error("expected partial batch to be flushed")
		}
		if elemCounter.Load() != 2 {
			t.Errorf("elemCounter = %d, want 2", elemCounter.Load())
		}
		if batchCounter.Load() != 1 {
			t.Errorf("batchCounter = %d, want 1", batchCounter.Load())
		}
	})

	t.Run("close empty is no-op", func(t *testing.T) {
		e, ch, _, _ := newTestEmitter(10)
		e.Close()
		select {
		case <-ch:
			t.Error("empty emitter should not send batch on close")
		default:
		}
	})

	t.Run("close nils fields", func(t *testing.T) {
		e, _, _, _ := newTestEmitter(10)
		e.Close()
		if e.buffer != nil || e.channel != nil || e.recycleChannel != nil {
			t.Error("Close should nil buffer, channel, recycleChannel")
		}
	})

	t.Run("counters track elements and batches", func(t *testing.T) {
		e, ch, elemCounter, batchCounter := newTestEmitter(2)
		*e.GetEmitPointer() = 1
		*e.GetEmitPointer() = 2
		*e.GetEmitPointer() = 3 // triggers flush of first batch
		e.Close()

		// Drain channel
		<-ch
		<-ch

		if elemCounter.Load() != 3 {
			t.Errorf("elemCounter = %d, want 3", elemCounter.Load())
		}
		if batchCounter.Load() != 2 {
			t.Errorf("batchCounter = %d, want 2", batchCounter.Load())
		}
	})

	t.Run("buffer recycling reuses slices", func(t *testing.T) {
		e, ch, _, _ := newTestEmitter(2)
		// Emit two values to fill the batch
		*e.GetEmitPointer() = 1
		*e.GetEmitPointer() = 2
		// Third emit triggers flush of batch [1,2]
		*e.GetEmitPointer() = 3
		// Receive the flushed batch and recycle it
		batch := <-ch
		batch.Recycle()
		// Now emit more to trigger another flush - should try to reuse recycled buffer
		*e.GetEmitPointer() = 4
		*e.GetEmitPointer() = 5 // triggers flush, should reuse recycled buffer
		e.Close()
		// Drain remaining
		<-ch
	})

	t.Run("recycle wrong capacity panics", func(t *testing.T) {
		e, ch, _, _ := newTestEmitter(2)
		// Fill the batch
		*e.GetEmitPointer() = 1
		*e.GetEmitPointer() = 2
		// Third emit triggers flush, putting a batch on ch
		// But first, preload the recycle channel with a wrong-capacity buffer
		wrongBuffer := make([]int, 5) // capacity 5 != batchSize 2
		e.recycleChannel <- wrongBuffer

		defer func() {
			r := recover()
			if r == nil {
				t.Error("expected panic for wrong capacity recycled buffer")
			}
			// Drain the channel to avoid goroutine leak
			select {
			case <-ch:
			default:
			}
		}()

		// This triggers flush which tries to recycle from recycleChannel
		*e.GetEmitPointer() = 3
	})

	t.Run("multiple batches", func(t *testing.T) {
		e, ch, _, _ := newTestEmitter(2)
		for i := 0; i < 6; i++ {
			*e.GetEmitPointer() = i
		}
		e.Close()
		// Should have 3 batches of 2
		count := 0
		for range 3 {
			<-ch
			count++
		}
		if count != 3 {
			t.Errorf("got %d batches, want 3", count)
		}
	})
}

// =============================================================================
// ChannelReceiver
// =============================================================================

func newTestReceiver(values ...int) (*ChannelReceiver[int], chan Batch[int]) {
	ch := make(chan Batch[int], 10)
	elemCounter := &atomic.Int64{}
	batchCounter := &atomic.Int64{}
	r := NewChannelReceiver[int](ch, elemCounter, batchCounter)

	// Send values as a single batch
	if len(values) > 0 {
		recycleCh := make(chan []int, 1)
		ch <- Batch[int]{Values: values, recycleChannel: recycleCh}
	}
	close(ch)
	return r, ch
}

func TestChannelReceiver(t *testing.T) {
	t.Run("iter values", func(t *testing.T) {
		r, _ := newTestReceiver(10, 20, 30)
		var collected []int
		for v := range r.IterValues() {
			collected = append(collected, *v)
		}
		if len(collected) != 3 || collected[0] != 10 || collected[1] != 20 || collected[2] != 30 {
			t.Errorf("collected = %v, want [10 20 30]", collected)
		}
	})

	t.Run("iter values updates counters", func(t *testing.T) {
		ch := make(chan Batch[int], 10)
		elemCounter := &atomic.Int64{}
		batchCounter := &atomic.Int64{}
		r := NewChannelReceiver[int](ch, elemCounter, batchCounter)
		recycleCh := make(chan []int, 1)
		ch <- Batch[int]{Values: []int{1, 2, 3}, recycleChannel: recycleCh}
		close(ch)
		for range r.IterValues() {
		}
		if elemCounter.Load() != 3 {
			t.Errorf("elemCounter = %d, want 3", elemCounter.Load())
		}
		if batchCounter.Load() != 1 {
			t.Errorf("batchCounter = %d, want 1", batchCounter.Load())
		}
	})

	t.Run("iter batches", func(t *testing.T) {
		r, _ := newTestReceiver(10, 20, 30)
		var batches int
		for batch := range r.IterBatches() {
			if len(batch) != 3 {
				t.Errorf("batch len = %d, want 3", len(batch))
			}
			batches++
		}
		if batches != 1 {
			t.Errorf("batches = %d, want 1", batches)
		}
	})

	t.Run("iter batches no recycle", func(t *testing.T) {
		r, _ := newTestReceiver(10, 20, 30)
		var batches int
		for batch := range r.IterBatchesNoRecycle() {
			if len(batch.Values) != 3 {
				t.Errorf("batch values len = %d, want 3", len(batch.Values))
			}
			batch.Recycle() // caller responsibility
			batches++
		}
		if batches != 1 {
			t.Errorf("batches = %d, want 1", batches)
		}
	})

	t.Run("first batch hook called once", func(t *testing.T) {
		r, _ := newTestReceiver(1, 2, 3)
		called := 0
		r.SetFirstBatchHook(func() { called++ })
		for range r.IterValues() {
		}
		if called != 1 {
			t.Errorf("firstBatchHook called %d times, want 1", called)
		}
	})

	t.Run("first batch hook not called on empty channel", func(t *testing.T) {
		r, _ := newTestReceiver() // no batches
		called := false
		r.SetFirstBatchHook(func() { called = true })
		for range r.IterValues() {
		}
		if called {
			t.Error("firstBatchHook should not be called on empty channel")
		}
	})

	t.Run("first batch hook on iter batches", func(t *testing.T) {
		ch := make(chan Batch[int], 10)
		elemCounter := &atomic.Int64{}
		batchCounter := &atomic.Int64{}
		r := NewChannelReceiver[int](ch, elemCounter, batchCounter)
		called := 0
		r.SetFirstBatchHook(func() { called++ })

		recycleCh := make(chan []int, 2)
		ch <- Batch[int]{Values: []int{1}, recycleChannel: recycleCh}
		ch <- Batch[int]{Values: []int{2}, recycleChannel: recycleCh}
		close(ch)

		for range r.IterBatches() {
		}
		if called != 1 {
			t.Errorf("firstBatchHook called %d times, want 1", called)
		}
	})

	t.Run("first batch hook on iter batches no recycle", func(t *testing.T) {
		ch := make(chan Batch[int], 10)
		elemCounter := &atomic.Int64{}
		batchCounter := &atomic.Int64{}
		r := NewChannelReceiver[int](ch, elemCounter, batchCounter)
		called := 0
		r.SetFirstBatchHook(func() { called++ })

		recycleCh := make(chan []int, 2)
		ch <- Batch[int]{Values: []int{1}, recycleChannel: recycleCh}
		ch <- Batch[int]{Values: []int{2}, recycleChannel: recycleCh}
		close(ch)

		for batch := range r.IterBatchesNoRecycle() {
			batch.Recycle()
		}
		if called != 1 {
			t.Errorf("firstBatchHook called %d times, want 1", called)
		}
	})

	t.Run("early break recycles batch", func(t *testing.T) {
		ch := make(chan Batch[int], 10)
		elemCounter := &atomic.Int64{}
		batchCounter := &atomic.Int64{}
		r := NewChannelReceiver[int](ch, elemCounter, batchCounter)
		recycleCh := make(chan []int, 1)
		ch <- Batch[int]{Values: []int{1, 2, 3, 4, 5}, recycleChannel: recycleCh}
		close(ch)
		// Break after first value
		for range r.IterValues() {
			break
		}
		// Should have recycled the batch
		select {
		case <-recycleCh:
		default:
			t.Error("batch should be recycled on early break")
		}
	})

	t.Run("iter batches early break", func(t *testing.T) {
		ch := make(chan Batch[int], 10)
		elemCounter := &atomic.Int64{}
		batchCounter := &atomic.Int64{}
		r := NewChannelReceiver[int](ch, elemCounter, batchCounter)
		recycleCh := make(chan []int, 1)
		ch <- Batch[int]{Values: []int{1, 2}, recycleChannel: recycleCh}
		ch <- Batch[int]{Values: []int{3, 4}, recycleChannel: recycleCh}
		close(ch)
		for range r.IterBatches() {
			break
		}
	})

	t.Run("iter batches no recycle early break", func(t *testing.T) {
		ch := make(chan Batch[int], 10)
		elemCounter := &atomic.Int64{}
		batchCounter := &atomic.Int64{}
		r := NewChannelReceiver[int](ch, elemCounter, batchCounter)
		recycleCh := make(chan []int, 1)
		ch <- Batch[int]{Values: []int{1, 2}, recycleChannel: recycleCh}
		ch <- Batch[int]{Values: []int{3, 4}, recycleChannel: recycleCh}
		close(ch)
		for range r.IterBatchesNoRecycle() {
			break
		}
	})

	t.Run("empty channel yields nothing", func(t *testing.T) {
		r, _ := newTestReceiver() // empty
		count := 0
		for range r.IterValues() {
			count++
		}
		if count != 0 {
			t.Errorf("count = %d, want 0", count)
		}
	})

	t.Run("multi batch iter values", func(t *testing.T) {
		ch := make(chan Batch[int], 10)
		elemCounter := &atomic.Int64{}
		batchCounter := &atomic.Int64{}
		r := NewChannelReceiver[int](ch, elemCounter, batchCounter)

		recycleCh := make(chan []int, 2)
		ch <- Batch[int]{Values: []int{1, 2}, recycleChannel: recycleCh}
		ch <- Batch[int]{Values: []int{3, 4}, recycleChannel: recycleCh}
		close(ch)

		var collected []int
		for v := range r.IterValues() {
			collected = append(collected, *v)
		}
		if len(collected) != 4 {
			t.Errorf("collected %d values, want 4", len(collected))
		}
		if elemCounter.Load() != 4 {
			t.Errorf("elemCounter = %d, want 4", elemCounter.Load())
		}
		if batchCounter.Load() != 2 {
			t.Errorf("batchCounter = %d, want 2", batchCounter.Load())
		}
	})
}

// =============================================================================
// IteratorReceiver
// =============================================================================

func TestIteratorReceiver(t *testing.T) {
	t.Run("iter values", func(t *testing.T) {
		values := []int{10, 20, 30}
		idx := 0
		iter := func(yield func(*int) bool) {
			for idx < len(values) {
				if !yield(&values[idx]) {
					return
				}
				idx++
			}
		}
		ir := NewIteratorReceiver[int](iter)
		var collected []int
		for v := range ir.IterValues() {
			collected = append(collected, *v)
		}
		if len(collected) != 3 {
			t.Errorf("collected = %v, want [10 20 30]", collected)
		}
		// Should nil the iterator after use
		if ir.iterator != nil {
			t.Error("iterator should be nil after IterValues")
		}
	})

	t.Run("ensure used on unused receiver", func(t *testing.T) {
		called := false
		iter := func(yield func(*int) bool) {
			called = true
			v := 42
			yield(&v)
		}
		ir := NewIteratorReceiver[int](iter)
		ir.EnsureUsed()
		if !called {
			t.Error("EnsureUsed should start the iterator")
		}
		if ir.iterator != nil {
			t.Error("iterator should be nil after EnsureUsed")
		}
	})

	t.Run("ensure used on already used receiver", func(t *testing.T) {
		iter := func(yield func(*int) bool) {
			v := 42
			yield(&v)
		}
		ir := NewIteratorReceiver[int](iter)
		for range ir.IterValues() {
		}
		// Should be a no-op
		ir.EnsureUsed()
	})
}
