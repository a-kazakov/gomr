package primitives

import (
	"sync/atomic"

	"github.com/a-kazakov/gomr/internal/must"
)

// Emitter is the write side of a pipeline collection.
// It buffers emitted values into batches of batchSize and sends them
// downstream via a channel. Supports buffer recycling.
type Emitter[TOut any] struct {
	batchSize       int
	buffer          []TOut
	bufferIndex     int
	channel         chan Batch[TOut]
	recycleChannel  chan []TOut
	elementsCounter *atomic.Int64
	batchesCounter  *atomic.Int64
}

func NewEmitter[TOut any](
	defaultParallelism int,
	batchSize int,
	channel chan Batch[TOut],
	elementsCounter *atomic.Int64,
	batchesCounter *atomic.Int64,
) *Emitter[TOut] {
	return &Emitter[TOut]{
		batchSize:       batchSize,
		buffer:          make([]TOut, batchSize),
		channel:         channel,
		recycleChannel:  make(chan []TOut, 2*defaultParallelism),
		elementsCounter: elementsCounter,
		batchesCounter:  batchesCounter,
	}
}

// GetEmitPointer returns a pointer to the next buffer slot for zero-copy emission.
// Flushes the current batch when full.
func (e *Emitter[TOut]) GetEmitPointer() *TOut {
	if e.bufferIndex >= e.batchSize {
		e.channel <- Batch[TOut]{
			Values:         e.buffer,
			recycleChannel: e.recycleChannel,
		}
		e.batchesCounter.Add(1)
		e.elementsCounter.Add(int64(e.bufferIndex))
		select {
		case recycledBuffer := <-e.recycleChannel:
			must.BeTrue(cap(recycledBuffer) == e.batchSize, "Recycled buffer has wrong capacity: %d != %d", cap(recycledBuffer), e.batchSize)
			e.buffer = recycledBuffer[:e.batchSize]
		default:
			e.buffer = make([]TOut, e.batchSize)
		}
		e.bufferIndex = 0
	}
	result := &e.buffer[e.bufferIndex]
	e.bufferIndex++
	return result
}

func (e *Emitter[TOut]) Emit(value *TOut) {
	*e.GetEmitPointer() = *value
}

func (e *Emitter[TOut]) Close() {
	if e.bufferIndex > 0 {
		e.channel <- Batch[TOut]{
			Values:         e.buffer[:e.bufferIndex],
			recycleChannel: e.recycleChannel,
		}
		e.elementsCounter.Add(int64(e.bufferIndex))
		e.batchesCounter.Add(1)
	}
	// Purge recycled channel
	e.recycleChannel = nil
	e.buffer = nil
	e.channel = nil
}
