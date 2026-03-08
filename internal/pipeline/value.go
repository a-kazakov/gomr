package pipeline

import (
	"sync"
	"sync/atomic"

	"github.com/a-kazakov/gomr/metrics"
)

// Value is a future/promise holding a single computed value.
// Resolve sets the value exactly once; Wait blocks until resolved.
type Value[T any] struct {
	Pipeline *Pipeline
	Metrics  *metrics.ValueMetrics
	Value    T
	waitChan chan struct{}
	done     atomic.Bool
	once     sync.Once
}

func (v *Value[T]) GetPipeline() *Pipeline { return v.Pipeline }

func NewValue[T any](pipeline *Pipeline, name string) *Value[T] {
	result := &Value[T]{
		Pipeline: pipeline,
		waitChan: make(chan struct{}),
	}
	result.Metrics = pipeline.Metrics.AddValue(name, func() bool { return result.done.Load() })
	return result
}

// Wait blocks until the value is resolved, then returns it.
func (v *Value[T]) Wait() T {
	if v.done.Load() {
		return v.Value
	}
	<-v.waitChan
	return v.Value
}

// Resolve sets the value and signals all waiters. Only the first call takes effect.
func (v *Value[T]) Resolve(value T) {
	v.once.Do(func() {
		v.Value = value
		v.done.Store(true)
		close(v.waitChan)
	})
}

// Ready returns a channel that is closed when the value is resolved. For use in select statements.
func (v *Value[T]) Ready() <-chan struct{} {
	return v.waitChan
}
