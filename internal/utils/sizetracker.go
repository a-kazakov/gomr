// Package utils provides small utility functions used across the gomr framework.
package utils

import "sync/atomic"

// SplittableSizeTracker is a hierarchical atomic size tracker.
// Additions propagate up to the parent, so a parent tracker sees the aggregate
// of all its children. Used by shard writers to track total disk usage.
type SplittableSizeTracker struct {
	parent   *SplittableSizeTracker
	value    atomic.Int64
	callback func()
}

func NewSplittableSizeTracker() *SplittableSizeTracker {
	return &SplittableSizeTracker{
		parent: nil,
		value:  atomic.Int64{},
	}
}

func (t *SplittableSizeTracker) Add(size int64) {
	t.value.Add(size)
	if t.parent != nil {
		t.parent.Add(size)
	}
	if t.callback != nil {
		t.callback()
	}
}

func (t *SplittableSizeTracker) Get() int64 {
	return t.value.Load()
}

func (t *SplittableSizeTracker) Set(value int64) {
	delta := value - t.Get()
	t.Add(delta)
}

func (t *SplittableSizeTracker) SetZero() {
	t.Set(0)
}

func (t *SplittableSizeTracker) CreateChild() *SplittableSizeTracker {
	return &SplittableSizeTracker{
		parent: t,
		value:  atomic.Int64{},
	}
}

func (t *SplittableSizeTracker) SetCallback(callback func()) {
	t.callback = callback
}
