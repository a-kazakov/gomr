// Package countermap provides a thread-safe named counter registry.
// It implements core.UserCountersMap and is used by the metrics and pipeline subsystems.
package countermap

import (
	"sync"
	"sync/atomic"
)

// CountersMap is a thread-safe map of named atomic counters.
// Counters are created lazily on first access via GetCounter.
type CountersMap struct {
	mu       *sync.Mutex
	counters map[string]*atomic.Int64
}

func NewCountersMap() *CountersMap {
	return &CountersMap{
		mu:       &sync.Mutex{},
		counters: make(map[string]*atomic.Int64),
	}
}

func (c *CountersMap) GetCounter(name string) *atomic.Int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	counter, ok := c.counters[name]
	if !ok {
		counter = &atomic.Int64{}
		c.counters[name] = counter
	}
	return counter
}

func (c *CountersMap) Snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	snapshot := make(map[string]int64, len(c.counters))
	for name, counter := range c.counters {
		snapshot[name] = counter.Load()
	}
	return snapshot
}
