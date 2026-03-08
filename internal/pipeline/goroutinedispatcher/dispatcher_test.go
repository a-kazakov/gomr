package goroutinedispatcher_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/a-kazakov/gomr/internal/pipeline/goroutinedispatcher"
)

func TestGoroutineDispatcher(t *testing.T) {
	t.Run("new dispatcher is non-nil", func(t *testing.T) {
		d := goroutinedispatcher.NewGoroutineDispatcher()
		if d == nil {
			t.Fatal("NewGoroutineDispatcher returned nil")
		}
	})

	t.Run("single tracked goroutine executes", func(t *testing.T) {
		d := goroutinedispatcher.NewGoroutineDispatcher()
		var executed atomic.Bool
		d.StartTrackedGoroutine(func() {
			executed.Store(true)
		})
		d.WaitForAllGoroutinesToFinish()
		if !executed.Load() {
			t.Error("goroutine was not executed")
		}
	})

	t.Run("multiple tracked goroutines execute", func(t *testing.T) {
		d := goroutinedispatcher.NewGoroutineDispatcher()
		var count atomic.Int32
		for i := 0; i < 5; i++ {
			d.StartTrackedGoroutine(func() {
				count.Add(1)
			})
		}
		d.WaitForAllGoroutinesToFinish()
		if count.Load() != 5 {
			t.Errorf("count = %d, want 5", count.Load())
		}
	})

	t.Run("parallel goroutines call all shard indices", func(t *testing.T) {
		d := goroutinedispatcher.NewGoroutineDispatcher()
		var indices sync.Map
		var cleanupCalled atomic.Bool
		d.StartParallelTrackedGoroutines(4, func(shardIndex int) {
			indices.Store(shardIndex, true)
		}, func() {
			cleanupCalled.Store(true)
		})
		d.WaitForAllGoroutinesToFinish()
		// Verify all indices were called
		for i := 0; i < 4; i++ {
			if _, ok := indices.Load(i); !ok {
				t.Errorf("shard index %d was not called", i)
			}
		}
		if !cleanupCalled.Load() {
			t.Error("cleanup was not called")
		}
	})

	t.Run("parallel goroutines cleanup runs after all work", func(t *testing.T) {
		d := goroutinedispatcher.NewGoroutineDispatcher()
		var workDone atomic.Int32
		var workCountAtCleanup int32
		d.StartParallelTrackedGoroutines(3, func(shardIndex int) {
			workDone.Add(1)
		}, func() {
			workCountAtCleanup = workDone.Load()
		})
		d.WaitForAllGoroutinesToFinish()
		if workCountAtCleanup != 3 {
			t.Errorf("workCountAtCleanup = %d, want 3 (cleanup should run after all work)", workCountAtCleanup)
		}
	})

	t.Run("wait with no goroutines does not block", func(t *testing.T) {
		d := goroutinedispatcher.NewGoroutineDispatcher()
		// Should not block with no goroutines
		d.WaitForAllGoroutinesToFinish()
	})

	t.Run("mixed tracked and parallel goroutines", func(t *testing.T) {
		d := goroutinedispatcher.NewGoroutineDispatcher()
		var count atomic.Int32
		d.StartTrackedGoroutine(func() {
			count.Add(1)
		})
		d.StartParallelTrackedGoroutines(2, func(shardIndex int) {
			count.Add(1)
		}, func() {
			count.Add(1)
		})
		d.WaitForAllGoroutinesToFinish()
		if count.Load() != 4 {
			t.Errorf("count = %d, want 4 (1 tracked + 2 parallel + 1 cleanup)", count.Load())
		}
	})
}
