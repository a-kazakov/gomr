package countermap_test

import (
	"sync"
	"testing"

	"github.com/a-kazakov/gomr/internal/countermap"
)

func TestCountersMap(t *testing.T) {
	t.Run("new map is empty", func(t *testing.T) {
		cm := countermap.NewCountersMap()
		if cm == nil {
			t.Fatal("NewCountersMap returned nil")
		}
		snap := cm.Snapshot()
		if len(snap) != 0 {
			t.Errorf("new map snapshot should be empty, got %d entries", len(snap))
		}
	})

	t.Run("get counter creates new counter", func(t *testing.T) {
		cm := countermap.NewCountersMap()
		c := cm.GetCounter("test")
		if c == nil {
			t.Fatal("GetCounter returned nil")
		}
		if c.Load() != 0 {
			t.Errorf("new counter should be 0, got %d", c.Load())
		}
	})

	t.Run("get counter returns existing counter", func(t *testing.T) {
		cm := countermap.NewCountersMap()
		c1 := cm.GetCounter("test")
		c1.Add(42)
		c2 := cm.GetCounter("test")
		if c1 != c2 {
			t.Error("GetCounter should return the same counter for the same name")
		}
		if c2.Load() != 42 {
			t.Errorf("counter value should be 42, got %d", c2.Load())
		}
	})

	t.Run("get counter is safe for concurrent access", func(t *testing.T) {
		cm := countermap.NewCountersMap()
		const goroutines = 100
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				c := cm.GetCounter("shared")
				c.Add(1)
			}()
		}
		wg.Wait()
		if cm.GetCounter("shared").Load() != goroutines {
			t.Errorf("expected %d, got %d", goroutines, cm.GetCounter("shared").Load())
		}
	})

	t.Run("snapshot returns populated entries", func(t *testing.T) {
		cm := countermap.NewCountersMap()
		cm.GetCounter("a").Add(10)
		cm.GetCounter("b").Add(20)
		snap := cm.Snapshot()
		if len(snap) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(snap))
		}
		if snap["a"] != 10 {
			t.Errorf("snap[a] = %d, want 10", snap["a"])
		}
		if snap["b"] != 20 {
			t.Errorf("snap[b] = %d, want 20", snap["b"])
		}
	})

	t.Run("snapshot of empty map has no entries", func(t *testing.T) {
		cm := countermap.NewCountersMap()
		snap := cm.Snapshot()
		if len(snap) != 0 {
			t.Errorf("empty map snapshot should have 0 entries, got %d", len(snap))
		}
	})

	t.Run("snapshot is isolated from the map", func(t *testing.T) {
		cm := countermap.NewCountersMap()
		cm.GetCounter("x").Add(5)
		snap := cm.Snapshot()
		// Modifying snapshot should not affect the map
		snap["x"] = 999
		snap["y"] = 100
		if cm.GetCounter("x").Load() != 5 {
			t.Error("modifying snapshot should not affect counters")
		}
		snap2 := cm.Snapshot()
		if _, ok := snap2["y"]; ok {
			t.Error("snapshot modification should not create new counters")
		}
	})
}
