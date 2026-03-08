package utils_test

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/parameters"
)

func TestSizeTracker(t *testing.T) {
	t.Run("add and get accumulates values", func(t *testing.T) {
		st := utils.NewSplittableSizeTracker()
		st.Add(100)
		st.Add(50)
		if st.Get() != 150 {
			t.Errorf("Get() = %d, want 150", st.Get())
		}
	})

	t.Run("set overwrites current value", func(t *testing.T) {
		st := utils.NewSplittableSizeTracker()
		st.Add(100)
		st.Set(42)
		if st.Get() != 42 {
			t.Errorf("Get() = %d, want 42", st.Get())
		}
	})

	t.Run("set zero resets to zero", func(t *testing.T) {
		st := utils.NewSplittableSizeTracker()
		st.Add(100)
		st.SetZero()
		if st.Get() != 0 {
			t.Errorf("Get() = %d, want 0", st.Get())
		}
	})

	t.Run("child propagates to parent", func(t *testing.T) {
		parent := utils.NewSplittableSizeTracker()
		child := parent.CreateChild()
		child.Add(50)
		if child.Get() != 50 {
			t.Errorf("child.Get() = %d, want 50", child.Get())
		}
		if parent.Get() != 50 {
			t.Errorf("parent.Get() = %d, want 50", parent.Get())
		}
		parent.Add(10)
		if parent.Get() != 60 {
			t.Errorf("parent.Get() = %d, want 60", parent.Get())
		}
		if child.Get() != 50 {
			t.Errorf("child.Get() should not be affected by parent.Add, got %d", child.Get())
		}
	})

	t.Run("callback fires on each add", func(t *testing.T) {
		st := utils.NewSplittableSizeTracker()
		var callCount int
		st.SetCallback(func() { callCount++ })
		st.Add(10)
		st.Add(20)
		if callCount != 2 {
			t.Errorf("callback called %d times, want 2", callCount)
		}
	})

	t.Run("concurrent adds are safe", func(t *testing.T) {
		st := utils.NewSplittableSizeTracker()
		const goroutines = 100
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				st.Add(1)
			}()
		}
		wg.Wait()
		if st.Get() != goroutines {
			t.Errorf("Get() = %d, want %d", st.Get(), goroutines)
		}
	})

	t.Run("child set propagates delta to parent", func(t *testing.T) {
		parent := utils.NewSplittableSizeTracker()
		child := parent.CreateChild()
		child.Add(100)
		child.Set(30)
		if child.Get() != 30 {
			t.Errorf("child.Get() = %d, want 30", child.Get())
		}
		if parent.Get() != 30 {
			t.Errorf("parent.Get() = %d, want 30 (delta should propagate)", parent.Get())
		}
	})

	t.Run("no callback does not panic", func(t *testing.T) {
		st := utils.NewSplittableSizeTracker()
		// Should not panic when no callback is set
		st.Add(10)
	})

	t.Run("child callback not inherited from parent", func(t *testing.T) {
		parent := utils.NewSplittableSizeTracker()
		var parentCalls atomic.Int32
		parent.SetCallback(func() { parentCalls.Add(1) })
		child := parent.CreateChild()
		child.Add(10)
		// Parent callback fires when child propagates
		if parentCalls.Load() != 1 {
			t.Errorf("parent callback called %d times, want 1", parentCalls.Load())
		}
	})

	t.Run("disable os read ahead smoke test", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "readahead")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		// Should not panic
		utils.DisableOSReadAhead(f)
	})
}

func TestFilterNilsInPlace(t *testing.T) {
	t.Run("mixed nils and values", func(t *testing.T) {
		a, b := 1, 2
		s := []*int{&a, nil, &b, nil}
		n := utils.FilterNilsInPlace(s)
		if n != 2 {
			t.Fatalf("returned %d, want 2", n)
		}
		if *s[0] != 1 || *s[1] != 2 {
			t.Errorf("s[:2] = [%d, %d], want [1, 2]", *s[0], *s[1])
		}
	})

	t.Run("all nil", func(t *testing.T) {
		s := []*int{nil, nil, nil}
		n := utils.FilterNilsInPlace(s)
		if n != 0 {
			t.Errorf("returned %d, want 0", n)
		}
	})

	t.Run("no nils", func(t *testing.T) {
		a, b, c := 1, 2, 3
		s := []*int{&a, &b, &c}
		n := utils.FilterNilsInPlace(s)
		if n != 3 {
			t.Errorf("returned %d, want 3", n)
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		var s []*int
		n := utils.FilterNilsInPlace(s)
		if n != 0 {
			t.Errorf("returned %d, want 0", n)
		}
	})
}

func TestGetOrDefault(t *testing.T) {
	t.Run("in bounds returns element", func(t *testing.T) {
		s := []string{"a", "b", "c"}
		if v := utils.GetOrDefault(s, 1, "x"); v != "b" {
			t.Errorf("got %q, want %q", v, "b")
		}
	})

	t.Run("out of bounds returns default", func(t *testing.T) {
		s := []string{"a"}
		if v := utils.GetOrDefault(s, 5, "default"); v != "default" {
			t.Errorf("got %q, want %q", v, "default")
		}
	})

	t.Run("negative index returns default", func(t *testing.T) {
		s := []string{"a"}
		if v := utils.GetOrDefault(s, -1, "default"); v != "default" {
			t.Errorf("got %q, want %q", v, "default")
		}
	})

	t.Run("opt set returns element at index", func(t *testing.T) {
		opt := parameters.Some([]int{10, 20, 30})
		if v := utils.OptGetOrDefault(opt, 1, -1); v != 20 {
			t.Errorf("got %d, want 20", v)
		}
	})

	t.Run("opt unset returns default", func(t *testing.T) {
		opt := parameters.None[[]int]()
		if v := utils.OptGetOrDefault(opt, 0, -1); v != -1 {
			t.Errorf("got %d, want -1", v)
		}
	})

	t.Run("opt set but out of bounds returns default", func(t *testing.T) {
		opt := parameters.Some([]int{10})
		if v := utils.OptGetOrDefault(opt, 5, -1); v != -1 {
			t.Errorf("got %d, want -1", v)
		}
	})
}

func TestStrOrDefault(t *testing.T) {
	t.Run("non-empty returns value", func(t *testing.T) {
		if v := utils.StrOrDefault("hello", "default"); v != "hello" {
			t.Errorf("got %q, want %q", v, "hello")
		}
	})

	t.Run("empty returns default", func(t *testing.T) {
		if v := utils.StrOrDefault("", "default"); v != "default" {
			t.Errorf("got %q, want %q", v, "default")
		}
	})

	t.Run("opt set returns value", func(t *testing.T) {
		opt := parameters.Some("hello")
		if v := utils.OptStrOrDefault(opt, "default"); v != "hello" {
			t.Errorf("got %q, want %q", v, "hello")
		}
	})

	t.Run("opt unset returns default", func(t *testing.T) {
		opt := parameters.None[string]()
		if v := utils.OptStrOrDefault(opt, "default"); v != "default" {
			t.Errorf("got %q, want %q", v, "default")
		}
	})

	t.Run("opt set but empty returns default", func(t *testing.T) {
		opt := parameters.Some("")
		if v := utils.OptStrOrDefault(opt, "default"); v != "default" {
			t.Errorf("got %q, want %q (empty string should use default)", v, "default")
		}
	})
}
