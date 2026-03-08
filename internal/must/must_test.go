package must_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/a-kazakov/gomr/internal/must"
)

func assertPanics(t *testing.T, substr string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic, got none")
		}
		msg := fmt.Sprint(r)
		if !strings.Contains(msg, substr) {
			t.Fatalf("panic message %q does not contain %q", msg, substr)
		}
	}()
	fn()
}

func TestBeTrue(t *testing.T) {
	t.Run("true does not panic", func(t *testing.T) {
		must.BeTrue(true, "should not panic")
	})

	t.Run("false panics with message", func(t *testing.T) {
		assertPanics(t, "bad thing", func() {
			must.BeTrue(false, "bad thing")
		})
	})

	t.Run("false panics with formatted message", func(t *testing.T) {
		assertPanics(t, "value is 42", func() {
			must.BeTrue(false, "value is %d", 42)
		})
	})
}

func TestNoError(t *testing.T) {
	t.Run("nil error returns value", func(t *testing.T) {
		v := must.NoError(42, nil).Else("should not panic")
		if v != 42 {
			t.Fatalf("expected 42, got %d", v)
		}
	})

	t.Run("error panics with message", func(t *testing.T) {
		assertPanics(t, "open file", func() {
			must.NoError(0, errors.New("not found")).Else("open file")
		})
	})

	t.Run("error panics with formatted message", func(t *testing.T) {
		assertPanics(t, "open shard 3", func() {
			must.NoError(0, errors.New("not found")).Else("open shard %d", 3)
		})
	})

	t.Run("wraps error with percent w", func(t *testing.T) {
		sentinel := errors.New("sentinel")
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic")
			}
			err, ok := r.(error)
			if !ok {
				t.Fatalf("panic value is not error: %T", r)
			}
			if !errors.Is(err, sentinel) {
				t.Fatal("errors.Is failed: wrapped error not found")
			}
		}()
		must.NoError(0, sentinel).Else("context")
	})
}

func TestOK(t *testing.T) {
	t.Run("nil error does not panic", func(t *testing.T) {
		must.OK(nil).Else("should not panic")
	})

	t.Run("error panics with message", func(t *testing.T) {
		assertPanics(t, "mkdir", func() {
			must.OK(errors.New("permission denied")).Else("mkdir")
		})
	})

	t.Run("error panics with formatted message", func(t *testing.T) {
		assertPanics(t, "mkdir /tmp/foo", func() {
			must.OK(errors.New("permission denied")).Else("mkdir %s", "/tmp/foo")
		})
	})

	t.Run("wraps error with percent w", func(t *testing.T) {
		sentinel := errors.New("sentinel")
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic")
			}
			err, ok := r.(error)
			if !ok {
				t.Fatalf("panic value is not error: %T", r)
			}
			if !errors.Is(err, sentinel) {
				t.Fatal("errors.Is failed: wrapped error not found")
			}
		}()
		must.OK(sentinel).Else("context")
	})
}
