package shuf

// Tests use package-internal access for: closersWrapper type, NewSortedBuffer() constructor.

import (
	"errors"
	"io"
	"testing"

	"github.com/a-kazakov/gomr/internal/pagedbuffer"
)

// mockCloser is a mock io.Closer for testing closersWrapper.
type mockCloser struct {
	err    error
	closed bool
}

func (m *mockCloser) Close() error {
	m.closed = true
	return m.err
}

// errorWriter is a mock kvWriter that always returns an error.
type errorWriter struct {
	err error
}

func (e *errorWriter) Write(key []byte, value []byte) error {
	return e.err
}

func TestClosersWrapper(t *testing.T) {
	t.Run("close success", func(t *testing.T) {
		c1 := &mockCloser{}
		c2 := &mockCloser{}
		c3 := &mockCloser{}
		w := &closersWrapper{closers: []io.Closer{c1, c2, c3}}
		err := w.Close()
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !c1.closed || !c2.closed || !c3.closed {
			t.Error("expected all closers to be closed")
		}
	})

	t.Run("close error stops at first failure", func(t *testing.T) {
		errTest := errors.New("close failed")
		c1 := &mockCloser{}
		c2 := &mockCloser{err: errTest}
		c3 := &mockCloser{}
		w := &closersWrapper{closers: []io.Closer{c1, c2, c3}}
		err := w.Close()
		if !errors.Is(err, errTest) {
			t.Fatalf("expected errTest, got %v", err)
		}
		if !c1.closed || !c2.closed {
			t.Error("expected c1 and c2 to be closed")
		}
		// c3 should NOT be closed because c2 returned an error and the loop stopped
		if c3.closed {
			t.Error("expected c3 to NOT be closed after error in c2")
		}
	})

	t.Run("close empty wrapper succeeds", func(t *testing.T) {
		w := &closersWrapper{closers: []io.Closer{}}
		err := w.Close()
		if err != nil {
			t.Fatalf("expected no error for empty closers, got %v", err)
		}
	})
}

func TestSortedBufferErrors(t *testing.T) {
	t.Run("flush write error", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buf := NewSortedBuffer(orchestrator)

		// Push some data
		buf.PushBack([]byte("key1"), []byte("val1"))
		buf.PushBack([]byte("key2"), []byte("val2"))

		tempBuffer := make([]byte, 4096)
		errTest := errors.New("write error")
		writer := &errorWriter{err: errTest}

		err := buf.FlushTo(writer, tempBuffer)
		if !errors.Is(err, errTest) {
			t.Fatalf("expected write error, got %v", err)
		}
	})

	t.Run("flush read at error on empty buffer", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buf := NewSortedBuffer(orchestrator)

		// Push some data to populate refs
		buf.PushBack([]byte("key1"), []byte("val1"))

		// Clear the data buffer to make ReadAt fail, but refs still exist
		buf.data.Clear()

		tempBuffer := make([]byte, 4096)
		goodWriter := &errorWriter{err: nil}

		err := buf.FlushTo(goodWriter, tempBuffer)
		if err == nil {
			t.Fatal("expected an error from ReadAt on empty buffer")
		}
	})

	t.Run("push back panics when key write exceeds max buffer size", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buf := NewSortedBuffer(orchestrator)

		// Set the data buffer Size to MAX_BUFFER_SIZE so the next Write call
		// returns an error (size + len(key) > MAX_BUFFER_SIZE), triggering the panic.
		buf.data.Size = pagedbuffer.MAX_BUFFER_SIZE

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic from PushBack when key write fails")
			}
		}()

		buf.PushBack([]byte("key1"), []byte("val1"))
	})
}
