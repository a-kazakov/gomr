package pagedbuffer_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/a-kazakov/gomr/internal/pagedbuffer"
)

// =============================================================================
// PagedBufferOrchestrator Tests
// =============================================================================

func TestPagedBufferOrchestrator(t *testing.T) {
	t.Run("NewPagedBufferOrchestrator creates with correct maxPages", func(t *testing.T) {
		// 1MB buffer with 128KB pages = 8 pages max
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0.2)

		// Should not be full initially
		if orchestrator.IsFull() {
			t.Error("expected orchestrator to not be full initially")
		}
	})

	t.Run("GetPage returns initialized page", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0.2)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Write something to trigger page allocation
		data := []byte("test data")
		n, err := buffer.Write(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != len(data) {
			t.Errorf("expected %d bytes written, got %d", len(data), n)
		}

		// Read it back using ReadAt
		readBuf := make([]byte, len(data))
		n, err = buffer.ReadAt(readBuf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(readBuf[:n], data) {
			t.Errorf("expected %q, got %q", data, readBuf[:n])
		}
	})

	t.Run("IsFull returns true when max pages used", func(t *testing.T) {
		// 128KB buffer = 1 page max
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(pagedbuffer.PAGE_SIZE, 0)

		if orchestrator.IsFull() {
			t.Error("should not be full before any pages allocated")
		}

		// Allocate a page by writing to a buffer
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)
		data := make([]byte, 100)
		buffer.Write(data)

		if !orchestrator.IsFull() {
			t.Error("should be full after allocating 1 page with 1 page max")
		}
	})

	t.Run("Multiple buffers share orchestrator page limit", func(t *testing.T) {
		// 2 pages max
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(2*pagedbuffer.PAGE_SIZE, 0)

		buffer1 := pagedbuffer.NewPagedBuffer(orchestrator)
		buffer2 := pagedbuffer.NewPagedBuffer(orchestrator)

		// Write to first buffer (allocates 1 page)
		buffer1.Write(make([]byte, 100))
		if orchestrator.IsFull() {
			t.Error("should not be full after 1 page")
		}

		// Write to second buffer (allocates 1 page)
		buffer2.Write(make([]byte, 100))
		if !orchestrator.IsFull() {
			t.Error("should be full after 2 pages")
		}
	})

	t.Run("Pages are reused after release", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(pagedbuffer.PAGE_SIZE, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Write to allocate a page
		data := []byte("test")
		buffer.Write(data)

		if !orchestrator.IsFull() {
			t.Error("should be full after allocating page")
		}

		// Clear releases the page
		buffer.Clear()

		// Write again should reuse the page
		buffer.Write(data)

		// Should still be at capacity (reusing page)
		if !orchestrator.IsFull() {
			t.Error("should still be full after reusing page")
		}
	})
}

// =============================================================================
// PagedBuffer Tests
// =============================================================================

func TestPagedBuffer(t *testing.T) {
	t.Run("NewPagedBuffer creates empty buffer", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Reading from empty buffer should return EOF
		buf := make([]byte, 10)
		n, err := buffer.ReadAt(buf, 0)
		if err != io.EOF {
			t.Errorf("expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes read, got %d", n)
		}
	})

	t.Run("Write then ReadAt round-trip", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		data := []byte("hello world")
		n, err := buffer.Write(data)
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if n != len(data) {
			t.Errorf("expected %d bytes written, got %d", len(data), n)
		}

		readBuf := make([]byte, len(data))
		n, err = buffer.ReadAt(readBuf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}
		if n != len(data) {
			t.Errorf("expected %d bytes read, got %d", len(data), n)
		}
		if !bytes.Equal(readBuf, data) {
			t.Errorf("expected %q, got %q", data, readBuf)
		}

		// Data should still be there (random access, doesn't consume)
		n, err = buffer.ReadAt(readBuf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}
		if !bytes.Equal(readBuf[:n], data) {
			t.Errorf("expected data to still be readable, got %q", readBuf[:n])
		}
	})

	t.Run("ReadAt in chunks", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		data := []byte("hello world, this is a longer message")
		buffer.Write(data)

		// Read in small chunks using ReadAt
		var result []byte
		chunk := make([]byte, 5)
		offset := 0
		for offset < len(data) {
			n, err := buffer.ReadAt(chunk, offset)
			if n > 0 {
				result = append(result, chunk[:n]...)
				offset += n
			}
			if err == io.EOF {
				break
			}
			if err != nil && err != io.EOF {
				t.Fatalf("unexpected error: %v", err)
			}
			if n == 0 {
				break
			}
		}

		if !bytes.Equal(result, data) {
			t.Errorf("expected %q, got %q", data, result)
		}
	})

	t.Run("Write spans multiple pages", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(10*pagedbuffer.PAGE_SIZE, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Write more than one page worth of data
		dataSize := pagedbuffer.PAGE_SIZE + 1000
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		n, err := buffer.Write(data)
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if n != dataSize {
			t.Errorf("expected %d bytes written, got %d", dataSize, n)
		}

		// Read it all back using ReadAt
		result := make([]byte, dataSize)
		n, err = buffer.ReadAt(result, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}

		if n != dataSize {
			t.Errorf("expected %d bytes read, got %d", dataSize, n)
		}
		if !bytes.Equal(result, data) {
			t.Error("data mismatch after multi-page write/read")
		}
	})

	t.Run("Multiple writes then ReadAt", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Multiple writes (appends)
		writes := []string{"first ", "second ", "third"}
		totalWritten := 0
		for _, s := range writes {
			n, _ := buffer.Write([]byte(s))
			totalWritten += n
		}

		// Read all at once using ReadAt
		expected := "first second third"
		readBuf := make([]byte, len(expected))
		n, err := buffer.ReadAt(readBuf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}

		if string(readBuf[:n]) != expected {
			t.Errorf("expected %q, got %q", expected, readBuf[:n])
		}
	})

	t.Run("ReadAt at different offsets", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Write some data
		data := []byte("hello world")
		buffer.Write(data)

		// Read from offset 0
		buf := make([]byte, 5)
		n, err := buffer.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		if string(buf[:n]) != "hello" {
			t.Errorf("expected 'hello', got %q", buf[:n])
		}

		// Read from offset 6
		n, err = buffer.ReadAt(buf, 6)
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		if string(buf[:n]) != "world" {
			t.Errorf("expected 'world', got %q", buf[:n])
		}

		// Read from offset 0 again (random access, data still there)
		n, err = buffer.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		if string(buf[:n]) != "hello" {
			t.Errorf("expected 'hello' again, got %q", buf[:n])
		}
	})

	t.Run("Large data spanning many pages", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(100*pagedbuffer.PAGE_SIZE, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Write 5 pages worth of data
		dataSize := 5 * pagedbuffer.PAGE_SIZE
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		n, err := buffer.Write(data)
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if n != dataSize {
			t.Errorf("expected %d bytes written, got %d", dataSize, n)
		}

		// Read it all back using ReadAt
		result := make([]byte, dataSize)
		n, err = buffer.ReadAt(result, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}

		if n != dataSize {
			t.Errorf("expected %d bytes, got %d", dataSize, n)
		}
		if !bytes.Equal(result, data) {
			t.Error("data mismatch after large write/read")
		}
	})

	t.Run("Empty write does not allocate page", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(pagedbuffer.PAGE_SIZE, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Empty write
		n, err := buffer.Write([]byte{})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes written, got %d", n)
		}

		// Should not have allocated a page
		if orchestrator.IsFull() {
			t.Error("empty write should not allocate a page")
		}

		// Reading should return EOF
		buf := make([]byte, 10)
		_, err = buffer.ReadAt(buf, 0)
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
	})

	t.Run("Write exactly one page", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(2*pagedbuffer.PAGE_SIZE, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		data := make([]byte, pagedbuffer.PAGE_SIZE)
		for i := range data {
			data[i] = byte(i % 256)
		}

		n, err := buffer.Write(data)
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if n != pagedbuffer.PAGE_SIZE {
			t.Errorf("expected %d bytes written, got %d", pagedbuffer.PAGE_SIZE, n)
		}

		// Read it all back using ReadAt
		result := make([]byte, pagedbuffer.PAGE_SIZE)
		n, err = buffer.ReadAt(result, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}

		if !bytes.Equal(result, data) {
			t.Error("data mismatch")
		}
	})

	t.Run("Clear releases pages", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(pagedbuffer.PAGE_SIZE, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Write data to allocate a page
		data := []byte("test data")
		buffer.Write(data)

		if !orchestrator.IsFull() {
			t.Error("should be full after allocating page")
		}

		// Clear should release pages
		buffer.Clear()

		if orchestrator.IsFull() {
			t.Error("should not be full after Clear")
		}

		// Buffer should be empty
		if buffer.Size != 0 {
			t.Errorf("expected Size 0 after Clear, got %d", buffer.Size)
		}

		// Reading should return EOF
		buf := make([]byte, 10)
		_, err := buffer.ReadAt(buf, 0)
		if err != io.EOF {
			t.Errorf("expected EOF after Clear, got %v", err)
		}
	})

	t.Run("ReadAt beyond buffer size returns EOF", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		data := []byte("hello")
		buffer.Write(data)

		// Read beyond buffer size
		buf := make([]byte, 10)
		n, err := buffer.ReadAt(buf, 100)
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes read, got %d", n)
		}
	})

	t.Run("ReadAt with negative offset returns EOF", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		data := []byte("hello")
		buffer.Write(data)

		buf := make([]byte, 10)
		n, err := buffer.ReadAt(buf, -1)
		if err != io.EOF {
			t.Errorf("expected EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes read, got %d", n)
		}
	})

	t.Run("CanTake checks capacity", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(1024*1024, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		if !buffer.CanTake(100) {
			t.Error("should be able to take 100 bytes")
		}

		// Write close to max
		largeData := make([]byte, pagedbuffer.MAX_BUFFER_SIZE-100)
		buffer.Write(largeData)

		if !buffer.CanTake(50) {
			t.Error("should be able to take 50 more bytes")
		}

		if buffer.CanTake(200) {
			t.Error("should not be able to take 200 more bytes")
		}
	})
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestPagedBufferIntegration(t *testing.T) {
	t.Run("Multiple buffers with shared orchestrator", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(10*pagedbuffer.PAGE_SIZE, 0)

		numBuffers := 5
		buffers := make([]pagedbuffer.PagedBuffer, numBuffers)
		for i := range buffers {
			buffers[i] = pagedbuffer.NewPagedBuffer(orchestrator)
		}

		// Write different data to each buffer
		for i := range buffers {
			data := bytes.Repeat([]byte{byte(i)}, 1000)
			buffers[i].Write(data)
		}

		// Read from each buffer and verify using ReadAt
		for i := range buffers {
			expected := bytes.Repeat([]byte{byte(i)}, 1000)
			result := make([]byte, 1000)
			n, err := buffers[i].ReadAt(result, 0)
			if err != nil && err != io.EOF {
				t.Fatalf("buffer %d: read error: %v", i, err)
			}
			if n != 1000 {
				t.Errorf("buffer %d: expected 1000 bytes, got %d", i, n)
			}
			if !bytes.Equal(result, expected) {
				t.Errorf("buffer %d: data mismatch", i)
			}
		}
	})

	t.Run("Buffer implements io.Writer interface", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(10*pagedbuffer.PAGE_SIZE, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Verify it can be used as io.Writer
		var writer io.Writer = &buffer
		data := []byte("test data")
		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if n != len(data) {
			t.Errorf("expected %d, got %d", len(data), n)
		}

		// Verify data was written using ReadAt
		readBuf := make([]byte, len(data))
		n, err = buffer.ReadAt(readBuf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}
		if !bytes.Equal(readBuf[:n], data) {
			t.Errorf("expected %q, got %q", data, readBuf[:n])
		}
	})

	t.Run("Random access across page boundaries", func(t *testing.T) {
		orchestrator := pagedbuffer.NewPagedBufferOrchestrator(10*pagedbuffer.PAGE_SIZE, 0)
		buffer := pagedbuffer.NewPagedBuffer(orchestrator)

		// Write data that spans multiple pages
		dataSize := 2*pagedbuffer.PAGE_SIZE + 1000
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}
		buffer.Write(data)

		// Read from middle of first page
		offset1 := pagedbuffer.PAGE_SIZE / 2
		buf1 := make([]byte, 100)
		n1, err := buffer.ReadAt(buf1, offset1)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}
		if !bytes.Equal(buf1[:n1], data[offset1:offset1+n1]) {
			t.Error("data mismatch at first page offset")
		}

		// Read from middle of second page
		offset2 := pagedbuffer.PAGE_SIZE + 500
		buf2 := make([]byte, 100)
		n2, err := buffer.ReadAt(buf2, offset2)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}
		if !bytes.Equal(buf2[:n2], data[offset2:offset2+n2]) {
			t.Error("data mismatch at second page offset")
		}

		// Read across page boundary
		offset3 := pagedbuffer.PAGE_SIZE - 50
		buf3 := make([]byte, 100)
		n3, err := buffer.ReadAt(buf3, offset3)
		if err != nil && err != io.EOF {
			t.Fatalf("read error: %v", err)
		}
		if !bytes.Equal(buf3[:n3], data[offset3:offset3+n3]) {
			t.Error("data mismatch across page boundary")
		}
	})
}
