package shardedfile

// Tests use package-internal access for: baseShardWriter/offsetFileReader unexported structs, filePath/shardEndings fields.

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/rw/compression"
)

// =============================================================================
// ShardedFile Internals Tests (GetName, GetSize, Destroy)
// =============================================================================

func TestShardedFileInternals(t *testing.T) {
	t.Run("get name returns valid path with dat extension", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := NewShardedFile(tmpDir, core.CompressionAlgorithmNone)
		name := sf.GetName()
		if name == "" {
			t.Fatal("GetName returned empty string")
		}
		dir := filepath.Dir(name)
		if dir != tmpDir {
			t.Fatalf("expected dir %q, got %q", tmpDir, dir)
		}
		ext := filepath.Ext(name)
		if ext != ".dat" {
			t.Fatalf("expected .dat extension, got %q", ext)
		}
	})

	t.Run("get size returns correct file size", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := NewShardedFile(tmpDir, core.CompressionAlgorithmNone)

		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)
		data := []byte("hello world")
		writer.Write(data)
		writer.Close()
		creator.Close()

		size := sf.GetSize()
		if size != int64(len(data)) {
			t.Fatalf("expected size %d, got %d", len(data), size)
		}
	})

	t.Run("get size panics on nonexistent file", func(t *testing.T) {
		sf := &ShardedFile{
			filePath: "/nonexistent/path/to/file.dat",
		}
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic for nonexistent file")
			}
		}()
		sf.GetSize()
	})

	t.Run("destroy clears file path and shard endings", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := NewShardedFile(tmpDir, core.CompressionAlgorithmNone)

		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)
		writer.Write([]byte("data"))
		writer.Close()
		creator.Close()

		if sf.filePath == "" {
			t.Fatal("filePath should not be empty before Destroy")
		}
		if sf.shardEndings == nil {
			t.Fatal("shardEndings should not be nil before Destroy")
		}

		sf.Destroy()

		if sf.filePath != "" {
			t.Fatalf("expected empty filePath after Destroy, got %q", sf.filePath)
		}
		if sf.shardEndings != nil {
			t.Fatal("expected nil shardEndings after Destroy")
		}
	})
}

// =============================================================================
// OpenCreator Error Path Tests
// =============================================================================

func TestOpenCreatorErrors(t *testing.T) {
	t.Run("panics when mkdir all fails", func(t *testing.T) {
		// Create a file where a directory is expected so MkdirAll fails
		tmpDir := t.TempDir()
		blockingFile := filepath.Join(tmpDir, "blocker")
		if err := os.WriteFile(blockingFile, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}

		sf := &ShardedFile{
			filePath:             filepath.Join(blockingFile, "subdir", "file.dat"),
			compressionAlgorithm: core.CompressionAlgorithmNone,
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic when MkdirAll fails")
			}
		}()
		sf.OpenCreator()
	})

	t.Run("panics when open file fails", func(t *testing.T) {
		// Point to a directory instead of a file so OpenFile fails
		tmpDir := t.TempDir()
		subDir := filepath.Join(tmpDir, "subdir")
		if err := os.MkdirAll(subDir, 0755); err != nil {
			t.Fatal(err)
		}

		sf := &ShardedFile{
			filePath:             subDir, // subDir is a directory, not a file
			compressionAlgorithm: core.CompressionAlgorithmNone,
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic when OpenFile fails on a directory")
			}
		}()
		sf.OpenCreator()
	})
}

// =============================================================================
// OpenAccessor Error Path Tests
// =============================================================================

func TestOpenAccessorErrors(t *testing.T) {
	t.Run("panics when opening nonexistent file", func(t *testing.T) {
		sf := &ShardedFile{
			filePath: "/nonexistent/path/to/file.dat",
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic when opening nonexistent file")
			}
		}()
		sf.OpenAccessor()
	})
}

// =============================================================================
// ShardWriter Internals Tests
// =============================================================================

// failWriter always returns an error on Write.
type failWriter struct{}

func (fw *failWriter) Write(p []byte) (int, error) {
	return 0, os.ErrClosed
}

// failAfterNWriter allows N successful writes, then returns errors.
type failAfterNWriter struct {
	remaining int
}

func (fw *failAfterNWriter) Write(p []byte) (int, error) {
	if fw.remaining <= 0 {
		return 0, fmt.Errorf("simulated write error")
	}
	fw.remaining--
	return len(p), nil
}

func TestShardWriterInternals(t *testing.T) {
	t.Run("panics when opening second writer without closing first", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := NewShardedFile(tmpDir, core.CompressionAlgorithmNone)
		creator := sf.OpenCreator()
		defer creator.Close()

		// Open first writer but don't close it
		creator.OpenShardWriter(0)

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic when opening second writer without closing first")
			}
		}()
		// Opening a second writer without closing the first should panic
		creator.OpenShardWriter(0)
	})

	t.Run("panics when writing after close", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := NewShardedFile(tmpDir, core.CompressionAlgorithmNone)
		creator := sf.OpenCreator()
		defer creator.Close()

		_, writer := creator.OpenShardWriter(0)
		writer.Write([]byte("data"))
		writer.Close()

		// The baseShardWriter's creator field is set to nil after Close.
		// Writing to the shardWriter after close should panic because
		// the compressor is closed, but we need to test baseShardWriter directly.
		bsw := &baseShardWriter{
			creator: nil, // simulate closed state
			writer:  nil,
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic when writing to closed baseShardWriter")
			}
		}()
		bsw.Write([]byte("test"))
	})

	t.Run("panics when underlying writer fails", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := NewShardedFile(tmpDir, core.CompressionAlgorithmNone)
		creator := sf.OpenCreator()
		defer creator.Close()

		bsw := &baseShardWriter{
			creator: creator,
			writer:  &failWriter{},
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic when underlying writer fails")
			}
		}()
		bsw.Write([]byte("test"))
	})

	t.Run("close returns error when compressor close fails", func(t *testing.T) {
		// Use zstd compression. Write some data, then replace the buffered writer's
		// underlying writer with a failing one. zstd.Encoder.Close() flushes all
		// remaining compressed data and returns an error if the underlying writer fails.
		tmpDir := t.TempDir()
		sf := NewShardedFile(tmpDir, core.CompressionAlgorithmNone)
		creator := sf.OpenCreator()
		defer creator.Close()

		// Start with a real working writer so we can write some data
		failing := &failAfterNWriter{remaining: 100}
		bw := bufio.NewWriterSize(failing, 1) // 1-byte buffer flushes immediately
		comp := compression.GetFileCompressor(bw, core.CompressionAlgorithmZstdFast)

		sw := &shardWriter{
			baseWriter: &baseShardWriter{
				creator: creator,
				writer:  failing,
			},
			bufferedWriter: bw,
			compressor:     comp,
		}

		// Write some data through the compressor so it has pending state
		data := make([]byte, 4096)
		for i := range data {
			data[i] = byte(i)
		}
		comp.Write(data)

		// Now make the underlying writer fail for all future writes
		failing.remaining = 0

		// zstd Close() will try to flush pending compressed data, which goes through
		// bw -> failing, and failing now returns an error.
		err := sw.Close()
		if err == nil {
			t.Fatal("expected error from Close when compressor.Close fails")
		}
	})

	t.Run("close returns error when flush fails", func(t *testing.T) {
		// Use no compression so compressor.Close() succeeds (returns nil for None).
		// Then make the buffered writer's underlying writer fail when Flush is called.
		// We need the buffered writer to have buffered data that it tries to flush.
		tmpDir := t.TempDir()
		sf := NewShardedFile(tmpDir, core.CompressionAlgorithmNone)
		creator := sf.OpenCreator()
		defer creator.Close()

		// The writer fails on all writes -- Flush will fail when it tries to write buffered data
		failing := &failAfterNWriter{remaining: 0}
		bw := bufio.NewWriterSize(failing, 65536) // large buffer so writes are buffered
		comp := compression.GetFileCompressor(bw, core.CompressionAlgorithmNone)

		sw := &shardWriter{
			baseWriter: &baseShardWriter{
				creator: creator,
				writer:  failing,
			},
			bufferedWriter: bw,
			compressor:     comp,
		}

		// Write some data so the buffer has something to flush
		sw.Write([]byte("data that will be buffered"))

		// compressor.Close() for None always returns nil.
		// bufferedWriter.Flush() should fail because the underlying writer fails.
		err := sw.Close()
		if err == nil {
			t.Fatal("expected error from Close when bufferedWriter.Flush fails")
		}
	})
}

// =============================================================================
// OffsetFileReader Error Tests
// =============================================================================

func TestOffsetFileReaderErrors(t *testing.T) {
	t.Run("panics when read at fails on closed file", func(t *testing.T) {
		// Create a file, then close it, then try to read through offsetFileReader
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "test.dat")
		f, err := os.Create(filePath)
		if err != nil {
			t.Fatal(err)
		}
		f.Write([]byte("hello world"))
		f.Close()

		// Reopen then close to get a closed file handle
		f2, err := os.Open(filePath)
		if err != nil {
			t.Fatal(err)
		}
		f2.Close() // close the file handle

		ofr := &offsetFileReader{
			reader:   f2, // closed file
			offset:   0,
			boundary: 11,
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic when ReadAt fails on closed file")
			}
		}()
		buf := make([]byte, 10)
		ofr.Read(buf)
	})
}
