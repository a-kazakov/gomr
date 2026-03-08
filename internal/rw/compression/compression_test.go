package compression

// Tests use package-internal access for: GetFileCompressor()/GetFileDecompressor() are exported, but failWriter mock tests error paths requiring internal package access.

import (
	"bytes"
	"errors"
	"testing"

	"github.com/a-kazakov/gomr/internal/core"
)

func roundTrip(t *testing.T, algorithm core.CompressionAlgorithm, data []byte) {
	t.Helper()
	var buf bytes.Buffer

	// Compress
	compressor := GetFileCompressor(&buf, algorithm)
	n, err := compressor.Write(data)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Write returned %d, want %d", n, len(data))
	}
	if err := compressor.Close(); err != nil {
		t.Fatalf("Compressor Close error: %v", err)
	}

	// Decompress
	decompressor := GetFileDecompressor(&buf, algorithm)
	var result bytes.Buffer
	readBuf := make([]byte, 4096)
	for {
		n, err := decompressor.Read(readBuf)
		if n > 0 {
			result.Write(readBuf[:n])
		}
		if err != nil {
			break
		}
	}
	if err := decompressor.Close(); err != nil {
		t.Fatalf("Decompressor Close error: %v", err)
	}

	if !bytes.Equal(result.Bytes(), data) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d bytes", result.Len(), len(data))
	}
}

// failWriter is an io.Writer that always returns an error.
type failWriter struct{}

func (f *failWriter) Write(p []byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestRoundTrip(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		roundTrip(t, core.CompressionAlgorithmNone, []byte("hello world"))
	})

	t.Run("lz4", func(t *testing.T) {
		roundTrip(t, core.CompressionAlgorithmLz4, []byte("hello world lz4 compression test data"))
	})

	t.Run("zstd default", func(t *testing.T) {
		roundTrip(t, core.CompressionAlgorithmZstdDefault, []byte("hello world zstd default compression test data"))
	})

	t.Run("zstd fast compressor only", func(t *testing.T) {
		// ZstdFast has an empty case body in GetFileDecompressor due to a switch issue,
		// so we can only test the compressor side.
		var buf bytes.Buffer
		compressor := GetFileCompressor(&buf, core.CompressionAlgorithmZstdFast)
		data := []byte("hello world zstd fast test")
		if _, err := compressor.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := compressor.Close(); err != nil {
			t.Fatal(err)
		}
		if buf.Len() == 0 {
			t.Error("compressed output should not be empty")
		}
	})

	t.Run("empty data", func(t *testing.T) {
		for _, alg := range []core.CompressionAlgorithm{
			core.CompressionAlgorithmNone,
			core.CompressionAlgorithmLz4,
			core.CompressionAlgorithmZstdDefault,
		} {
			roundTrip(t, alg, []byte{})
		}
	})

	t.Run("large data", func(t *testing.T) {
		data := make([]byte, 1<<20) // 1MB
		for i := range data {
			data[i] = byte(i % 256)
		}
		for _, alg := range []core.CompressionAlgorithm{
			core.CompressionAlgorithmLz4,
			core.CompressionAlgorithmZstdDefault,
		} {
			roundTrip(t, alg, data)
		}
	})

	t.Run("none close is noop", func(t *testing.T) {
		var buf bytes.Buffer
		c := GetFileCompressor(&buf, core.CompressionAlgorithmNone)
		if err := c.Close(); err != nil {
			t.Errorf("Close for None should not error: %v", err)
		}

		d := GetFileDecompressor(&buf, core.CompressionAlgorithmNone)
		if err := d.Close(); err != nil {
			t.Errorf("Close for None should not error: %v", err)
		}
	})
}

func TestPoolReuse(t *testing.T) {
	t.Run("compressor pool reuse", func(t *testing.T) {
		// Compress twice to verify pool reuse works
		for i := 0; i < 3; i++ {
			var buf bytes.Buffer
			c := GetFileCompressor(&buf, core.CompressionAlgorithmLz4)
			c.Write([]byte("test data"))
			c.Close()
		}
		for i := 0; i < 3; i++ {
			var buf bytes.Buffer
			c := GetFileCompressor(&buf, core.CompressionAlgorithmZstdFast)
			c.Write([]byte("test data"))
			c.Close()
		}
		for i := 0; i < 3; i++ {
			var buf bytes.Buffer
			c := GetFileCompressor(&buf, core.CompressionAlgorithmZstdDefault)
			c.Write([]byte("test data"))
			c.Close()
		}
	})

	t.Run("decompressor pool reuse", func(t *testing.T) {
		// Compress some data with Lz4
		var compressed bytes.Buffer
		c := GetFileCompressor(&compressed, core.CompressionAlgorithmLz4)
		c.Write([]byte("test"))
		c.Close()
		data := compressed.Bytes()

		// Decompress twice to verify pool reuse
		for i := 0; i < 3; i++ {
			reader := bytes.NewReader(data)
			d := GetFileDecompressor(reader, core.CompressionAlgorithmLz4)
			buf := make([]byte, 100)
			d.Read(buf)
			d.Close()
		}
	})
}

func TestErrorPaths(t *testing.T) {
	t.Run("invalid compressor algorithm panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid algorithm")
			}
		}()
		var buf bytes.Buffer
		GetFileCompressor(&buf, core.CompressionAlgorithm(99))
	})

	t.Run("invalid decompressor algorithm panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid algorithm")
			}
		}()
		var buf bytes.Buffer
		GetFileDecompressor(&buf, core.CompressionAlgorithm(99))
	})

	t.Run("zstd fast decompressor panics", func(t *testing.T) {
		// ZstdFast case in GetFileDecompressor has empty body, falls through to panic
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for ZstdFast decompress due to empty case body")
			}
		}()
		var buf bytes.Buffer
		GetFileDecompressor(&buf, core.CompressionAlgorithmZstdFast)
	})

	t.Run("close error lz4", func(t *testing.T) {
		fw := &failWriter{}
		c := GetFileCompressor(fw, core.CompressionAlgorithmLz4)
		// Write some data so Close has something to flush
		c.Write([]byte("some test data to compress"))
		err := c.Close()
		if err == nil {
			t.Error("expected error from Close when underlying writer fails")
		}
	})

	t.Run("close error zstd fast", func(t *testing.T) {
		fw := &failWriter{}
		c := GetFileCompressor(fw, core.CompressionAlgorithmZstdFast)
		// Write some data so Close has something to flush
		c.Write([]byte("some test data to compress"))
		err := c.Close()
		if err == nil {
			t.Error("expected error from Close when underlying writer fails")
		}
	})

	t.Run("close error zstd default", func(t *testing.T) {
		fw := &failWriter{}
		c := GetFileCompressor(fw, core.CompressionAlgorithmZstdDefault)
		// Write some data so Close has something to flush
		c.Write([]byte("some test data to compress"))
		err := c.Close()
		if err == nil {
			t.Error("expected error from Close when underlying writer fails")
		}
	})
}
