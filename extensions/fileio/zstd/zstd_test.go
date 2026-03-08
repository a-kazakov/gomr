package zstd

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/a-kazakov/gomr/extensions/fileio"
)

func TestZstd_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "data.zst")

	original := []byte("The quick brown fox jumps over the lazy dog. Repeated content for compression. Repeated content for compression.")

	// Write compressed.
	w, err := fileio.Create(p, fileio.WithCompressor(NewCompressor()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(original); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// The file on disk should differ from the original.
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(raw, original) {
		t.Fatal("on-disk data should be compressed, but it matches the original")
	}

	// Read decompressed.
	r, err := fileio.Open(p, fileio.WithDecompressor(NewDecompressor()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(original))
	}
}
