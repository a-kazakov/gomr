package fileio

import "io"

// Compressor wraps a writer with compression.
type Compressor interface {
	WrapWriter(w io.WriteCloser) io.WriteCloser
}

// Decompressor wraps a reader with decompression.
type Decompressor interface {
	WrapReader(r io.ReadCloser) io.ReadCloser
}
