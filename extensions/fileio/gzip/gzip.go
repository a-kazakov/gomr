package gzip

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/a-kazakov/gomr/extensions/fileio"
)

type compressor struct{}

// NewCompressor returns a Compressor that applies gzip compression.
func NewCompressor() fileio.Compressor {
	return compressor{}
}

func (compressor) WrapWriter(w io.WriteCloser) io.WriteCloser {
	return fileio.WrapWriteCloser(w, func(w io.WriteCloser) io.WriteCloser {
		return gzip.NewWriter(w)
	})
}

type decompressor struct{}

// NewDecompressor returns a Decompressor that reads gzip-compressed data.
func NewDecompressor() fileio.Decompressor {
	return decompressor{}
}

func (decompressor) WrapReader(r io.ReadCloser) io.ReadCloser {
	return fileio.WrapReadCloser(r, func(r io.ReadCloser) io.ReadCloser {
		gr, err := gzip.NewReader(r)
		if err != nil {
			panic(fmt.Errorf("failed to create gzip reader: %w", err))
		}
		return gr
	})
}
