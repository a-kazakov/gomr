package zstd

import (
	"fmt"
	"io"

	"github.com/a-kazakov/gomr/extensions/fileio"
	"github.com/klauspost/compress/zstd"
)

type compressor struct{}

// NewCompressor returns a Compressor that applies zstd compression.
func NewCompressor() fileio.Compressor {
	return compressor{}
}

func (compressor) WrapWriter(w io.WriteCloser) io.WriteCloser {
	return fileio.WrapWriteCloser(w, func(w io.WriteCloser) io.WriteCloser {
		zw, err := zstd.NewWriter(w)
		if err != nil {
			panic(fmt.Errorf("failed to create zstd writer: %w", err))
		}
		return zw
	})
}

type decompressor struct{}

// NewDecompressor returns a Decompressor that reads zstd-compressed data.
func NewDecompressor() fileio.Decompressor {
	return decompressor{}
}

func (decompressor) WrapReader(r io.ReadCloser) io.ReadCloser {
	return fileio.WrapReadCloser(r, func(r io.ReadCloser) io.ReadCloser {
		zr, err := zstd.NewReader(r)
		if err != nil {
			panic(fmt.Errorf("failed to create zstd reader: %w", err))
		}
		return zr.IOReadCloser()
	})
}
