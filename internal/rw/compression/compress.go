// Package compression provides pooled compressor/decompressor wrappers
// for the supported compression algorithms (None, Lz4, ZstdFast, ZstdDefault).
package compression

import (
	"io"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// FileCompressor wraps a compression writer with pool-based lifecycle management.
// Create via GetFileCompressor; call Close to flush and return the writer to its pool.
type FileCompressor struct {
	algorithm core.CompressionAlgorithm
	writer    io.Writer
}

// Panics for unsupported algorithms.
func GetFileCompressor(writer io.Writer, algorithm core.CompressionAlgorithm) *FileCompressor {
	switch algorithm {
	case core.CompressionAlgorithmNone:
		return &FileCompressor{
			algorithm: algorithm,
			writer:    writer,
		}
	case core.CompressionAlgorithmLz4:
		lz4Writer := lz4WriterPool.Get().(*lz4.Writer)
		lz4Writer.Reset(writer)
		return &FileCompressor{
			algorithm: algorithm,
			writer:    lz4Writer,
		}
	case core.CompressionAlgorithmZstdFast:
		zstdWriter := zstdFastWriterPool.Get().(*zstd.Encoder)
		zstdWriter.Reset(writer)
		return &FileCompressor{
			algorithm: algorithm,
			writer:    zstdWriter,
		}
	case core.CompressionAlgorithmZstdDefault:
		zstdWriter := zstdDefaultWriterPool.Get().(*zstd.Encoder)
		zstdWriter.Reset(writer)
		return &FileCompressor{
			algorithm: algorithm,
			writer:    zstdWriter,
		}
	default:
		must.BeTrue(false, "unsupported compression algorithm: %d", algorithm)
		return nil
	}
}

func (c *FileCompressor) Write(p []byte) (n int, err error) {
	return c.writer.Write(p)
}

func (c *FileCompressor) Close() error {
	switch c.algorithm {
	case core.CompressionAlgorithmNone:
		return nil
	case core.CompressionAlgorithmLz4:
		lz4Writer := c.writer.(*lz4.Writer)
		if err := lz4Writer.Close(); err != nil {
			return err
		}
		lz4WriterPool.Put(lz4Writer)
	case core.CompressionAlgorithmZstdFast:
		zstdWriter := c.writer.(*zstd.Encoder)
		if err := zstdWriter.Close(); err != nil {
			return err
		}
		zstdFastWriterPool.Put(zstdWriter)
	case core.CompressionAlgorithmZstdDefault:
		zstdWriter := c.writer.(*zstd.Encoder)
		if err := zstdWriter.Close(); err != nil {
			return err
		}
		zstdDefaultWriterPool.Put(zstdWriter)
	}
	return nil
}
