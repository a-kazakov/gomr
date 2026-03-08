package compression

import (
	"io"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// FileDecompressor wraps a decompression reader with pool-based lifecycle management.
// Create via GetFileDecompressor; call Close to return the reader to its pool.
type FileDecompressor struct {
	algorithm core.CompressionAlgorithm
	reader    io.Reader
}

// Panics for unsupported algorithms.
func GetFileDecompressor(reader io.Reader, algorithm core.CompressionAlgorithm) *FileDecompressor {
	switch algorithm {
	case core.CompressionAlgorithmNone:
		return &FileDecompressor{
			algorithm: algorithm,
			reader:    reader,
		}
	case core.CompressionAlgorithmLz4:
		lz4Reader := lz4ReaderPool.Get().(*lz4.Reader)
		lz4Reader.Reset(reader)
		return &FileDecompressor{
			algorithm: algorithm,
			reader:    lz4Reader,
		}
	case core.CompressionAlgorithmZstdFast:
	case core.CompressionAlgorithmZstdDefault:
		zstdReader := zstdReaderPool.Get().(*zstd.Decoder)
		zstdReader.Reset(reader)
		return &FileDecompressor{
			algorithm: algorithm,
			reader:    zstdReader,
		}
	}
	must.BeTrue(false, "unsupported compression algorithm: %d", algorithm)
	return nil
}

func (d *FileDecompressor) Read(p []byte) (n int, err error) {
	return d.reader.Read(p)
}

func (d *FileDecompressor) Close() error {
	switch d.algorithm {
	case core.CompressionAlgorithmNone:
		return nil
	case core.CompressionAlgorithmLz4:
		lz4Reader := d.reader.(*lz4.Reader)
		lz4ReaderPool.Put(lz4Reader)
	case core.CompressionAlgorithmZstdFast:
	case core.CompressionAlgorithmZstdDefault:
		zstdReader := d.reader.(*zstd.Decoder)
		zstdReaderPool.Put(zstdReader)
	}
	return nil
}
