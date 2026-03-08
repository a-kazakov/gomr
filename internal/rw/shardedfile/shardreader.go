package shardedfile

import (
	"bufio"
	"io"
	"os"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/rw/compression"
)

type offsetFileReader struct {
	reader   *os.File
	offset   int64
	boundary int64
}

func (r *offsetFileReader) Read(p []byte) (n int, err error) {
	if r.offset >= r.boundary {
		return 0, io.EOF
	}
	canRead := min(len(p), int(r.boundary-r.offset))
	n, err = r.reader.ReadAt(p[:canRead], r.offset)
	must.OK(err).Else("failed to read from scratch file")

	r.offset += int64(n)
	return n, err
}

func GetShardReader(
	osFile *os.File,
	offset int64,
	boundary int64,
	bufferSize int,
	compressionAlgorithm core.CompressionAlgorithm,
) io.ReadCloser {
	offsetReader := &offsetFileReader{
		reader:   osFile,
		offset:   offset,
		boundary: boundary,
	}
	bufferedReader := bufio.NewReaderSize(offsetReader, bufferSize)
	return compression.GetFileDecompressor(bufferedReader, compressionAlgorithm)
}
