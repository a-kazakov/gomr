package compression

import (
	"sync"

	"github.com/a-kazakov/gomr/internal/must"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

var lz4ReaderPool = sync.Pool{
	New: func() any {
		return lz4.NewReader(nil)
	},
}

var lz4WriterPool = sync.Pool{
	New: func() any {
		return lz4.NewWriter(nil)
	},
}

var zstdReaderPool = sync.Pool{
	New: func() any {
		return must.NoError(zstd.NewReader(nil)).Else("failed to create zstd reader")
	},
}

var zstdFastWriterPool = sync.Pool{
	New: func() any {
		return must.NoError(zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))).Else("failed to create zstd writer")
	},
}

var zstdDefaultWriterPool = sync.Pool{
	New: func() any {
		return must.NoError(zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))).Else("failed to create zstd writer")
	},
}
