package shardedfile

import (
	"bufio"
	"io"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/rw/compression"
)

type baseShardWriter struct {
	creator *Creator
	writer  io.Writer
}

func (w *baseShardWriter) Write(p []byte) (n int, err error) {
	must.BeTrue(w.creator != nil, "Writer is closed")
	n, err = w.writer.Write(p)
	must.OK(err).Else("failed to write to scratch file")
	w.creator.fileOffset += int64(n)
	return n, err
}

func (w *baseShardWriter) Close() {
	w.creator.writeShardEnding()
	w.creator = nil
}

type shardWriter struct {
	baseWriter     *baseShardWriter
	bufferedWriter *bufio.Writer
	compressor     *compression.FileCompressor
	closed         bool
}

func NewShardWriter(
	creator *Creator,
	fileWriter io.Writer,
	bufferSize int,
	compressionAlgorithm core.CompressionAlgorithm,
) *shardWriter {
	baseWriter := &baseShardWriter{
		creator: creator,
		writer:  fileWriter,
	}
	bufferedWriter := bufio.NewWriterSize(baseWriter, bufferSize)
	compressor := compression.GetFileCompressor(bufferedWriter, compressionAlgorithm)
	return &shardWriter{
		baseWriter:     baseWriter,
		bufferedWriter: bufferedWriter,
		compressor:     compressor,
	}
}

func (w *shardWriter) Write(p []byte) (n int, err error) {
	return w.compressor.Write(p)
}

func (w *shardWriter) Close() error {
	if err := w.compressor.Close(); err != nil {
		return err
	}
	if err := w.bufferedWriter.Flush(); err != nil {
		return err
	}
	w.baseWriter.Close()
	return nil
}
