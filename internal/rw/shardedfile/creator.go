package shardedfile

import (
	"io"
	"os"

	"github.com/a-kazakov/gomr/internal/must"
)

type Creator struct {
	shardedFile   *ShardedFile
	currentShard  int32
	fileOffset    int64
	osFile        *os.File
	currentWriter *shardWriter
}

func (c *Creator) OpenShardWriter(bufferSize int) (int32, io.WriteCloser) {
	must.BeTrue(c.currentWriter == nil, "currentWriter is not nil")
	c.currentShard++
	currentShard := c.currentShard
	c.currentWriter = NewShardWriter(c, c.osFile, bufferSize, c.shardedFile.compressionAlgorithm)
	return currentShard, c.currentWriter
}

func (c *Creator) writeShardEnding() {
	c.shardedFile.shardEndings = append(c.shardedFile.shardEndings, c.fileOffset)
	c.currentWriter = nil
}

func (c *Creator) Close() {
	c.osFile.Close()
}
