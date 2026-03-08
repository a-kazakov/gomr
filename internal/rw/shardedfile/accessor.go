package shardedfile

import (
	"io"
	"os"
)

type Accessor struct {
	shardedFile *ShardedFile
	osFile      *os.File
}

func (a *Accessor) GetShardReader(shardId int32, bufferSize int) io.ReadCloser {
	var offset int64
	boundary := a.shardedFile.shardEndings[shardId]
	if shardId > 0 {
		offset = a.shardedFile.shardEndings[shardId-1]
	}
	return GetShardReader(a.osFile, offset, boundary, bufferSize, a.shardedFile.compressionAlgorithm)
}

func (a *Accessor) Close() {
	a.osFile.Close()
}
