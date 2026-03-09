package shardedfile

import (
	"fmt"
	"os"
	"path"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/oklog/ulid/v2"
)

type ShardedFile struct {
	filePath             string
	shardEndings         []int64
	compressionAlgorithm core.CompressionAlgorithm
}

func (f *ShardedFile) GetName() string {
	return f.filePath
}

func NewShardedFile(dirPath string, compressionAlgorithm core.CompressionAlgorithm) *ShardedFile {
	newPath := path.Join(dirPath, fmt.Sprintf("%s.dat", ulid.Make().String()))
	return &ShardedFile{
		filePath:             newPath,
		compressionAlgorithm: compressionAlgorithm,
	}
}

func NewDerivedShardedFile(others ...*ShardedFile) *ShardedFile {
	dirPath := path.Dir(others[0].filePath)
	newPath := path.Join(dirPath, fmt.Sprintf("%s.dat", ulid.Make().String()))
	return &ShardedFile{
		filePath:             newPath,
		compressionAlgorithm: others[0].compressionAlgorithm,
	}
}

func (f *ShardedFile) OpenCreator() *Creator {
	dirName := path.Dir(f.filePath)
	must.OK(os.MkdirAll(dirName, 0755)).Else("failed to create directory")
	file := must.NoError(os.OpenFile(f.filePath, os.O_CREATE|os.O_WRONLY, 0644)).Else("failed to open sharded file")
	return &Creator{
		currentShard: -1,
		fileOffset:   0,
		shardedFile:  f,
		osFile:       file,
	}
}

func (f *ShardedFile) OpenAccessor() *Accessor {
	file := must.NoError(os.Open(f.filePath)).Else("failed to open sharded file")
	utils.DisableOSReadAhead(file) // The access pattern is close to random, so we disable read-ahead
	return &Accessor{
		shardedFile: f,
		osFile:      file,
	}
}

func (f *ShardedFile) GetSize() int64 {
	fileInfo := must.NoError(os.Stat(f.filePath)).Else("failed to get file size")
	return fileInfo.Size()
}

func (f *ShardedFile) Destroy() {
	os.Remove(f.filePath)
	f.filePath = ""
	f.shardEndings = nil
}
