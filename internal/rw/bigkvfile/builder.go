package bigkvfile

import (
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/rw/kv"
	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
)

type builderConfig struct {
	mergeThreshold     int
	numShards          int32
	readBufferSize     int
	writeBufferSize    int
	targetWriteLatency time.Duration
}

type Builder struct {
	config builderConfig
	levels [][]*shardedfile.ShardedFile
}

func NewBuilder(numShards int32, mergeThreshold int, readBufferSize int, writeBufferSize int, targetWriteLatency time.Duration) *Builder {
	return &Builder{
		config: builderConfig{
			mergeThreshold:     mergeThreshold,
			numShards:          numShards,
			readBufferSize:     readBufferSize,
			writeBufferSize:    writeBufferSize,
			targetWriteLatency: targetWriteLatency,
		},
		levels: make([][]*shardedfile.ShardedFile, 4),
	}
}

func (w *Builder) PushFile(file *shardedfile.ShardedFile) {
	filePath := file.GetName()
	fileInfo := must.NoError(os.Stat(filePath)).Else("failed to get file size")
	slog.Debug("Pushing file", slog.String("file", file.GetName()), slog.Int64("fileSize", fileInfo.Size()))
	leftover := file
	for levelIdx := range w.levels {
		level := &w.levels[levelIdx]
		*level = append(*level, leftover)
		if len(*level) >= int(w.config.mergeThreshold) {
			slog.Debug("Merging level", slog.Int("level", len(*level)), slog.Int("mergeThreshold", w.config.mergeThreshold))
			leftover = w.merge(*level)
			*level = (*level)[:0]
		} else {
			leftover = nil
			break
		}
	}
	if leftover != nil {
		newLevel := make([]*shardedfile.ShardedFile, 1, w.config.mergeThreshold)
		newLevel[0] = leftover
		w.levels = append(w.levels, newLevel)
	}
}

func (w *Builder) Finalize() *shardedfile.ShardedFile {
	// Just plain merge everything, the total size should never reasonably exceed 3x of max degree
	totalShardsCount := 0
	for levelIdx := range w.levels {
		totalShardsCount += len(w.levels[levelIdx])
	}
	flattenedList := make([]*shardedfile.ShardedFile, 0, totalShardsCount)
	for levelIdx := range w.levels {
		flattenedList = append(flattenedList, w.levels[levelIdx]...)
	}
	return w.merge(flattenedList)
}

func (w *Builder) merge(files []*shardedfile.ShardedFile) *shardedfile.ShardedFile {
	if len(files) == 0 {
		return nil
	}
	valueBuffer := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
	outFile := shardedfile.NewDerivedShardedFile(files...)
	creator := outFile.OpenCreator()
	defer creator.Close()
	accessors := make([]*shardedfile.Accessor, len(files))
	for idx := range files {
		accessor := files[idx].OpenAccessor()
		accessors[idx] = accessor
		defer accessor.Close()
		defer files[idx].Destroy()
	}
	closers := make([]io.Closer, len(files))
	readers := make([]io.Reader, len(files))
	for shardId := range w.config.numShards {
		for idx := range accessors {
			r := accessors[idx].GetShardReader(shardId, w.config.readBufferSize)
			closers[idx] = r
			readers[idx] = r
		}
		multiReader := kv.NewMultiReader(readers)
		writerShardId, shardWriter := creator.OpenShardWriter(w.config.writeBufferSize)
		if writerShardId != shardId {
			must.BeTrue(false, "Shard ID mismatch; processing %d, got writer for %d", shardId, writerShardId)
		}
		throttledWriter := NewThrottledWriter(shardWriter, w.config.targetWriteLatency)
		kvWriter := kv.NewWriter(throttledWriter)
		for key := multiReader.PeekKey(); key != nil; key = multiReader.PeekKey() {
			keyReader := multiReader.GetKeyReader()
			n, err := keyReader.ReadValue(valueBuffer)
			for ; err == nil; n, err = keyReader.ReadValue(valueBuffer) {
				writeStartTime := time.Now()
				kvWriter.Write(key, valueBuffer[:n])
				writeDuration := time.Since(writeStartTime)
				sleepPenalty := writeDuration - w.config.targetWriteLatency
				if sleepPenalty > 0 { // Back off to avoid overwhelming the disk
					time.Sleep(min(sleepPenalty, 1*time.Second))
				}
			}
			if err != io.EOF {
				must.OK(err).Else("Got unexpected error while merging files")
			}
		}
		for idx := range closers {
			closers[idx].Close()
		}
		shardWriter.Close()
	}
	return outFile
}
