package operators

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/a-kazakov/gomr/internal/capprobe"
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/rw/compression"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
	"github.com/oklog/ulid/v2"
)

func SpillBuffer[
	TSerializer core.ElementSerializer[TValue],
	TValue any,
](
	collection *pipeline.Collection[TValue],
	opts *options.SpillBufferOptions,
) *pipeline.Collection[TValue] {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Spill buffer")
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, collection.Metrics.Name)
	params := collection.GetPipeline().Parameters

	// Determine output batch size
	var outBatchSize int
	if opts.OutBatchSize.IsSet() {
		outBatchSize = opts.OutBatchSize.Get()
	} else if collection.Metrics.BatchSize > 0 {
		outBatchSize = collection.Metrics.BatchSize
	} else {
		outBatchSize = params.Collections.DefaultBatchSize.Get()
	}

	// Determine spill directories
	var spillDirectories []string
	if opts.SpillDirectories.IsSet() {
		spillDirectories = opts.SpillDirectories.Get()
	}
	if len(spillDirectories) == 0 {
		spillDirectories = params.Disk.GetScratchSpacePaths()
	}
	must.BeTrue(len(spillDirectories) > 0, "no spill directories available")

	// Resolve parameters with defaults
	maxSpillFileSize := params.SpillBuffer.DefaultMaxSpillFileSize.Resolve(opts.MaxSpillFileSize)
	writeBufferSize := params.SpillBuffer.DefaultWriteBufferSize.Resolve(opts.WriteBufferSize)
	readBufferSize := params.SpillBuffer.DefaultReadBufferSize.Resolve(opts.ReadBufferSize)
	writeParallelism := params.SpillBuffer.DefaultWriteParallelism.Resolve(opts.WriteParallelism)
	readParallelism := params.SpillBuffer.DefaultReadParallelism.Resolve(opts.ReadParallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := params.SpillBuffer.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)

	// Create output collection
	outCollection, outChannel := pipeline.NewDerivedCollection[TValue](outCollectionName, outBatchSize, outChannelCapacity, collection)
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SPILL_BUFFER, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)

	// Channel for ready files (closed files ready to be restored)
	readyFiles := make(chan string, 1000000)

	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	opMetrics.Parallelism = readParallelism + writeParallelism

	writeOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection.Metrics}, nil, writeParallelism, opts.UserOperatorContext.GetOr(nil))
	readOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{outCollection.Metrics}, nil, readParallelism, opts.UserOperatorContext.GetOr(nil))

	// Start the Spiller (Hot/Cold path)
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(writeParallelism, func(_ int) {
		// Create a separate serializer instance for thread safety
		var spillerSerializer TSerializer
		spillerSerializerSetup := capprobe.GetElementSerializerSetup(spillerSerializer)
		if spillerSerializerSetup != nil {
			spillerSerializerSetup.Setup(writeOpContexts[0])
		}

		var currentFile *os.File
		var currentBufferedWriter *bufio.Writer
		var currentCompressor *compression.FileCompressor
		var currentFileSize int64
		var currentFilePath string
		var directoryIndex int
		elementBuffer := make([]byte, params.SpillBuffer.MaxSerializedElementSize.Get())
		var short32Buffer [4]byte

		// Cleanup function for current file
		closeCurrentFile := func() {
			if currentCompressor != nil {
				currentCompressor.Close()
				currentCompressor = nil
			}
			if currentBufferedWriter != nil {
				currentBufferedWriter.Flush()
				currentBufferedWriter = nil
			}
			if currentFile != nil {
				currentFile.Close()
				currentFile = nil
				if currentFilePath != "" {
					readyFiles <- currentFilePath
					currentFilePath = ""
				}
				currentFileSize = 0
			}
		}

		// Process batches
		for batch := range receiver.IterBatchesNoRecycle() {
			values := batch.Values

			// Try hot path first (non-blocking send)
			select {
			case outChannel <- batch:
				// Successfully sent on hot path, continue
				outMetrics.ElementsProduced.Add(int64(len(values)))
				outMetrics.BatchesProduced.Add(1)
				continue
			default:
				// Hot path blocked, need to spill
			}

			// Cold path: serialize and write batch to disk

			if currentFileSize > maxSpillFileSize {
				closeCurrentFile()
				currentBufferedWriter = nil
				currentFile = nil
			}

			if currentFile == nil {
				// Create new file
				selectedDir := spillDirectories[directoryIndex%len(spillDirectories)]
				directoryIndex++
				fileName := fmt.Sprintf("%s.dat", ulid.Make().String())
				currentFilePath = filepath.Join(selectedDir, "gomr", p.GetJobID(), "spill_buffers", outMetrics.Id, fileName)
				must.OK(os.MkdirAll(filepath.Dir(currentFilePath), 0755)).Else("failed to create spill file directory")
				currentFile = must.NoError(os.OpenFile(currentFilePath, os.O_CREATE|os.O_WRONLY, 0644)).Else("failed to create spill file")
				currentBufferedWriter = bufio.NewWriterSize(currentFile, writeBufferSize)
				currentCompressor = compression.GetFileCompressor(currentBufferedWriter, compressionAlgorithm)
				currentFileSize = 0
			}

			for i := range values {
				value := &values[i]
				elementSize := spillerSerializer.MarshalElementToBytes(value, elementBuffer)
				binary.NativeEndian.PutUint32(short32Buffer[:], uint32(elementSize))
				if _, err := currentCompressor.Write(short32Buffer[:]); err != nil {
					must.OK(err).Else("failed to write element length")
				}
				if _, err := currentCompressor.Write(elementBuffer[:elementSize]); err != nil {
					must.OK(err).Else("failed to write element data")
				}
				currentFileSize += 4 + int64(elementSize)
			}

			// Recycle the batch, since it was not sent on the hot path
			batch.Recycle()
		}

		closeCurrentFile()
	}, func() {
		close(readyFiles)
	})

	// Start the Restorer (Cold path reader)
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(readParallelism, func(_ int) {
		// Create a separate serializer instance for thread safety
		var restorerSerializer TSerializer
		restorerSerializerSetup := capprobe.GetElementSerializerSetup(restorerSerializer)
		if restorerSerializerSetup != nil {
			restorerSerializerSetup.Setup(readOpContexts[0])
		}

		elementBuffer := make([]byte, params.SpillBuffer.MaxSerializedElementSize.Get())
		var short32Buffer [4]byte

		emitter := primitives.NewEmitter(params.Processing.DefaultParallelism.Get(), outBatchSize, outChannel, &outMetrics.ElementsProduced, &outMetrics.BatchesProduced)

		for filePath := range readyFiles {
			// Open file for reading
			file := must.NoError(os.Open(filePath)).Else("failed to open spill file for reading")

			bufferedReader := bufio.NewReaderSize(file, readBufferSize)
			decompressor := compression.GetFileDecompressor(bufferedReader, compressionAlgorithm)

			for {
				if _, err := io.ReadFull(decompressor, short32Buffer[:]); err != nil {
					if err == io.EOF {
						break
					}
					file.Close()
					must.OK(err).Else("failed to read element length")
				}
				elementLength := binary.NativeEndian.Uint32(short32Buffer[:])
				_, err := io.ReadFull(decompressor, elementBuffer[:elementLength])
				if err != nil {
					file.Close()
					must.OK(err).Else("failed to read element data")
				}

				var value TValue
				restorerSerializer.UnmarshalElementFromBytes(elementBuffer[:elementLength], &value)
				*emitter.GetEmitPointer() = value
			}

			decompressor.Close()
			file.Close()
			if err := os.Remove(filePath); err != nil {
				slog.Error("failed to delete spill file", "file", filePath, "error", err)
			}
		}

		emitter.Close()
	}, func() {
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})

	return outCollection
}
