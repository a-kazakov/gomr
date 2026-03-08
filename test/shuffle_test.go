package test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/a-kazakov/gomr"
)

type lastDigitShuffleSerializer struct {
	operatorId string
}

func (s *lastDigitShuffleSerializer) Setup(ctx gomr.OperatorContext) {
	s.operatorId = ctx.OperatorId
}
func (s *lastDigitShuffleSerializer) MarshalKeyToBytes(value *int, dest []byte, cursor int64) (int, int64) {
	dest[0] = byte(*value % 10)
	return 1, gomr.StopEmitting
}
func (s *lastDigitShuffleSerializer) MarshalValueToBytes(value *int, dest []byte) int {
	for i := 0; i < *value; i++ {
		// dest[i] = byte((67 * i) % 255)
		dest[i] = byte(*value % 256)
	}
	return *value
}
func (s *lastDigitShuffleSerializer) UnmarshalValueFromBytes(key []byte, data []byte, dest *int) {
	*dest = len(data)
	for i := 0; i < len(data); i++ {
		// if data[i] != byte((67*i)%255) {
		if data[i] != byte(*dest%256) {
			panic(fmt.Errorf("invalid data: %v != [%d] x %d for key %d", data, byte(*dest%256), len(data), key[0]))
		}
		// }
	}
}

type sumReducer struct{}

func (r *sumReducer) Setup(ctx gomr.OperatorContext)     {}
func (r *sumReducer) Teardown(emitter gomr.Emitter[int]) {}
func (r *sumReducer) Reduce(key []byte, receiver gomr.ShuffleReceiver[int], emitter gomr.Emitter[int]) {
	sum := 0
	for inValue := range receiver.IterValues() {
		sum += *inValue
	}
	*emitter.GetEmitPointer() = sum
}

type sumReducer2To3 struct {
	ctx gomr.OperatorContext
}

func (r *sumReducer2To3) Setup(ctx gomr.OperatorContext) {
	r.ctx = ctx
}
func (r *sumReducer2To3) Teardown(_ gomr.Emitter[int], _ gomr.Emitter[string], reporting gomr.Emitter[string]) {
	*reporting.GetEmitPointer() = fmt.Sprintf("[%s] Reducer teardown", r.ctx.OperatorId)
}
func (r *sumReducer2To3) Reduce(key []byte, smallReceiver gomr.ShuffleReceiver[int], largeReceiver gomr.ShuffleReceiver[int], intValues gomr.Emitter[int], stringValues gomr.Emitter[string], reporting gomr.Emitter[string]) {
	sum := 0
	switch key[0] % 4 {
	case 0: // First small
		for inValue := range smallReceiver.IterValues() {
			sum += *inValue
		}
		for inValue := range largeReceiver.IterValues() {
			sum += *inValue
		}
	case 1: // First large
		for inValue := range largeReceiver.IterValues() {
			sum += *inValue
		}
		for inValue := range smallReceiver.IterValues() {
			sum += *inValue
		}
	case 2: // Do not consume at all
		break
	case 3: // Interleave
		for inValue := range interleave(smallReceiver.IterValues(), largeReceiver.IterValues()) {
			sum += *inValue
		}
	}
	*intValues.GetEmitPointer() = sum
	*stringValues.GetEmitPointer() = fmt.Sprintf("Result: %d", sum)
	*reporting.GetEmitPointer() = fmt.Sprintf("[%s] Key %d, sum %d", r.ctx.OperatorId, key[0], sum)
}

func TestShuffle(t *testing.T) {
	t.Run("Simple 1:1 shuffle", func(t *testing.T) {
		// Sum up by last digit
		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 200; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		shuffled := gomr.Shuffle[*lastDigitShuffleSerializer, *sumReducer](values)
		result := collectToSliceValue(shuffled)
		verifySliceValue(t, result, func(yield func(value int) bool) {
			var sums [10]int
			for i := 0; i < 200; i++ {
				sums[i%10] += i
			}
			for i := 0; i < 10; i++ {
				if !yield(sums[i]) {
					break
				}
			}
		})
	})

	t.Run("1:1 shuffle with big chunks", func(t *testing.T) {
		const baseSize = 100
		const numItems = 10
		// Sum up by last digit
		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := baseSize; i < baseSize+numItems; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		shuffled := gomr.Shuffle[*lastDigitShuffleSerializer, *sumReducer](values,
			gomr.WithLocalShuffleBufferSize(128*1024*5),
			gomr.WithScatterParallelism(1),
			gomr.WithGatherParallelism(1),
			gomr.WithNumShards(2),
		)
		result := collectToSliceValue(shuffled)
		verifySliceValue(t, result, func(yield func(value int) bool) {
			var sums [10]int
			for i := baseSize; i < baseSize+numItems; i++ {
				sums[i%10] += i
			}
			for i := 0; i < 10; i++ {
				if !yield(sums[i]) {
					break
				}
			}
		})
	})

	t.Run("1:1 shuffle with guaranteed spill", func(t *testing.T) {
		// Sum up by last digit
		// Use 500 items with 128KB buffer to guarantee spill,
		// but keep parallelism low to avoid timeout under the race detector.
		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 500; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		shuffled := gomr.Shuffle[*lastDigitShuffleSerializer, *sumReducer](values,
			gomr.WithLocalShuffleBufferSize(128*1024*1), // 1 page maximum (128KB)
			gomr.WithScatterParallelism(4),              // Moderate competition for the buffer
		)
		result := collectToSliceValue(shuffled)
		verifySliceValue(t, result, func(yield func(value int) bool) {
			var sums [10]int
			for i := 0; i < 500; i++ {
				sums[i%10] += i
			}
			for i := 0; i < 10; i++ {
				if !yield(sums[i]) {
					break
				}
			}
		})
	})

	t.Run("2:3 shuffle", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		smallValues := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 100; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		largeValues := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 10000; i < 10100; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		shuffledInts, shuffledStrings, shuffledReports := gomr.Shuffle2To3[*lastDigitShuffleSerializer, *lastDigitShuffleSerializer, *sumReducer2To3](
			smallValues, largeValues,
			gomr.WithGatherParallelism(5),
			gomr.WithNumShards(8),
		)
		intsSlice := collectToSliceValue(shuffledInts)
		stringsSlice := collectToSliceValue(shuffledStrings)
		reportsSlice := collectToSliceValue(shuffledReports)
		var sums [10]int
		for i := 0; i < 100; i++ {
			if i%10%4 != 2 {
				sums[i%10] += i
			}
		}
		for i := 10000; i < 10100; i++ {
			if i%10%4 != 2 {
				sums[i%10] += i
			}
		}
		var operatorId string
		for _, m := range pipeline.Metrics.Operations {
			if m.Kind == gomr.OPERATION_KIND_SHUFFLE {
				operatorId = m.Id
				break
			}
		}
		verifySliceValue(t, intsSlice, func(yield func(value int) bool) {
			for i := 0; i < 10; i++ {
				if !yield(sums[i]) {
					break
				}
			}
		})
		verifySliceValue(t, stringsSlice, func(yield func(value string) bool) {
			for i := 0; i < 10; i++ {
				if !yield(fmt.Sprintf("Result: %d", sums[i])) {
					break
				}
			}
		})
		verifySliceValue(t, reportsSlice, func(yield func(value string) bool) {
			for i := 0; i < 10; i++ {
				if !yield(fmt.Sprintf("[%s] Key %d, sum %d", operatorId, i, sums[i])) {
					return
				}
			}
			for i := 0; i < 8; i++ { // 1 reducer per shard
				if !yield(fmt.Sprintf("[%s] Reducer teardown", operatorId)) {
					return
				}
			}
		})
	})

}

func TestShuffleCleanup(t *testing.T) {
	t.Run("temp files cleaned up after shuffle", func(t *testing.T) {
		tmpDir := t.TempDir()

		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 200; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		shuffled := gomr.Shuffle[*lastDigitShuffleSerializer, *sumReducer](values,
			gomr.WithScratchSpacePaths(tmpDir),
			gomr.WithLocalShuffleBufferSize(128*1024*3), // 3 pages - forces spilling with 200 items
		)
		result := collectToSliceValue(shuffled)
		verifySliceValue(t, result, func(yield func(value int) bool) {
			var sums [10]int
			for i := 0; i < 200; i++ {
				sums[i%10] += i
			}
			for i := 0; i < 10; i++ {
				if !yield(sums[i]) {
					return
				}
			}
		})

		// After pipeline completion, no spill files should remain.
		// The framework may leave empty parent directories (e.g. "gomr/"),
		// so we check that no regular files exist under tmpDir.
		var remainingFiles []string
		_ = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				rel, _ := filepath.Rel(tmpDir, path)
				remainingFiles = append(remainingFiles, rel)
			}
			return nil
		})
		if len(remainingFiles) > 0 {
			t.Errorf("spill files not cleaned up: %v", remainingFiles)
		}
	})
}
