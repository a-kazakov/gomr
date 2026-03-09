package test

import (
	"math"
	"testing"

	"github.com/a-kazakov/gomr"
)

type terribleIntSerializer struct{}

func (s terribleIntSerializer) Setup(ctx gomr.OperatorContext) {}
func (s terribleIntSerializer) MarshalElementToBytes(value *int, dest []byte) int {
	for i := 0; i < *value; i++ {
		dest[i] = byte((67 * i) % 255)
	}
	return *value
}

func (s terribleIntSerializer) UnmarshalElementFromBytes(data []byte, dest *int) {
	for i := 0; i < len(data); i++ {
		if data[i] != byte((67*i)%255) {
			panic("invalid data")
		}
	}
	*dest = len(data)
}

func TestSpillBuffer(t *testing.T) {
	t.Run("Spill buffer for variance calculation", func(t *testing.T) {
		const collectionSize = 200
		pipeline := newTestPipeline(t)
		pipeline.Parameters.LoadFromSource(
			func(lookup string) (string, bool) {
				if lookup == "collections.default_capacity" {
					return "1", true
				}
				return "", false
			},
		)
		values1, values2, values3 := gomr.ForkTo3(
			gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
				for i := 0; i < collectionSize; i++ {
					*emitter.GetEmitPointer() = i
				}
			}, gomr.WithOutBatchSize(2)),
		)
		values2 = gomr.SpillBuffer[terribleIntSerializer](values2,
			gomr.WithMaxSpillFileSize(collectionSize),
			gomr.WithOutBatchSize(2),
		)
		values3 = gomr.SpillBuffer[terribleIntSerializer](values3,
			gomr.WithMaxSpillFileSize(collectionSize),
			gomr.WithOutBatchSize(2),
		)

		collectionCount := gomr.Collect(values1, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
			count := 0
			for batch := range receiver.IterBatches() {
				count += len(batch)
			}
			return count
		}, func(ctx gomr.OperatorContext, intermediates []int) int {
			sum := 0
			for _, intermediate := range intermediates {
				sum += intermediate
			}
			return sum
		})

		collectionMean := gomr.CollectWithSideValue(values2, collectionCount, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], _ int) int {
			sum := 0
			for value := range receiver.IterValues() {
				sum += *value
			}
			return sum
		}, func(ctx gomr.OperatorContext, intermediates []int, n int) float64 {
			sum := 0
			for _, intermediate := range intermediates {
				sum += intermediate
			}
			return float64(sum) / float64(n)
		}, gomr.WithParallelism(1))

		squareDiffs := gomr.MapWithSideValue(values3, collectionMean, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[float64], mean float64) {
			for value := range receiver.IterValues() {
				diff := float64(*value) - mean
				*emitter.GetEmitPointer() = diff * diff
			}
		}, gomr.WithOutBatchSize(2))

		variance := gomr.CollectWithSideValue(squareDiffs, collectionCount, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[float64], _ int) float64 {
			sum := 0.0
			for value := range receiver.IterValues() {
				sum += *value
			}
			return sum
		}, func(ctx gomr.OperatorContext, intermediates []float64, n int) float64 {
			sum := 0.0
			for _, intermediate := range intermediates {
				sum += intermediate
			}
			return sum / float64(n)
		})

		pipeline.WaitForCompletion()

		actualVariance := variance.Wait()
		expectedVariance := float64(collectionSize*collectionSize-1) / 12.0
		if math.Abs(actualVariance-expectedVariance) > 0.000001 {
			t.Errorf("Expected variance %f, got %f", expectedVariance, actualVariance)
		}

	})

	t.Run("temp files cleaned up after completion", func(t *testing.T) {
		pipeline := newTestPipeline(t)
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 100; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		spilled := gomr.SpillBuffer[terribleIntSerializer](values,
			gomr.WithMaxSpillFileSize(512), // small file size to force spilling
		)
		result := collectToSliceValue(spilled)
		// Consume the result to ensure pipeline completes
		verifySliceValue(t, result, func(yield func(value int) bool) {
			for i := 0; i < 100; i++ {
				if !yield(i) {
					return
				}
			}
		})

	})
}
