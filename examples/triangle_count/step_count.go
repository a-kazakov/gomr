package trianglecount

import (
	"github.com/a-kazakov/gomr"
)

func CountElements[T any](inputCollection gomr.Collection[T]) gomr.Value[uint64] {
	return gomr.Collect(
		inputCollection,
		func(context gomr.OperatorContext, receiver gomr.CollectionReceiver[T]) uint64 {
			result := uint64(0)
			for inBatch := range receiver.IterBatches() {
				result += uint64(len(inBatch))
			}
			return result
		},
		func(context gomr.OperatorContext, intermediateValues []uint64) uint64 {
			result := uint64(0)
			for _, intermediateValue := range intermediateValues {
				result += intermediateValue
			}
			return result
		},
		gomr.WithOperationName("Count elements"),
		gomr.WithOutValueName("Triangles count"),
	)
}
