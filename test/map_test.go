package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func TestMap(t *testing.T) {
	t.Run("Map 1 to 1", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 10; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		mapped := gomr.Map(initial, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
			for value := range receiver.IterValues() {
				*emitter.GetEmitPointer() = *value * 2
				*emitter.GetEmitPointer() = *value * 3
			}
		})
		collected := collectToSliceValue(mapped)
		pipeline.WaitForCompletion()
		verifySliceValue(t, collected, func(yield func(value int) bool) {
			for i := 0; i < 10; i++ {
				if !yield(i*2) || !yield(i*3) {
					break
				}
			}
		})
	})

	t.Run("Map 1 to 1 with batch fragmentation", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 100; i++ {
				*emitter.GetEmitPointer() = i
			}
		}, gomr.WithOutBatchSize(3))
		mapped := gomr.Map(initial, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
			for value := range receiver.IterValues() {
				*emitter.GetEmitPointer() = *value * 2
				*emitter.GetEmitPointer() = *value * 3
			}
		},
			gomr.WithOutBatchSize(5),
			gomr.WithParallelism(5),
		)
		collected := collectToSliceValue(mapped)
		pipeline.WaitForCompletion()
		verifySliceValue(t, collected, func(yield func(value int) bool) {
			for i := 0; i < 100; i++ {
				if !yield(i*2) || !yield(i*3) {
					break
				}
			}
		})
	})

	t.Run("Map 1 to 1 with side value", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 10; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		sideValue := createSliceValue(pipeline, []int{2, 3, 5})
		mapped := gomr.MapWithSideValue(initial, sideValue, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int], multipliers []int) {
			for value := range receiver.IterValues() {
				for _, multiplier := range multipliers {
					*emitter.GetEmitPointer() = *value * multiplier
				}
			}
		})
		collected := collectToSliceValue(mapped)
		pipeline.WaitForCompletion()
		verifySliceValue(t, collected, func(yield func(value int) bool) {
			for i := 0; i < 10; i++ {
				if !yield(i*2) || !yield(i*3) || !yield(i*5) {
					break
				}
			}
		})
	})

	t.Run("Map 1 to 3", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 100; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		odds, evens, all := gomr.MapTo3(initial, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], odds gomr.Emitter[int], evens gomr.Emitter[int], all gomr.Emitter[int]) {
			for value := range receiver.IterValues() {
				if *value%2 == 0 {
					*evens.GetEmitPointer() = *value
				} else {
					*odds.GetEmitPointer() = *value
				}
				*all.GetEmitPointer() = *value
			}
		})
		collectedOdds := collectToSliceValue(odds)
		collectedEvens := collectToSliceValue(evens)
		collectedAll := collectToSliceValue(all)
		pipeline.WaitForCompletion()
		verifySliceValue(t, collectedOdds, func(yield func(value int) bool) {
			for i := 1; i < 100; i += 2 {
				if !yield(i) {
					break
				}
			}
		})
		verifySliceValue(t, collectedEvens, func(yield func(value int) bool) {
			for i := 0; i < 100; i += 2 {
				if !yield(i) {
					break
				}
			}
		})
		verifySliceValue(t, collectedAll, func(yield func(value int) bool) {
			for i := 0; i < 100; i++ {
				if !yield(i) {
					break
				}
			}
		})
	})
}
