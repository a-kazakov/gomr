package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func TestFork(t *testing.T) {
	t.Run("Fork to 2", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 100; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		f1, f2 := gomr.ForkTo2(initial)
		collectedF1 := collectToSliceValue(f1)
		collectedF2 := collectToSliceValue(f2)
		pipeline.WaitForCompletion()
		verifySliceValue(t, collectedF1, func(yield func(value int) bool) {
			for i := 0; i < 100; i++ {
				if !yield(i) {
					break
				}
			}
		})
		verifySliceValue(t, collectedF2, func(yield func(value int) bool) {
			for i := 0; i < 100; i++ {
				if !yield(i) {
					break
				}
			}
		})
	})

	t.Run("Fork to any", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 100; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		forked := gomr.ForkToAny(initial, 10)
		collected := make([]gomr.Value[[]int], 0, 10)
		for idx := range forked {
			collected = append(collected, collectToSliceValue(forked[idx]))
		}
		pipeline.WaitForCompletion()
		for idx := range collected {
			verifySliceValue(t, collected[idx], func(yield func(value int) bool) {
				for i := 0; i < 100; i++ {
					if !yield(i) {
						break
					}
				}
			})
		}
	})
}
