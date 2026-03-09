package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func TestMerge(t *testing.T) {
	t.Run("basic merge of two forks", func(t *testing.T) {
		pipeline, _ := newTestPipeline(t)
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 5; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		forks := gomr.ForkToAny(values, 2)
		merged := gomr.Merge(forks)
		result := collectToSliceValue(merged)
		pipeline.WaitForCompletion()
		// Each value appears twice (once from each fork)
		verifySliceValue(t, result, func(yield func(value int) bool) {
			for i := 0; i < 5; i++ {
				if !yield(i) {
					return
				}
				if !yield(i) {
					return
				}
			}
		})
	})

	t.Run("merge from independent sources", func(t *testing.T) {
		pipeline, _ := newTestPipeline(t)
		source1 := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 5; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		source2 := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 100; i < 105; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		merged := gomr.Merge([]gomr.Collection[int]{source1, source2})
		result := collectToSliceValue(merged)
		pipeline.WaitForCompletion()
		verifySliceValue(t, result, func(yield func(value int) bool) {
			for i := 0; i < 5; i++ {
				if !yield(i) {
					return
				}
			}
			for i := 100; i < 105; i++ {
				if !yield(i) {
					return
				}
			}
		})
	})

	t.Run("merge of many forks", func(t *testing.T) {
		pipeline, _ := newTestPipeline(t)
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 3; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		forks := gomr.ForkToAny(values, 5)
		merged := gomr.Merge(forks)
		result := collectToSliceValue(merged)
		pipeline.WaitForCompletion()
		// Each value appears 5 times
		verifySliceValue(t, result, func(yield func(value int) bool) {
			for i := 0; i < 3; i++ {
				for j := 0; j < 5; j++ {
					if !yield(i) {
						return
					}
				}
			}
		})
	})

	t.Run("merge single collection", func(t *testing.T) {
		pipeline, _ := newTestPipeline(t)
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 5; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		merged := gomr.Merge([]gomr.Collection[int]{values})
		result := collectToSliceValue(merged)
		pipeline.WaitForCompletion()
		verifySliceValue(t, result, func(yield func(value int) bool) {
			for i := 0; i < 5; i++ {
				if !yield(i) {
					return
				}
			}
		})
	})
}
