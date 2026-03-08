package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func TestEdgeCases(t *testing.T) {
	t.Run("empty seed collect", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			// emit nothing
		})
		result := collectToSliceValue(values)
		pipeline.WaitForCompletion()
		verifySliceValue(t, result, func(yield func(value int) bool) {
			// expect empty
		})
	})

	t.Run("empty seed map collect", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			// emit nothing
		})
		mapped := gomr.Map(values, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
			for v := range receiver.IterValues() {
				*emitter.GetEmitPointer() = *v * 2
			}
		})
		result := collectToSliceValue(mapped)
		pipeline.WaitForCompletion()
		verifySliceValue(t, result, func(yield func(value int) bool) {
			// expect empty
		})
	})

	t.Run("empty seed fork merge collect", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			// emit nothing
		})
		f1, f2 := gomr.ForkTo2(values)
		merged := gomr.Merge([]gomr.Collection[int]{f1, f2})
		result := collectToSliceValue(merged)
		pipeline.WaitForCompletion()
		verifySliceValue(t, result, func(yield func(value int) bool) {
			// expect empty
		})
	})

	t.Run("single element map collect", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			*emitter.GetEmitPointer() = 42
		})
		mapped := gomr.Map(values, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
			for v := range receiver.IterValues() {
				*emitter.GetEmitPointer() = *v + 1
			}
		})
		result := collectToSliceValue(mapped)
		pipeline.WaitForCompletion()
		verifySliceValue(t, result, func(yield func(value int) bool) {
			yield(43)
		})
	})

	t.Run("single element fork merge", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			*emitter.GetEmitPointer() = 7
		})
		f1, f2 := gomr.ForkTo2(values)
		merged := gomr.Merge([]gomr.Collection[int]{f1, f2})
		result := collectToSliceValue(merged)
		pipeline.WaitForCompletion()
		verifySliceValue(t, result, func(yield func(value int) bool) {
			yield(7)
			yield(7)
		})
	})
}
