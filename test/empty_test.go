package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func TestEmptyPipeline(t *testing.T) {
	t.Run("empty pipeline", func(t *testing.T) {
		pipeline := newTestPipeline(t)
		pipeline.WaitForCompletion()
	})

	t.Run("empty seed collect", func(t *testing.T) {
		pipeline := newTestPipeline(t)
		values := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			// emit nothing
		})
		result := collectToSliceValue(values)
		pipeline.WaitForCompletion()
		verifySliceValue(t, result, func(yield func(value int) bool) {
			// expect empty
		})
	})
}
