package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func TestEmptyPipeline(t *testing.T) {
	t.Run("empty pipeline", func(t *testing.T) {
		pipeline := gomr.NewPipeline()
		pipeline.WaitForCompletion()
	})
}
