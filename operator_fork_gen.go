package gomr

import (
	"github.com/a-kazakov/gomr/internal/operators"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

func ForkTo2[T any](collection *pipeline.Collection[T], opts ...options.ForkOption) (*pipeline.Collection[T], *pipeline.Collection[T]) {
	return operators.ForkTo2(collection, options.ApplyForkOptions(opts...))
}

func ForkTo3[T any](collection *pipeline.Collection[T], opts ...options.ForkOption) (*pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T]) {
	return operators.ForkTo3(collection, options.ApplyForkOptions(opts...))
}

func ForkTo4[T any](collection *pipeline.Collection[T], opts ...options.ForkOption) (*pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T]) {
	return operators.ForkTo4(collection, options.ApplyForkOptions(opts...))
}

func ForkTo5[T any](collection *pipeline.Collection[T], opts ...options.ForkOption) (*pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T]) {
	return operators.ForkTo5(collection, options.ApplyForkOptions(opts...))
}
