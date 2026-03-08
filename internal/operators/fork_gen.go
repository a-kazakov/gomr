package operators

import (
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

func ForkTo2[T any](collection *pipeline.Collection[T], opts *options.ForkOptions) (*pipeline.Collection[T], *pipeline.Collection[T]) {
	collections := ForkToAny(collection, 2, opts)
	return collections[0], collections[1]
}

func ForkTo3[T any](collection *pipeline.Collection[T], opts *options.ForkOptions) (*pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T]) {
	collections := ForkToAny(collection, 3, opts)
	return collections[0], collections[1], collections[2]
}

func ForkTo4[T any](collection *pipeline.Collection[T], opts *options.ForkOptions) (*pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T]) {
	collections := ForkToAny(collection, 4, opts)
	return collections[0], collections[1], collections[2], collections[3]
}

func ForkTo5[T any](collection *pipeline.Collection[T], opts *options.ForkOptions) (*pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T], *pipeline.Collection[T]) {
	collections := ForkToAny(collection, 5, opts)
	return collections[0], collections[1], collections[2], collections[3], collections[4]
}
