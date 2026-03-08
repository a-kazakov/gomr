package gomr

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

type Mapper[TIn any, TOut0 any] = core.MapperTo1[TIn, TOut0]

func Map[TIn any, TOut any](collection *pipeline.Collection[TIn], mapper Mapper[TIn, TOut], opts ...options.MapOption) *pipeline.Collection[TOut] {
	return operators.MapTo1(collection, mapper, options.ApplyMapOptions(opts...))
}

func MapWithSideValue[TIn any, TOut any, TSideValue any](collection *pipeline.Collection[TIn], sideValue *pipeline.Value[TSideValue], mapper core.MapperTo1WithSideValue[TIn, TOut, TSideValue], opts ...options.MapOption) *pipeline.Collection[TOut] {
	return operators.MapTo1WithSideValue(collection, sideValue, mapper, options.ApplyMapOptions(opts...))
}

//go:generate go run ./internal/operators/codegen/map/map.go alias

func MapValue[TIn any, TOut any](input *pipeline.Value[TIn], mapper func(context *core.OperatorContext, input0 TIn) TOut, opts ...options.MapValueOption) *pipeline.Value[TOut] {
	return operators.MapValue1To1(input, mapper, options.ApplyMapValueOptions(opts...))
}

//go:generate go run ./internal/operators/codegen/mapvalue/mapvalue.go alias

func ForkToAny[T any](collection *pipeline.Collection[T], n int, opts ...options.ForkOption) []*pipeline.Collection[T] {
	return operators.ForkToAny(collection, n, options.ApplyForkOptions(opts...))
}

//go:generate go run ./internal/operators/codegen/fork/fork.go alias

func Merge[T any](collections []*pipeline.Collection[T], opts ...options.MergeOption) *pipeline.Collection[T] {
	return operators.Merge(collections, options.ApplyMergeOptions(opts...))
}

func Collect[TIn, TIntermediate, TOut any](
	collection *pipeline.Collection[TIn],
	preCollector operators.PreCollector[TIn, TIntermediate],
	postCollector operators.PostCollector[TIntermediate, TOut],
	opts ...options.CollectOption,
) *pipeline.Value[TOut] {
	return operators.Collect(collection, preCollector, postCollector, options.ApplyCollectOptions(opts...))
}

func CollectWithSideValue[TIn, TIntermediate, TOut, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	preCollector operators.PreCollectorWithSideValue[TIn, TIntermediate, TSideValue],
	postCollector operators.PostCollectorWithSideValue[TIntermediate, TOut, TSideValue],
	opts ...options.CollectOption,
) *pipeline.Value[TOut] {
	return operators.CollectWithSideValue(collection, sideValue, preCollector, postCollector, options.ApplyCollectOptions(opts...))
}

type Reducer[TIn0 any, TOut0 any] = core.Reducer1To1[TIn0, TOut0]

func Shuffle[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To1[TIntermediate0, TOut0]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any,
](
	collection *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle1To1[TSerializer0, TReducer](collection, options.ApplyShuffleOptions(opts...))
}

func ShuffleWithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To1[TIntermediate0, TOut0]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TSideValue any,
](
	collection *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle1To1WithSideValue[TSerializer0, TReducer](collection, sideValue, options.ApplyShuffleOptions(opts...))
}

//go:generate go run ./internal/operators/codegen/shuffle/shuffle.go alias

func Ignore[T any](collection *pipeline.Collection[T], opts ...options.IgnoreOption) {
	operators.Ignore(collection, options.ApplyIgnoreOptions(opts...))
}

func SpillBuffer[
	TSerializer core.ElementSerializer[TValue],
	TValue any,
](
	collection *pipeline.Collection[TValue],
	opts ...options.SpillBufferOption,
) *pipeline.Collection[TValue] {
	return operators.SpillBuffer[TSerializer, TValue](collection, options.ApplySpillBufferOptions(opts...))
}

func ToCollection[T any](value *pipeline.Value[T], opts ...options.ToCollectionOption) *pipeline.Collection[T] {
	return operators.ToCollection(value, options.ApplyToCollectionOptions(opts...))
}
