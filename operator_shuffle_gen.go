package gomr

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

// ========== Shuffle{N}To{M} - full aliases ==========

type Reducer1To1[TIn0 any, TOut0 any] = core.Reducer1To1[TIn0, TOut0]

func Shuffle1To1[
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
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle1To1[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0](collection0, options.ApplyShuffleOptions(opts...))
}

func Shuffle1To1WithSideValue[
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
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle1To1WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer1To2[TIn0 any, TOut0 any, TOut1 any] = core.Reducer1To2[TIn0, TOut0, TOut1]

func Shuffle1To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To2[TIntermediate0, TOut0, TOut1]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle1To2[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1](collection0, options.ApplyShuffleOptions(opts...))
}

func Shuffle1To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To2[TIntermediate0, TOut0, TOut1]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle1To2WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer1To3[TIn0 any, TOut0 any, TOut1 any, TOut2 any] = core.Reducer1To3[TIn0, TOut0, TOut1, TOut2]

func Shuffle1To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To3[TIntermediate0, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle1To3[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2](collection0, options.ApplyShuffleOptions(opts...))
}

func Shuffle1To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To3[TIntermediate0, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle1To3WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer1To4[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] = core.Reducer1To4[TIn0, TOut0, TOut1, TOut2, TOut3]

func Shuffle1To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To4[TIntermediate0, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle1To4[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TOut3](collection0, options.ApplyShuffleOptions(opts...))
}

func Shuffle1To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To4[TIntermediate0, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle1To4WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TOut3, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer1To5[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] = core.Reducer1To5[TIn0, TOut0, TOut1, TOut2, TOut3, TOut4]

func Shuffle1To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To5[TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle1To5[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4](collection0, options.ApplyShuffleOptions(opts...))
}

func Shuffle1To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To5[TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle1To5WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer2To1[TIn0 any, TIn1 any, TOut0 any] = core.Reducer2To1[TIn0, TIn1, TOut0]

func Shuffle2To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To1[TIntermediate0, TIntermediate1, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle2To1[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0](collection0, collection1, options.ApplyShuffleOptions(opts...))
}

func Shuffle2To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To1[TIntermediate0, TIntermediate1, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle2To1WithSideValue[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TSideValue](collection0, collection1, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer2To2[TIn0 any, TIn1 any, TOut0 any, TOut1 any] = core.Reducer2To2[TIn0, TIn1, TOut0, TOut1]

func Shuffle2To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To2[TIntermediate0, TIntermediate1, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle2To2[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TOut1](collection0, collection1, options.ApplyShuffleOptions(opts...))
}

func Shuffle2To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To2[TIntermediate0, TIntermediate1, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle2To2WithSideValue[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TOut1, TSideValue](collection0, collection1, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer2To3[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any] = core.Reducer2To3[TIn0, TIn1, TOut0, TOut1, TOut2]

func Shuffle2To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To3[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle2To3[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2](collection0, collection1, options.ApplyShuffleOptions(opts...))
}

func Shuffle2To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To3[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle2To3WithSideValue[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TSideValue](collection0, collection1, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer2To4[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] = core.Reducer2To4[TIn0, TIn1, TOut0, TOut1, TOut2, TOut3]

func Shuffle2To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To4[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle2To4[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3](collection0, collection1, options.ApplyShuffleOptions(opts...))
}

func Shuffle2To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To4[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle2To4WithSideValue[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3, TSideValue](collection0, collection1, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer2To5[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] = core.Reducer2To5[TIn0, TIn1, TOut0, TOut1, TOut2, TOut3, TOut4]

func Shuffle2To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To5[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle2To5[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3, TOut4](collection0, collection1, options.ApplyShuffleOptions(opts...))
}

func Shuffle2To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To5[TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle2To5WithSideValue[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue](collection0, collection1, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer3To1[TIn0 any, TIn1 any, TIn2 any, TOut0 any] = core.Reducer3To1[TIn0, TIn1, TIn2, TOut0]

func Shuffle3To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To1[TIntermediate0, TIntermediate1, TIntermediate2, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle3To1[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0](collection0, collection1, collection2, options.ApplyShuffleOptions(opts...))
}

func Shuffle3To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To1[TIntermediate0, TIntermediate1, TIntermediate2, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle3To1WithSideValue[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TSideValue](collection0, collection1, collection2, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer3To2[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any] = core.Reducer3To2[TIn0, TIn1, TIn2, TOut0, TOut1]

func Shuffle3To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To2[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle3To2[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1](collection0, collection1, collection2, options.ApplyShuffleOptions(opts...))
}

func Shuffle3To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To2[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle3To2WithSideValue[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TSideValue](collection0, collection1, collection2, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer3To3[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any] = core.Reducer3To3[TIn0, TIn1, TIn2, TOut0, TOut1, TOut2]

func Shuffle3To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To3[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle3To3[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2](collection0, collection1, collection2, options.ApplyShuffleOptions(opts...))
}

func Shuffle3To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To3[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle3To3WithSideValue[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TSideValue](collection0, collection1, collection2, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer3To4[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] = core.Reducer3To4[TIn0, TIn1, TIn2, TOut0, TOut1, TOut2, TOut3]

func Shuffle3To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To4[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle3To4[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3](collection0, collection1, collection2, options.ApplyShuffleOptions(opts...))
}

func Shuffle3To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To4[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle3To4WithSideValue[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3, TSideValue](collection0, collection1, collection2, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer3To5[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] = core.Reducer3To5[TIn0, TIn1, TIn2, TOut0, TOut1, TOut2, TOut3, TOut4]

func Shuffle3To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To5[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle3To5[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3, TOut4](collection0, collection1, collection2, options.ApplyShuffleOptions(opts...))
}

func Shuffle3To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To5[TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle3To5WithSideValue[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue](collection0, collection1, collection2, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer4To1[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any] = core.Reducer4To1[TIn0, TIn1, TIn2, TIn3, TOut0]

func Shuffle4To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle4To1[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0](collection0, collection1, collection2, collection3, options.ApplyShuffleOptions(opts...))
}

func Shuffle4To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle4To1WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TSideValue](collection0, collection1, collection2, collection3, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer4To2[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any] = core.Reducer4To2[TIn0, TIn1, TIn2, TIn3, TOut0, TOut1]

func Shuffle4To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To2[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle4To2[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1](collection0, collection1, collection2, collection3, options.ApplyShuffleOptions(opts...))
}

func Shuffle4To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To2[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle4To2WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TSideValue](collection0, collection1, collection2, collection3, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer4To3[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any] = core.Reducer4To3[TIn0, TIn1, TIn2, TIn3, TOut0, TOut1, TOut2]

func Shuffle4To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To3[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle4To3[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2](collection0, collection1, collection2, collection3, options.ApplyShuffleOptions(opts...))
}

func Shuffle4To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To3[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle4To3WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TSideValue](collection0, collection1, collection2, collection3, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer4To4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] = core.Reducer4To4[TIn0, TIn1, TIn2, TIn3, TOut0, TOut1, TOut2, TOut3]

func Shuffle4To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To4[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle4To4[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3](collection0, collection1, collection2, collection3, options.ApplyShuffleOptions(opts...))
}

func Shuffle4To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To4[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle4To4WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3, TSideValue](collection0, collection1, collection2, collection3, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer4To5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] = core.Reducer4To5[TIn0, TIn1, TIn2, TIn3, TOut0, TOut1, TOut2, TOut3, TOut4]

func Shuffle4To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To5[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle4To5[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3, TOut4](collection0, collection1, collection2, collection3, options.ApplyShuffleOptions(opts...))
}

func Shuffle4To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To5[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle4To5WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue](collection0, collection1, collection2, collection3, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer5To1[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any] = core.Reducer5To1[TIn0, TIn1, TIn2, TIn3, TIn4, TOut0]

func Shuffle5To1[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle5To1[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0](collection0, collection1, collection2, collection3, collection4, options.ApplyShuffleOptions(opts...))
}

func Shuffle5To1WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle5To1WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TSideValue](collection0, collection1, collection2, collection3, collection4, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer5To2[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any] = core.Reducer5To2[TIn0, TIn1, TIn2, TIn3, TIn4, TOut0, TOut1]

func Shuffle5To2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To2[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle5To2[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1](collection0, collection1, collection2, collection3, collection4, options.ApplyShuffleOptions(opts...))
}

func Shuffle5To2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To2[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle5To2WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TSideValue](collection0, collection1, collection2, collection3, collection4, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer5To3[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any] = core.Reducer5To3[TIn0, TIn1, TIn2, TIn3, TIn4, TOut0, TOut1, TOut2]

func Shuffle5To3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To3[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle5To3[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2](collection0, collection1, collection2, collection3, collection4, options.ApplyShuffleOptions(opts...))
}

func Shuffle5To3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To3[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle5To3WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TSideValue](collection0, collection1, collection2, collection3, collection4, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer5To4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] = core.Reducer5To4[TIn0, TIn1, TIn2, TIn3, TIn4, TOut0, TOut1, TOut2, TOut3]

func Shuffle5To4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To4[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle5To4[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3](collection0, collection1, collection2, collection3, collection4, options.ApplyShuffleOptions(opts...))
}

func Shuffle5To4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To4[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle5To4WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3, TSideValue](collection0, collection1, collection2, collection3, collection4, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer5To5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] = core.Reducer5To5[TIn0, TIn1, TIn2, TIn3, TIn4, TOut0, TOut1, TOut2, TOut3, TOut4]

func Shuffle5To5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To5[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle5To5[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3, TOut4](collection0, collection1, collection2, collection3, collection4, options.ApplyShuffleOptions(opts...))
}

func Shuffle5To5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To5[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle5To5WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue](collection0, collection1, collection2, collection3, collection4, sideValue, options.ApplyShuffleOptions(opts...))
}

// ========== ShuffleTo{M} -> Shuffle1To{M} (single input, M outputs) ==========

type ReducerTo1[TIn0 any, TOut0 any] = core.Reducer1To1[TIn0, TOut0]

func ShuffleTo1[
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
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle1To1[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0](collection0, options.ApplyShuffleOptions(opts...))
}

func ShuffleTo1WithSideValue[
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
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle1To1WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type ReducerTo2[TIn0 any, TOut0 any, TOut1 any] = core.Reducer1To2[TIn0, TOut0, TOut1]

func ShuffleTo2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To2[TIntermediate0, TOut0, TOut1]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle1To2[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1](collection0, options.ApplyShuffleOptions(opts...))
}

func ShuffleTo2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To2[TIntermediate0, TOut0, TOut1]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.Shuffle1To2WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type ReducerTo3[TIn0 any, TOut0 any, TOut1 any, TOut2 any] = core.Reducer1To3[TIn0, TOut0, TOut1, TOut2]

func ShuffleTo3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To3[TIntermediate0, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle1To3[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2](collection0, options.ApplyShuffleOptions(opts...))
}

func ShuffleTo3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To3[TIntermediate0, TOut0, TOut1, TOut2]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.Shuffle1To3WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type ReducerTo4[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] = core.Reducer1To4[TIn0, TOut0, TOut1, TOut2, TOut3]

func ShuffleTo4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To4[TIntermediate0, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle1To4[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TOut3](collection0, options.ApplyShuffleOptions(opts...))
}

func ShuffleTo4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To4[TIntermediate0, TOut0, TOut1, TOut2, TOut3]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.Shuffle1To4WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TOut3, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type ReducerTo5[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] = core.Reducer1To5[TIn0, TOut0, TOut1, TOut2, TOut3, TOut4]

func ShuffleTo5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To5[TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any,
](
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle1To5[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4](collection0, options.ApplyShuffleOptions(opts...))
}

func ShuffleTo5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To5[TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.Shuffle1To5WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

// ========== Shuffle{N} -> Shuffle{N}To1 (N inputs, single output) ==========

type Reducer1[TIn0 any, TOut0 any] = core.Reducer1To1[TIn0, TOut0]

func Shuffle1[
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
	collection0 *pipeline.Collection[TIn0],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle1To1[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0](collection0, options.ApplyShuffleOptions(opts...))
}

func Shuffle1WithSideValue[
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
	collection0 *pipeline.Collection[TIn0],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle1To1WithSideValue[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, TOut0, TSideValue](collection0, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer2[TIn0 any, TIn1 any, TOut0 any] = core.Reducer2To1[TIn0, TIn1, TOut0]

func Shuffle2[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To1[TIntermediate0, TIntermediate1, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle2To1[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0](collection0, collection1, options.ApplyShuffleOptions(opts...))
}

func Shuffle2WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TReducer interface {
		*TReducerState
		core.Reducer2To1[TIntermediate0, TIntermediate1, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIntermediate0 any, TIntermediate1 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle2To1WithSideValue[TSerializer0, TSerializer1, TReducer, TSerializerState0, TSerializerState1, TReducerState, TIn0, TIn1, TIntermediate0, TIntermediate1, TOut0, TSideValue](collection0, collection1, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer3[TIn0 any, TIn1 any, TIn2 any, TOut0 any] = core.Reducer3To1[TIn0, TIn1, TIn2, TOut0]

func Shuffle3[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To1[TIntermediate0, TIntermediate1, TIntermediate2, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle3To1[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0](collection0, collection1, collection2, options.ApplyShuffleOptions(opts...))
}

func Shuffle3WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TReducer interface {
		*TReducerState
		core.Reducer3To1[TIntermediate0, TIntermediate1, TIntermediate2, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle3To1WithSideValue[TSerializer0, TSerializer1, TSerializer2, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TReducerState, TIn0, TIn1, TIn2, TIntermediate0, TIntermediate1, TIntermediate2, TOut0, TSideValue](collection0, collection1, collection2, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any] = core.Reducer4To1[TIn0, TIn1, TIn2, TIn3, TOut0]

func Shuffle4[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle4To1[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0](collection0, collection1, collection2, collection3, options.ApplyShuffleOptions(opts...))
}

func Shuffle4WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TReducer interface {
		*TReducerState
		core.Reducer4To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle4To1WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TReducerState, TIn0, TIn1, TIn2, TIn3, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TOut0, TSideValue](collection0, collection1, collection2, collection3, sideValue, options.ApplyShuffleOptions(opts...))
}

type Reducer5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any] = core.Reducer5To1[TIn0, TIn1, TIn2, TIn3, TIn4, TOut0]

func Shuffle5[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle5To1[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0](collection0, collection1, collection2, collection3, collection4, options.ApplyShuffleOptions(opts...))
}

func Shuffle5WithSideValue[
	TSerializer0 interface {
		*TSerializerState0
		core.ShuffleSerializer[TIn0, TIntermediate0]
	},
	TSerializer1 interface {
		*TSerializerState1
		core.ShuffleSerializer[TIn1, TIntermediate1]
	},
	TSerializer2 interface {
		*TSerializerState2
		core.ShuffleSerializer[TIn2, TIntermediate2]
	},
	TSerializer3 interface {
		*TSerializerState3
		core.ShuffleSerializer[TIn3, TIntermediate3]
	},
	TSerializer4 interface {
		*TSerializerState4
		core.ShuffleSerializer[TIn4, TIntermediate4]
	},
	TReducer interface {
		*TReducerState
		core.Reducer5To1[TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0]
	},
	TSerializerState0 any, TSerializerState1 any, TSerializerState2 any, TSerializerState3 any, TSerializerState4 any,
	TReducerState any,
	TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TIntermediate0 any, TIntermediate1 any, TIntermediate2 any, TIntermediate3 any, TIntermediate4 any, TOut0 any, TSideValue any,
](
	collection0 *pipeline.Collection[TIn0], collection1 *pipeline.Collection[TIn1], collection2 *pipeline.Collection[TIn2], collection3 *pipeline.Collection[TIn3], collection4 *pipeline.Collection[TIn4],
	sideValue *pipeline.Value[TSideValue],
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle5To1WithSideValue[TSerializer0, TSerializer1, TSerializer2, TSerializer3, TSerializer4, TReducer, TSerializerState0, TSerializerState1, TSerializerState2, TSerializerState3, TSerializerState4, TReducerState, TIn0, TIn1, TIn2, TIn3, TIn4, TIntermediate0, TIntermediate1, TIntermediate2, TIntermediate3, TIntermediate4, TOut0, TSideValue](collection0, collection1, collection2, collection3, collection4, sideValue, options.ApplyShuffleOptions(opts...))
}
