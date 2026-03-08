package gomr

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

type MapperTo1[TIn any, TOut0 any] = core.MapperTo1[TIn, TOut0]

func MapTo1[TIn any, TOut0 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo1[TIn, TOut0],
	opts ...options.MapOption,
) *pipeline.Collection[TOut0] {
	return operators.MapTo1[TIn, TOut0](collection, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo1WithSideValue[TIn any, TOut0 any, TSideValue any] = core.MapperTo1WithSideValue[TIn, TOut0, TSideValue]

func MapTo1WithSideValue[TIn any, TOut0 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo1WithSideValue[TIn, TOut0, TSideValue],
	opts ...options.MapOption,
) *pipeline.Collection[TOut0] {
	return operators.MapTo1WithSideValue[TIn, TOut0, TSideValue](collection, sideValue, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo2[TIn any, TOut0 any, TOut1 any] = core.MapperTo2[TIn, TOut0, TOut1]

func MapTo2[TIn any, TOut0 any, TOut1 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo2[TIn, TOut0, TOut1],
	opts ...options.MapOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.MapTo2[TIn, TOut0, TOut1](collection, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo2WithSideValue[TIn any, TOut0 any, TOut1 any, TSideValue any] = core.MapperTo2WithSideValue[TIn, TOut0, TOut1, TSideValue]

func MapTo2WithSideValue[TIn any, TOut0 any, TOut1 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo2WithSideValue[TIn, TOut0, TOut1, TSideValue],
	opts ...options.MapOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1]) {
	return operators.MapTo2WithSideValue[TIn, TOut0, TOut1, TSideValue](collection, sideValue, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo3[TIn any, TOut0 any, TOut1 any, TOut2 any] = core.MapperTo3[TIn, TOut0, TOut1, TOut2]

func MapTo3[TIn any, TOut0 any, TOut1 any, TOut2 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo3[TIn, TOut0, TOut1, TOut2],
	opts ...options.MapOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.MapTo3[TIn, TOut0, TOut1, TOut2](collection, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo3WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TSideValue any] = core.MapperTo3WithSideValue[TIn, TOut0, TOut1, TOut2, TSideValue]

func MapTo3WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo3WithSideValue[TIn, TOut0, TOut1, TOut2, TSideValue],
	opts ...options.MapOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2]) {
	return operators.MapTo3WithSideValue[TIn, TOut0, TOut1, TOut2, TSideValue](collection, sideValue, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo4[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] = core.MapperTo4[TIn, TOut0, TOut1, TOut2, TOut3]

func MapTo4[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo4[TIn, TOut0, TOut1, TOut2, TOut3],
	opts ...options.MapOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.MapTo4[TIn, TOut0, TOut1, TOut2, TOut3](collection, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo4WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any] = core.MapperTo4WithSideValue[TIn, TOut0, TOut1, TOut2, TOut3, TSideValue]

func MapTo4WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo4WithSideValue[TIn, TOut0, TOut1, TOut2, TOut3, TSideValue],
	opts ...options.MapOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3]) {
	return operators.MapTo4WithSideValue[TIn, TOut0, TOut1, TOut2, TOut3, TSideValue](collection, sideValue, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo5[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] = core.MapperTo5[TIn, TOut0, TOut1, TOut2, TOut3, TOut4]

func MapTo5[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	collection *pipeline.Collection[TIn],
	mapper core.MapperTo5[TIn, TOut0, TOut1, TOut2, TOut3, TOut4],
	opts ...options.MapOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.MapTo5[TIn, TOut0, TOut1, TOut2, TOut3, TOut4](collection, mapper, options.ApplyMapOptions(opts...))
}

type MapperTo5WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any] = core.MapperTo5WithSideValue[TIn, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue]

func MapTo5WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any](
	collection *pipeline.Collection[TIn],
	sideValue *pipeline.Value[TSideValue],
	mapper core.MapperTo5WithSideValue[TIn, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue],
	opts ...options.MapOption,
) (*pipeline.Collection[TOut0], *pipeline.Collection[TOut1], *pipeline.Collection[TOut2], *pipeline.Collection[TOut3], *pipeline.Collection[TOut4]) {
	return operators.MapTo5WithSideValue[TIn, TOut0, TOut1, TOut2, TOut3, TOut4, TSideValue](collection, sideValue, mapper, options.ApplyMapOptions(opts...))
}
