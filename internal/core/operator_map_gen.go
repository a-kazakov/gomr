package core

import "github.com/a-kazakov/gomr/internal/primitives"

type MapperTo1[TIn any, TOut0 any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0],
)

type MapperTo1WithSideValue[TIn any, TOut0 any, TSideValue any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0],
	sideValue TSideValue,
)

type MapperTo2[TIn any, TOut0 any, TOut1 any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1],
)

type MapperTo2WithSideValue[TIn any, TOut0 any, TOut1 any, TSideValue any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1],
	sideValue TSideValue,
)

type MapperTo3[TIn any, TOut0 any, TOut1 any, TOut2 any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2],
)

type MapperTo3WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TSideValue any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2],
	sideValue TSideValue,
)

type MapperTo4[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3],
)

type MapperTo4WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TSideValue any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3],
	sideValue TSideValue,
)

type MapperTo5[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3], emitter4 *primitives.Emitter[TOut4],
)

type MapperTo5WithSideValue[TIn any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any, TSideValue any] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3], emitter4 *primitives.Emitter[TOut4],
	sideValue TSideValue,
)
