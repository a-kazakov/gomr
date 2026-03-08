package core

import "github.com/a-kazakov/gomr/internal/primitives"

type Reducer1To1[TIn0 any, TOut0 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], emitter0 *primitives.Emitter[TOut0])
}

type Reducer1To2[TIn0 any, TOut0 any, TOut1 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1])
}

type Reducer1To3[TIn0 any, TOut0 any, TOut1 any, TOut2 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2])
}

type Reducer1To4[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3])
}

type Reducer1To5[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3], emitter4 *primitives.Emitter[TOut4])
}

type Reducer2To1[TIn0 any, TIn1 any, TOut0 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], emitter0 *primitives.Emitter[TOut0])
}

type Reducer2To2[TIn0 any, TIn1 any, TOut0 any, TOut1 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1])
}

type Reducer2To3[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2])
}

type Reducer2To4[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3])
}

type Reducer2To5[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3], emitter4 *primitives.Emitter[TOut4])
}

type Reducer3To1[TIn0 any, TIn1 any, TIn2 any, TOut0 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], emitter0 *primitives.Emitter[TOut0])
}

type Reducer3To2[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1])
}

type Reducer3To3[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2])
}

type Reducer3To4[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3])
}

type Reducer3To5[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3], emitter4 *primitives.Emitter[TOut4])
}

type Reducer4To1[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], emitter0 *primitives.Emitter[TOut0])
}

type Reducer4To2[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1])
}

type Reducer4To3[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2])
}

type Reducer4To4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3])
}

type Reducer4To5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3], emitter4 *primitives.Emitter[TOut4])
}

type Reducer5To1[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], receiver4 ShuffleReceiver[TIn4], emitter0 *primitives.Emitter[TOut0])
}

type Reducer5To2[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], receiver4 ShuffleReceiver[TIn4], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1])
}

type Reducer5To3[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], receiver4 ShuffleReceiver[TIn4], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2])
}

type Reducer5To4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], receiver4 ShuffleReceiver[TIn4], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3])
}

type Reducer5To5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] interface {
	Reduce(key []byte, receiver0 ShuffleReceiver[TIn0], receiver1 ShuffleReceiver[TIn1], receiver2 ShuffleReceiver[TIn2], receiver3 ShuffleReceiver[TIn3], receiver4 ShuffleReceiver[TIn4], emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3], emitter4 *primitives.Emitter[TOut4])
}

type ReducerTo1Teardown[TOut0 any] interface {
	Teardown(emitter0 *primitives.Emitter[TOut0])
}

type ReducerTo2Teardown[TOut0 any, TOut1 any] interface {
	Teardown(emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1])
}

type ReducerTo3Teardown[TOut0 any, TOut1 any, TOut2 any] interface {
	Teardown(emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2])
}

type ReducerTo4Teardown[TOut0 any, TOut1 any, TOut2 any, TOut3 any] interface {
	Teardown(emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3])
}

type ReducerTo5Teardown[TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any] interface {
	Teardown(emitter0 *primitives.Emitter[TOut0], emitter1 *primitives.Emitter[TOut1], emitter2 *primitives.Emitter[TOut2], emitter3 *primitives.Emitter[TOut3], emitter4 *primitives.Emitter[TOut4])
}
