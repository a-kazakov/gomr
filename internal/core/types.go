// Package core defines the fundamental interfaces, types, and constants used
// across all gomr subsystems.
package core

import (
	"iter"
	"math"
	"sync/atomic"
)

// StopEmitting is a sentinel cursor value returned by ShuffleSerializer.MarshalKeyToBytes
// to signal that no more keys should be emitted for a given input record.
const StopEmitting = math.MinInt64

// ShuffleSerializer defines how to serialize and deserialize shuffle key-value pairs.
// MarshalKeyToBytes max output is 1 KB; MarshalValueToBytes max output is 16 MB.
type ShuffleSerializer[TIn any, TIntermediate any] interface {
	// MarshalKeyToBytes serializes a key from value into dest starting at cursor.
	// Returns the key length and the next cursor (or StopEmitting to stop).
	MarshalKeyToBytes(value *TIn, dest []byte, cursor int64) (int, int64)
	// MarshalValueToBytes serializes the value part of an input record into dest.
	MarshalValueToBytes(value *TIn, dest []byte) int
	// UnmarshalValueFromBytes reconstructs an intermediate value from key and data bytes.
	UnmarshalValueFromBytes(key []byte, data []byte, dest *TIntermediate)
}

type ShuffleSerializerSetup interface {
	Setup(ctx *OperatorContext)
}

type ShuffleSerializerSetupWithSideValue[TSideValue any] interface {
	Setup(ctx *OperatorContext, sideValue TSideValue)
}

type ShuffleReducerSetup interface {
	Setup(ctx *OperatorContext)
}

type ShuffleReducerSetupWithSideValue[TSideValue any] interface {
	Setup(ctx *OperatorContext, sideValue TSideValue)
}

// ElementSerializer defines how to serialize and deserialize individual elements,
// used by SpillBuffer for disk spilling.
type ElementSerializer[TValue any] interface {
	// MarshalElementToBytes serializes value into dest, returning the number of bytes written.
	MarshalElementToBytes(value *TValue, dest []byte) int
	// UnmarshalElementFromBytes deserializes data bytes into dest.
	UnmarshalElementFromBytes(data []byte, dest *TValue)
}

type ElementSerializerSetup interface {
	Setup(ctx *OperatorContext)
}

type ShuffleReceiver[TIn any] interface {
	IterValues() iter.Seq[*TIn]
}

type CollectionReceiver[TIn any] interface {
	ShuffleReceiver[TIn]
	IterBatches() iter.Seq[[]TIn]
}

type UserCountersMap interface {
	GetCounter(name string) *atomic.Int64
}

type CompressionAlgorithm int

const (
	CompressionAlgorithmNone    CompressionAlgorithm = iota
	CompressionAlgorithmLz4
	CompressionAlgorithmZstdFast
	CompressionAlgorithmZstdDefault
)
