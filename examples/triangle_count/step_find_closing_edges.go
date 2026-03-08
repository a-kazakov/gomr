package trianglecount

import (
	"encoding/binary"

	"github.com/a-kazakov/gomr"
)

type findClosingEdgesSerializer struct{}

func (s findClosingEdgesSerializer) MarshalKeyToBytes(value *GraphEdge, dest []byte, cursor int64) (int, int64) {
	binary.NativeEndian.PutUint32(dest, value.Lower)
	binary.NativeEndian.PutUint32(dest[4:], value.Upper)
	return 8, gomr.StopEmitting
}

func (s findClosingEdgesSerializer) UnmarshalValueFromBytes(key []byte, data []byte, dest *GraphEdge) {
	dest.Lower = binary.NativeEndian.Uint32(data)
	dest.Upper = binary.NativeEndian.Uint32(data[4:])
}

func (s findClosingEdgesSerializer) MarshalValueToBytes(value *GraphEdge, dest []byte) int {
	binary.NativeEndian.PutUint32(dest, value.Lower)
	binary.NativeEndian.PutUint32(dest[4:], value.Upper)
	return 8
}

type findClosingEdgesReducer struct{}

func (r findClosingEdgesReducer) Reduce(key []byte, potentialReceiver gomr.ShuffleReceiver[GraphEdge], controlReceiver gomr.ShuffleReceiver[GraphEdge], emitter gomr.Emitter[GraphEdge]) {
	hasControlEdge := false
	for range controlReceiver.IterValues() {
		hasControlEdge = true
		break
	}
	if !hasControlEdge {
		return
	}
	for e := range potentialReceiver.IterValues() {
		*emitter.GetEmitPointer() = *e
	}
}

func FindClosingEdges(potentialEdges gomr.Collection[GraphEdge], controlEdges gomr.Collection[GraphEdge]) gomr.Collection[GraphEdge] {
	return gomr.Shuffle2[*findClosingEdgesSerializer, *findClosingEdgesSerializer, *findClosingEdgesReducer](
		potentialEdges,
		controlEdges,
		gomr.WithOperationName("Find closing edges"),
		gomr.WithOutCollectionNames("Closing edges"),
	)
}
