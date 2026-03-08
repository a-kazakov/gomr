package trianglecount

import (
	"encoding/binary"

	"github.com/a-kazakov/gomr"
)

type findPotentialEdgesLeftSerializer struct{}

func (s findPotentialEdgesLeftSerializer) Setup(context gomr.OperatorContext) {}

func (s findPotentialEdgesLeftSerializer) MarshalKeyToBytes(value *GraphEdge, dest []byte, cursor int64) (int, int64) {
	binary.NativeEndian.PutUint32(dest, value.Upper)
	return 4, gomr.StopEmitting
}

func (s findPotentialEdgesLeftSerializer) MarshalValueToBytes(value *GraphEdge, dest []byte) int {
	binary.NativeEndian.PutUint32(dest, value.Lower)
	binary.NativeEndian.PutUint32(dest[4:], value.Upper)
	return 8
}

func (s findPotentialEdgesLeftSerializer) UnmarshalValueFromBytes(key []byte, data []byte, dest *GraphEdge) {
	dest.Lower = binary.NativeEndian.Uint32(data)
	dest.Upper = binary.NativeEndian.Uint32(data[4:])
}

type findpotentialEdgesRightSerializer struct{}

func (s findpotentialEdgesRightSerializer) MarshalKeyToBytes(value *GraphEdge, dest []byte, cursor int64) (int, int64) {
	binary.NativeEndian.PutUint32(dest, value.Lower)
	return 4, gomr.StopEmitting
}

func (s findpotentialEdgesRightSerializer) MarshalValueToBytes(value *GraphEdge, dest []byte) int {
	binary.NativeEndian.PutUint32(dest, value.Lower)
	binary.NativeEndian.PutUint32(dest[4:], value.Upper)
	return 8
}

func (s findpotentialEdgesRightSerializer) UnmarshalValueFromBytes(key []byte, data []byte, dest *GraphEdge) {
	dest.Lower = binary.NativeEndian.Uint32(data)
	dest.Upper = binary.NativeEndian.Uint32(data[4:])
}

type findPotentialEdgesReducer struct {
	rightEdgeBuffer []GraphEdge
}

func (r *findPotentialEdgesReducer) Setup(context gomr.OperatorContext) {
	r.rightEdgeBuffer = make([]GraphEdge, 0, 1000)
}
func (r *findPotentialEdgesReducer) Teardown(emitter gomr.Emitter[GraphEdge]) {
	r.rightEdgeBuffer = nil
}
func (r *findPotentialEdgesReducer) Reduce(key []byte, leftReceiver gomr.ShuffleReceiver[GraphEdge], rightReceiver gomr.ShuffleReceiver[GraphEdge], emitter gomr.Emitter[GraphEdge]) {
	r.rightEdgeBuffer = r.rightEdgeBuffer[:0]
	for rightEdge := range rightReceiver.IterValues() {
		r.rightEdgeBuffer = append(r.rightEdgeBuffer, *rightEdge)
	}
	for leftEdge := range leftReceiver.IterValues() {
		for _, rightEdge := range r.rightEdgeBuffer {
			*emitter.GetEmitPointer() = GraphEdge{
				Lower: leftEdge.Lower,
				Upper: rightEdge.Upper,
			}
		}
	}
}

func FindPotentialEdges(
	leftEdges gomr.Collection[GraphEdge],
	rightEdges gomr.Collection[GraphEdge],
) gomr.Collection[GraphEdge] {
	return gomr.Shuffle2[*findPotentialEdgesLeftSerializer, *findpotentialEdgesRightSerializer, *findPotentialEdgesReducer](
		leftEdges,
		rightEdges,
		gomr.WithOperationName("Find potential edges"),
		gomr.WithOutCollectionNames("Potential edges"),
	)
}
