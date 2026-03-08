package trianglecount

import (
	"github.com/a-kazakov/gomr"
)

func UnpackEdges(inputCollection gomr.Collection[NodeWithAdjacentEdges]) gomr.Collection[GraphEdge] {
	return gomr.Map[NodeWithAdjacentEdges, GraphEdge](
		inputCollection,
		func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[NodeWithAdjacentEdges], emitter gomr.Emitter[GraphEdge]) {
			for inValue := range receiver.IterValues() {
				for _, edge := range inValue.Edges {
					*emitter.GetEmitPointer() = GraphEdge{
						Lower: min(inValue.Node, edge),
						Upper: max(inValue.Node, edge),
					}
				}
			}
		},
		gomr.WithOperationName("Unpack edges"),
		gomr.WithOutCollectionNames("All edges"),
		gomr.WithOutBatchSize(4096),
	)
}
