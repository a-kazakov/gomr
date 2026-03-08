package trianglecount

type NodeWithAdjacentEdges struct {
	Node  uint32
	Edges []uint32
}

type GraphEdge struct {
	Lower uint32
	Upper uint32
}
