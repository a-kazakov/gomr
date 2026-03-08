package trianglecount

import "github.com/a-kazakov/gomr"

func SeedClique(p gomr.Pipeline, cliqueSize uint32, firstCliqueNodeId uint32) gomr.Collection[NodeWithAdjacentEdges] {
	return gomr.NewSeedCollection(p, func(context gomr.OperatorContext, emitter gomr.Emitter[NodeWithAdjacentEdges]) {
		for i := uint32(0); i < cliqueSize; i++ {
			edges := make([]uint32, 0, cliqueSize-i-1)
			for j := i + 1; j < cliqueSize; j++ {
				edges = append(edges, firstCliqueNodeId+j)
			}
			*emitter.GetEmitPointer() = NodeWithAdjacentEdges{
				Node:  firstCliqueNodeId + i,
				Edges: edges,
			}
		}
	},
		gomr.WithOperationName("Seed clique"),
		gomr.WithOutCollectionName("Clique nodes"),
	)
}

func SeedPairwise(p gomr.Pipeline, pairwiseSize uint32, firstPairwiseNodeId uint32) gomr.Collection[NodeWithAdjacentEdges] {
	return gomr.NewSeedCollection(p, func(context gomr.OperatorContext, emitter gomr.Emitter[NodeWithAdjacentEdges]) {
		for i := uint32(0); i < pairwiseSize; i++ {
			edges := make([]uint32, 0, (pairwiseSize-i-1)/2+1)
			for j := i + 1; j < pairwiseSize; j += 2 {
				edges = append(edges, firstPairwiseNodeId+j)
			}
			*emitter.GetEmitPointer() = NodeWithAdjacentEdges{
				Node:  firstPairwiseNodeId + i,
				Edges: edges,
			}
		}
	},
		gomr.WithOperationName("Seed pairwise"),
		gomr.WithOutCollectionName("Pairwise nodes"),
	)
}
