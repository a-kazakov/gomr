package trianglecount

import (
	"github.com/a-kazakov/gomr"
)

func Build(p gomr.Pipeline, cliqueSize uint32, pairwiseSize uint32) gomr.Value[uint64] {
	firstCliqueNodeId := uint32(0)
	firstPairwiseNodeId := firstCliqueNodeId + cliqueSize

	cliqueNodes := SeedClique(p, cliqueSize, firstCliqueNodeId)
	pairwiseNodes := SeedPairwise(p, pairwiseSize, firstPairwiseNodeId)
	allNodes := gomr.Merge([]gomr.Collection[NodeWithAdjacentEdges]{cliqueNodes, pairwiseNodes},
		gomr.WithOperationName("Merge clique and pairwise"),
		gomr.WithOutCollectionName("All nodes"),
	)

	allEdges := UnpackEdges(allNodes)

	leftEdges, rightEdges, controlEdges := gomr.ForkTo3(allEdges,
		gomr.WithOutCollectionNames("Left edges", "Right edges", "Control edges"),
	)

	potentialEdges := FindPotentialEdges(leftEdges, rightEdges)
	closingEdges := FindClosingEdges(potentialEdges, controlEdges)

	totalCount := CountElements(closingEdges)

	return totalCount
}
