package parameters

import "runtime"

// ProcessingParameters holds defaults for operator parallelism.
type ProcessingParameters struct {
	DefaultParallelism *Parameter[int]
}

func NewProcessingParameters() *ProcessingParameters {
	return &ProcessingParameters{
		DefaultParallelism: NewIntParamDynamic(
			"processing", "parallelism",
			"Default parallelism for processing operations",
			func() int { return runtime.GOMAXPROCS(0) },
			Positive[int],
		),
	}
}

func (pp *ProcessingParameters) RegisterFlags(f Flagger) {
	RegisterAllFlags(pp, f)
}

func (pp *ProcessingParameters) LoadFromSource(lookup Source) error {
	return LoadAllFromSource(pp, lookup)
}
