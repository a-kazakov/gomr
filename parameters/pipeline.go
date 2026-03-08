package parameters

import (
	"github.com/oklog/ulid/v2"
)

// PipelineParameters holds pipeline-level configuration (e.g., job ID).
type PipelineParameters struct {
	JobID *Parameter[string]
}

func NewPipelineParameters() *PipelineParameters {
	return &PipelineParameters{
		JobID: NewStringParamDynamic(
			"pipeline", "job_id",
			"Unique identifier for the pipeline job",
			func() string { return ulid.Make().String() },
			NotEmpty,
		),
	}
}

func (pp *PipelineParameters) RegisterFlags(f Flagger) {
	RegisterAllFlags(pp, f)
}

func (pp *PipelineParameters) LoadFromSource(lookup Source) error {
	return LoadAllFromSource(pp, lookup)
}
