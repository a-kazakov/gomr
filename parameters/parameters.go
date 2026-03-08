// Package parameters provides hierarchical configuration for the gomr framework.
// It supports flag registration, environment variable loading, and per-operator overrides.
package parameters

type Parameters struct {
	Pipeline    *PipelineParameters
	Collections *CollectionsParameters
	Processing  *ProcessingParameters
	Shuffle     *ShuffleParameters
	Metrics     *MetricsParameters
	Disk        *DiskParameters
	SpillBuffer *SpillBufferParameters
}

func NewParameters() *Parameters {
	return &Parameters{
		Pipeline:    NewPipelineParameters(),
		Collections: NewCollectionsParameters(),
		Processing:  NewProcessingParameters(),
		Shuffle:     NewShuffleParameters(),
		Metrics:     NewMetricsParameters(),
		Disk:        NewDiskParameters(),
		SpillBuffer: NewSpillBufferParameters(),
	}
}

func (p *Parameters) RegisterFlags(f Flagger) {
	p.Pipeline.RegisterFlags(f)
	p.Collections.RegisterFlags(f)
	p.Processing.RegisterFlags(f)
	p.Shuffle.RegisterFlags(f)
	p.Metrics.RegisterFlags(f)
	p.Disk.RegisterFlags(f)
	p.SpillBuffer.RegisterFlags(f)
}

func (p *Parameters) LoadFromSource(lookup Source) error {
	if err := p.Pipeline.LoadFromSource(lookup); err != nil {
		return err
	}
	if err := p.Collections.LoadFromSource(lookup); err != nil {
		return err
	}
	if err := p.Processing.LoadFromSource(lookup); err != nil {
		return err
	}
	if err := p.Shuffle.LoadFromSource(lookup); err != nil {
		return err
	}
	if err := p.Metrics.LoadFromSource(lookup); err != nil {
		return err
	}
	if err := p.Disk.LoadFromSource(lookup); err != nil {
		return err
	}
	if err := p.SpillBuffer.LoadFromSource(lookup); err != nil {
		return err
	}
	return nil
}
