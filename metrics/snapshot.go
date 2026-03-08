package metrics

import (
	"time"

	"github.com/a-kazakov/gomr/internal/core"
)

// PipelineMetricsSnapshot is a JSON-serializable point-in-time snapshot of all pipeline metrics.
type PipelineMetricsSnapshot struct {
	Operations   map[string]*OperationMetricsSnapshot  `json:"operations"`
	Collections  map[string]*CollectionMetricsSnapshot `json:"collections"`
	Values       map[string]*ValueMetricsSnapshot      `json:"values"`
	UserCounters map[string]int64                      `json:"user_counters"`
}

func (p *PipelineMetrics) toSnapshot() *PipelineMetricsSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := &PipelineMetricsSnapshot{
		Operations:   make(map[string]*OperationMetricsSnapshot, len(p.Operations)),
		Collections:  make(map[string]*CollectionMetricsSnapshot, len(p.Collections)),
		Values:       make(map[string]*ValueMetricsSnapshot, len(p.Values)),
		UserCounters: p.UserCounters.Snapshot(),
	}
	for id, op := range p.Operations {
		result.Operations[id] = op.toSnapshot()
	}
	for id, coll := range p.Collections {
		result.Collections[id] = coll.toSnapshot()
	}
	for id, value := range p.Values {
		result.Values[id] = value.toSnapshot()
	}
	return result
}

type ShuffleMetricsSnapshot struct {
	SpillsCount      int64 `json:"spills_count"`
	DiskUsage        int64 `json:"disk_usage"`
	ElementsGathered int64 `json:"elements_gathered"`
	GroupsGathered   int64 `json:"groups_gathered"`
}

func (s *ShuffleMetrics) toSnapshot() *ShuffleMetricsSnapshot {
	if s == nil {
		return nil
	}
	return &ShuffleMetricsSnapshot{
		SpillsCount:      s.SpillsCount.Load(),
		DiskUsage:        s.DiskUsage.Load(),
		ElementsGathered: s.ElementsGathered.Load(),
		GroupsGathered:   s.GroupsGathered.Load(),
	}
}

type InputCollectionMetricsSnapshot struct {
	Id               string `json:"id"`
	ElementsConsumed int64  `json:"elements_consumed"`
	BatchesConsumed  int64  `json:"batches_consumed"`
}

func (c *InputCollectionMetrics) toSnapshot() *InputCollectionMetricsSnapshot {
	return &InputCollectionMetricsSnapshot{
		Id:               c.Id,
		ElementsConsumed: c.ElementsConsumed.Load(),
		BatchesConsumed:  c.BatchesConsumed.Load(),
	}
}

type OutputCollectionMetricsSnapshot struct {
	Id               string `json:"id"`
	ElementsProduced int64  `json:"elements_produced"`
	BatchesProduced  int64  `json:"batches_produced"`
}

func (c *OutputCollectionMetrics) toSnapshot() *OutputCollectionMetricsSnapshot {
	return &OutputCollectionMetricsSnapshot{
		Id:               c.Id,
		ElementsProduced: c.ElementsProduced.Load(),
		BatchesProduced:  c.BatchesProduced.Load(),
	}
}

type InputValueMetricsSnapshot struct {
	Id         string `json:"id"`
	IsConsumed bool   `json:"is_consumed"`
}

func (v *InputValueMetrics) toSnapshot() *InputValueMetricsSnapshot {
	return &InputValueMetricsSnapshot{
		Id:         v.Id,
		IsConsumed: v.IsConsumed,
	}
}

type OutputValueMetricsSnapshot struct {
	Id         string `json:"id"`
	IsProduced bool   `json:"is_produced"`
}

func (v *OutputValueMetrics) toSnapshot() *OutputValueMetricsSnapshot {
	return &OutputValueMetricsSnapshot{
		Id:         v.Id,
		IsProduced: v.IsProduced,
	}
}

type PhaseSwitchSnapshot struct {
	PhaseName       string    `json:"phase_name"`
	SwitchTimestamp time.Time `json:"switch_timestamp"`
}

type OperationMetricsSnapshot struct {
	Id                string                             `json:"id"`
	Name              string                             `json:"name"`
	Kind              string                             `json:"kind"`
	Parallelism       int                                `json:"parallelism"`
	Phase             string                             `json:"phase"`
	Phases            []PhaseSwitchSnapshot              `json:"phases"`
	Shuffle           *ShuffleMetricsSnapshot            `json:"shuffle"`
	InputCollections  []*InputCollectionMetricsSnapshot  `json:"input_collections"`
	OutputCollections []*OutputCollectionMetricsSnapshot `json:"output_collections"`
	InputValues       []*InputValueMetricsSnapshot       `json:"input_values"`
	OutputValues      []*OutputValueMetricsSnapshot      `json:"output_values"`
	UserCounters      map[string]int64                   `json:"user_counters"`
}

func (o *OperationMetrics) toSnapshot() *OperationMetricsSnapshot {
	currentPhaseId := o.CurrentPhase.Load()
	phaseName := ""
	if int(currentPhaseId) < len(core.PhaseNames) {
		phaseName = core.PhaseNames[currentPhaseId]
	}

	// Copy phases with mutex protection
	o.phasesMutex.Lock()
	phasesSnapshot := make([]PhaseSwitchSnapshot, len(o.Phases))
	for i, p := range o.Phases {
		pName := ""
		if int(p.PhaseId) < len(core.PhaseNames) {
			pName = core.PhaseNames[p.PhaseId]
		}
		phasesSnapshot[i] = PhaseSwitchSnapshot{
			PhaseName:       pName,
			SwitchTimestamp: p.SwitchTimestamp,
		}
	}
	o.phasesMutex.Unlock()

	result := &OperationMetricsSnapshot{
		Id:                o.Id,
		Name:              o.Name,
		Kind:              o.Kind,
		Parallelism:       o.Parallelism,
		Phase:             phaseName,
		Phases:            phasesSnapshot,
		Shuffle:           o.Shuffle.toSnapshot(),
		InputCollections:  make([]*InputCollectionMetricsSnapshot, len(o.InputCollections)),
		OutputCollections: make([]*OutputCollectionMetricsSnapshot, len(o.OutputCollections)),
		InputValues:       make([]*InputValueMetricsSnapshot, len(o.InputValues)),
		OutputValues:      make([]*OutputValueMetricsSnapshot, len(o.OutputValues)),
		UserCounters:      o.UserCounters.Snapshot(),
	}
	for i, coll := range o.InputCollections {
		result.InputCollections[i] = coll.toSnapshot()
	}
	for i, coll := range o.OutputCollections {
		result.OutputCollections[i] = coll.toSnapshot()
	}
	for i, value := range o.InputValues {
		result.InputValues[i] = value.toSnapshot()
	}
	for i, value := range o.OutputValues {
		result.OutputValues[i] = value.toSnapshot()
	}
	return result
}

type CollectionMetricsSnapshot struct {
	Id           string           `json:"id"`
	Name         string           `json:"name"`
	BatchSize    int              `json:"batch_size"`
	Pressure     int              `json:"pressure"`
	Capacity     int              `json:"capacity"`
	Completed    bool             `json:"completed"`
	UserCounters map[string]int64 `json:"user_counters"`
}

func (c *CollectionMetrics) toSnapshot() *CollectionMetricsSnapshot {
	pressure, capacity := c.GetChannelInfo()
	return &CollectionMetricsSnapshot{
		Id:           c.Id,
		Name:         c.Name,
		BatchSize:    c.BatchSize,
		Pressure:     pressure,
		Capacity:     capacity,
		Completed:    c.Completed,
		UserCounters: c.UserCounters.Snapshot(),
	}
}

type ValueMetricsSnapshot struct {
	Id           string           `json:"id"`
	Name         string           `json:"name"`
	IsResolved   bool             `json:"is_resolved"`
	UserCounters map[string]int64 `json:"user_counters"`
}

func (v *ValueMetrics) toSnapshot() *ValueMetricsSnapshot {
	return &ValueMetricsSnapshot{
		Id:           v.Id,
		Name:         v.Name,
		IsResolved:   v.GetIsResolved(),
		UserCounters: v.UserCounters.Snapshot(),
	}
}
