package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-kazakov/gomr/internal/countermap"
)

type InputValueMetrics struct {
	Id         string
	IsConsumed bool
}

type OutputValueMetrics struct {
	Id         string
	IsProduced bool
}

// InputCollectionMetrics tracks consumption counters for an operation's collection input.
// Counters are atomically updated by the consuming goroutine.
type InputCollectionMetrics struct {
	Id               string
	ElementsConsumed atomic.Int64
	BatchesConsumed  atomic.Int64
}

// OutputCollectionMetrics tracks production counters for an operation's collection output.
// Counters are atomically updated by the producing goroutine.
type OutputCollectionMetrics struct {
	Id               string
	ElementsProduced atomic.Int64
	BatchesProduced  atomic.Int64
}

// ShuffleMetrics tracks shuffle-specific counters (spills, disk usage, gather progress).
// All fields are atomically updated and safe for concurrent access.
type ShuffleMetrics struct {
	SpillsCount      atomic.Int64
	DiskUsage        atomic.Int64
	ElementsGathered atomic.Int64
	GroupsGathered   atomic.Int64
}

type PhaseSwitch struct {
	PhaseId         int32
	SwitchTimestamp time.Time
}

// OperationMetrics tracks runtime state for a single pipeline operation.
// Thread-safe: phase transitions use atomic CAS, input/output lists are append-only.
type OperationMetrics struct {
	// Auto-generated
	Id string
	// Passed from parameters
	Name string
	Kind string
	// Update as job progresses
	Parallelism  int
	CurrentPhase atomic.Int32
	Phases       []PhaseSwitch
	phasesMutex  sync.Mutex
	// In/out metrics
	InputCollections  []*InputCollectionMetrics
	InputValues       []*InputValueMetrics
	OutputCollections []*OutputCollectionMetrics
	OutputValues      []*OutputValueMetrics
	// Shuffle-specific counters
	Shuffle *ShuffleMetrics
	// User-defined counters
	UserCounters *countermap.CountersMap
}

// CollectionMetrics tracks metadata and runtime state for a pipeline collection.
// GetChannelInfo is called during snapshot to read current channel pressure/capacity.
type CollectionMetrics struct {
	Id             string
	Name           string
	BatchSize      int
	GetChannelInfo func() (pressure int, capacity int)
	Completed      bool
	UserCounters   *countermap.CountersMap
}

// ValueMetrics tracks metadata and resolution state for a pipeline value.
// GetIsResolved is called during snapshot to check if the value has been resolved.
type ValueMetrics struct {
	Id            string
	Name          string
	GetIsResolved func() bool
	UserCounters  *countermap.CountersMap
}

func (o *OperationMetrics) AddInputCollection(c *CollectionMetrics) *InputCollectionMetrics {
	result := &InputCollectionMetrics{
		Id: c.Id,
	}
	o.InputCollections = append(o.InputCollections, result)
	return result
}

func (o *OperationMetrics) AddOutputCollection(c *CollectionMetrics) *OutputCollectionMetrics {
	result := &OutputCollectionMetrics{
		Id: c.Id,
	}
	o.OutputCollections = append(o.OutputCollections, result)
	return result
}

func (o *OperationMetrics) AddInputValue(v *ValueMetrics) *InputValueMetrics {
	result := &InputValueMetrics{
		Id: v.Id,
	}
	o.InputValues = append(o.InputValues, result)
	return result
}

func (o *OperationMetrics) AddOutputValue(v *ValueMetrics) *OutputValueMetrics {
	result := &OutputValueMetrics{
		Id: v.Id,
	}
	o.OutputValues = append(o.OutputValues, result)
	return result
}

func (o *OperationMetrics) SetPhase(phaseId int32) {
	oldPhase := o.CurrentPhase.Swap(phaseId)
	if oldPhase != phaseId {
		o.appendPhase(phaseId)
	}
}

// TrySetPhase atomically transitions from expectedPhaseId to newPhaseId.
// Returns true if the transition was successful, false if current phase != expected.
func (o *OperationMetrics) TrySetPhase(expectedPhaseId, newPhaseId int32) bool {
	if o.CurrentPhase.CompareAndSwap(expectedPhaseId, newPhaseId) {
		o.appendPhase(newPhaseId)
		return true
	}
	return false
}

func (o *OperationMetrics) appendPhase(phaseId int32) {
	o.phasesMutex.Lock()
	o.Phases = append(o.Phases, PhaseSwitch{
		PhaseId:         phaseId,
		SwitchTimestamp: time.Now(),
	})
	o.phasesMutex.Unlock()
}
