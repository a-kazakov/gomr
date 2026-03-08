package core

import (
	"sync"
)

// OperatorContext is passed to every operator worker and provides identity,
// parallelism info, counters, and user-supplied context.
type OperatorContext struct {
	OperatorId                 string
	WorkerIndex                int
	TotalWorkers               int
	OperationUserCounters      UserCountersMap
	PipelineUserCounters       UserCountersMap
	OutCollectionsUserCounters []UserCountersMap
	OutValuesUserCounters      []UserCountersMap
	Mutex                      *sync.Mutex
	UserOperatorContext        any
	UserContext                any
}

const (
	OPERATION_KIND_SEED          = "seed"
	OPERATION_KIND_MAP           = "map"
	OPERATION_KIND_MAP_VALUE     = "map_value"
	OPERATION_KIND_SHUFFLE       = "shuffle"
	OPERATION_KIND_FORK          = "fork"
	OPERATION_KIND_MERGE         = "merge"
	OPERATION_KIND_COLLECT       = "collect"
	OPERATION_KIND_TO_COLLECTION = "to_collection"
	OPERATION_KIND_SPILL_BUFFER  = "spill_buffer"
)

// Phase ID constants represent operator lifecycle phases.
// Used with atomic compare-and-swap for phase transitions.
const (
	PHASE_PENDING     int32 = 0
	PHASE_RUNNING     int32 = 1
	PHASE_SCATTERING  int32 = 2
	PHASE_FLUSHING    int32 = 3
	PHASE_GATHERING   int32 = 4
	PHASE_PRE_COLLECT int32 = 5
	PHASE_AGGREGATE   int32 = 6
	PHASE_COMPLETED   int32 = 7
)

var PhaseNames = []string{
	"pending",
	"running",
	"scattering",
	"flushing",
	"gathering",
	"pre-collect",
	"aggregate",
	"completed",
}

// Deprecated: use PHASE_* constants instead.
const (
	OPERATION_PHASE_PENDING    = ""
	OPERATION_PHASE_RUNNING    = "running"
	OPERATION_PHASE_SCATTERING = "scattering"
	OPERATION_PHASE_GATHERING  = "gathering"
	OPERATION_PHASE_COMPLETED  = "completed"
)
