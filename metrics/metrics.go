// Package metrics provides runtime metrics collection and push-based reporting
// for pipeline operations, collections, and values.
package metrics

import (
	"sync"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/countermap"
	"github.com/oklog/ulid/v2"
)

// PipelineMetrics is the top-level metrics container for a pipeline execution.
// Thread-safe for concurrent AddOperation/AddCollection/AddValue calls.
type PipelineMetrics struct {
	mu           sync.RWMutex
	Operations   map[string]*OperationMetrics
	Collections  map[string]*CollectionMetrics
	Values       map[string]*ValueMetrics
	UserCounters *countermap.CountersMap
}

func NewPipelineMetrics() *PipelineMetrics {
	return &PipelineMetrics{
		Operations:   make(map[string]*OperationMetrics),
		Collections:  make(map[string]*CollectionMetrics),
		Values:       make(map[string]*ValueMetrics),
		UserCounters: countermap.NewCountersMap(),
	}
}

// AddOperation registers a new operation with the given kind and name.
// Shuffle operations automatically get a ShuffleMetrics struct.
func (p *PipelineMetrics) AddOperation(kind string, name string) *OperationMetrics {
	var shuffle *ShuffleMetrics
	if kind == core.OPERATION_KIND_SHUFFLE {
		shuffle = &ShuffleMetrics{}
	}
	value := &OperationMetrics{
		Id:                ulid.Make().String(),
		Name:              name,
		Kind:              kind,
		InputCollections:  make([]*InputCollectionMetrics, 0),
		OutputCollections: make([]*OutputCollectionMetrics, 0),
		InputValues:       make([]*InputValueMetrics, 0),
		OutputValues:      make([]*OutputValueMetrics, 0),
		Shuffle:           shuffle,
		UserCounters:      countermap.NewCountersMap(),
	}
	p.mu.Lock()
	p.Operations[value.Id] = value
	p.mu.Unlock()
	return value
}

func (p *PipelineMetrics) AddCollection(
	name string,
	batchSize int,
	getChannelInfo func() (pressure int, capacity int),
) *CollectionMetrics {
	value := &CollectionMetrics{
		Id:             ulid.Make().String(),
		Name:           name,
		BatchSize:      batchSize,
		GetChannelInfo: getChannelInfo,
		UserCounters:   countermap.NewCountersMap(),
	}
	p.mu.Lock()
	p.Collections[value.Id] = value
	p.mu.Unlock()
	return value
}

func (p *PipelineMetrics) AddValue(
	name string,
	getIsResolved func() bool,
) *ValueMetrics {
	value := &ValueMetrics{
		Id:            ulid.Make().String(),
		Name:          name,
		GetIsResolved: getIsResolved,
		UserCounters:  countermap.NewCountersMap(),
	}
	p.mu.Lock()
	p.Values[value.Id] = value
	p.mu.Unlock()
	return value
}
