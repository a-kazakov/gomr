// Package pipeline implements the core Pipeline, Collection, Value, and Seed types
// that form the runtime backbone of a gomr data processing pipeline.
package pipeline

import (
	"sync"
	"time"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/pipeline/goroutinedispatcher"
	"github.com/a-kazakov/gomr/metrics"
	"github.com/a-kazakov/gomr/parameters"
)

type pipelineCollection interface {
	isConsumed() bool
	GetMetrics() *metrics.CollectionMetrics
}

type Pipeline struct {
	Parameters          *parameters.Parameters
	GoroutineDispatcher *goroutinedispatcher.GoroutineDispatcher
	Metrics             *metrics.PipelineMetrics
	collections         []pipelineCollection
	metricsPusher       *metrics.MetricsPusher
	jobID               string
	UserContext         any
}

func NewPipeline() *Pipeline {
	return NewPipelineWithParameters(parameters.NewParameters())
}

func NewPipelineWithParameters(params *parameters.Parameters) *Pipeline {
	p := &Pipeline{
		Parameters:          params,
		GoroutineDispatcher: goroutinedispatcher.NewGoroutineDispatcher(),
		Metrics:             metrics.NewPipelineMetrics(),
		collections:         make([]pipelineCollection, 0, 32),
		jobID:               params.Pipeline.JobID.Get(),
	}

	// Start metrics push if configured
	pushURL := p.Parameters.Metrics.PushURL.Get()
	pushInterval := p.Parameters.Metrics.GetPushInterval()
	pushAuthHeader := p.Parameters.Metrics.PushAuthHeader.Get()
	if pushURL != "" && pushInterval > 0 {
		p.startMetricsPush(pushURL, pushInterval, p.jobID, pushAuthHeader)
	}

	return p
}

// WaitForCompletion waits for all goroutines to finish and pushes final metrics.
// Panics if any registered collection has not been consumed.
func (p *Pipeline) WaitForCompletion() {
	for _, collection := range p.collections {
		must.BeTrue(collection.isConsumed(), "collection %q not consumed", collection.GetMetrics().Name)
	}
	p.GoroutineDispatcher.WaitForAllGoroutinesToFinish()
	// Push final metrics state
	p.stopMetricsPush()
}

func (p *Pipeline) GetMetrics() *metrics.PipelineMetrics {
	return p.Metrics
}

func (p *Pipeline) GetJobID() string {
	return p.jobID
}

func (p *Pipeline) RegisterCollection(collection pipelineCollection) {
	p.collections = append(p.collections, collection)
}

func (p *Pipeline) BuildOperatorContexts(
	operatorMetrics *metrics.OperationMetrics,
	outCollectionMetrics []*metrics.CollectionMetrics,
	outValueMetrics []*metrics.ValueMetrics,
	parallelism int,
	userOperatorContext any,
) []*core.OperatorContext {
	result := make([]*core.OperatorContext, parallelism)
	outCollectionsUserCounters := make([]core.UserCountersMap, len(outCollectionMetrics))
	for i, collectionMetrics := range outCollectionMetrics {
		outCollectionsUserCounters[i] = collectionMetrics.UserCounters
	}
	outValuesUserCounters := make([]core.UserCountersMap, len(outValueMetrics))
	for i, valueMetrics := range outValueMetrics {
		outValuesUserCounters[i] = valueMetrics.UserCounters
	}
	mutex := &sync.Mutex{}
	for i := range parallelism {
		result[i] = &core.OperatorContext{
			OperatorId:                 operatorMetrics.Id,
			WorkerIndex:                i,
			TotalWorkers:               parallelism,
			OperationUserCounters:      operatorMetrics.UserCounters,
			PipelineUserCounters:       p.Metrics.UserCounters,
			OutCollectionsUserCounters: outCollectionsUserCounters,
			OutValuesUserCounters:      outValuesUserCounters,
			Mutex:                      mutex,
			UserOperatorContext:        userOperatorContext,
			UserContext:                p.UserContext,
		}
	}
	return result
}

func (p *Pipeline) startMetricsPush(pushURL string, pushInterval time.Duration, jobID string, authHeader string) {
	if pushURL == "" || pushInterval <= 0 {
		return
	}

	pusher := metrics.NewMetricsPusher(pushURL, pushInterval, jobID, authHeader, p.Metrics)
	pusher.Start()
	p.metricsPusher = pusher
}

func (p *Pipeline) stopMetricsPush() {
	if p.metricsPusher != nil {
		p.metricsPusher.Stop()
		p.metricsPusher = nil
	}
}
