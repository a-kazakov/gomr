package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type MetricsPusher struct {
	url        string
	interval   time.Duration
	jobID      string
	authHeader string
	metrics    *PipelineMetrics
	stopChan   chan struct{}
	wg         sync.WaitGroup
	httpClient *http.Client
}

func NewMetricsPusher(url string, interval time.Duration, jobID string, authHeader string, metrics *PipelineMetrics) *MetricsPusher {
	return &MetricsPusher{
		url:        url,
		interval:   interval,
		jobID:      jobID,
		authHeader: authHeader,
		metrics:    metrics,
		stopChan:   make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (mp *MetricsPusher) Start() {
	if mp.url == "" || mp.interval <= 0 {
		return
	}

	mp.wg.Add(1)
	go mp.pushLoop()
}

// Stop stops the pusher and sends a final metrics snapshot before returning.
func (mp *MetricsPusher) Stop() {
	close(mp.stopChan)
	mp.wg.Wait()
	// Push final state
	mp.pushOnce()
}

func (mp *MetricsPusher) pushLoop() {
	defer mp.wg.Done()

	ticker := time.NewTicker(mp.interval)
	defer ticker.Stop()

	// Push immediately
	mp.pushOnce()

	for {
		select {
		case <-ticker.C:
			mp.pushOnce()
		case <-mp.stopChan:
			return
		}
	}
}

func (mp *MetricsPusher) pushOnce() {
	metrics := mp.metrics
	if metrics == nil {
		return
	}

	// Create snapshot
	snapshot := metrics.toSnapshot()

	// Serialize metrics to JSON
	data, err := json.Marshal(snapshot)
	if err != nil {
		slog.Error("failed to marshal metrics", "error", err)
		return
	}

	// Create request
	url := fmt.Sprintf("%s/sink/%s", mp.url, mp.jobID)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		slog.Error("failed to create metrics push request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if mp.authHeader != "" {
		req.Header.Set("Authorization", mp.authHeader)
	}

	// Send request
	resp, err := mp.httpClient.Do(req)
	if err != nil {
		slog.Debug("failed to push metrics", "error", err, "url", url)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		slog.Warn("metrics push returned non-OK status", "status", resp.StatusCode, "url", url)
	}
}
