package parameters

import (
	"fmt"
	"time"
)

// MetricsParameters holds configuration for the metrics push system.
type MetricsParameters struct {
	PushURL      *Parameter[string]
	PushInterval *Parameter[time.Duration]
}

func NewMetricsParameters() *MetricsParameters {
	return &MetricsParameters{
		PushURL: NewStringParam(
			"metrics", "push_url",
			"URL to push metrics to (empty to disable)",
			"",
			validateMetricsURL,
		),
		PushInterval: NewDurationParam(
			"metrics", "push_interval",
			"Interval between metrics pushes",
			5*time.Second,
			PositiveDuration,
		),
	}
}

func validateMetricsURL(value string) error {
	if value == "" {
		return nil // empty URL is allowed (disables metrics push)
	}
	if len(value) < 7 || (value[:7] != "http://" && (len(value) < 8 || value[:8] != "https://")) {
		return fmt.Errorf("URL must start with http:// or https://")
	}
	return nil
}

func (mp *MetricsParameters) RegisterFlags(f Flagger) {
	RegisterAllFlags(mp, f)
}

func (mp *MetricsParameters) LoadFromSource(lookup Source) error {
	return LoadAllFromSource(mp, lookup)
}

// Deprecated: Use PushInterval.Get() directly.
func (mp *MetricsParameters) GetPushInterval() time.Duration {
	return mp.PushInterval.Get()
}
