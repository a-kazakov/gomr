package parameters

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// DiskParameters holds configuration for scratch space paths and disk write latency targets.
type DiskParameters struct {
	ScratchSpacePaths  *Parameter[string]
	TargetWriteLatency *Parameter[time.Duration]
}

func NewDiskParameters() *DiskParameters {
	return &DiskParameters{
		ScratchSpacePaths: NewStringParamDynamic(
			"disk", "scratch_paths",
			"Comma-separated list of scratch space directories",
			func() string { return os.TempDir() },
			validateDiskPaths,
		),
		TargetWriteLatency: NewDurationParam(
			"disk", "target_write_latency",
			"Target latency for disk writes",
			50*time.Millisecond,
			PositiveDuration,
		),
	}
}

func validateDiskPaths(value string) error {
	for _, path := range strings.Split(value, ",") {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" {
			continue
		}
		if _, err := os.Stat(trimmed); err != nil {
			return fmt.Errorf("invalid path %q: %w", trimmed, err)
		}
	}
	return nil
}

func (dp *DiskParameters) RegisterFlags(f Flagger) {
	RegisterAllFlags(dp, f)
}

func (dp *DiskParameters) LoadFromSource(lookup Source) error {
	return LoadAllFromSource(dp, lookup)
}

// GetScratchSpacePaths splits the comma-separated scratch paths, trimming whitespace.
// Returns os.TempDir() as fallback if all paths are empty.
func (dp *DiskParameters) GetScratchSpacePaths() []string {
	value := dp.ScratchSpacePaths.Get()
	paths := strings.Split(value, ",")
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		trimmed := strings.TrimSpace(path)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		result = append(result, os.TempDir())
	}
	return result
}
