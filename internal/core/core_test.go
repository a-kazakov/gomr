package core_test

import (
	"math"
	"testing"

	"github.com/a-kazakov/gomr/internal/core"
)

func TestConstants(t *testing.T) {
	t.Run("stop emitting equals math.MinInt64", func(t *testing.T) {
		if core.StopEmitting != math.MinInt64 {
			t.Errorf("StopEmitting = %d, want math.MinInt64 (%d)", core.StopEmitting, math.MinInt64)
		}
	})

	t.Run("compression algorithm iota order", func(t *testing.T) {
		if core.CompressionAlgorithmNone != 0 {
			t.Errorf("CompressionAlgorithmNone = %d, want 0", core.CompressionAlgorithmNone)
		}
		if core.CompressionAlgorithmLz4 != 1 {
			t.Errorf("CompressionAlgorithmLz4 = %d, want 1", core.CompressionAlgorithmLz4)
		}
		if core.CompressionAlgorithmZstdFast != 2 {
			t.Errorf("CompressionAlgorithmZstdFast = %d, want 2", core.CompressionAlgorithmZstdFast)
		}
		if core.CompressionAlgorithmZstdDefault != 3 {
			t.Errorf("CompressionAlgorithmZstdDefault = %d, want 3", core.CompressionAlgorithmZstdDefault)
		}
	})

	t.Run("operation kinds are non-empty with correct values", func(t *testing.T) {
		kinds := map[string]string{
			"SEED":          core.OPERATION_KIND_SEED,
			"MAP":           core.OPERATION_KIND_MAP,
			"MAP_VALUE":     core.OPERATION_KIND_MAP_VALUE,
			"SHUFFLE":       core.OPERATION_KIND_SHUFFLE,
			"FORK":          core.OPERATION_KIND_FORK,
			"MERGE":         core.OPERATION_KIND_MERGE,
			"COLLECT":       core.OPERATION_KIND_COLLECT,
			"TO_COLLECTION": core.OPERATION_KIND_TO_COLLECTION,
			"SPILL_BUFFER":  core.OPERATION_KIND_SPILL_BUFFER,
		}
		for name, value := range kinds {
			if value == "" {
				t.Errorf("OPERATION_KIND_%s should not be empty", name)
			}
		}
		// Verify specific string values
		if core.OPERATION_KIND_SEED != "seed" {
			t.Errorf("OPERATION_KIND_SEED = %q, want %q", core.OPERATION_KIND_SEED, "seed")
		}
		if core.OPERATION_KIND_SHUFFLE != "shuffle" {
			t.Errorf("OPERATION_KIND_SHUFFLE = %q, want %q", core.OPERATION_KIND_SHUFFLE, "shuffle")
		}
	})
}

func TestPhases(t *testing.T) {
	t.Run("phase constants have sequential iota values", func(t *testing.T) {
		phases := []struct {
			name  string
			value int32
		}{
			{"PHASE_PENDING", core.PHASE_PENDING},
			{"PHASE_RUNNING", core.PHASE_RUNNING},
			{"PHASE_SCATTERING", core.PHASE_SCATTERING},
			{"PHASE_FLUSHING", core.PHASE_FLUSHING},
			{"PHASE_GATHERING", core.PHASE_GATHERING},
			{"PHASE_PRE_COLLECT", core.PHASE_PRE_COLLECT},
			{"PHASE_AGGREGATE", core.PHASE_AGGREGATE},
			{"PHASE_COMPLETED", core.PHASE_COMPLETED},
		}
		for i, p := range phases {
			if p.value != int32(i) {
				t.Errorf("%s = %d, want %d", p.name, p.value, i)
			}
		}
	})

	t.Run("phase names length matches phase count", func(t *testing.T) {
		expected := int(core.PHASE_COMPLETED + 1)
		if len(core.PhaseNames) != expected {
			t.Errorf("len(PhaseNames) = %d, want %d", len(core.PhaseNames), expected)
		}
	})

	t.Run("phase names have correct string values", func(t *testing.T) {
		expected := []string{
			"pending", "running", "scattering", "flushing",
			"gathering", "pre-collect", "aggregate", "completed",
		}
		for i, name := range expected {
			if core.PhaseNames[i] != name {
				t.Errorf("PhaseNames[%d] = %q, want %q", i, core.PhaseNames[i], name)
			}
		}
	})

	t.Run("deprecated phase aliases have correct values", func(t *testing.T) {
		if core.OPERATION_PHASE_PENDING != "" {
			t.Errorf("OPERATION_PHASE_PENDING = %q, want empty string", core.OPERATION_PHASE_PENDING)
		}
		if core.OPERATION_PHASE_RUNNING != "running" {
			t.Errorf("OPERATION_PHASE_RUNNING = %q, want %q", core.OPERATION_PHASE_RUNNING, "running")
		}
		if core.OPERATION_PHASE_SCATTERING != "scattering" {
			t.Errorf("OPERATION_PHASE_SCATTERING = %q, want %q", core.OPERATION_PHASE_SCATTERING, "scattering")
		}
		if core.OPERATION_PHASE_GATHERING != "gathering" {
			t.Errorf("OPERATION_PHASE_GATHERING = %q, want %q", core.OPERATION_PHASE_GATHERING, "gathering")
		}
		if core.OPERATION_PHASE_COMPLETED != "completed" {
			t.Errorf("OPERATION_PHASE_COMPLETED = %q, want %q", core.OPERATION_PHASE_COMPLETED, "completed")
		}
	})
}
