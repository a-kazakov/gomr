package constants_test

import (
	"testing"

	"github.com/a-kazakov/gomr/internal/constants"
)

func TestConstants(t *testing.T) {
	t.Run("max shuffle key size is power of two", func(t *testing.T) {
		if constants.MAX_SHUFFLE_KEY_SIZE != 1<<constants.MAX_SHUFFLE_KEY_SIZE_POW2 {
			t.Errorf("MAX_SHUFFLE_KEY_SIZE = %d, want 1 << %d = %d",
				constants.MAX_SHUFFLE_KEY_SIZE, constants.MAX_SHUFFLE_KEY_SIZE_POW2, 1<<constants.MAX_SHUFFLE_KEY_SIZE_POW2)
		}
	})

	t.Run("max shuffle key size equals 1024", func(t *testing.T) {
		if constants.MAX_SHUFFLE_KEY_SIZE != 1024 {
			t.Errorf("MAX_SHUFFLE_KEY_SIZE = %d, want 1024", constants.MAX_SHUFFLE_KEY_SIZE)
		}
	})

	t.Run("max shuffle key size mask is consistent", func(t *testing.T) {
		if constants.MAX_SHUFFLE_KEY_SIZE_MASK != constants.MAX_SHUFFLE_KEY_SIZE-1 {
			t.Errorf("MAX_SHUFFLE_KEY_SIZE_MASK = %d, want %d", constants.MAX_SHUFFLE_KEY_SIZE_MASK, constants.MAX_SHUFFLE_KEY_SIZE-1)
		}
		// Mask should have all lower bits set
		if constants.MAX_SHUFFLE_KEY_SIZE_MASK&constants.MAX_SHUFFLE_KEY_SIZE != 0 {
			t.Error("mask & size should be 0 for power-of-two sizes")
		}
	})

	t.Run("max shuffle value size is power of two", func(t *testing.T) {
		if constants.MAX_SHUFFLE_VALUE_SIZE != 1<<constants.MAX_SHUFFLE_VALUE_SIZE_POW2 {
			t.Errorf("MAX_SHUFFLE_VALUE_SIZE = %d, want 1 << %d = %d",
				constants.MAX_SHUFFLE_VALUE_SIZE, constants.MAX_SHUFFLE_VALUE_SIZE_POW2, 1<<constants.MAX_SHUFFLE_VALUE_SIZE_POW2)
		}
	})

	t.Run("max shuffle value size equals 16 MiB", func(t *testing.T) {
		if constants.MAX_SHUFFLE_VALUE_SIZE != 16*1024*1024 {
			t.Errorf("MAX_SHUFFLE_VALUE_SIZE = %d, want %d", constants.MAX_SHUFFLE_VALUE_SIZE, 16*1024*1024)
		}
	})

	t.Run("max shuffle value size mask is consistent", func(t *testing.T) {
		if constants.MAX_SHUFFLE_VALUE_SIZE_MASK != constants.MAX_SHUFFLE_VALUE_SIZE-1 {
			t.Errorf("MAX_SHUFFLE_VALUE_SIZE_MASK = %d, want %d", constants.MAX_SHUFFLE_VALUE_SIZE_MASK, constants.MAX_SHUFFLE_VALUE_SIZE-1)
		}
		if constants.MAX_SHUFFLE_VALUE_SIZE_MASK&constants.MAX_SHUFFLE_VALUE_SIZE != 0 {
			t.Error("mask & size should be 0 for power-of-two sizes")
		}
	})

	t.Run("inter stage chan capacity equals 1000", func(t *testing.T) {
		if constants.INTER_STAGE_CHAN_CAPACITY != 1000 {
			t.Errorf("INTER_STAGE_CHAN_CAPACITY = %d, want 1000", constants.INTER_STAGE_CHAN_CAPACITY)
		}
	})

	t.Run("stage recycle chan capacity is double inter stage", func(t *testing.T) {
		if constants.STAGE_RECYCLE_CHAN_CAPACITY != 2*constants.INTER_STAGE_CHAN_CAPACITY {
			t.Errorf("STAGE_RECYCLE_CHAN_CAPACITY = %d, want 2 * %d = %d",
				constants.STAGE_RECYCLE_CHAN_CAPACITY, constants.INTER_STAGE_CHAN_CAPACITY, 2*constants.INTER_STAGE_CHAN_CAPACITY)
		}
	})
}
