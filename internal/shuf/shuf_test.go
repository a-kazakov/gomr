package shuf_test

import (
	"encoding/binary"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/a-kazakov/gomr"
	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
	"github.com/a-kazakov/gomr/internal/shuf"
	"github.com/a-kazakov/gomr/metrics"
)

// TestItem represents a key-value pair for testing
type TestItem struct {
	Key   string
	Value int64
}

// TestSerializer implements core.ShufflerSerializer[TestItem]
type TestSerializer struct{}

func (s *TestSerializer) Setup(ctx *core.OperatorContext) {
}

func (s *TestSerializer) MarshalKeyToBytes(value *TestItem, dest []byte, cursor int64) (int, int64) {
	copy(dest, []byte(value.Key))
	return len(value.Key), gomr.StopEmitting
}

func (s *TestSerializer) MarshalValueToBytes(value *TestItem, dest []byte) int {
	binary.NativeEndian.PutUint64(dest, uint64(value.Value))
	return 8
}

func (s *TestSerializer) UnmarshalValueFromBytes(key []byte, data []byte, dest *TestItem) {
	dest.Value = int64(binary.NativeEndian.Uint64(data))
}

// collectAllFromGatherer reads all key-value pairs from a Gatherer
func collectAllFromGatherer(t *testing.T, gatherer *shuf.Gatherer, numShards int32) map[string][]int64 {
	t.Helper()
	result := make(map[string][]int64)
	serializer := &TestSerializer{}
	buf := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)

	for shardId := int32(0); shardId < numShards; shardId++ {
		reader, closer := gatherer.GetShardReader(shardId, 0)
		_ = closer
		for {
			key := reader.PeekKey()
			if key == nil {
				break
			}
			keyStr := string(key)
			keyReader := reader.GetKeyReader()
			for {
				n, err := keyReader.ReadValue(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("error reading value: %v", err)
				}
				var item TestItem
				serializer.UnmarshalValueFromBytes(key, buf[:n], &item)
				result[keyStr] = append(result[keyStr], item.Value)
			}
		}
	}
	return result
}

func TestScattererGatherer(t *testing.T) {
	t.Run("Single input stream with multiple keys", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(4)
		numInputs := int32(1)
		maxBufferSize := int64(1024 * 1024) // 1MB

		// Create test data
		items := []TestItem{
			{Key: "apple", Value: 1},
			{Key: "banana", Value: 2},
			{Key: "apple", Value: 3},
			{Key: "cherry", Value: 4},
			{Key: "banana", Value: 5},
			{Key: "apple", Value: 6},
		}

		// Scatter
		scatterer := shuf.NewScatterer(shuf.ScattererArguments{
			NumLogicalShards:    numShards,
			NumInputs:           numInputs,
			WorkingDirectory:    tmpDir,
			MaxBufferSize:       maxBufferSize,
			MaxBufferSizeJitter: 0,
			FileMergeThreshold:  10,
			ReadBufferSize:      1024,
			WriteBufferSize:     1024,
			TargetWriteLatency:  time.Second,
			Metrics:             &metrics.ShuffleMetrics{},
		})
		serializer := &TestSerializer{}
		shuf.AddBatchToScatterer(scatterer, items, serializer, int(numInputs), 0)
		result := scatterer.Finalize()

		if result == nil {
			t.Fatal("expected non-nil result from Finalize")
		}

		// Gather
		gatherer := shuf.NewGatherer([]*shardedfile.ShardedFile{result}, &metrics.ShuffleMetrics{})
		collected := collectAllFromGatherer(t, gatherer, numShards*numInputs)

		// Verify all data was collected
		expectedKeys := map[string][]int64{
			"apple":  {1, 3, 6},
			"banana": {2, 5},
			"cherry": {4},
		}

		if len(collected) != len(expectedKeys) {
			t.Fatalf("expected %d keys, got %d", len(expectedKeys), len(collected))
		}

		for key, expectedValues := range expectedKeys {
			actualValues, ok := collected[key]
			if !ok {
				t.Errorf("missing key %q", key)
				continue
			}
			sort.Slice(actualValues, func(i, j int) bool { return actualValues[i] < actualValues[j] })
			sort.Slice(expectedValues, func(i, j int) bool { return expectedValues[i] < expectedValues[j] })
			if len(actualValues) != len(expectedValues) {
				t.Errorf("key %q: expected %d values, got %d", key, len(expectedValues), len(actualValues))
				continue
			}
			for i := range expectedValues {
				if actualValues[i] != expectedValues[i] {
					t.Errorf("key %q: value mismatch at index %d: expected %d, got %d",
						key, i, expectedValues[i], actualValues[i])
				}
			}
		}
	})

	t.Run("Multiple input streams merge correctly", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(4)
		numInputs := int32(3)
		maxBufferSize := int64(1024 * 1024) // 1MB

		// Create test data for each input stream
		items0 := []TestItem{
			{Key: "shared", Value: 100},
			{Key: "only0", Value: 1},
		}
		items1 := []TestItem{
			{Key: "shared", Value: 200},
			{Key: "only1", Value: 2},
		}
		items2 := []TestItem{
			{Key: "shared", Value: 300},
			{Key: "only2", Value: 3},
		}

		// Scatter from all inputs
		scatterer := shuf.NewScatterer(shuf.ScattererArguments{
			NumLogicalShards:    numShards,
			NumInputs:           numInputs,
			WorkingDirectory:    tmpDir,
			MaxBufferSize:       maxBufferSize,
			MaxBufferSizeJitter: 0,
			FileMergeThreshold:  10,
			ReadBufferSize:      1024,
			WriteBufferSize:     1024,
			TargetWriteLatency:  time.Second,
			Metrics:             &metrics.ShuffleMetrics{},
		})
		serializer := &TestSerializer{}
		shuf.AddBatchToScatterer(scatterer, items0, serializer, int(numInputs), 0)
		shuf.AddBatchToScatterer(scatterer, items1, serializer, int(numInputs), 1)
		shuf.AddBatchToScatterer(scatterer, items2, serializer, int(numInputs), 2)
		result := scatterer.Finalize()

		// Gather
		gatherer := shuf.NewGatherer([]*shardedfile.ShardedFile{result}, &metrics.ShuffleMetrics{})
		collected := collectAllFromGatherer(t, gatherer, numShards*numInputs)

		// Verify: "shared" key should have values from all inputs
		sharedValues := collected["shared"]
		sort.Slice(sharedValues, func(i, j int) bool { return sharedValues[i] < sharedValues[j] })
		expectedShared := []int64{100, 200, 300}
		if len(sharedValues) != len(expectedShared) {
			t.Fatalf("expected %d values for 'shared', got %d: %v", len(expectedShared), len(sharedValues), sharedValues)
		}
		for i, v := range expectedShared {
			if sharedValues[i] != v {
				t.Errorf("shared[%d]: expected %d, got %d", i, v, sharedValues[i])
			}
		}

		// Verify unique keys
		if len(collected["only0"]) != 1 || collected["only0"][0] != 1 {
			t.Errorf("only0: expected [1], got %v", collected["only0"])
		}
		if len(collected["only1"]) != 1 || collected["only1"][0] != 2 {
			t.Errorf("only1: expected [2], got %v", collected["only1"])
		}
		if len(collected["only2"]) != 1 || collected["only2"][0] != 3 {
			t.Errorf("only2: expected [3], got %v", collected["only2"])
		}
	})

	t.Run("Large dataset triggers buffer flush", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(2)
		numInputs := int32(1)
		maxBufferSize := int64(4096) // Small buffer to trigger flushes

		// Create many items to trigger multiple flushes
		numItems := 1000
		items := make([]TestItem, numItems)
		for i := 0; i < numItems; i++ {
			items[i] = TestItem{
				Key:   string(rune('A' + (i % 26))), // Keys A-Z
				Value: int64(i),
			}
		}

		// Scatter
		scatterer := shuf.NewScatterer(shuf.ScattererArguments{
			NumLogicalShards:    numShards,
			NumInputs:           numInputs,
			WorkingDirectory:    tmpDir,
			MaxBufferSize:       maxBufferSize,
			MaxBufferSizeJitter: 0,
			FileMergeThreshold:  10,
			ReadBufferSize:      1024,
			WriteBufferSize:     1024,
			TargetWriteLatency:  time.Second,
			Metrics:             &metrics.ShuffleMetrics{},
		})
		serializer := &TestSerializer{}
		shuf.AddBatchToScatterer(scatterer, items, serializer, int(numInputs), 0)
		result := scatterer.Finalize()

		// Gather
		gatherer := shuf.NewGatherer([]*shardedfile.ShardedFile{result}, &metrics.ShuffleMetrics{})
		collected := collectAllFromGatherer(t, gatherer, numShards*numInputs)

		// Verify total count
		totalValues := 0
		for _, values := range collected {
			totalValues += len(values)
		}
		if totalValues != numItems {
			t.Fatalf("expected %d total values, got %d", numItems, totalValues)
		}

		// Verify each key has the right count (each letter appears ~38-39 times in 1000 items)
		for i := 0; i < 26; i++ {
			key := string(rune('A' + i))
			expectedCount := numItems / 26
			if i < numItems%26 {
				expectedCount++
			}
			actualCount := len(collected[key])
			if actualCount != expectedCount {
				t.Errorf("key %q: expected %d values, got %d", key, expectedCount, actualCount)
			}
		}
	})

	t.Run("Empty input produces empty result", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(4)
		numInputs := int32(1)
		maxBufferSize := int64(1024 * 1024)

		// Scatter empty batch
		scatterer := shuf.NewScatterer(shuf.ScattererArguments{
			NumLogicalShards:    numShards,
			NumInputs:           numInputs,
			WorkingDirectory:    tmpDir,
			MaxBufferSize:       maxBufferSize,
			MaxBufferSizeJitter: 0,
			FileMergeThreshold:  10,
			ReadBufferSize:      1024,
			WriteBufferSize:     1024,
			TargetWriteLatency:  time.Second,
			Metrics:             &metrics.ShuffleMetrics{},
		})
		serializer := &TestSerializer{}
		shuf.AddBatchToScatterer(scatterer, []TestItem{}, serializer, int(numInputs), 0)
		result := scatterer.Finalize()

		// Result may be nil for empty input
		if result == nil {
			return // This is acceptable
		}

		// If not nil, gatherer should return empty data
		gatherer := shuf.NewGatherer([]*shardedfile.ShardedFile{result}, &metrics.ShuffleMetrics{})
		collected := collectAllFromGatherer(t, gatherer, numShards*numInputs)

		if len(collected) != 0 {
			t.Errorf("expected empty result, got %d keys", len(collected))
		}
	})

	t.Run("Keys with same hash go to same shard", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(8)
		numInputs := int32(1)
		maxBufferSize := int64(1024 * 1024)

		// Create items with the same key from multiple batches
		batch1 := []TestItem{{Key: "testkey", Value: 1}}
		batch2 := []TestItem{{Key: "testkey", Value: 2}}
		batch3 := []TestItem{{Key: "testkey", Value: 3}}

		scatterer := shuf.NewScatterer(shuf.ScattererArguments{
			NumLogicalShards:    numShards,
			NumInputs:           numInputs,
			WorkingDirectory:    tmpDir,
			MaxBufferSize:       maxBufferSize,
			MaxBufferSizeJitter: 0,
			FileMergeThreshold:  10,
			ReadBufferSize:      1024,
			WriteBufferSize:     1024,
			TargetWriteLatency:  time.Second,
			Metrics:             &metrics.ShuffleMetrics{},
		})
		serializer := &TestSerializer{}
		shuf.AddBatchToScatterer(scatterer, batch1, serializer, int(numInputs), 0)
		shuf.AddBatchToScatterer(scatterer, batch2, serializer, int(numInputs), 0)
		shuf.AddBatchToScatterer(scatterer, batch3, serializer, int(numInputs), 0)
		result := scatterer.Finalize()

		gatherer := shuf.NewGatherer([]*shardedfile.ShardedFile{result}, &metrics.ShuffleMetrics{})
		collected := collectAllFromGatherer(t, gatherer, numShards*numInputs)

		values := collected["testkey"]
		if len(values) != 3 {
			t.Fatalf("expected 3 values for 'testkey', got %d: %v", len(values), values)
		}

		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		expected := []int64{1, 2, 3}
		for i, v := range expected {
			if values[i] != v {
				t.Errorf("testkey[%d]: expected %d, got %d", i, v, values[i])
			}
		}
	})

	t.Run("Binary keys work correctly", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(4)
		numInputs := int32(1)
		maxBufferSize := int64(1024 * 1024)

		// Create items with binary keys (including null bytes)
		items := []TestItem{
			{Key: string([]byte{0x00, 0x01, 0x02}), Value: 1},
			{Key: string([]byte{0xFF, 0xFE, 0xFD}), Value: 2},
			{Key: string([]byte{0x00, 0x01, 0x02}), Value: 3}, // Same binary key
		}

		scatterer := shuf.NewScatterer(shuf.ScattererArguments{
			NumLogicalShards:    numShards,
			NumInputs:           numInputs,
			WorkingDirectory:    tmpDir,
			MaxBufferSize:       maxBufferSize,
			MaxBufferSizeJitter: 0,
			FileMergeThreshold:  10,
			ReadBufferSize:      1024,
			WriteBufferSize:     1024,
			TargetWriteLatency:  time.Second,
			Metrics:             &metrics.ShuffleMetrics{},
		})
		serializer := &TestSerializer{}
		shuf.AddBatchToScatterer(scatterer, items, serializer, int(numInputs), 0)
		result := scatterer.Finalize()

		gatherer := shuf.NewGatherer([]*shardedfile.ShardedFile{result}, &metrics.ShuffleMetrics{})
		collected := collectAllFromGatherer(t, gatherer, numShards*numInputs)

		key1 := string([]byte{0x00, 0x01, 0x02})
		key2 := string([]byte{0xFF, 0xFE, 0xFD})

		if len(collected[key1]) != 2 {
			t.Errorf("binary key 1: expected 2 values, got %d", len(collected[key1]))
		}
		if len(collected[key2]) != 1 {
			t.Errorf("binary key 2: expected 1 value, got %d", len(collected[key2]))
		}
	})
}
