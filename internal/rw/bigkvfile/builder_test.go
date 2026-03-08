package bigkvfile_test

import (
	"bufio"
	"bytes"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/rw/bigkvfile"
	"github.com/a-kazakov/gomr/internal/rw/kv"
	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
)

// Helper to read all key-value pairs from a sharded file
func readAllFromShardedFile(t *testing.T, sf *shardedfile.ShardedFile, numShards int32) map[string][][]byte {
	t.Helper()
	result := make(map[string][][]byte)
	accessor := sf.OpenAccessor()
	defer accessor.Close()

	for shardId := int32(0); shardId < numShards; shardId++ {
		rawReader := accessor.GetShardReader(shardId, 0)
		bufferedReader := bufio.NewReader(rawReader)
		reader := kv.NewReader(bufferedReader)

		for {
			key := reader.PeekKey()
			if key == nil {
				break
			}
			keyStr := string(key)
			keyReader := reader.GetKeyReader()
			buf := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			for {
				n, err := keyReader.ReadValue(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("error reading value: %v", err)
				}
				value := make([]byte, n)
				copy(value, buf[:n])
				result[keyStr] = append(result[keyStr], value)
			}
		}
	}
	return result
}

// Helper to create a sharded file with KV data
func createShardedFileWithData(t *testing.T, tmpDir string, numShards int32, shardData map[int32]map[string][][]byte) *shardedfile.ShardedFile {
	t.Helper()
	sf := shardedfile.NewShardedFile(tmpDir, 0)
	creator := sf.OpenCreator()

	for shardId := int32(0); shardId < numShards; shardId++ {
		_, writer := creator.OpenShardWriter(0)
		bufferedWriter := bufio.NewWriter(writer)
		kvWriter := kv.NewWriter(bufferedWriter)

		if data, ok := shardData[shardId]; ok {
			// Sort keys for proper KV file format
			keys := make([]string, 0, len(data))
			for k := range data {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, key := range keys {
				for _, value := range data[key] {
					kvWriter.Write([]byte(key), value)
				}
			}
		}

		bufferedWriter.Flush()
		writer.Close()
	}
	creator.Close()
	return sf
}

// assertValuesEqual checks that two slices of byte slices contain the same elements (order-independent)
func assertValuesEqual(t *testing.T, expected, actual [][]byte) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("expected %d values, got %d", len(expected), len(actual))
	}
	sortByteSlices := func(s [][]byte) {
		sort.Slice(s, func(i, j int) bool {
			return bytes.Compare(s[i], s[j]) < 0
		})
	}
	expectedCopy := make([][]byte, len(expected))
	copy(expectedCopy, expected)
	actualCopy := make([][]byte, len(actual))
	copy(actualCopy, actual)
	sortByteSlices(expectedCopy)
	sortByteSlices(actualCopy)
	for i := range expectedCopy {
		if !bytes.Equal(expectedCopy[i], actualCopy[i]) {
			t.Fatalf("value mismatch at index %d: expected %q, got %q", i, expectedCopy[i], actualCopy[i])
		}
	}
}

// =============================================================================
// Builder Tests
// =============================================================================

func TestBuilder(t *testing.T) {
	t.Run("NewBuilder creates builder with correct parameters", func(t *testing.T) {
		builder := bigkvfile.NewBuilder(10, 4, 1024, 1024, time.Second)
		// Builder should be usable
		result := builder.Finalize()
		if result != nil {
			t.Fatal("expected nil result for empty builder")
		}
	})

	t.Run("PushFile with single file", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(3)

		// Create a sharded file with data
		shardData := map[int32]map[string][][]byte{
			0: {"aaa": {[]byte("v1")}, "bbb": {[]byte("v2")}},
			1: {"ccc": {[]byte("v3")}},
			2: {"ddd": {[]byte("v4"), []byte("v5")}},
		}
		sf := createShardedFileWithData(t, tmpDir, numShards, shardData)

		builder := bigkvfile.NewBuilder(numShards, 4, 1024, 1024, time.Second)
		builder.PushFile(sf)
		result := builder.Finalize()

		if result == nil {
			t.Fatal("expected non-nil result")
		}

		// Verify data
		resultData := readAllFromShardedFile(t, result, numShards)
		assertValuesEqual(t, shardData[0]["aaa"], resultData["aaa"])
		assertValuesEqual(t, shardData[0]["bbb"], resultData["bbb"])
		assertValuesEqual(t, shardData[1]["ccc"], resultData["ccc"])
		assertValuesEqual(t, shardData[2]["ddd"], resultData["ddd"])
	})

	t.Run("PushFile merges multiple files", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(2)

		// Create first file
		shardData1 := map[int32]map[string][][]byte{
			0: {"aaa": {[]byte("a1")}, "ccc": {[]byte("c1")}},
			1: {"eee": {[]byte("e1")}},
		}
		sf1 := createShardedFileWithData(t, tmpDir, numShards, shardData1)

		// Create second file with overlapping keys
		shardData2 := map[int32]map[string][][]byte{
			0: {"aaa": {[]byte("a2")}, "bbb": {[]byte("b1")}},
			1: {"ddd": {[]byte("d1")}},
		}
		sf2 := createShardedFileWithData(t, tmpDir, numShards, shardData2)

		builder := bigkvfile.NewBuilder(numShards, 4, 1024, 1024, time.Second)
		builder.PushFile(sf1)
		builder.PushFile(sf2)
		result := builder.Finalize()

		if result == nil {
			t.Fatal("expected non-nil result")
		}

		// Verify merged data
		resultData := readAllFromShardedFile(t, result, numShards)

		// Key "aaa" should have values from both files
		assertValuesEqual(t, [][]byte{[]byte("a1"), []byte("a2")}, resultData["aaa"])
		assertValuesEqual(t, [][]byte{[]byte("b1")}, resultData["bbb"])
		assertValuesEqual(t, [][]byte{[]byte("c1")}, resultData["ccc"])
		assertValuesEqual(t, [][]byte{[]byte("d1")}, resultData["ddd"])
		assertValuesEqual(t, [][]byte{[]byte("e1")}, resultData["eee"])
	})

	t.Run("PushFile triggers merge at max degree", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(2)
		maxMergeDegree := 3

		// Create files
		files := make([]*shardedfile.ShardedFile, maxMergeDegree+1)
		for i := 0; i <= maxMergeDegree; i++ {
			key := string([]byte{'a' + byte(i)})
			shardData := map[int32]map[string][][]byte{
				0: {key: {[]byte("v" + string([]byte{'0' + byte(i)}))}},
				1: {},
			}
			files[i] = createShardedFileWithData(t, tmpDir, numShards, shardData)
		}

		builder := bigkvfile.NewBuilder(numShards, maxMergeDegree, 1024, 1024, time.Second)
		for _, sf := range files {
			builder.PushFile(sf)
		}
		result := builder.Finalize()

		if result == nil {
			t.Fatal("expected non-nil result")
		}

		// Verify all data is present
		resultData := readAllFromShardedFile(t, result, numShards)
		expectedKeys := []string{"a", "b", "c", "d"}
		for i, key := range expectedKeys {
			if _, ok := resultData[key]; !ok {
				t.Fatalf("missing key %q", key)
			}
			expectedValue := []byte("v" + string([]byte{'0' + byte(i)}))
			assertValuesEqual(t, [][]byte{expectedValue}, resultData[key])
		}
	})

	t.Run("Finalize merges all levels", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(1)
		maxMergeDegree := 2

		// Push many files to create multiple levels
		numFiles := 10
		expectedValues := make(map[string][]byte)

		builder := bigkvfile.NewBuilder(numShards, maxMergeDegree, 1024, 1024, time.Second)
		for i := 0; i < numFiles; i++ {
			key := string([]byte{'a' + byte(i%26)})
			value := []byte{byte(i)}
			expectedValues[key] = value

			shardData := map[int32]map[string][][]byte{
				0: {key: {value}},
			}
			sf := createShardedFileWithData(t, tmpDir, numShards, shardData)
			builder.PushFile(sf)
		}
		result := builder.Finalize()

		if result == nil {
			t.Fatal("expected non-nil result")
		}

		// Verify all data is present
		resultData := readAllFromShardedFile(t, result, numShards)
		for key, value := range expectedValues {
			if _, ok := resultData[key]; !ok {
				t.Fatalf("missing key %q", key)
			}
			// At least one value should exist
			found := false
			for _, v := range resultData[key] {
				if bytes.Equal(v, value) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("key %q: value %v not found in %v", key, value, resultData[key])
			}
		}
	})

	t.Run("Finalize with empty builder returns nil", func(t *testing.T) {
		builder := bigkvfile.NewBuilder(5, 4, 1024, 1024, time.Second)
		result := builder.Finalize()
		if result != nil {
			t.Fatal("expected nil result for empty builder")
		}
	})
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestBuilderIntegration(t *testing.T) {
	t.Run("Multiple shards with sorted keys", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(4)

		// Create files with data distributed across shards
		shardData1 := map[int32]map[string][][]byte{
			0: {"a1": {[]byte("v1")}},
			1: {"b1": {[]byte("v2")}},
			2: {"c1": {[]byte("v3")}},
			3: {"d1": {[]byte("v4")}},
		}
		sf1 := createShardedFileWithData(t, tmpDir, numShards, shardData1)

		shardData2 := map[int32]map[string][][]byte{
			0: {"a2": {[]byte("v5")}},
			1: {"b2": {[]byte("v6")}},
			2: {"c2": {[]byte("v7")}},
			3: {"d2": {[]byte("v8")}},
		}
		sf2 := createShardedFileWithData(t, tmpDir, numShards, shardData2)

		builder := bigkvfile.NewBuilder(numShards, 4, 1024, 1024, time.Second)
		builder.PushFile(sf1)
		builder.PushFile(sf2)
		result := builder.Finalize()

		// Verify each shard
		resultData := readAllFromShardedFile(t, result, numShards)

		expectedKeys := map[int32][]string{
			0: {"a1", "a2"},
			1: {"b1", "b2"},
			2: {"c1", "c2"},
			3: {"d1", "d2"},
		}

		for shardId, keys := range expectedKeys {
			for _, key := range keys {
				if _, ok := resultData[key]; !ok {
					t.Fatalf("shard %d: missing key %q", shardId, key)
				}
			}
		}
	})

	t.Run("Large values", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(2)

		// Create large values
		largeValue := make([]byte, 100000) // 100KB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		shardData := map[int32]map[string][][]byte{
			0: {"large": {largeValue}},
			1: {"small": {[]byte("tiny")}},
		}
		sf := createShardedFileWithData(t, tmpDir, numShards, shardData)

		builder := bigkvfile.NewBuilder(numShards, 4, 1024, 1024, time.Second)
		builder.PushFile(sf)
		result := builder.Finalize()

		// Verify
		resultData := readAllFromShardedFile(t, result, numShards)
		if !bytes.Equal(resultData["large"][0], largeValue) {
			t.Fatal("large value mismatch")
		}
		if !bytes.Equal(resultData["small"][0], []byte("tiny")) {
			t.Fatal("small value mismatch")
		}
	})

	t.Run("Same key with many values across files", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(1)
		numFiles := 5
		valuesPerFile := 3

		var expectedValues [][]byte
		builder := bigkvfile.NewBuilder(numShards, 10, 1024, 1024, time.Second)

		for i := 0; i < numFiles; i++ {
			var values [][]byte
			for j := 0; j < valuesPerFile; j++ {
				v := []byte{byte(i), byte(j)}
				values = append(values, v)
				expectedValues = append(expectedValues, v)
			}

			shardData := map[int32]map[string][][]byte{
				0: {"shared": values},
			}
			sf := createShardedFileWithData(t, tmpDir, numShards, shardData)
			builder.PushFile(sf)
		}

		result := builder.Finalize()
		resultData := readAllFromShardedFile(t, result, numShards)

		assertValuesEqual(t, expectedValues, resultData["shared"])
	})

	t.Run("Empty shards in files", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(3)

		// First file has data only in shard 0
		shardData1 := map[int32]map[string][][]byte{
			0: {"key1": {[]byte("v1")}},
			1: {},
			2: {},
		}
		sf1 := createShardedFileWithData(t, tmpDir, numShards, shardData1)

		// Second file has data only in shard 2
		shardData2 := map[int32]map[string][][]byte{
			0: {},
			1: {},
			2: {"key2": {[]byte("v2")}},
		}
		sf2 := createShardedFileWithData(t, tmpDir, numShards, shardData2)

		builder := bigkvfile.NewBuilder(numShards, 4, 1024, 1024, time.Second)
		builder.PushFile(sf1)
		builder.PushFile(sf2)
		result := builder.Finalize()

		resultData := readAllFromShardedFile(t, result, numShards)
		assertValuesEqual(t, [][]byte{[]byte("v1")}, resultData["key1"])
		assertValuesEqual(t, [][]byte{[]byte("v2")}, resultData["key2"])
	})
}
