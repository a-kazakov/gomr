package bigkvfile

// Tests use package-internal access for: NewThrottledWriter() constructor, levels field on Builder.

import (
	"bufio"
	"io"
	"os"
	"testing"
	"time"

	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/rw/kv"
	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
)

// =============================================================================
// Helpers
// =============================================================================

// createShardedFileWithData creates a sharded file with sorted KV data per shard.
func createTestShardedFile(t *testing.T, tmpDir string, numShards int32, shardData map[int32]map[string][][]byte) *shardedfile.ShardedFile {
	t.Helper()
	sf := shardedfile.NewShardedFile(tmpDir, 0)
	creator := sf.OpenCreator()

	for shardId := int32(0); shardId < numShards; shardId++ {
		_, writer := creator.OpenShardWriter(0)
		bufferedWriter := bufio.NewWriter(writer)
		kvWriter := kv.NewWriter(bufferedWriter)
		if data, ok := shardData[shardId]; ok {
			// Keys must be sorted; for simplicity we use single-char keys.
			keys := make([]string, 0, len(data))
			for k := range data {
				keys = append(keys, k)
			}
			// Simple sort for small key sets
			for i := 0; i < len(keys); i++ {
				for j := i + 1; j < len(keys); j++ {
					if keys[i] > keys[j] {
						keys[i], keys[j] = keys[j], keys[i]
					}
				}
			}
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

// readAllFromShardedFile reads all KV pairs from all shards.
func readAllFromTestShardedFile(t *testing.T, sf *shardedfile.ShardedFile, numShards int32) map[string][][]byte {
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

// =============================================================================
// ThrottledWriter tests
// =============================================================================

// slowWriter is a writer that artificially takes time to write.
type slowWriter struct {
	buf   []byte
	delay time.Duration
}

func (sw *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(sw.delay)
	sw.buf = append(sw.buf, p...)
	return len(p), nil
}

func TestThrottledWriter(t *testing.T) {
	t.Run("slow write triggers penalty", func(t *testing.T) {
		// Use a very short target latency so any write will exceed it.
		sw := &slowWriter{delay: 5 * time.Millisecond}
		tw := NewThrottledWriter(sw, 1*time.Nanosecond)

		data := []byte("hello world")
		start := time.Now()
		n, err := tw.Write(data)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != len(data) {
			t.Fatalf("expected %d bytes written, got %d", len(data), n)
		}
		// The write should have taken at least the slow writer's delay plus a penalty sleep.
		// The penalty = duration - targetLatency, capped at 1s.
		// Since the write takes ~5ms and target is 1ns, penalty is ~5ms.
		// Total should be at least ~10ms.
		if elapsed < 8*time.Millisecond {
			t.Fatalf("expected at least 8ms elapsed (penalty sleep), got %v", elapsed)
		}
	})

	t.Run("fast write has no penalty", func(t *testing.T) {
		// Use a long target latency so no penalty is triggered.
		sw := &slowWriter{delay: 0}
		tw := NewThrottledWriter(sw, 10*time.Second)

		data := []byte("hello")
		start := time.Now()
		n, err := tw.Write(data)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != len(data) {
			t.Fatalf("expected %d bytes written, got %d", len(data), n)
		}
		// Should be very fast (no penalty sleep).
		if elapsed > 100*time.Millisecond {
			t.Fatalf("expected fast write without penalty, got %v", elapsed)
		}
	})
}

// =============================================================================
// Builder internals tests
// =============================================================================

func TestBuilderInternals(t *testing.T) {
	t.Run("push file overflow to new level", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(1)
		mergeThreshold := 2

		// With 4 initial levels and mergeThreshold=2, we need 2^4=16 pushes to fill all
		// levels and trigger the overflow (leftover != nil at the end of the loop).
		builder := NewBuilder(numShards, mergeThreshold, 1024, 1024, time.Second)

		numFiles := 16
		for i := 0; i < numFiles; i++ {
			key := string([]byte{byte('a' + i)})
			shardData := map[int32]map[string][][]byte{
				0: {key: {[]byte("v")}},
			}
			sf := createTestShardedFile(t, tmpDir, numShards, shardData)
			builder.PushFile(sf)
		}

		// The builder should have created a 5th level.
		if len(builder.levels) <= 4 {
			t.Fatalf("expected more than 4 levels after overflow, got %d", len(builder.levels))
		}

		result := builder.Finalize()
		if result == nil {
			t.Fatal("expected non-nil result")
		}

		// Verify all data is present
		resultData := readAllFromTestShardedFile(t, result, numShards)
		for i := 0; i < numFiles; i++ {
			key := string([]byte{byte('a' + i)})
			if _, ok := resultData[key]; !ok {
				t.Fatalf("missing key %q after overflow merge", key)
			}
		}
	})

	t.Run("stat error panics", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(1)

		// Create a valid sharded file, then delete it from disk.
		sf := createTestShardedFile(t, tmpDir, numShards, map[int32]map[string][][]byte{
			0: {"a": {[]byte("v1")}},
		})
		// Remove the underlying file so os.Stat will fail.
		os.Remove(sf.GetName())

		builder := NewBuilder(numShards, 2, 1024, 1024, time.Second)

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic when stat fails on deleted file")
			}
		}()
		builder.PushFile(sf)
	})

	t.Run("merge with slow write triggers backoff", func(t *testing.T) {
		tmpDir := t.TempDir()
		numShards := int32(1)

		// Create two files so merge is triggered.
		sf1 := createTestShardedFile(t, tmpDir, numShards, map[int32]map[string][][]byte{
			0: {"a": {[]byte("v1")}},
		})
		sf2 := createTestShardedFile(t, tmpDir, numShards, map[int32]map[string][][]byte{
			0: {"b": {[]byte("v2")}},
		})

		// Use a very short targetWriteLatency (1ns) so that any real write will exceed it,
		// triggering the sleep penalty in the merge loop.
		builder := NewBuilder(numShards, 2, 1024, 1024, 1*time.Nanosecond)
		builder.PushFile(sf1)
		builder.PushFile(sf2) // This triggers merge because mergeThreshold=2

		result := builder.Finalize()
		if result == nil {
			t.Fatal("expected non-nil result")
		}

		resultData := readAllFromTestShardedFile(t, result, numShards)
		if string(resultData["a"][0]) != "v1" {
			t.Fatalf("expected 'v1', got %q", string(resultData["a"][0]))
		}
		if string(resultData["b"][0]) != "v2" {
			t.Fatalf("expected 'v2', got %q", string(resultData["b"][0]))
		}
	})
}
