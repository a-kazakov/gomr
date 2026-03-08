package kv_test

import (
	"bytes"
	"io"
	"sort"
	"testing"

	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/rw/kv"
)

// Helper to read all values for a key using a KeyReader
func readAllValues(t *testing.T, keyReader kv.KeyReader) [][]byte {
	t.Helper()
	var values [][]byte
	buf := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
	for {
		n, err := keyReader.ReadValue(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error reading value: %v", err)
		}
		value := make([]byte, n)
		copy(value, buf[:n])
		values = append(values, value)
	}
	return values
}

// Helper to collect all key-value groups from a Reader
func collectAllFromReader(t *testing.T, reader *kv.Reader) map[string][][]byte {
	t.Helper()
	result := make(map[string][][]byte)
	for {
		key := reader.PeekKey()
		if key == nil {
			break
		}
		keyStr := string(key)
		keyReader := reader.GetKeyReader()
		values := readAllValues(t, keyReader)
		result[keyStr] = append(result[keyStr], values...)
	}
	return result
}

// Helper to collect all key-value groups from a MultiReader
func collectAllFromMultiReader(t *testing.T, reader *kv.MultiReader) map[string][][]byte {
	t.Helper()
	result := make(map[string][][]byte)
	for {
		key := reader.PeekKey()
		if key == nil {
			break
		}
		keyStr := string(key)
		keyReader := reader.GetKeyReader()
		values := readAllValues(t, keyReader)
		result[keyStr] = append(result[keyStr], values...)
	}
	return result
}

// assertValuesEqual checks that two slices of byte slices contain the same elements (order-independent)
func assertValuesEqual(t *testing.T, expected, actual [][]byte) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("expected %d values, got %d", len(expected), len(actual))
	}
	// Sort both for comparison
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

func TestReader(t *testing.T) {
	t.Run("single key value", func(t *testing.T) {
		// Write a single key-value pair
		var buf bytes.Buffer
		writer := kv.NewWriter(&buf)
		writer.Write([]byte("key1"), []byte("value1"))

		// Read it back
		reader := kv.NewReader(&buf)

		// Verify key
		key := reader.PeekKey()
		if key == nil {
			t.Fatal("expected key, got nil")
		}
		if string(key) != "key1" {
			t.Fatalf("expected key 'key1', got %q", string(key))
		}

		// Get key reader and read value
		keyReader := reader.GetKeyReader()
		valueBuf := make([]byte, 100)
		n, err := keyReader.ReadValue(valueBuf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(valueBuf[:n]) != "value1" {
			t.Fatalf("expected 'value1', got %q", string(valueBuf[:n]))
		}

		// No more values for this key
		_, err = keyReader.ReadValue(valueBuf)
		if err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}

		// No more keys
		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})

	t.Run("multiple values for same key", func(t *testing.T) {
		var buf bytes.Buffer
		writer := kv.NewWriter(&buf)

		// Write multiple values for the same key
		writer.Write([]byte("key1"), []byte("value1"))
		writer.Write([]byte("key1"), []byte("value2"))
		writer.Write([]byte("key1"), []byte("value3"))

		reader := kv.NewReader(&buf)

		// Verify key
		key := reader.PeekKey()
		if string(key) != "key1" {
			t.Fatalf("expected 'key1', got %q", string(key))
		}

		// Read all values
		keyReader := reader.GetKeyReader()
		values := readAllValues(t, keyReader)

		expected := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
		if len(values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(values))
		}
		for i, v := range values {
			if !bytes.Equal(v, expected[i]) {
				t.Fatalf("value %d: expected %q, got %q", i, expected[i], v)
			}
		}

		// No more keys
		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})

	t.Run("multiple different keys", func(t *testing.T) {
		var buf bytes.Buffer
		writer := kv.NewWriter(&buf)

		// Write values for multiple keys (must be sorted)
		writer.Write([]byte("aaa"), []byte("val_a1"))
		writer.Write([]byte("aaa"), []byte("val_a2"))
		writer.Write([]byte("bbb"), []byte("val_b1"))
		writer.Write([]byte("ccc"), []byte("val_c1"))
		writer.Write([]byte("ccc"), []byte("val_c2"))
		writer.Write([]byte("ccc"), []byte("val_c3"))

		reader := kv.NewReader(&buf)

		// Key "aaa"
		key := reader.PeekKey()
		if string(key) != "aaa" {
			t.Fatalf("expected 'aaa', got %q", string(key))
		}
		values := readAllValues(t, reader.GetKeyReader())
		assertValuesEqual(t, [][]byte{[]byte("val_a1"), []byte("val_a2")}, values)

		// Key "bbb"
		key = reader.PeekKey()
		if string(key) != "bbb" {
			t.Fatalf("expected 'bbb', got %q", string(key))
		}
		values = readAllValues(t, reader.GetKeyReader())
		assertValuesEqual(t, [][]byte{[]byte("val_b1")}, values)

		// Key "ccc"
		key = reader.PeekKey()
		if string(key) != "ccc" {
			t.Fatalf("expected 'ccc', got %q", string(key))
		}
		values = readAllValues(t, reader.GetKeyReader())
		assertValuesEqual(t, [][]byte{[]byte("val_c1"), []byte("val_c2"), []byte("val_c3")}, values)

		// No more keys
		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})

	t.Run("empty value", func(t *testing.T) {
		var buf bytes.Buffer
		writer := kv.NewWriter(&buf)

		writer.Write([]byte("key1"), []byte(""))
		writer.Write([]byte("key1"), []byte("non-empty"))
		writer.Write([]byte("key1"), []byte(""))

		reader := kv.NewReader(&buf)

		key := reader.PeekKey()
		if string(key) != "key1" {
			t.Fatalf("expected 'key1', got %q", string(key))
		}

		values := readAllValues(t, reader.GetKeyReader())
		expected := [][]byte{[]byte(""), []byte("non-empty"), []byte("")}
		if len(values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(values))
		}
		for i, v := range values {
			if !bytes.Equal(v, expected[i]) {
				t.Fatalf("value %d: expected %q, got %q", i, expected[i], v)
			}
		}
	})

	t.Run("empty file", func(t *testing.T) {
		var buf bytes.Buffer
		// Write nothing

		reader := kv.NewReader(&buf)

		if reader.PeekKey() != nil {
			t.Fatal("expected nil key for empty file")
		}
	})

	t.Run("large values", func(t *testing.T) {
		var buf bytes.Buffer
		writer := kv.NewWriter(&buf)

		// Create a large value (1MB)
		largeValue := make([]byte, 1024*1024)
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		writer.Write([]byte("bigkey"), largeValue)

		reader := kv.NewReader(&buf)

		key := reader.PeekKey()
		if string(key) != "bigkey" {
			t.Fatalf("expected 'bigkey', got %q", string(key))
		}

		keyReader := reader.GetKeyReader()
		readBuf := make([]byte, 2*1024*1024)
		n, err := keyReader.ReadValue(readBuf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != len(largeValue) {
			t.Fatalf("expected %d bytes, got %d", len(largeValue), n)
		}
		if !bytes.Equal(readBuf[:n], largeValue) {
			t.Fatal("large value mismatch")
		}
	})

	t.Run("key reader version isolation", func(t *testing.T) {
		// Test that a KeyReader obtained for one key doesn't read values from another key
		var buf bytes.Buffer
		writer := kv.NewWriter(&buf)

		writer.Write([]byte("key1"), []byte("v1"))
		writer.Write([]byte("key2"), []byte("v2"))

		reader := kv.NewReader(&buf)

		// Get reader for key1
		key1Reader := reader.GetKeyReader()

		// Read value from key1
		valueBuf := make([]byte, 100)
		n, err := key1Reader.ReadValue(valueBuf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(valueBuf[:n]) != "v1" {
			t.Fatalf("expected 'v1', got %q", string(valueBuf[:n]))
		}

		// Now key1Reader should return EOF (key moved to key2)
		_, err = key1Reader.ReadValue(valueBuf)
		if err != io.EOF {
			t.Fatalf("expected EOF for old key reader, got %v", err)
		}

		// Get reader for key2
		key := reader.PeekKey()
		if string(key) != "key2" {
			t.Fatalf("expected 'key2', got %q", string(key))
		}
		key2Reader := reader.GetKeyReader()
		n, err = key2Reader.ReadValue(valueBuf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(valueBuf[:n]) != "v2" {
			t.Fatalf("expected 'v2', got %q", string(valueBuf[:n]))
		}
	})

	t.Run("key deduplication", func(t *testing.T) {
		// Test that the writer correctly deduplicates consecutive same keys
		// by writing 0 as key length
		var buf bytes.Buffer
		writer := kv.NewWriter(&buf)

		// Write same key multiple times
		writer.Write([]byte("samekey"), []byte("v1"))
		writer.Write([]byte("samekey"), []byte("v2"))
		writer.Write([]byte("samekey"), []byte("v3"))

		// The buffer should be smaller than if we wrote the key 3 times
		// Key "samekey" is 7 bytes
		// Without dedup: 3 * (2 + 7 + 4 + 2) = 45 bytes (key_len + key + val_len + val)
		// With dedup: (2 + 7 + 4 + 2) + 2*(2 + 4 + 2) = 15 + 16 = 31 bytes
		// But let's just verify we can read it back correctly

		reader := kv.NewReader(bytes.NewReader(buf.Bytes()))
		values := readAllValues(t, reader.GetKeyReader())

		expected := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
		if len(values) != 3 {
			t.Fatalf("expected 3 values, got %d", len(values))
		}
		for i, v := range values {
			if !bytes.Equal(v, expected[i]) {
				t.Fatalf("value %d: expected %q, got %q", i, expected[i], v)
			}
		}
	})
}

func TestMultiReader(t *testing.T) {
	t.Run("single file", func(t *testing.T) {
		var buf bytes.Buffer
		writer := kv.NewWriter(&buf)

		writer.Write([]byte("key1"), []byte("v1"))
		writer.Write([]byte("key1"), []byte("v2"))
		writer.Write([]byte("key2"), []byte("v3"))

		reader := kv.NewMultiReader([]io.Reader{bytes.NewReader(buf.Bytes())})

		result := collectAllFromMultiReader(t, reader)

		if len(result) != 2 {
			t.Fatalf("expected 2 keys, got %d", len(result))
		}
		assertValuesEqual(t, [][]byte{[]byte("v1"), []byte("v2")}, result["key1"])
		assertValuesEqual(t, [][]byte{[]byte("v3")}, result["key2"])
	})

	t.Run("multiple files with distinct keys", func(t *testing.T) {
		// File 1: keys "aaa", "ccc"
		var buf1 bytes.Buffer
		w1 := kv.NewWriter(&buf1)
		w1.Write([]byte("aaa"), []byte("a1"))
		w1.Write([]byte("ccc"), []byte("c1"))

		// File 2: keys "bbb", "ddd"
		var buf2 bytes.Buffer
		w2 := kv.NewWriter(&buf2)
		w2.Write([]byte("bbb"), []byte("b1"))
		w2.Write([]byte("ddd"), []byte("d1"))

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
		})

		// Verify merge order: aaa, bbb, ccc, ddd
		expectedOrder := []string{"aaa", "bbb", "ccc", "ddd"}
		var actualOrder []string

		for {
			key := reader.PeekKey()
			if key == nil {
				break
			}
			actualOrder = append(actualOrder, string(key))
			keyReader := reader.GetKeyReader()
			readAllValues(t, keyReader) // consume values
		}

		if len(actualOrder) != len(expectedOrder) {
			t.Fatalf("expected %d keys, got %d", len(expectedOrder), len(actualOrder))
		}
		for i, exp := range expectedOrder {
			if actualOrder[i] != exp {
				t.Fatalf("key %d: expected %q, got %q", i, exp, actualOrder[i])
			}
		}
	})

	t.Run("multiple files with same key", func(t *testing.T) {
		// This tests the bug that was fixed: multiple files with the same key
		// File 1: key "shared" -> values [v1, v2]
		var buf1 bytes.Buffer
		w1 := kv.NewWriter(&buf1)
		w1.Write([]byte("shared"), []byte("v1"))
		w1.Write([]byte("shared"), []byte("v2"))

		// File 2: key "shared" -> values [v3, v4]
		var buf2 bytes.Buffer
		w2 := kv.NewWriter(&buf2)
		w2.Write([]byte("shared"), []byte("v3"))
		w2.Write([]byte("shared"), []byte("v4"))

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
		})

		key := reader.PeekKey()
		if string(key) != "shared" {
			t.Fatalf("expected 'shared', got %q", string(key))
		}

		keyReader := reader.GetKeyReader()
		values := readAllValues(t, keyReader)

		// Should get all 4 values
		expected := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4")}
		assertValuesEqual(t, expected, values)

		// No more keys
		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})

	t.Run("mixed shared and distinct keys", func(t *testing.T) {
		// File 1: keys "aaa" -> [a1], "bbb" -> [b1], "ccc" -> [c1]
		var buf1 bytes.Buffer
		w1 := kv.NewWriter(&buf1)
		w1.Write([]byte("aaa"), []byte("a1"))
		w1.Write([]byte("bbb"), []byte("b1"))
		w1.Write([]byte("ccc"), []byte("c1"))

		// File 2: keys "bbb" -> [b2, b3], "ddd" -> [d1]
		var buf2 bytes.Buffer
		w2 := kv.NewWriter(&buf2)
		w2.Write([]byte("bbb"), []byte("b2"))
		w2.Write([]byte("bbb"), []byte("b3"))
		w2.Write([]byte("ddd"), []byte("d1"))

		// File 3: keys "bbb" -> [b4], "ccc" -> [c2]
		var buf3 bytes.Buffer
		w3 := kv.NewWriter(&buf3)
		w3.Write([]byte("bbb"), []byte("b4"))
		w3.Write([]byte("ccc"), []byte("c2"))

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
			bytes.NewReader(buf3.Bytes()),
		})

		result := collectAllFromMultiReader(t, reader)

		if len(result) != 4 {
			t.Fatalf("expected 4 keys, got %d", len(result))
		}

		assertValuesEqual(t, [][]byte{[]byte("a1")}, result["aaa"])
		assertValuesEqual(t, [][]byte{[]byte("b1"), []byte("b2"), []byte("b3"), []byte("b4")}, result["bbb"])
		assertValuesEqual(t, [][]byte{[]byte("c1"), []byte("c2")}, result["ccc"])
		assertValuesEqual(t, [][]byte{[]byte("d1")}, result["ddd"])
	})

	t.Run("empty files mixed with data", func(t *testing.T) {
		// File 1: empty
		var buf1 bytes.Buffer

		// File 2: has data
		var buf2 bytes.Buffer
		w2 := kv.NewWriter(&buf2)
		w2.Write([]byte("key1"), []byte("v1"))

		// File 3: empty
		var buf3 bytes.Buffer

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
			bytes.NewReader(buf3.Bytes()),
		})

		key := reader.PeekKey()
		if string(key) != "key1" {
			t.Fatalf("expected 'key1', got %q", string(key))
		}

		values := readAllValues(t, reader.GetKeyReader())
		assertValuesEqual(t, [][]byte{[]byte("v1")}, values)

		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})

	t.Run("all empty files", func(t *testing.T) {
		var buf1, buf2, buf3 bytes.Buffer

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
			bytes.NewReader(buf3.Bytes()),
		})

		if reader.PeekKey() != nil {
			t.Fatal("expected nil key for all empty files")
		}
	})

	t.Run("no files", func(t *testing.T) {
		reader := kv.NewMultiReader([]io.Reader{})

		if reader.PeekKey() != nil {
			t.Fatal("expected nil key for no files")
		}
	})

	t.Run("three files same key merge", func(t *testing.T) {
		// Stress test the same-key merging across 3 files
		var buf1 bytes.Buffer
		w1 := kv.NewWriter(&buf1)
		w1.Write([]byte("X"), []byte("1"))
		w1.Write([]byte("X"), []byte("2"))

		var buf2 bytes.Buffer
		w2 := kv.NewWriter(&buf2)
		w2.Write([]byte("X"), []byte("3"))

		var buf3 bytes.Buffer
		w3 := kv.NewWriter(&buf3)
		w3.Write([]byte("X"), []byte("4"))
		w3.Write([]byte("X"), []byte("5"))
		w3.Write([]byte("X"), []byte("6"))

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
			bytes.NewReader(buf3.Bytes()),
		})

		key := reader.PeekKey()
		if string(key) != "X" {
			t.Fatalf("expected 'X', got %q", string(key))
		}

		values := readAllValues(t, reader.GetKeyReader())
		expected := [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5"), []byte("6")}
		assertValuesEqual(t, expected, values)

		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})

	t.Run("key reader version isolation", func(t *testing.T) {
		var buf1 bytes.Buffer
		w1 := kv.NewWriter(&buf1)
		w1.Write([]byte("key1"), []byte("v1"))
		w1.Write([]byte("key2"), []byte("v2"))

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
		})

		// Get reader for key1
		key1Reader := reader.GetKeyReader()

		// Read value from key1
		valueBuf := make([]byte, 100)
		n, err := key1Reader.ReadValue(valueBuf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(valueBuf[:n]) != "v1" {
			t.Fatalf("expected 'v1', got %q", string(valueBuf[:n]))
		}

		// key1Reader should return EOF now
		_, err = key1Reader.ReadValue(valueBuf)
		if err != io.EOF {
			t.Fatalf("expected EOF, got %v", err)
		}

		// Verify key2 is next
		key := reader.PeekKey()
		if string(key) != "key2" {
			t.Fatalf("expected 'key2', got %q", string(key))
		}

		// key1Reader still returns EOF (version mismatch)
		_, err = key1Reader.ReadValue(valueBuf)
		if err != io.EOF {
			t.Fatalf("expected EOF for old key reader, got %v", err)
		}
	})

	t.Run("interleaved keys", func(t *testing.T) {
		// File 1: a, c, e
		var buf1 bytes.Buffer
		w1 := kv.NewWriter(&buf1)
		w1.Write([]byte("a"), []byte("a1"))
		w1.Write([]byte("c"), []byte("c1"))
		w1.Write([]byte("e"), []byte("e1"))

		// File 2: b, d, f
		var buf2 bytes.Buffer
		w2 := kv.NewWriter(&buf2)
		w2.Write([]byte("b"), []byte("b1"))
		w2.Write([]byte("d"), []byte("d1"))
		w2.Write([]byte("f"), []byte("f1"))

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
		})

		expectedKeys := []string{"a", "b", "c", "d", "e", "f"}
		expectedValues := []string{"a1", "b1", "c1", "d1", "e1", "f1"}

		for i, expectedKey := range expectedKeys {
			key := reader.PeekKey()
			if key == nil {
				t.Fatalf("expected key %q, got nil", expectedKey)
			}
			if string(key) != expectedKey {
				t.Fatalf("expected key %q, got %q", expectedKey, string(key))
			}

			keyReader := reader.GetKeyReader()
			valueBuf := make([]byte, 100)
			n, err := keyReader.ReadValue(valueBuf)
			if err != nil {
				t.Fatalf("unexpected error for key %q: %v", expectedKey, err)
			}
			if string(valueBuf[:n]) != expectedValues[i] {
				t.Fatalf("expected value %q, got %q", expectedValues[i], string(valueBuf[:n]))
			}

			// Drain remaining (none expected)
			_, err = keyReader.ReadValue(valueBuf)
			if err != io.EOF {
				t.Fatalf("expected EOF after reading single value for key %q", expectedKey)
			}
		}

		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})

	t.Run("binary keys", func(t *testing.T) {
		// Test with binary (non-UTF8) keys
		key1 := []byte{0x00, 0x01, 0x02}
		key2 := []byte{0x00, 0x01, 0x03}
		key3 := []byte{0xFF, 0xFE, 0xFD}

		var buf1 bytes.Buffer
		w1 := kv.NewWriter(&buf1)
		w1.Write(key1, []byte("v1"))
		w1.Write(key3, []byte("v3"))

		var buf2 bytes.Buffer
		w2 := kv.NewWriter(&buf2)
		w2.Write(key2, []byte("v2"))

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
		})

		// Keys should be in byte order: key1 < key2 < key3
		key := reader.PeekKey()
		if !bytes.Equal(key, key1) {
			t.Fatalf("expected key1 %v, got %v", key1, key)
		}
		readAllValues(t, reader.GetKeyReader())

		key = reader.PeekKey()
		if !bytes.Equal(key, key2) {
			t.Fatalf("expected key2 %v, got %v", key2, key)
		}
		readAllValues(t, reader.GetKeyReader())

		key = reader.PeekKey()
		if !bytes.Equal(key, key3) {
			t.Fatalf("expected key3 %v, got %v", key3, key)
		}
		readAllValues(t, reader.GetKeyReader())

		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})

	t.Run("same key different files sequential", func(t *testing.T) {
		// Test that when we have files with overlapping key sequences,
		// the multi-reader correctly merges them

		// File 1: A -> [a1], B -> [b1]
		var buf1 bytes.Buffer
		w1 := kv.NewWriter(&buf1)
		w1.Write([]byte("A"), []byte("a1"))
		w1.Write([]byte("B"), []byte("b1"))

		// File 2: A -> [a2], C -> [c1]
		var buf2 bytes.Buffer
		w2 := kv.NewWriter(&buf2)
		w2.Write([]byte("A"), []byte("a2"))
		w2.Write([]byte("C"), []byte("c1"))

		reader := kv.NewMultiReader([]io.Reader{
			bytes.NewReader(buf1.Bytes()),
			bytes.NewReader(buf2.Bytes()),
		})

		// Key A: should merge values from both files
		key := reader.PeekKey()
		if string(key) != "A" {
			t.Fatalf("expected 'A', got %q", string(key))
		}
		values := readAllValues(t, reader.GetKeyReader())
		assertValuesEqual(t, [][]byte{[]byte("a1"), []byte("a2")}, values)

		// Key B: only from file 1
		key = reader.PeekKey()
		if string(key) != "B" {
			t.Fatalf("expected 'B', got %q", string(key))
		}
		values = readAllValues(t, reader.GetKeyReader())
		assertValuesEqual(t, [][]byte{[]byte("b1")}, values)

		// Key C: only from file 2
		key = reader.PeekKey()
		if string(key) != "C" {
			t.Fatalf("expected 'C', got %q", string(key))
		}
		values = readAllValues(t, reader.GetKeyReader())
		assertValuesEqual(t, [][]byte{[]byte("c1")}, values)

		if reader.PeekKey() != nil {
			t.Fatal("expected no more keys")
		}
	})
}
