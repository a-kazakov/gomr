package shardedfile_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
)

// =============================================================================
// ShardedFile Tests
// =============================================================================

func TestShardedFile(t *testing.T) {
	t.Run("NewShardedFile creates file in directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		// File should not exist yet (created on OpenCreator)
		creator := sf.OpenCreator()
		defer creator.Close()

		// Verify the directory was created
		entries, err := os.ReadDir(tmpDir)
		if err != nil {
			t.Fatalf("failed to read temp dir: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 file, got %d", len(entries))
		}
		if filepath.Ext(entries[0].Name()) != ".dat" {
			t.Fatalf("expected .dat extension, got %s", entries[0].Name())
		}
	})

	t.Run("NewDerivedShardedFile uses same directory as parent", func(t *testing.T) {
		tmpDir := t.TempDir()
		parent := shardedfile.NewShardedFile(tmpDir, 0)

		// Create parent file
		creator := parent.OpenCreator()
		shardId, writer := creator.OpenShardWriter(0)
		if shardId != 0 {
			t.Fatalf("expected shard 0, got %d", shardId)
		}
		writer.Write([]byte("data"))
		writer.Close()
		creator.Close()

		// Create derived file
		derived := shardedfile.NewDerivedShardedFile(parent)
		derivedCreator := derived.OpenCreator()
		defer derivedCreator.Close()

		// Verify both files are in the same directory
		entries, err := os.ReadDir(tmpDir)
		if err != nil {
			t.Fatalf("failed to read temp dir: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 files, got %d", len(entries))
		}
	})
}

// =============================================================================
// Creator Tests
// =============================================================================

func TestCreator(t *testing.T) {
	t.Run("OpenShardWriter returns sequential shard IDs", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)
		creator := sf.OpenCreator()
		defer creator.Close()

		for expectedId := int32(0); expectedId < 5; expectedId++ {
			shardId, writer := creator.OpenShardWriter(0)
			if shardId != expectedId {
				t.Fatalf("expected shard %d, got %d", expectedId, shardId)
			}
			writer.Write([]byte("test"))
			writer.Close()
		}
	})

	t.Run("Close properly closes underlying file", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)
		creator := sf.OpenCreator()

		shardId, writer := creator.OpenShardWriter(0)
		if shardId != 0 {
			t.Fatalf("expected shard 0, got %d", shardId)
		}
		writer.Write([]byte("data"))
		writer.Close()

		creator.Close()

		// File should exist and be readable
		accessor := sf.OpenAccessor()
		defer accessor.Close()
	})
}

// =============================================================================
// Accessor Tests
// =============================================================================

func TestAccessor(t *testing.T) {
	t.Run("GetShardReader returns reader for correct shard", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		// Write data to multiple shards
		creator := sf.OpenCreator()
		shardData := [][]byte{
			[]byte("shard0-data"),
			[]byte("shard1-data"),
			[]byte("shard2-data"),
		}

		for _, data := range shardData {
			_, writer := creator.OpenShardWriter(0)
			writer.Write(data)
			writer.Close()
		}
		creator.Close()

		// Read back from each shard
		accessor := sf.OpenAccessor()
		defer accessor.Close()

		for i, expected := range shardData {
			reader := accessor.GetShardReader(int32(i), 0)
			buf := make([]byte, len(expected))
			n, err := io.ReadFull(reader, buf)
			if err != nil {
				t.Fatalf("shard %d: unexpected error: %v", i, err)
			}
			if n != len(expected) {
				t.Fatalf("shard %d: expected %d bytes, got %d", i, len(expected), n)
			}
			if !bytes.Equal(buf, expected) {
				t.Fatalf("shard %d: expected %q, got %q", i, expected, buf)
			}
		}
	})

	t.Run("Close properly closes underlying file", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)
		writer.Write([]byte("test"))
		writer.Close()
		creator.Close()

		accessor := sf.OpenAccessor()
		accessor.Close()
		// No panic means success
	})
}

// =============================================================================
// ShardReader Tests
// =============================================================================

func TestShardReader(t *testing.T) {
	t.Run("Read returns data within shard boundaries", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		// Write data to two shards
		creator := sf.OpenCreator()

		_, writer0 := creator.OpenShardWriter(0)
		shard0Data := []byte("first-shard-content")
		writer0.Write(shard0Data)
		writer0.Close()

		_, writer1 := creator.OpenShardWriter(0)
		shard1Data := []byte("second-shard-content")
		writer1.Write(shard1Data)
		writer1.Close()

		creator.Close()

		// Read back
		accessor := sf.OpenAccessor()
		defer accessor.Close()

		// Read shard 0 - should get exactly shard 0 data
		reader0 := accessor.GetShardReader(0, 0)
		buf0 := make([]byte, 100)
		n0, err := reader0.Read(buf0)
		if err != nil {
			t.Fatalf("shard 0: unexpected error: %v", err)
		}
		if !bytes.Equal(buf0[:n0], shard0Data) {
			t.Fatalf("shard 0: expected %q, got %q", shard0Data, buf0[:n0])
		}

		// Further reads should return EOF
		_, err = reader0.Read(buf0)
		if err != io.EOF {
			t.Fatalf("shard 0: expected EOF, got %v", err)
		}

		// Read shard 1
		reader1 := accessor.GetShardReader(1, 0)
		buf1 := make([]byte, 100)
		n1, err := reader1.Read(buf1)
		if err != nil {
			t.Fatalf("shard 1: unexpected error: %v", err)
		}
		if !bytes.Equal(buf1[:n1], shard1Data) {
			t.Fatalf("shard 1: expected %q, got %q", shard1Data, buf1[:n1])
		}
	})

	t.Run("Read with small buffer reads in chunks", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)
		data := []byte("0123456789ABCDEF")
		writer.Write(data)
		writer.Close()
		creator.Close()

		accessor := sf.OpenAccessor()
		defer accessor.Close()

		reader := accessor.GetShardReader(0, 0)
		var result []byte
		buf := make([]byte, 4) // small buffer

		for {
			n, err := reader.Read(buf)
			if n > 0 {
				result = append(result, buf[:n]...)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		if !bytes.Equal(result, data) {
			t.Fatalf("expected %q, got %q", data, result)
		}
	})

	t.Run("Read empty shard returns EOF immediately", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)
		// Write nothing
		writer.Close()
		creator.Close()

		accessor := sf.OpenAccessor()
		defer accessor.Close()

		reader := accessor.GetShardReader(0, 0)
		buf := make([]byte, 10)
		_, err := reader.Read(buf)
		if err != io.EOF {
			t.Fatalf("expected EOF for empty shard, got %v", err)
		}
	})
}

// =============================================================================
// ShardWriter Tests
// =============================================================================

func TestShardWriter(t *testing.T) {
	t.Run("Write appends data to file", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)

		// Write in multiple chunks
		writer.Write([]byte("hello"))
		writer.Write([]byte(" "))
		writer.Write([]byte("world"))
		writer.Close()
		creator.Close()

		// Verify
		accessor := sf.OpenAccessor()
		defer accessor.Close()

		reader := accessor.GetShardReader(0, 0)
		buf := make([]byte, 100)
		n, _ := io.ReadFull(reader, buf)
		expected := []byte("hello world")
		if !bytes.Equal(buf[:n], expected) {
			t.Fatalf("expected %q, got %q", expected, buf[:n])
		}
	})

	t.Run("Close marks shard ending", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		creator := sf.OpenCreator()

		// Write to shard 0
		_, writer0 := creator.OpenShardWriter(0)
		writer0.Write([]byte("12345"))
		writer0.Close()

		// Write to shard 1
		_, writer1 := creator.OpenShardWriter(0)
		writer1.Write([]byte("67890"))
		writer1.Close()

		creator.Close()

		// Verify each shard has correct data
		accessor := sf.OpenAccessor()
		defer accessor.Close()

		reader0 := accessor.GetShardReader(0, 0)
		buf0 := make([]byte, 10)
		n0, _ := reader0.Read(buf0)
		if string(buf0[:n0]) != "12345" {
			t.Fatalf("shard 0: expected '12345', got %q", buf0[:n0])
		}

		reader1 := accessor.GetShardReader(1, 0)
		buf1 := make([]byte, 10)
		n1, _ := reader1.Read(buf1)
		if string(buf1[:n1]) != "67890" {
			t.Fatalf("shard 1: expected '67890', got %q", buf1[:n1])
		}
	})

	t.Run("Write returns correct byte count", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)

		data := []byte("test data 123")
		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != len(data) {
			t.Fatalf("expected %d bytes written, got %d", len(data), n)
		}

		writer.Close()
		creator.Close()
	})
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestShardedFileIntegration(t *testing.T) {
	t.Run("Write and read multiple shards with varying sizes", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		// Create shards with different sizes
		shardData := [][]byte{
			[]byte("small"),
			bytes.Repeat([]byte("medium"), 100),
			bytes.Repeat([]byte("large-data-"), 1000),
			[]byte(""), // empty shard
			[]byte("final"),
		}

		// Write all shards
		creator := sf.OpenCreator()
		for _, data := range shardData {
			_, writer := creator.OpenShardWriter(0)
			if len(data) > 0 {
				writer.Write(data)
			}
			writer.Close()
		}
		creator.Close()

		// Read back and verify
		accessor := sf.OpenAccessor()
		defer accessor.Close()

		for i, expected := range shardData {
			reader := accessor.GetShardReader(int32(i), 0)
			var result []byte
			buf := make([]byte, 256)
			for {
				n, err := reader.Read(buf)
				if n > 0 {
					result = append(result, buf[:n]...)
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("shard %d: unexpected error: %v", i, err)
				}
			}
			if !bytes.Equal(result, expected) {
				t.Fatalf("shard %d: data mismatch (len expected=%d, got=%d)", i, len(expected), len(result))
			}
		}
	})

	t.Run("Multiple accessors can read same file", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		// Write data
		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)
		expected := []byte("shared data")
		writer.Write(expected)
		writer.Close()
		creator.Close()

		// Open two accessors
		accessor1 := sf.OpenAccessor()
		defer accessor1.Close()
		accessor2 := sf.OpenAccessor()
		defer accessor2.Close()

		// Read from both
		reader1 := accessor1.GetShardReader(0, 0)
		buf1 := make([]byte, 100)
		n1, _ := reader1.Read(buf1)

		reader2 := accessor2.GetShardReader(0, 0)
		buf2 := make([]byte, 100)
		n2, _ := reader2.Read(buf2)

		if !bytes.Equal(buf1[:n1], expected) {
			t.Fatalf("accessor1: expected %q, got %q", expected, buf1[:n1])
		}
		if !bytes.Equal(buf2[:n2], expected) {
			t.Fatalf("accessor2: expected %q, got %q", expected, buf2[:n2])
		}
	})

	t.Run("Shard readers are independent", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		// Write data
		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)
		writer.Write([]byte("0123456789"))
		writer.Close()
		creator.Close()

		accessor := sf.OpenAccessor()
		defer accessor.Close()

		// Get two readers for the same shard
		reader1 := accessor.GetShardReader(0, 0)
		reader2 := accessor.GetShardReader(0, 0)

		// Read partially from reader1
		buf1 := make([]byte, 5)
		reader1.Read(buf1)

		// Reader2 should still start from beginning
		buf2 := make([]byte, 5)
		reader2.Read(buf2)

		if string(buf1) != "01234" {
			t.Fatalf("reader1: expected '01234', got %q", buf1)
		}
		if string(buf2) != "01234" {
			t.Fatalf("reader2: expected '01234', got %q", buf2)
		}
	})

	t.Run("Large shard data", func(t *testing.T) {
		tmpDir := t.TempDir()
		sf := shardedfile.NewShardedFile(tmpDir, 0)

		// Create a 1MB shard
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		creator := sf.OpenCreator()
		_, writer := creator.OpenShardWriter(0)
		writer.Write(largeData)
		writer.Close()
		creator.Close()

		// Read back
		accessor := sf.OpenAccessor()
		defer accessor.Close()

		reader := accessor.GetShardReader(0, 0)
		result := make([]byte, len(largeData))
		_, err := io.ReadFull(reader, result)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !bytes.Equal(result, largeData) {
			t.Fatal("large data mismatch")
		}
	})
}
