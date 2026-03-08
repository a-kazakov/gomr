package tfrecord

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestWriter_SingleRecord(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	data := []byte("hello tfrecord")
	n, err := w.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	// Expected size: 8 (length) + 4 (length CRC) + len(data) + 4 (data CRC)
	expectedSize := 8 + 4 + len(data) + 4
	if n != expectedSize {
		t.Fatalf("Write returned %d bytes, want %d", n, expectedSize)
	}
	if buf.Len() != expectedSize {
		t.Fatalf("buffer has %d bytes, want %d", buf.Len(), expectedSize)
	}

	// Parse and verify the record format.
	raw := buf.Bytes()

	// 8-byte little-endian length.
	length := binary.LittleEndian.Uint64(raw[:8])
	if length != uint64(len(data)) {
		t.Fatalf("length field = %d, want %d", length, len(data))
	}

	// 4-byte CRC of the length field.
	lengthCRC := binary.LittleEndian.Uint32(raw[8:12])
	wantLenCRC := crc32.Checksum(raw[:8], crc32.MakeTable(crc32.Castagnoli))
	if lengthCRC != wantLenCRC {
		t.Fatalf("length CRC = %08x, want %08x", lengthCRC, wantLenCRC)
	}

	// Data bytes.
	gotData := raw[12 : 12+len(data)]
	if !bytes.Equal(gotData, data) {
		t.Fatalf("data = %q, want %q", gotData, data)
	}

	// 4-byte CRC of the data.
	dataCRC := binary.LittleEndian.Uint32(raw[12+len(data):])
	wantDataCRC := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if dataCRC != wantDataCRC {
		t.Fatalf("data CRC = %08x, want %08x", dataCRC, wantDataCRC)
	}
}

func TestWriter_MultipleRecords(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	records := [][]byte{
		[]byte("first"),
		[]byte("second record"),
		[]byte("third"),
	}
	totalExpected := 0
	for _, rec := range records {
		n, err := w.Write(rec)
		if err != nil {
			t.Fatal(err)
		}
		expected := 8 + 4 + len(rec) + 4
		if n != expected {
			t.Fatalf("Write(%q) = %d, want %d", rec, n, expected)
		}
		totalExpected += expected
	}
	if buf.Len() != totalExpected {
		t.Fatalf("total bytes = %d, want %d", buf.Len(), totalExpected)
	}
}

func TestWriter_EmptyRecord(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	n, err := w.Write([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	// 8 (length=0) + 4 (length CRC) + 0 (data) + 4 (data CRC) = 16
	if n != 16 {
		t.Fatalf("Write(empty) = %d bytes, want 16", n)
	}

	raw := buf.Bytes()
	length := binary.LittleEndian.Uint64(raw[:8])
	if length != 0 {
		t.Fatalf("length = %d, want 0", length)
	}
}
