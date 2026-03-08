package tfrecord

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// Writer writes records in TFRecord format to an underlying io.Writer.
// TFRecord format consists of records with:
//   - Length of data (uint64, little endian)
//   - CRC32C checksum of length field
//   - Data bytes
//   - CRC32C checksum of data
type Writer struct {
	w io.Writer
}

// NewWriter creates a new TFRecord writer that writes to the given io.Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Write writes a single record to the TFRecord file.
func (w *Writer) Write(data []byte) (int, error) {
	var n int
	var err error

	lengthBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(lengthBytes, uint64(len(data)))
	if n, err = w.w.Write(lengthBytes); err != nil {
		return n, err
	}
	bytesWritten := n

	lengthCrc := crc32.Checksum(lengthBytes, crc32.MakeTable(crc32.Castagnoli))
	lengthCrcBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthCrcBytes, lengthCrc)
	if n, err = w.w.Write(lengthCrcBytes); err != nil {
		return bytesWritten + n, err
	}
	bytesWritten += n

	if n, err = w.w.Write(data); err != nil {
		return bytesWritten + n, err
	}
	bytesWritten += n

	dataCrc := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	dataCrcBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataCrcBytes, dataCrc)
	if n, err = w.w.Write(dataCrcBytes); err != nil {
		return bytesWritten + n, err
	}
	bytesWritten += n

	return bytesWritten, nil
}
