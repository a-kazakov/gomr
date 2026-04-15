package kv

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/a-kazakov/gomr/internal/constants"
)

type Writer struct {
	writer    io.Writer
	lastKey   []byte
	lengthBuf [4]byte // reusable buffer for writing uint16/uint32 lengths without heap allocation
}

func NewWriter(writer io.Writer) *Writer {
	return &Writer{
		writer:  writer,
		lastKey: make([]byte, constants.MAX_SHUFFLE_KEY_SIZE),
	}
}

func (w *Writer) Write(key []byte, value []byte) error {
	if bytes.Equal(w.lastKey, key) {
		binary.NativeEndian.PutUint16(w.lengthBuf[:2], 0)
		_, err := w.writer.Write(w.lengthBuf[:2])
		if err != nil {
			return err
		}
	} else {
		binary.NativeEndian.PutUint16(w.lengthBuf[:2], uint16(len(key)))
		_, err := w.writer.Write(w.lengthBuf[:2])
		if err != nil {
			return err
		}
		_, err = w.writer.Write(key)
		if err != nil {
			return err
		}
		w.lastKey = w.lastKey[:len(key)]
		copy(w.lastKey, key)
	}
	binary.NativeEndian.PutUint32(w.lengthBuf[:4], uint32(len(value)))
	_, err := w.writer.Write(w.lengthBuf[:4])
	if err != nil {
		return err
	}
	_, err = w.writer.Write(value)
	return err
}
