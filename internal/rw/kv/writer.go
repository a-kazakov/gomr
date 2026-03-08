package kv

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/a-kazakov/gomr/internal/constants"
)

type Writer struct {
	writer  io.Writer
	lastKey []byte
}

func NewWriter(writer io.Writer) *Writer {
	return &Writer{
		writer:  writer,
		lastKey: make([]byte, constants.MAX_SHUFFLE_KEY_SIZE),
	}
}

func (w *Writer) Write(key []byte, value []byte) error {
	if bytes.Equal(w.lastKey, key) {
		err := binary.Write(w.writer, binary.NativeEndian, uint16(0))
		if err != nil {
			return err
		}
	} else {
		err := binary.Write(w.writer, binary.NativeEndian, uint16(len(key)))
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
	err := binary.Write(w.writer, binary.NativeEndian, uint32(len(value)))
	if err != nil {
		return err
	}
	_, err = w.writer.Write(value)
	if err != nil {
		return err
	}
	return nil
}
