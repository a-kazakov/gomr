package kv

import (
	"encoding/binary"
	"io"

	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/must"
)

type Reader struct {
	ioReader        io.Reader
	nextKeyVersion  int64
	nextKey         []byte
	peekKeyBuffer   []byte
	keyLengthBuf    [2]byte // reusable buffer for reading key length without heap allocation
	valueLengthBuf  [4]byte // reusable buffer for reading value length without heap allocation
}

func NewReader(fileReader io.Reader) *Reader {
	result := &Reader{
		ioReader:      fileReader,
		nextKey:       make([]byte, 0, constants.MAX_SHUFFLE_KEY_SIZE),
		peekKeyBuffer: make([]byte, 0, constants.MAX_SHUFFLE_KEY_SIZE),
	}
	result.loadKey()
	return result
}

func (r *Reader) loadKey() {
	_, err := io.ReadFull(r.ioReader, r.keyLengthBuf[:])
	if err != nil {
		if err != io.EOF {
			must.OK(err).Else("error reading key length")
		}
		r.nextKey = nil
		r.nextKeyVersion += 1
		return
	}
	keyLength := int(binary.NativeEndian.Uint16(r.keyLengthBuf[:]))
	if keyLength == 0 {
		return
	}
	r.nextKeyVersion += 1
	r.nextKey = r.nextKey[:keyLength]
	_, err = io.ReadFull(r.ioReader, r.nextKey)
	if err != nil {
		must.OK(err).Else("error reading key")
	}
}

func (r *Reader) readValue(expectedKeyVersion int64, p []byte) (int, error) {
	if expectedKeyVersion != r.nextKeyVersion {
		return 0, io.EOF
	}
	ioReader := r.ioReader
	_, err := io.ReadFull(ioReader, r.valueLengthBuf[:])
	if err != nil {
		must.OK(err).Else("error reading value length")
	}
	valueLength := int(binary.NativeEndian.Uint32(r.valueLengthBuf[:]))
	_, err = io.ReadFull(ioReader, p[:valueLength])
	if err != nil {
		must.OK(err).Else("error reading value")
	}
	r.loadKey()
	return valueLength, nil
}

func (r *Reader) PeekKey() []byte {
	if r.nextKey == nil {
		return nil
	}
	r.peekKeyBuffer = r.peekKeyBuffer[:len(r.nextKey)]
	copy(r.peekKeyBuffer, r.nextKey)
	return r.peekKeyBuffer
}

func (r *Reader) GetKeyReader() KeyReader {
	return KeyReader{
		keyVersion: r.nextKeyVersion,
		reader:     r,
	}
}
