package kv

import (
	"bytes"
	"container/heap"
	"io"

	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/must"
)

type MultiReader struct {
	readers           []*Reader
	currentReader     *Reader
	currentKey        []byte
	currentKeyReader  KeyReader
	currentKeyVersion int64
	peekKeyBuffer     []byte
}

func (r *MultiReader) Len() int {
	return len(r.readers)
}

func (r *MultiReader) Less(i, j int) bool {
	return bytes.Compare(r.readers[i].PeekKey(), r.readers[j].PeekKey()) < 0
}

func (r *MultiReader) Swap(i, j int) {
	r.readers[i], r.readers[j] = r.readers[j], r.readers[i]
}

func (r *MultiReader) Push(x any) {
	r.readers = append(r.readers, x.(*Reader))
}

func (r *MultiReader) Pop() any {
	old := r.readers
	n := len(old)
	x := old[n-1]
	r.readers = old[0 : n-1]
	return x
}

func NewMultiReader(readers []io.Reader) *MultiReader {
	fileReaders := make([]*Reader, 0, len(readers))
	for _, reader := range readers {
		fileReader := NewReader(reader)
		if fileReader.PeekKey() != nil {
			fileReaders = append(fileReaders, fileReader)
		}
	}
	result := &MultiReader{
		readers:       fileReaders,
		currentKey:    make([]byte, 0, constants.MAX_SHUFFLE_KEY_SIZE),
		peekKeyBuffer: make([]byte, 0, constants.MAX_SHUFFLE_KEY_SIZE),
	}
	heap.Init(result)
	result.loadKeyReader()
	return result
}

func (r *MultiReader) PeekKey() []byte {
	if r.currentReader == nil {
		return nil
	}
	r.peekKeyBuffer = r.peekKeyBuffer[:len(r.currentKey)]
	copy(r.peekKeyBuffer, r.currentKey)
	return r.peekKeyBuffer
}

func (r *MultiReader) loadKeyReader() {
	if r.currentReader != nil {
		heap.Push(r, r.currentReader)
		r.currentReader = nil
	}
	if len(r.readers) == 0 {
		return
	}
	nextReader := heap.Pop(r).(*Reader)
	nextKey := nextReader.PeekKey()
	if nextKey == nil {
		r.loadKeyReader()
		return
	}
	if !bytes.Equal(nextKey, r.currentKey) {
		r.currentKeyVersion += 1
		r.currentKey = r.currentKey[:len(nextKey)]
		copy(r.currentKey, nextKey)
	}
	r.currentReader = nextReader
	r.currentKeyReader = r.currentReader.GetKeyReader()
}

func (r *MultiReader) GetKeyReader() KeyReader {
	return KeyReader{
		keyVersion: r.currentKeyVersion,
		reader:     r,
	}
}

func (r *MultiReader) readValue(expectedKeyVersion int64, p []byte) (int, error) {
	if r.currentKeyVersion != expectedKeyVersion {
		return 0, io.EOF
	}
	if r.currentReader == nil {
		return 0, io.EOF
	}
	n, err := r.currentKeyReader.ReadValue(p)
	if err != nil {
		if err != io.EOF {
			must.OK(err).Else("error reading value")
		}
		r.loadKeyReader()
		return r.readValue(expectedKeyVersion, p)
	}
	return n, nil
}
