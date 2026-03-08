package shuf

import (
	"bytes"
	"sort"

	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/must"
	"github.com/a-kazakov/gomr/internal/pagedbuffer"
)

type kvWriter interface {
	Write(key []byte, value []byte) error
}

type SortedBuffer struct {
	data       pagedbuffer.PagedBuffer
	refs       pagedbuffer.Uint64View
	tempBuffer []byte
}

func NewSortedBuffer(orchestrator *pagedbuffer.PagedBufferOrchestrator) SortedBuffer {
	return SortedBuffer{
		data: pagedbuffer.NewPagedBuffer(orchestrator),
		refs: pagedbuffer.NewUint64View(orchestrator),
	}
}

func (b *SortedBuffer) PushBack(key []byte, value []byte) {
	offset := b.data.Size
	keyLength := len(key)
	valueLength := len(value)
	combinedValue := uint64(offset)
	combinedValue = combinedValue<<constants.MAX_SHUFFLE_KEY_SIZE_POW2 | uint64(keyLength)
	combinedValue = combinedValue<<constants.MAX_SHUFFLE_VALUE_SIZE_POW2 | uint64(valueLength)
	_, err := b.data.Write(key)
	must.OK(err).Else("failed to write key")
	_, err = b.data.Write(value)
	must.OK(err).Else("failed to write value")
	b.refs.PushBack(combinedValue)
}

func (b *SortedBuffer) GetMetadata(index int) (int, int, int) {
	combinedValue := b.refs.Get(index)
	valueLength := combinedValue & constants.MAX_SHUFFLE_VALUE_SIZE_MASK
	combinedValue = combinedValue >> constants.MAX_SHUFFLE_VALUE_SIZE_POW2
	keyLength := combinedValue & constants.MAX_SHUFFLE_KEY_SIZE_MASK
	combinedValue = combinedValue >> constants.MAX_SHUFFLE_KEY_SIZE_POW2
	offset := combinedValue
	return int(offset), int(keyLength), int(valueLength)
}

func (b *SortedBuffer) Len() int {
	return b.refs.Len()
}

func (b *SortedBuffer) Sort(tempBuffer []byte) {
	b.tempBuffer = tempBuffer
	sort.Sort(b)
	b.tempBuffer = nil
}

func (b *SortedBuffer) Less(i int, j int) bool {
	offsetI, keyLengthI, _ := b.GetMetadata(i)
	offsetJ, keyLengthJ, _ := b.GetMetadata(j)
	keyOffsetI := offsetI
	keyOffsetJ := offsetJ
	keySliceI := b.tempBuffer[:keyLengthI]
	keySliceJ := b.tempBuffer[keyLengthI : keyLengthI+keyLengthJ]
	b.data.ReadAt(keySliceI, keyOffsetI)
	b.data.ReadAt(keySliceJ, keyOffsetJ)
	return bytes.Compare(keySliceI, keySliceJ) < 0
}

func (b *SortedBuffer) Swap(i int, j int) {
	b.refs.Swap(i, j)
}

func (b *SortedBuffer) IsFull() bool {
	return !b.data.CanTake(constants.MAX_SHUFFLE_KEY_SIZE+constants.MAX_SHUFFLE_VALUE_SIZE) || b.refs.IsFull()
}

func (b *SortedBuffer) FlushTo(writer kvWriter, tempBuffer []byte) error {
	b.tempBuffer = tempBuffer
	defer func() { b.tempBuffer = nil }()
	for i := 0; i < b.refs.Len(); i++ {
		offset, keyLength, valueLength := b.GetMetadata(i)
		totalLength := keyLength + valueLength
		transferBuffer := b.tempBuffer[:totalLength]
		_, err := b.data.ReadAt(transferBuffer, offset)
		if err != nil {
			return err
		}
		err = writer.Write(transferBuffer[:keyLength], transferBuffer[keyLength:])
		if err != nil {
			return err
		}
	}
	b.data.Clear()
	b.refs.Clear()
	return nil
}
