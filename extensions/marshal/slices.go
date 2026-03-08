package marshal

import "encoding/binary"

func MarshalBytes(value []byte, dest []byte) int {
	binary.NativeEndian.PutUint32(dest, uint32(len(value)))
	copiedBytes := copy(dest[4:], value)
	return 4 + copiedBytes
}

func UnmarshalBytes(data []byte, dest []byte) int {
	destLen := int(binary.NativeEndian.Uint32(data))
	if len(data) < 4+destLen {
		panicNotEnoughData(data, 4+destLen)
	}
	copiedBytes := copy(dest, data[4:4+destLen])
	return 4 + copiedBytes
}

func UnmarshalBytesAlloc(data []byte) ([]byte, int) {
	destLen := int(binary.NativeEndian.Uint32(data))
	if len(data) < 4+destLen {
		panicNotEnoughData(data, 4+destLen)
	}
	result := make([]byte, destLen)
	copy(result, data[4:4+destLen])
	return result, 4 + destLen
}
