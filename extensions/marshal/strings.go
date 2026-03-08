package marshal

import "encoding/binary"

func MarshalString(data string, dest []byte) int {
	length := uint32(len(data))
	binary.NativeEndian.PutUint32(dest, length)
	copy(dest[4:], data)
	return 4 + len(data)
}

func UnmarshalString(data []byte, dest *string) int {
	length := int(binary.NativeEndian.Uint32(data))
	if len(data) < 4+length {
		panicNotEnoughData(data, 4+length)
	}
	*dest = string(data[4 : 4+length])
	return 4 + length
}
