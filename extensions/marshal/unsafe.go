package marshal

import "unsafe"

func MarshalStructUnsafe[T any](data *T, dest []byte) int {
	size := int(unsafe.Sizeof(*data))
	src := unsafe.Slice((*byte)(unsafe.Pointer(data)), size)
	return copy(dest[:size], src)
}

func UnmarshalStructUnsafe[T any](data []byte, dest *T) int {
	size := int(unsafe.Sizeof(*dest))
	destBytes := unsafe.Slice((*byte)(unsafe.Pointer(dest)), size)
	return copy(destBytes, data[:size])
}
