package marshal

import (
	"encoding/binary"
	"math"
)

func MarshalBool(data bool, dest []byte) int {
	if data {
		dest[0] = 1
	} else {
		dest[0] = 0
	}
	return 1
}

func UnmarshalBool(data []byte, dest *bool) int {
	*dest = data[0] == 1
	return 1
}

func MarshalIntAs64(data int, dest []byte) int {
	binary.NativeEndian.PutUint64(dest, uint64(data))
	return 8
}

func UnmarshalIntAs64(data []byte, dest *int) int {
	*dest = int(binary.NativeEndian.Uint64(data))
	return 8
}

func MarshalIntAs32(data int, dest []byte) int {
	binary.NativeEndian.PutUint32(dest, uint32(data))
	return 4
}

func UnmarshalIntAs32(data []byte, dest *int) int {
	*dest = int(binary.NativeEndian.Uint32(data))
	return 4
}

func MarshalIntAs16(data int, dest []byte) int {
	binary.NativeEndian.PutUint16(dest, uint16(data))
	return 2
}

func UnmarshalIntAs16(data []byte, dest *int) int {
	*dest = int(binary.NativeEndian.Uint16(data))
	return 2
}

func MarshalIntAs8(data int, dest []byte) int {
	dest[0] = byte(data)
	return 1
}

func UnmarshalIntAs8(data []byte, dest *int) int {
	*dest = int(data[0])
	return 1
}

func MarshalInt64(data int64, dest []byte) int {
	binary.NativeEndian.PutUint64(dest, uint64(data))
	return 8
}

func UnmarshalInt64(data []byte, dest *int64) int {
	*dest = int64(binary.NativeEndian.Uint64(data))
	return 8
}

func MarshalInt32(data int32, dest []byte) int {
	binary.NativeEndian.PutUint32(dest, uint32(data))
	return 4
}

func UnmarshalInt32(data []byte, dest *int32) int {
	*dest = int32(binary.NativeEndian.Uint32(data))
	return 4
}

func MarshalInt16(data int16, dest []byte) int {
	binary.NativeEndian.PutUint16(dest, uint16(data))
	return 2
}

func UnmarshalInt16(data []byte, dest *int16) int {
	*dest = int16(binary.NativeEndian.Uint16(data))
	return 2
}

func MarshalByte(data byte, dest []byte) int {
	dest[0] = data
	return 1
}

func UnmarshalByte(data []byte, dest *byte) int {
	*dest = data[0]
	return 1
}

func MarshalUint64(data uint64, dest []byte) int {
	binary.NativeEndian.PutUint64(dest, data)
	return 8
}

func UnmarshalUint64(data []byte, dest *uint64) int {
	*dest = binary.NativeEndian.Uint64(data)
	return 8
}

func MarshalUint32(data uint32, dest []byte) int {
	binary.NativeEndian.PutUint32(dest, data)
	return 4
}

func UnmarshalUint32(data []byte, dest *uint32) int {
	*dest = binary.NativeEndian.Uint32(data)
	return 4
}

func MarshalUint16(data uint16, dest []byte) int {
	binary.NativeEndian.PutUint16(dest, data)
	return 2
}

func UnmarshalUint16(data []byte, dest *uint16) int {
	*dest = binary.NativeEndian.Uint16(data)
	return 2
}

func MarshalUint8(data uint8, dest []byte) int {
	dest[0] = data
	return 1
}

func UnmarshalUint8(data []byte, dest *uint8) int {
	*dest = data[0]
	return 1
}

func MarshalFloat64(data float64, dest []byte) int {
	binary.NativeEndian.PutUint64(dest, math.Float64bits(data))
	return 8
}

func UnmarshalFloat64(data []byte, dest *float64) int {
	*dest = math.Float64frombits(binary.NativeEndian.Uint64(data))
	return 8
}

func MarshalFloat32(data float32, dest []byte) int {
	binary.NativeEndian.PutUint32(dest, math.Float32bits(data))
	return 4
}

func UnmarshalFloat32(data []byte, dest *float32) int {
	*dest = math.Float32frombits(binary.NativeEndian.Uint32(data))
	return 4
}
