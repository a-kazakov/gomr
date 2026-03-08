package marshal

import "fmt"

//go:noinline
func panicNotEnoughData(data []byte, expectedSize int) {
	panic(fmt.Sprintf("data length is not enough to unmarshal: %d < %d", len(data), expectedSize))
}
