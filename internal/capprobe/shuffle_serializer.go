package capprobe

import (
	"github.com/a-kazakov/gomr/internal/core"
)

func GetShuffleSerializerSetup(serializer any) core.ShuffleSerializerSetup {
	result, ok := serializer.(core.ShuffleSerializerSetup)
	if ok {
		return result
	}
	verifyNoTypos[core.ShuffleSerializerSetup](serializer)
	return nil
}

func GetShuffleSerializerSetupWithSideValue[TSideValue any](serializer any) core.ShuffleSerializerSetupWithSideValue[TSideValue] {
	result, ok := serializer.(core.ShuffleSerializerSetupWithSideValue[TSideValue])
	if ok {
		return result
	}
	verifyNoTypos[core.ShuffleSerializerSetupWithSideValue[TSideValue]](serializer)
	return nil
}

//go:generate go run ../../internal/operators/codegen/shuffle/shuffle.go capprobe
