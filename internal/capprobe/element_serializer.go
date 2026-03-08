package capprobe

import "github.com/a-kazakov/gomr/internal/core"

func GetElementSerializerSetup(serializer any) core.ElementSerializerSetup {
	result, ok := serializer.(core.ElementSerializerSetup)
	if ok {
		return result
	}
	verifyNoTypos[core.ElementSerializerSetup](serializer)
	return nil
}
