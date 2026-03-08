package capprobe

import "github.com/a-kazakov/gomr/internal/core"

func GetReducerSetup(reducer any) core.ShuffleReducerSetup {
	result, ok := reducer.(core.ShuffleReducerSetup)
	if ok {
		return result
	}
	verifyNoTypos[core.ShuffleReducerSetup](reducer)
	return nil
}

func GetReducerSetupWithSideValue[TSideValue any](reducer any) core.ShuffleReducerSetupWithSideValue[TSideValue] {
	result, ok := reducer.(core.ShuffleReducerSetupWithSideValue[TSideValue])
	if ok {
		return result
	}
	verifyNoTypos[core.ShuffleReducerSetupWithSideValue[TSideValue]](reducer)
	return nil
}
