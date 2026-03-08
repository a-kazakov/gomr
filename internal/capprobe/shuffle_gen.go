package capprobe

import "github.com/a-kazakov/gomr/internal/core"

func GetReducerTo1Teardown[TOut0 any](reducer any) core.ReducerTo1Teardown[TOut0] {
	result, ok := reducer.(core.ReducerTo1Teardown[TOut0])
	if ok {
		return result
	}
	verifyNoTypos[core.ReducerTo1Teardown[TOut0]](reducer)
	return nil
}

func GetReducerTo2Teardown[TOut0 any, TOut1 any](reducer any) core.ReducerTo2Teardown[TOut0, TOut1] {
	result, ok := reducer.(core.ReducerTo2Teardown[TOut0, TOut1])
	if ok {
		return result
	}
	verifyNoTypos[core.ReducerTo2Teardown[TOut0, TOut1]](reducer)
	return nil
}

func GetReducerTo3Teardown[TOut0 any, TOut1 any, TOut2 any](reducer any) core.ReducerTo3Teardown[TOut0, TOut1, TOut2] {
	result, ok := reducer.(core.ReducerTo3Teardown[TOut0, TOut1, TOut2])
	if ok {
		return result
	}
	verifyNoTypos[core.ReducerTo3Teardown[TOut0, TOut1, TOut2]](reducer)
	return nil
}

func GetReducerTo4Teardown[TOut0 any, TOut1 any, TOut2 any, TOut3 any](reducer any) core.ReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3] {
	result, ok := reducer.(core.ReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3])
	if ok {
		return result
	}
	verifyNoTypos[core.ReducerTo4Teardown[TOut0, TOut1, TOut2, TOut3]](reducer)
	return nil
}

func GetReducerTo5Teardown[TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](reducer any) core.ReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4] {
	result, ok := reducer.(core.ReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4])
	if ok {
		return result
	}
	verifyNoTypos[core.ReducerTo5Teardown[TOut0, TOut1, TOut2, TOut3, TOut4]](reducer)
	return nil
}
