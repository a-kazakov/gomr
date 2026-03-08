package utils

import "github.com/a-kazakov/gomr/parameters"

// FilterNilsInPlace compacts non-nil pointers to the front of the slice in-place.
// Returns the count of non-nil elements. Caller should sub-slice to [:returnValue].
func FilterNilsInPlace[T any](s []*T) int {
	nextPtr := 0
	for i := range s {
		if s[i] != nil {
			s[nextPtr] = s[i]
			nextPtr++
		}
	}
	return nextPtr
}

func GetOrDefault[T any](s []T, index int, defaultValue T) T {
	if index < 0 || index >= len(s) {
		return defaultValue
	}
	return s[index]
}

func OptGetOrDefault[T any](opt parameters.Optional[[]T], index int, defaultValue T) T {
	if !opt.IsSet() {
		return defaultValue
	}
	s := opt.Get()
	if index < 0 || index >= len(s) {
		return defaultValue
	}
	return s[index]
}
