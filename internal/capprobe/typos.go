// Package capprobe provides capability probing functions that check whether
// a concrete type implements an optional interface. If it doesn't, it verifies
// there are no near-miss method names (typos) that might indicate a mistake.
package capprobe

import (
	"reflect"
	"strings"

	"github.com/a-kazakov/gomr/internal/must"
)

// verifyNoTypos panics if actual has a method whose normalized name matches
// any method in TargetInterface, indicating a likely typo or signature mismatch.
func verifyNoTypos[TargetInterface any](actual any) {
	actualType := reflect.TypeOf(actual)
	targetType := reflect.TypeOf((*TargetInterface)(nil)).Elem()

	// Build map of normalized method names -> original names for actual type
	actualMethods := make(map[string]string)
	for i := 0; i < actualType.NumMethod(); i++ {
		method := actualType.Method(i)
		normalized := normalizeMethodName(method.Name)
		actualMethods[normalized] = method.Name
	}

	// Check each method in target interface for normalized matches
	for i := 0; i < targetType.NumMethod(); i++ {
		targetMethod := targetType.Method(i)
		normalized := normalizeMethodName(targetMethod.Name)

		actualMethodName := actualMethods[normalized]
		must.BeTrue(actualMethodName == "", "type %s has method %q which normalizes to %q, same as interface %s method %q - possible typo or signature mismatch",
			actualType.String(),
			actualMethodName,
			normalized,
			targetType.String(),
			targetMethod.Name,
		)
	}
}

func normalizeMethodName(name string) string {
	var result strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			result.WriteRune(r)
		}
	}
	return result.String()
}
