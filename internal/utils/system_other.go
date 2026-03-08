//go:build !linux && !darwin

package utils

import (
	"os"
)

// DisableOSReadAhead is a no-op on Windows and other unsupported systems.
func DisableOSReadAhead(f *os.File) {
}
