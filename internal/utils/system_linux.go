//go:build linux

package utils

import (
	"os"

	"golang.org/x/sys/unix"
)

// disableOSReadAhead hints to the kernel that we will read randomly.
func DisableOSReadAhead(f *os.File) {
	// Ignore errors; this is just a hint.
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_RANDOM)
}
