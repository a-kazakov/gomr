//go:build darwin

package utils

import (
	"os"

	"golang.org/x/sys/unix"
)

// DisableOSReadAhead disables kernel read-ahead on the given file descriptor.
// On macOS, uses fcntl(F_RDAHEAD, 0). Used by shuffle readers to optimize random I/O.
func DisableOSReadAhead(f *os.File) {
	_, _ = unix.FcntlInt(f.Fd(), unix.F_RDAHEAD, 0)
}
