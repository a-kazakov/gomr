package fileio

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Backend is the interface for file system operations.
type Backend interface {
	Open(path string) (io.ReadCloser, error)
	Create(path string) (io.WriteCloser, error)
	Glob(pattern string) ([]string, error)
}

// LocalBackend is the default backend for local disk I/O.
var LocalBackend Backend = &localBackend{}

type localBackend struct{}

func (b *localBackend) Open(path string) (io.ReadCloser, error) {
	return os.Open(strings.TrimPrefix(path, "file://"))
}

func (b *localBackend) Create(path string) (io.WriteCloser, error) {
	return os.Create(strings.TrimPrefix(path, "file://"))
}

func (b *localBackend) Glob(pattern string) ([]string, error) {
	matches, err := filepath.Glob(strings.TrimPrefix(pattern, "file://"))
	if err != nil {
		return nil, err
	}
	var files []string
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil {
			continue
		}
		if !info.IsDir() {
			files = append(files, m)
		}
	}
	return files, nil
}
