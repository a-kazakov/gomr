package fileio

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
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
	matches, err := doublestar.FilepathGlob(strings.TrimPrefix(pattern, "file://"))
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

// ---------------------------------------------------------------------------
// BackendRouter — dispatches to scheme-specific backends
// ---------------------------------------------------------------------------

// BackendRouter implements Backend by inspecting the URL scheme of each path
// and dispatching to the appropriate registered backend. Paths with no scheme
// or the "file://" scheme are handled by the local backend.
type BackendRouter struct {
	local    Backend
	backends map[string]Backend
}

// NewBackendRouter returns a BackendRouter that handles local/file:// paths
// out of the box. Use Register to add scheme-specific backends (e.g. "s3").
func NewBackendRouter() *BackendRouter {
	return &BackendRouter{
		local:    LocalBackend,
		backends: make(map[string]Backend),
	}
}

// AsBackendRouter returns r's Backend as a *BackendRouter. If it is already a
// *BackendRouter it is returned directly; otherwise a new router is created
// with the existing backend as the local fallback.
func AsBackendRouter(b Backend) *BackendRouter {
	if r, ok := b.(*BackendRouter); ok {
		return r
	}
	return &BackendRouter{
		local:    b,
		backends: make(map[string]Backend),
	}
}

// Register adds a backend for the given URL scheme (e.g. "s3").
func (r *BackendRouter) Register(scheme string, b Backend) {
	r.backends[scheme] = b
}

func (r *BackendRouter) resolve(path string) (Backend, error) {
	scheme := extractScheme(path)
	if scheme == "" || scheme == "file" {
		return r.local, nil
	}
	b, ok := r.backends[scheme]
	if !ok {
		return nil, fmt.Errorf("no backend registered for scheme %q in path %q", scheme, path)
	}
	return b, nil
}

func (r *BackendRouter) Open(path string) (io.ReadCloser, error) {
	b, err := r.resolve(path)
	if err != nil {
		return nil, err
	}
	return b.Open(path)
}

func (r *BackendRouter) Create(path string) (io.WriteCloser, error) {
	b, err := r.resolve(path)
	if err != nil {
		return nil, err
	}
	return b.Create(path)
}

func (r *BackendRouter) Glob(pattern string) ([]string, error) {
	b, err := r.resolve(pattern)
	if err != nil {
		return nil, err
	}
	return b.Glob(pattern)
}

// extractScheme returns the scheme portion of a "scheme://..." string,
// or "" if no scheme is present.
func extractScheme(path string) string {
	if idx := strings.Index(path, "://"); idx > 0 {
		return path[:idx]
	}
	return ""
}
