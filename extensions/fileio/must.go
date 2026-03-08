package fileio

import "io"

// MustOpen is like Open but panics on error.
func MustOpen(path string, opts ...ReadOption) io.ReadCloser {
	r, err := Open(path, opts...)
	if err != nil {
		panic(err)
	}
	return r
}

// MustCreate is like Create but panics on error.
func MustCreate(path string, opts ...WriteOption) io.WriteCloser {
	w, err := Create(path, opts...)
	if err != nil {
		panic(err)
	}
	return w
}

// MustGlob is like Glob but panics on error.
func MustGlob(pattern string, opts ...GlobOption) []string {
	files, err := Glob(pattern, opts...)
	if err != nil {
		panic(err)
	}
	return files
}
