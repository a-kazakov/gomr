package fileio

import "io"

type derivedReadCloser struct {
	original io.ReadCloser
	derived  io.ReadCloser
}

// WrapReadCloser creates a layered ReadCloser. On Close, it closes the derived
// reader first, then the original.
func WrapReadCloser(r io.ReadCloser, derive func(io.ReadCloser) io.ReadCloser) io.ReadCloser {
	return &derivedReadCloser{original: r, derived: derive(r)}
}

func (d *derivedReadCloser) Read(p []byte) (int, error) {
	return d.derived.Read(p)
}

func (d *derivedReadCloser) Close() error {
	if err := d.derived.Close(); err != nil {
		return err
	}
	return d.original.Close()
}

type derivedWriteCloser struct {
	original io.WriteCloser
	derived  io.WriteCloser
}

// WrapWriteCloser creates a layered WriteCloser. On Close, it closes the derived
// writer first, then the original.
func WrapWriteCloser(w io.WriteCloser, derive func(io.WriteCloser) io.WriteCloser) io.WriteCloser {
	return &derivedWriteCloser{original: w, derived: derive(w)}
}

func (d *derivedWriteCloser) Write(p []byte) (int, error) {
	return d.derived.Write(p)
}

func (d *derivedWriteCloser) Close() error {
	if err := d.derived.Close(); err != nil {
		return err
	}
	return d.original.Close()
}

type fakeWriteCloser struct {
	writer io.Writer
}

// NewFakeWriteCloser wraps an io.Writer as an io.WriteCloser with a no-op Close.
func NewFakeWriteCloser(w io.Writer) io.WriteCloser {
	return &fakeWriteCloser{writer: w}
}

func (w *fakeWriteCloser) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *fakeWriteCloser) Close() error {
	return nil
}
