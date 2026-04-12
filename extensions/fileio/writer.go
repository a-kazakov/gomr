package fileio

import (
	"bufio"
	"io"
)

type closableBufferedWriter struct {
	writer *bufio.Writer
}

func (w *closableBufferedWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *closableBufferedWriter) Close() error {
	return w.writer.Flush()
}

// Create creates a file for writing, applying the given options.
func Create(path string, opts ...WriteOption) (io.WriteCloser, error) {
	cfg := &WriteConfig{Backend: NewBackendRouter()}
	for _, o := range opts {
		o.ApplyWriteConfig(cfg)
	}
	w, err := cfg.Backend.Create(path)
	if err != nil {
		return nil, err
	}
	if cfg.BufferSize > 0 {
		w = WrapWriteCloser(w, func(w io.WriteCloser) io.WriteCloser {
			return &closableBufferedWriter{writer: bufio.NewWriterSize(w, cfg.BufferSize)}
		})
	}
	if cfg.Compressor != nil {
		w = cfg.Compressor.WrapWriter(w)
	}
	return w, nil
}
