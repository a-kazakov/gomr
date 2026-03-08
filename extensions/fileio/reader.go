package fileio

import "io"

// Open opens a file for reading, applying the given options.
func Open(path string, opts ...ReadOption) (io.ReadCloser, error) {
	cfg := &ReadConfig{Backend: LocalBackend}
	for _, o := range opts {
		o.ApplyReadConfig(cfg)
	}
	r, err := cfg.Backend.Open(path)
	if err != nil {
		return nil, err
	}
	if cfg.Decompressor != nil {
		r = cfg.Decompressor.WrapReader(r)
	}
	return r, nil
}
