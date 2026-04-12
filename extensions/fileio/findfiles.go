package fileio

// Glob returns paths matching the given pattern.
func Glob(pattern string, opts ...GlobOption) ([]string, error) {
	cfg := &GlobConfig{Backend: NewBackendRouter()}
	for _, o := range opts {
		o.ApplyGlobConfig(cfg)
	}
	return cfg.Backend.Glob(pattern)
}
