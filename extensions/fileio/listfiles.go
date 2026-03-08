package fileio

import "github.com/a-kazakov/gomr"

// ListFiles returns a Collection of file paths matching the glob pattern.
// Use this when you need custom per-file processing via a downstream Map.
func ListFiles(p gomr.Pipeline, pattern string, opts ...ReadFilesOption) gomr.Collection[string] {
	cfg := defaultReadFilesConfig()
	for _, o := range opts {
		o.ApplyReadFilesConfig(cfg)
	}
	return listFilesFromConfig(p, pattern, cfg)
}

func defaultReadFilesConfig() *ReadFilesConfig {
	return &ReadFilesConfig{
		Backend:       LocalBackend,
		MaxLineLength: 1 << 20,
		OperationName: "Read Files",
		CollectionName: "Lines",
	}
}

func listFilesFromConfig(p gomr.Pipeline, pattern string, cfg *ReadFilesConfig) gomr.Collection[string] {
	backend := cfg.Backend
	seedOpts := []gomr.SeedOption{
		gomr.WithOperationName(cfg.OperationName + " (list)"),
		gomr.WithOutCollectionName("File Paths"),
		gomr.WithOutBatchSize(1),
		gomr.WithOutChannelCapacity(1000),
	}
	return gomr.NewSeedCollection(p, func(ctx gomr.OperatorContext, emitter gomr.Emitter[string]) {
		files, err := backend.Glob(pattern)
		if err != nil {
			panic(err)
		}
		for _, f := range files {
			*emitter.GetEmitPointer() = f
		}
	}, seedOpts...)
}
