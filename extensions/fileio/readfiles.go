package fileio

import "github.com/a-kazakov/gomr"

// ReadFiles globs for files matching pattern, reads each file line by line,
// and converts each line via convert. Returns a Collection of converted values.
//
// Internally creates two gomr operators:
//  1. Seed: globs the pattern to produce file paths
//  2. Map: reads each file (with optional decompression), converts lines
func ReadFiles[T any](
	p gomr.Pipeline,
	pattern string,
	convert func([]byte) T,
	opts ...ReadFilesOption,
) gomr.Collection[T] {
	cfg := defaultReadFilesConfig()
	for _, o := range opts {
		o.ApplyReadFilesConfig(cfg)
	}

	filePaths := listFilesFromConfig(p, pattern, cfg)

	mapOpts := []gomr.MapOption{
		gomr.WithOperationName(cfg.OperationName),
		gomr.WithOutCollectionNames(cfg.CollectionName),
	}
	if cfg.Parallelism > 0 {
		mapOpts = append(mapOpts, gomr.WithParallelism(cfg.Parallelism))
	}
	if cfg.BatchSize > 0 {
		mapOpts = append(mapOpts, gomr.WithOutBatchSize(cfg.BatchSize))
	}
	if cfg.ChannelCapacity > 0 {
		mapOpts = append(mapOpts, gomr.WithOutChannelCapacity(cfg.ChannelCapacity))
	}

	maxLineLength := cfg.MaxLineLength
	backend := cfg.Backend
	decompressor := cfg.Decompressor

	return gomr.Map(filePaths, func(
		ctx gomr.OperatorContext,
		receiver gomr.CollectionReceiver[string],
		emitter gomr.Emitter[T],
	) {
		for path := range receiver.IterValues() {
			r, err := backend.Open(*path)
			if err != nil {
				panic(err)
			}
			if decompressor != nil {
				r = decompressor.WrapReader(r)
			}
			for value := range ReadLines(r, maxLineLength, convert) {
				*emitter.GetEmitPointer() = value
			}
			r.Close()
		}
	}, mapOpts...)
}
