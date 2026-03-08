package options

func ApplySeedOptions(opts ...SeedOption) *SeedOptions {
	o := &SeedOptions{}
	for _, opt := range opts {
		opt.applyToSeed(o)
	}
	return o
}

func ApplyForkOptions(opts ...ForkOption) *ForkOptions {
	o := &ForkOptions{}
	for _, opt := range opts {
		opt.applyToFork(o)
	}
	return o
}

func ApplyMergeOptions(opts ...MergeOption) *MergeOptions {
	o := &MergeOptions{}
	for _, opt := range opts {
		opt.applyToMerge(o)
	}
	return o
}

func ApplyMapOptions(opts ...MapOption) *MapOptions {
	o := &MapOptions{}
	for _, opt := range opts {
		opt.applyToMap(o)
	}
	return o
}

func ApplyShuffleOptions(opts ...ShuffleOption) *ShuffleOptions {
	o := &ShuffleOptions{}
	for _, opt := range opts {
		opt.applyToShuffle(o)
	}
	return o
}

func ApplyCollectOptions(opts ...CollectOption) *CollectOptions {
	o := &CollectOptions{}
	for _, opt := range opts {
		opt.applyToCollect(o)
	}
	return o
}

func ApplyMapValueOptions(opts ...MapValueOption) *MapValueOptions {
	o := &MapValueOptions{}
	for _, opt := range opts {
		opt.applyToMapValue(o)
	}
	return o
}

func ApplyIgnoreOptions(opts ...IgnoreOption) *IgnoreOptions {
	o := &IgnoreOptions{}
	for _, opt := range opts {
		opt.applyToIgnore(o)
	}
	return o
}

func ApplyToCollectionOptions(opts ...ToCollectionOption) *ToCollectionOptions {
	o := &ToCollectionOptions{}
	for _, opt := range opts {
		opt.applyToToCollection(o)
	}
	return o
}

func ApplySpillBufferOptions(opts ...SpillBufferOption) *SpillBufferOptions {
	o := &SpillBufferOptions{}
	for _, opt := range opts {
		opt.applyToSpillBuffer(o)
	}
	return o
}
