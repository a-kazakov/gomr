package parameters

// CollectionsParameters holds defaults for collection batch sizes and channel capacities.
type CollectionsParameters struct {
	DefaultBatchSize *Parameter[int]
	DefaultCapacity  *Parameter[int]
}

func NewCollectionsParameters() *CollectionsParameters {
	return &CollectionsParameters{
		DefaultBatchSize: NewIntParam(
			"collections", "batch_size",
			"Default batch size for collections",
			1024,
			Positive[int],
		),
		DefaultCapacity: NewIntParam(
			"collections", "capacity",
			"Default channel capacity for collections",
			1000,
			Positive[int],
		),
	}
}

func (cp *CollectionsParameters) RegisterFlags(f Flagger) {
	RegisterAllFlags(cp, f)
}

func (cp *CollectionsParameters) LoadFromSource(lookup Source) error {
	return LoadAllFromSource(cp, lookup)
}
