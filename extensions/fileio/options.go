package fileio

import "github.com/a-kazakov/gomr"

// =============================================================================
// Low-level I/O configs (Open, Create, Glob)
// =============================================================================

// ReadConfig holds configuration for Open.
type ReadConfig struct {
	Backend      Backend
	Decompressor Decompressor
}

// WriteConfig holds configuration for Create.
type WriteConfig struct {
	Backend    Backend
	Compressor Compressor
	BufferSize int
}

// GlobConfig holds configuration for Glob.
type GlobConfig struct {
	Backend Backend
}

// ReadOption configures a read operation.
type ReadOption interface {
	ApplyReadConfig(*ReadConfig)
}

// WriteOption configures a write operation.
type WriteOption interface {
	ApplyWriteConfig(*WriteConfig)
}

// GlobOption configures a glob operation.
type GlobOption interface {
	ApplyGlobConfig(*GlobConfig)
}

// =============================================================================
// Pipeline-level configs (ReadFiles, WriteFiles)
// =============================================================================

// ReadFilesConfig holds configuration for ReadFiles and ListFiles.
type ReadFilesConfig struct {
	Backend         Backend
	Decompressor    Decompressor
	MaxLineLength   int    // default 1<<20
	OperationName   string // default "Read Files" — applied to Map step
	CollectionName  string // default "Lines"
	Parallelism     int    // 0 = gomr default
	BatchSize       int    // 0 = gomr default
	ChannelCapacity int    // 0 = gomr default
}

// WriteFilesConfig holds configuration for WriteFiles.
type WriteFilesConfig struct {
	Backend            Backend
	BufferSize         int // per-file write buffer, default 16MB
	OperationName      string
	CollectionName     string
	BatchSize          int
	ChannelCapacity    int
	NumShards          int32
	ScatterParallelism int
	GatherParallelism  int
	ExtraShuffleOpts   []gomr.ShuffleOption
}

// ReadFilesOption configures a ReadFiles or ListFiles operation.
type ReadFilesOption interface {
	ApplyReadFilesConfig(*ReadFilesConfig)
}

// WriteFilesOption configures a WriteFiles operation.
type WriteFilesOption interface {
	ApplyWriteFilesConfig(*WriteFilesConfig)
}

// =============================================================================
// Option types
// =============================================================================

type backendOption struct{ value Backend }

// WithBackend returns an option that sets the backend for I/O operations.
func WithBackend(b Backend) backendOption {
	return backendOption{value: b}
}

func (o backendOption) ApplyReadConfig(c *ReadConfig)           { c.Backend = o.value }
func (o backendOption) ApplyWriteConfig(c *WriteConfig)         { c.Backend = o.value }
func (o backendOption) ApplyGlobConfig(c *GlobConfig)           { c.Backend = o.value }
func (o backendOption) ApplyReadFilesConfig(c *ReadFilesConfig) { c.Backend = o.value }
func (o backendOption) ApplyWriteFilesConfig(c *WriteFilesConfig) {
	c.Backend = o.value
}

type compressorOption struct{ value Compressor }

// WithCompressor returns an option that sets the compressor for write operations.
func WithCompressor(c Compressor) compressorOption {
	return compressorOption{value: c}
}

func (o compressorOption) ApplyWriteConfig(c *WriteConfig) { c.Compressor = o.value }

type decompressorOption struct{ value Decompressor }

// WithDecompressor returns an option that sets the decompressor for read operations.
func WithDecompressor(d Decompressor) decompressorOption {
	return decompressorOption{value: d}
}

func (o decompressorOption) ApplyReadConfig(c *ReadConfig)           { c.Decompressor = o.value }
func (o decompressorOption) ApplyReadFilesConfig(c *ReadFilesConfig) { c.Decompressor = o.value }

type bufferSizeOption struct{ value int }

// WithBufferSize returns an option that sets the buffer size for write operations.
func WithBufferSize(size int) bufferSizeOption {
	return bufferSizeOption{value: size}
}

func (o bufferSizeOption) ApplyWriteConfig(c *WriteConfig) { c.BufferSize = o.value }
func (o bufferSizeOption) ApplyWriteFilesConfig(c *WriteFilesConfig) {
	c.BufferSize = o.value
}

// --- Pipeline-specific option types ---

type operationNameOption struct{ value string }

// WithOperationName returns an option that sets the operation display name.
func WithOperationName(name string) operationNameOption {
	return operationNameOption{value: name}
}

func (o operationNameOption) ApplyReadFilesConfig(c *ReadFilesConfig) {
	c.OperationName = o.value
}
func (o operationNameOption) ApplyWriteFilesConfig(c *WriteFilesConfig) {
	c.OperationName = o.value
}

type collectionNameOption struct{ value string }

// WithCollectionName returns an option that sets the output collection display name.
func WithCollectionName(name string) collectionNameOption {
	return collectionNameOption{value: name}
}

func (o collectionNameOption) ApplyReadFilesConfig(c *ReadFilesConfig) {
	c.CollectionName = o.value
}
func (o collectionNameOption) ApplyWriteFilesConfig(c *WriteFilesConfig) {
	c.CollectionName = o.value
}

type batchSizeOption struct{ value int }

// WithBatchSize returns an option that sets the output batch size.
func WithBatchSize(n int) batchSizeOption {
	return batchSizeOption{value: n}
}

func (o batchSizeOption) ApplyReadFilesConfig(c *ReadFilesConfig)     { c.BatchSize = o.value }
func (o batchSizeOption) ApplyWriteFilesConfig(c *WriteFilesConfig)   { c.BatchSize = o.value }

type parallelismOption struct{ value int }

// WithParallelism returns an option that sets the Map parallelism for file reading.
func WithParallelism(n int) parallelismOption {
	return parallelismOption{value: n}
}

func (o parallelismOption) ApplyReadFilesConfig(c *ReadFilesConfig) { c.Parallelism = o.value }

type channelCapacityOption struct{ value int }

// WithChannelCapacity returns an option that sets the output channel capacity.
func WithChannelCapacity(n int) channelCapacityOption {
	return channelCapacityOption{value: n}
}

func (o channelCapacityOption) ApplyReadFilesConfig(c *ReadFilesConfig) {
	c.ChannelCapacity = o.value
}
func (o channelCapacityOption) ApplyWriteFilesConfig(c *WriteFilesConfig) {
	c.ChannelCapacity = o.value
}

type maxLineLengthOption struct{ value int }

// WithMaxLineLength returns an option that sets the maximum bytes per line.
func WithMaxLineLength(n int) maxLineLengthOption {
	return maxLineLengthOption{value: n}
}

func (o maxLineLengthOption) ApplyReadFilesConfig(c *ReadFilesConfig) {
	c.MaxLineLength = o.value
}

type numShardsOption struct{ value int32 }

// WithNumShards returns an option that sets the shuffle shard count.
func WithNumShards(n int32) numShardsOption {
	return numShardsOption{value: n}
}

func (o numShardsOption) ApplyWriteFilesConfig(c *WriteFilesConfig) { c.NumShards = o.value }

type scatterParallelismOption struct{ value int }

// WithScatterParallelism returns an option that sets the shuffle scatter parallelism.
func WithScatterParallelism(n int) scatterParallelismOption {
	return scatterParallelismOption{value: n}
}

func (o scatterParallelismOption) ApplyWriteFilesConfig(c *WriteFilesConfig) {
	c.ScatterParallelism = o.value
}

type gatherParallelismOption struct{ value int }

// WithGatherParallelism returns an option that sets the shuffle gather parallelism.
func WithGatherParallelism(n int) gatherParallelismOption {
	return gatherParallelismOption{value: n}
}

func (o gatherParallelismOption) ApplyWriteFilesConfig(c *WriteFilesConfig) {
	c.GatherParallelism = o.value
}

type shuffleOptionsOption struct{ value []gomr.ShuffleOption }

// WithShuffleOptions returns an option that passes through gomr ShuffleOptions.
func WithShuffleOptions(opts ...gomr.ShuffleOption) shuffleOptionsOption {
	return shuffleOptionsOption{value: opts}
}

func (o shuffleOptionsOption) ApplyWriteFilesConfig(c *WriteFilesConfig) {
	c.ExtraShuffleOpts = append(c.ExtraShuffleOpts, o.value...)
}
