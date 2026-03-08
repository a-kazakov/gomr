package options

import "github.com/a-kazakov/gomr/parameters"

// =============================================================================
// SpillBuffer-specific options
// =============================================================================

type spillDirectoriesOption []string

func (o spillDirectoriesOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.SpillDirectories = parameters.Some([]string(o))
}

// WithSpillDirectories sets the spill directories.
func WithSpillDirectories(dirs ...string) spillDirectoriesOption { return spillDirectoriesOption(dirs) }

type maxSpillFileSizeOption int64

func (o maxSpillFileSizeOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.MaxSpillFileSize = parameters.Some(int64(o))
}

// WithMaxSpillFileSize sets the maximum spill file size in bytes.
func WithMaxSpillFileSize(size int64) maxSpillFileSizeOption { return maxSpillFileSizeOption(size) }

type spillWriteBufferSizeOption int

func (o spillWriteBufferSizeOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.WriteBufferSize = parameters.Some(int(o))
}

// WithSpillWriteBufferSize sets the write buffer size for spill files.
func WithSpillWriteBufferSize(size int) spillWriteBufferSizeOption {
	return spillWriteBufferSizeOption(size)
}

type spillReadBufferSizeOption int

func (o spillReadBufferSizeOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.ReadBufferSize = parameters.Some(int(o))
}

// WithSpillReadBufferSize sets the read buffer size for spill files.
func WithSpillReadBufferSize(size int) spillReadBufferSizeOption {
	return spillReadBufferSizeOption(size)
}

type spillWriteParallelismOption int

func (o spillWriteParallelismOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.WriteParallelism = parameters.Some(int(o))
}

// WithSpillWriteParallelism sets the write parallelism for spill buffer.
func WithSpillWriteParallelism(p int) spillWriteParallelismOption {
	return spillWriteParallelismOption(p)
}

type spillReadParallelismOption int

func (o spillReadParallelismOption) applyToSpillBuffer(opts *SpillBufferOptions) {
	opts.ReadParallelism = parameters.Some(int(o))
}

// WithSpillReadParallelism sets the read parallelism for spill buffer.
func WithSpillReadParallelism(p int) spillReadParallelismOption { return spillReadParallelismOption(p) }
