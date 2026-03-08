# Configuration & Parameters

## Creating Parameters

```go
// Default parameters
params := parameters.NewParameters()

// Use with a pipeline
p := gomr.NewPipelineWithParameters(params)
```

Or use the convenience constructor for defaults:

```go
p := gomr.NewPipeline() // uses NewParameters() internally
```

## Parameter Groups

Parameters are organized into groups:

```go
params := parameters.NewParameters()
params.Pipeline     // PipelineParameters
params.Collections  // CollectionsParameters
params.Processing   // ProcessingParameters
params.Shuffle      // ShuffleParameters
params.Metrics      // MetricsParameters
params.Disk         // DiskParameters
params.SpillBuffer  // SpillBufferParameters
```

## All Parameters

### Pipeline

| Parameter | Flag | Default | Description |
|---|---|---|---|
| `Pipeline.JobID` | `pipeline.job_id` | Generated ULID | Unique identifier for this pipeline run |

### Collections

| Parameter | Flag | Default | Description |
|---|---|---|---|
| `Collections.DefaultBatchSize` | `collections.batch_size` | 1024 | Number of elements per batch |
| `Collections.DefaultCapacity` | `collections.capacity` | 1000 | Channel capacity (number of batches) |

### Processing

| Parameter | Flag | Default | Description |
|---|---|---|---|
| `Processing.DefaultParallelism` | `processing.parallelism` | GOMAXPROCS | Default parallel workers per operator |

### Shuffle

| Parameter | Flag | Default | Description |
|---|---|---|---|
| `Shuffle.DefaultNumShards` | `shuffle.num_shards` | 4 × GOMAXPROCS | Number of shards |
| `Shuffle.DefaultScatterParallelism` | `shuffle.scatter_parallelism` | GOMAXPROCS + 8 | Scatter phase workers |
| `Shuffle.DefaultGatherParallelism` | `shuffle.gather_parallelism` | GOMAXPROCS | Gather phase workers |
| `Shuffle.DefaultLocalShuffleBufferSize` | `shuffle.buffer_size` | 512 MiB | Per-worker buffer before spilling |
| `Shuffle.DefaultLocalShuffleBufferSizeJitter` | `shuffle.buffer_size_jitter` | 0.2 | Random jitter factor (0.0–1.0) |
| `Shuffle.DefaultFileMergeThreshold` | `shuffle.file_merge_threshold` | 100 | Files per shard before merge |
| `Shuffle.DefaultReadBufferSize` | `shuffle.read_buffer_size` | 1 MiB | I/O read buffer |
| `Shuffle.DefaultWriteBufferSize` | `shuffle.write_buffer_size` | 8 MiB | I/O write buffer |
| `Shuffle.DefaultCompressionAlgorithm` | `shuffle.compression_algorithm` | lz4 | Compression: none, lz4, zstd, zstd-fast, zstd-default |

### Metrics

| Parameter | Flag | Default | Description |
|---|---|---|---|
| `Metrics.PushURL` | `metrics.push_url` | (empty/disabled) | URL to POST metrics snapshots |
| `Metrics.PushInterval` | `metrics.push_interval` | 5s | Interval between metric pushes |

### Disk

| Parameter | Flag | Default | Description |
|---|---|---|---|
| `Disk.ScratchSpacePaths` | `disk.scratch_paths` | OS temp dir | Comma-separated scratch directories |
| `Disk.TargetWriteLatency` | `disk.target_write_latency` | 50ms | Target write latency for throttling |

### SpillBuffer

| Parameter | Flag | Default | Description |
|---|---|---|---|
| `SpillBuffer.MaxSerializedElementSize` | `spill_buffer.max_element_size` | 16 MiB | Maximum serialized element size |
| `SpillBuffer.DefaultWriteParallelism` | `spill_buffer.write_parallelism` | GOMAXPROCS | Spill write workers |
| `SpillBuffer.DefaultReadParallelism` | `spill_buffer.read_parallelism` | GOMAXPROCS | Spill read workers |
| `SpillBuffer.DefaultMaxSpillFileSize` | `spill_buffer.max_file_size` | 64 MiB | Max size per spill file |
| `SpillBuffer.DefaultWriteBufferSize` | `spill_buffer.write_buffer_size` | 256 KiB | Write buffer |
| `SpillBuffer.DefaultReadBufferSize` | `spill_buffer.read_buffer_size` | 256 KiB | Read buffer |
| `SpillBuffer.DefaultCompressionAlgorithm` | `spill_buffer.compression_algorithm` | lz4 | Compression algorithm |

## Per-Operator Overrides

Most parameters can be overridden on a per-operator basis using `With*` options and `Optional[T]`:

```go
// Override batch size for a specific operator
gomr.Map(input, mapper,
    gomr.WithOutBatchSize(gomr.Some(4096)),
    gomr.WithParallelism(gomr.Some(16)),
    gomr.WithOutChannelCapacity(gomr.Some(500)),
)

// Override shuffle-specific settings
gomr.Shuffle[*ser, *red](input,
    gomr.WithNumShards(gomr.Some(int32(64))),
    gomr.WithLocalShuffleBufferSize(gomr.Some(int64(1 << 30))), // 1 GiB
    gomr.WithScatterParallelism(gomr.Some(20)),
)
```

The resolution order is: **per-operator option > pipeline parameter > default**.

## Optional[T]

`Optional[T]` distinguishes "explicitly set" from "not set", enabling the override chain:

```go
gomr.Some(42)       // Optional with value 42
gomr.None[int]()    // Empty optional — falls through to pipeline default

// Example: conditionally override
var batchSize gomr.Optional[int]
if largeDataset {
    batchSize = gomr.Some(8192)
} else {
    batchSize = gomr.None[int]() // use default
}
gomr.Map(input, mapper, gomr.WithOutBatchSize(batchSize))
```

## Flag Registration

Register all parameters as command-line flags using any `flag.FlagSet`-compatible interface:

```go
import (
    "flag"
    "github.com/a-kazakov/gomr/parameters"
)

params := parameters.NewParameters()
params.RegisterFlags(flag.CommandLine)
flag.Parse()

p := gomr.NewPipelineWithParameters(params)
```

Then run your binary with:

```bash
./mypipeline -collections.batch_size=4096 -shuffle.num_shards=128
```

## Loading from a Source

Load parameters from environment variables, config files, or any key-value source:

```go
// Source is a function: func(key string) (string, bool)
params := parameters.NewParameters()
err := params.LoadFromSource(func(key string) (string, bool) {
    // e.g., look up "GOMR_COLLECTIONS_BATCH_SIZE" for "collections.batch_size"
    envKey := "GOMR_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
    val, ok := os.LookupEnv(envKey)
    return val, ok
})
```

## Value Parsing

Parameters support human-friendly suffixes:

- **Decimal**: `K` (1,000), `M` (1,000,000), `G` (1,000,000,000), `T` (1,000,000,000,000)
- **Binary**: `Ki`/`K` (2^10), `Mi`/`M` (2^20), `Gi`/`G` (2^30), `Ti`/`T` (2^40) — for byte-size parameters
- **Duration**: Go standard format (`500ms`, `2s`, `1m30s`)
- **Compression**: `none`, `lz4`, `zstd`, `zstd-fast`, `zstd-default`
- **Boolean**: `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`
