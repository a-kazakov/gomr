# Performance Guide

## Batch Size

Batch size controls how many elements are grouped together before being sent through a channel.

- **Larger batches** (e.g., 4096–8192): Higher throughput, less channel overhead, but more memory per batch and higher latency for the first element.
- **Smaller batches** (e.g., 256–512): Lower latency, less memory, but more channel send/receive overhead.

Default: **1024**.

```go
// Pipeline-wide
params.Collections.DefaultBatchSize.Set(4096)

// Per-operator
gomr.Map(input, mapper, gomr.WithOutBatchSize(gomr.Some(8192)))
```

## Channel Capacity

Channel capacity determines how many batches can be buffered between operators.

- **Higher capacity**: More buffering, smoother throughput with bursty producers, but more memory.
- **Lower capacity**: Tighter backpressure, less memory, but producers block sooner.

Default: **1000** batches.

```go
params.Collections.DefaultCapacity.Set(500)
```

## Parallelism

Map and Collect operators run with multiple workers. Each worker processes a portion of the input.

Default: **GOMAXPROCS**.

```go
// Pipeline-wide
params.Processing.DefaultParallelism.Set(16)

// Per-operator
gomr.Map(input, mapper, gomr.WithParallelism(gomr.Some(32)))
```

Use higher parallelism for CPU-bound operators and lower for I/O-bound ones that would contend on external resources.

## IterBatches vs IterValues

When possible, prefer `IterBatches()` over `IterValues()`:

```go
// Slower — per-element iteration overhead
for v := range receiver.IterValues() {
    process(*v)
}

// Faster — batch-level iteration, less overhead
for batch := range receiver.IterBatches() {
    for _, v := range batch {
        process(v)
    }
}
```

`IterBatches` avoids the per-element wrapper and gives you direct access to the underlying slice. Use it when you're iterating all elements anyway (counting, aggregating, bulk writing). `IterValues` is more convenient when you need pointer semantics or when you're filtering/transforming individual elements.

Note: `IterBatches` is only available on `CollectionReceiver` (Map, Collect). Shuffle reducers only have `ShuffleReceiver` which provides `IterValues`.

## Shuffle Tuning

### Shard Count

More shards = better parallelism in the gather phase and more even key distribution, but more files on disk.

Default: **4 × GOMAXPROCS**.

```go
gomr.Shuffle[*ser, *red](input, gomr.WithNumShards(gomr.Some(int32(128))))
```

### Scatter and Gather Parallelism

- **Scatter parallelism**: Workers reading input and writing sharded files. Default: GOMAXPROCS + 8 (extra workers to hide I/O latency).
- **Gather parallelism**: Workers reading shards and calling the reducer. Default: GOMAXPROCS.

```go
gomr.Shuffle[*ser, *red](input,
    gomr.WithScatterParallelism(gomr.Some(24)),
    gomr.WithGatherParallelism(gomr.Some(16)),
)
```

### Buffer Sizes

**Local shuffle buffer** (per scatter worker): Controls how much data is buffered in memory before spilling to disk. Larger = fewer spills = fewer disk files to merge.

Default: **512 MiB**.

```go
gomr.Shuffle[*ser, *red](input,
    gomr.WithLocalShuffleBufferSize(gomr.Some(int64(1 << 30))), // 1 GiB
)
```

**Buffer size jitter**: Randomizes buffer sizes across workers to avoid all workers spilling simultaneously (thundering herd).

Default: **0.2** (±20%).

```go
gomr.Shuffle[*ser, *red](input,
    gomr.WithLocalShuffleBufferSizeJitter(gomr.Some(0.3)), // ±30%
)
```

### I/O Buffer Sizes

- **Write buffer**: Buffered writes to disk. Default: 8 MiB.
- **Read buffer**: Buffered reads from disk. Default: 1 MiB.

```go
gomr.Shuffle[*ser, *red](input,
    gomr.WithShuffleWriteBufferSize(gomr.Some(16 * 1024 * 1024)),
    gomr.WithShuffleReadBufferSize(gomr.Some(4 * 1024 * 1024)),
)
```

### File Merge Threshold

When a shard accumulates this many files, they're merged into a single file. Lower = fewer files open during gather, higher = less merge overhead during scatter.

Default: **100**.

## Compression

Shuffle and SpillBuffer support compression for disk I/O:

| Algorithm | Flag Value | Description |
|---|---|---|
| None | `none` | No compression |
| LZ4 | `lz4` | **Default**. Very fast compression/decompression, moderate ratio |
| Zstd Fast | `zstd-fast` | Fast zstd preset, better ratio than LZ4 |
| Zstd Default | `zstd-default` | Standard zstd, best ratio, slower |

LZ4 is the default because it typically reduces disk I/O without becoming a CPU bottleneck.

```go
params.Shuffle.DefaultCompressionAlgorithm.Set(core.CompressionAlgorithmZstdFast)
```

## SpillBuffer

Use `SpillBuffer` when a downstream operator is significantly slower than the upstream, and you don't want backpressure to stall the faster upstream.

SpillBuffer acts as a pass-through: when the output channel is full, it writes incoming batches to disk. When space opens up, it reads back from disk. This decouples producer speed from consumer speed at the cost of disk I/O.

```go
buffered := gomr.SpillBuffer[mySerializer](collection,
    gomr.WithSpillDirectories(gomr.Some("/fast-ssd/spill")),
    gomr.WithMaxSpillFileSize(gomr.Some(int64(128 << 20))),
)
```

Key settings:
- `WithMaxSpillFileSize`: Max size per spill file (default: 64 MiB)
- `WithSpillWriteBufferSize` / `WithSpillReadBufferSize`: I/O buffers (default: 256 KiB each)
- `WithSpillWriteParallelism` / `WithSpillReadParallelism`: Workers (default: GOMAXPROCS each)

## Scratch Space

Distribute disk I/O across multiple drives by specifying multiple scratch paths:

```go
params.Disk.ScratchSpacePaths.Set("/ssd1/scratch,/ssd2/scratch,/ssd3/scratch")
```

Or per-operator for shuffle:

```go
gomr.Shuffle[*ser, *red](input,
    gomr.WithScratchSpacePaths(gomr.Some("/ssd1/scratch,/ssd2/scratch")),
)
```

Shards are distributed round-robin across the paths.

## Target Write Latency

The disk subsystem monitors write latency and throttles when it exceeds the target. This prevents overwhelming slow disks.

Default: **50ms**.

```go
params.Disk.TargetWriteLatency.Set(100 * time.Millisecond)
```

## Memory Considerations

- **Batch buffers**: `batch_size × sizeof(T) × channel_capacity × num_collections`. A pipeline with many large-element collections at high capacity can use significant memory.
- **Shuffle buffers**: `local_shuffle_buffer_size × scatter_parallelism`. With defaults (512 MiB × GOMAXPROCS+8), this can be substantial.
- **Buffer jitter**: The jitter setting staggers spills so workers don't all flush at once, smoothing I/O patterns.

To reduce memory usage:
1. Lower `collections.capacity` (fewer buffered batches)
2. Lower `shuffle.buffer_size` (smaller in-memory buffers before spill)
3. Lower parallelism for memory-heavy operators
4. Use SpillBuffer to allow faster producers to spill rather than hold batches in channels
