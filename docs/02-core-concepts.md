# Core Concepts

## Pipeline

A `Pipeline` is the top-level container that owns all collections, orchestrates goroutine scheduling, and manages metrics.

### Creation

```go
// Default parameters
p := gomr.NewPipeline()

// Custom parameters
params := parameters.NewParameters()
p := gomr.NewPipelineWithParameters(params)
```

### Lifecycle

1. Create the pipeline
2. Wire up operators (seed, map, shuffle, collect, etc.)
3. Call `p.WaitForCompletion()` to block until all operators finish

`WaitForCompletion` panics if any collection has not been consumed by an operator. This is a safety check — every collection must have exactly one consumer.

### User Context

Attach arbitrary application state accessible from every operator:

```go
p.UserContext = &MyState{...}

// In operators:
state := ctx.UserContext.(*MyState)
```

## Collection[T]

A `Collection[T]` is a typed stream of values flowing through the pipeline. Under the hood, values are grouped into batches and sent through a Go channel.

Key properties:
- **Single consumer**: Each collection can only be consumed by one downstream operator. Use `Fork` to split a collection.
- **Typed**: The type parameter `T` determines the element type.
- **Backpressure**: When the downstream operator is slow, the channel fills up and the upstream operator blocks, providing natural backpressure.

Collections are created by operators like `NewSeedCollection`, `Map`, `Shuffle`, `Merge`, and `ToCollection`. They are consumed by `Map`, `Shuffle`, `Collect`, `Fork`, `Merge`, `SpillBuffer`, and `Ignore`.

## Value[T]

A `Value[T]` is a future/promise that resolves to a single computed value.

```go
// Block until the value is ready
result := myValue.Wait()
```

Values are produced by `Collect` and `MapValue` operators. They are consumed as:
- **Side values** for `MapWithSideValue`, `ShuffleWithSideValue`, `CollectWithSideValue`
- **Inputs** to `MapValue` (1–5 input values)
- **Source** for `ToCollection` (converts a value into a single-element collection)

The pipeline automatically manages dependencies — operators that depend on a `Value` will wait for it to resolve before starting.

## Emitter[T]

The `Emitter[T]` is how operators produce output values. It handles batching transparently.

### Usage Pattern

```go
func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[string]) {
    for v := range receiver.IterValues() {
        *emitter.GetEmitPointer() = fmt.Sprintf("%d", *v)
    }
}
```

`GetEmitPointer()` returns a pointer to the next slot in the current batch. Write your value to the pointed location. When the batch is full, it's automatically flushed to the downstream collection's channel.

You don't need to close emitters — the framework handles cleanup.

### Performance Note

`GetEmitPointer()` is the preferred emit pattern because it avoids copies. The pointer points directly into the batch buffer, so your value is written in-place.

## Receiver

Operators that consume collections receive a `CollectionReceiver[T]` (for Map, Collect) or `ShuffleReceiver[T]` (for Shuffle reducers).

### IterValues

```go
for v := range receiver.IterValues() {
    // v is *T — a pointer to the value in the batch buffer
    process(*v)
}
```

Iterates over individual values. The pointer `v` is only valid until the next iteration, so don't store it.

### IterBatches

```go
for batch := range receiver.IterBatches() {
    // batch is []T — a slice of values
    for _, v := range batch {
        process(v)
    }
}
```

Iterates over raw batches. More efficient when you need to process values in bulk (e.g., counting, writing to files) because it avoids per-element overhead. Only available on `CollectionReceiver` (not `ShuffleReceiver`).

## OperatorContext

Every operator function receives an `OperatorContext` with:

| Field | Type | Description |
|---|---|---|
| `OperatorId` | `string` | Unique identifier for this operator instance |
| `WorkerIndex` | `int` | Index of this worker (0-based), for parallel operators |
| `TotalWorkers` | `int` | Total number of parallel workers for this operator |
| `Mutex` | `*sync.Mutex` | Shared mutex across all workers of this operator |
| `UserContext` | `any` | Pipeline-level user context (set via `p.UserContext`) |
| `UserOperatorContext` | `any` | Per-operator user context (set via `WithUserOperatorContext`) |

### User Counters

`OperatorContext` provides access to user-defined counters at multiple levels:

```go
// Pipeline-level counter (shared across all operators)
ctx.PipelineUserCounters.GetCounter("total_errors").Add(1)

// Operation-level counter (scoped to this operator)
ctx.OperationUserCounters.GetCounter("items_processed").Add(1)

// Output collection-level counters
ctx.OutCollectionsUserCounters[0].GetCounter("filtered_out").Add(1)

// Output value-level counters
ctx.OutValuesUserCounters[0].GetCounter("computation_steps").Add(1)
```

Counters are `*atomic.Int64` and safe for concurrent use. They appear in metrics snapshots.

### Per-Operator Context

Use `WithUserOperatorContext` to pass operator-specific state:

```go
db := openDB()
gomr.Map(collection, myMapper, gomr.WithUserOperatorContext(db))

// In the mapper:
func myMapper(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[string]) {
    db := ctx.UserOperatorContext.(*sql.DB)
    // ...
}
```
