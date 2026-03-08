# Operators Reference

## NewSeedCollection

Creates a source collection by emitting values from a user-provided function.

```go
collection := gomr.NewSeedCollection(p, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
    for i := 0; i < 1000; i++ {
        *emitter.GetEmitPointer() = i
    }
},
    gomr.WithOperationName("Generate numbers"),
    gomr.WithOutCollectionName("Numbers"),
)
```

The seed function runs in a single goroutine. Use the emitter to produce values — don't close it, the framework handles that.

**Options**: `WithOperationName`, `WithOutCollectionName`, `WithOutBatchSize`, `WithOutChannelCapacity`, `WithUserOperatorContext`

## Map

Parallel transform operator. Reads from one input collection and writes to one or more output collections.

### Single Output

```go
output := gomr.Map(input, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[string]) {
    for v := range receiver.IterValues() {
        *emitter.GetEmitPointer() = fmt.Sprintf("%d", *v)
    }
},
    gomr.WithParallelism(gomr.Some(4)),
)
```

### Multiple Outputs (MapTo2 through MapTo5)

Split a collection into multiple typed outputs:

```go
odds, evens := gomr.MapTo2(input,
    func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitOdds gomr.Emitter[int], emitEvens gomr.Emitter[int]) {
        for v := range receiver.IterValues() {
            if *v%2 != 0 {
                *emitOdds.GetEmitPointer() = *v
            } else {
                *emitEvens.GetEmitPointer() = *v
            }
        }
    },
    gomr.WithOutCollectionNames("Odds", "Evens"),
)
```

Available variants: `Map` (alias for `MapTo1`), `MapTo2`, `MapTo3`, `MapTo4`, `MapTo5`.

### With Side Value

Access a `Value[T]` in your mapper. The map operator waits for the value to resolve before starting:

```go
output := gomr.MapWithSideValue(input, myValue,
    func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int], threshold int) {
        for v := range receiver.IterValues() {
            if *v > threshold {
                *emitter.GetEmitPointer() = *v
            }
        }
    },
)
```

All `MapToN` variants have corresponding `MapToNWithSideValue` variants.

### Parallelism

Map operators run with `processing.parallelism` workers by default (GOMAXPROCS). Each worker reads from the same input channel and writes to its own emitter. Override per-operator:

```go
gomr.Map(input, mapper, gomr.WithParallelism(gomr.Some(8)))
```

**Options**: `WithOperationName`, `WithOutCollectionName(s)`, `WithOutBatchSize`, `WithOutChannelCapacity`, `WithParallelism`, `WithUserOperatorContext`

## Fork

Replicates a collection into multiple independent copies. Uses ref-counted batches to avoid copying data.

### Fixed Arity (ForkTo2 through ForkTo5)

```go
a, b := gomr.ForkTo2(input)
// a and b receive the same data independently
```

### Dynamic Arity (ForkToAny)

```go
copies := gomr.ForkToAny(input, 3)
// copies[0], copies[1], copies[2]
```

Each forked collection can be consumed by a different downstream operator. The original collection is consumed by the Fork and cannot be used further.

**Options**: `WithOperationName`, `WithOutCollectionNames`, `WithOutBatchSize`, `WithOutChannelCapacity`

## Merge

Combines multiple collections of the same type into one:

```go
merged := gomr.Merge([]gomr.Collection[int]{collA, collB, collC},
    gomr.WithOperationName("Merge inputs"),
    gomr.WithOutCollectionName("All inputs"),
)
```

Order is non-deterministic — values from different input collections may interleave.

**Options**: `WithOperationName`, `WithOutCollectionName`, `WithOutBatchSize`, `WithOutChannelCapacity`

## Collect

Two-phase reduction: parallel pre-collect across workers, then single-threaded post-collect to merge results.

```go
total := gomr.Collect(
    collection,
    // Pre-collect: runs in parallel (once per worker)
    func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int64 {
        var sum int64
        for v := range receiver.IterValues() {
            sum += int64(*v)
        }
        return sum
    },
    // Post-collect: runs once, receives all intermediate values
    func(ctx gomr.OperatorContext, intermediates []int64) int64 {
        var sum int64
        for _, v := range intermediates {
            sum += v
        }
        return sum
    },
    gomr.WithOutValueName("Total"),
)

// Use the result
p.WaitForCompletion()
fmt.Println(total.Wait())
```

### With Side Value

```go
result := gomr.CollectWithSideValue(collection, sideValue,
    func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], sv string) int64 {
        // sv is the resolved side value
        // ...
        return partial
    },
    func(ctx gomr.OperatorContext, intermediates []int64, sv string) string {
        // merge intermediates with access to side value
        return finalResult
    },
)
```

**Options**: `WithOperationName`, `WithOutValueName`, `WithParallelism`, `WithUserOperatorContext`

## MapValue

Transforms one or more `Value[T]` into one or more output values. Waits for all input values to resolve before executing.

### Single Input, Single Output

```go
doubled := gomr.MapValue(myValue, func(ctx gomr.OperatorContext, v int) int {
    return v * 2
})
```

### Multiple Inputs and/or Outputs

Naming convention: `MapValueNToM` where N is input count and M is output count.

```go
// 2 inputs → 2 outputs
sum, product := gomr.MapValue2To2(valA, valB,
    func(ctx gomr.OperatorContext, a int, b int) (int, int) {
        return a + b, a * b
    },
)
```

Shorthand aliases:
- `MapValue` / `MapValue1` / `MapValueTo1` / `MapValue1To1` — all equivalent (1→1)
- `MapValue2` — 2 inputs → 1 output
- `MapValue3` — 3 inputs → 1 output
- `MapValueTo2` — 1 input → 2 outputs
- Full form: `MapValueNToM` for any N,M in 1–5

**Options**: `WithOperationName`, `WithOutValueName(s)`, `WithUserOperatorContext`

## ToCollection

Converts a `Value[T]` into a single-element `Collection[T]`:

```go
collection := gomr.ToCollection(myValue)
```

Useful for feeding a computed value back into a collection-based operator.

**Options**: `WithOperationName`, `WithOutCollectionName`, `WithOutBatchSize`, `WithOutChannelCapacity`

## Ignore

Terminal operator that consumes a collection without doing anything with it. Use this when an operator produces a collection you don't need — every collection must have a consumer.

```go
gomr.Ignore(unusedCollection)
```

**Options**: `WithOperationName`

## SpillBuffer

Pass-through operator that buffers data to disk when the downstream operator is slow. Requires an `ElementSerializer` implementation.

```go
type mySerializer struct{}

func (s mySerializer) MarshalElementToBytes(value *MyType, dest []byte) int {
    // serialize value into dest, return bytes written
}

func (s mySerializer) UnmarshalElementFromBytes(data []byte, dest *MyType) {
    // deserialize data into dest
}

buffered := gomr.SpillBuffer[mySerializer](collection,
    gomr.WithSpillDirectories(gomr.Some("/scratch/spill")),
    gomr.WithMaxSpillFileSize(gomr.Some(int64(128 * 1024 * 1024))),
)
```

When the output channel is full, SpillBuffer writes incoming batches to disk. When the channel has space again, it reads back from disk and forwards. This prevents a slow downstream operator from blocking the entire pipeline.

The serializer may optionally implement `Setup(ctx *OperatorContext)` for initialization.

**Options**: `WithOperationName`, `WithOutCollectionName`, `WithOutBatchSize`, `WithOutChannelCapacity`, `WithSpillDirectories`, `WithMaxSpillFileSize`, `WithSpillWriteBufferSize`, `WithSpillReadBufferSize`, `WithSpillWriteParallelism`, `WithSpillReadParallelism`

## Common Options

These options are available across multiple operators:

| Option | Applies To | Description |
|---|---|---|
| `WithOperationName(name)` | All | Human-readable name for metrics |
| `WithOutCollectionName(name)` | Seed, Map, Shuffle, Fork, Merge, SpillBuffer, ToCollection | Name for the output collection |
| `WithOutCollectionNames(names...)` | MapToN, ForkToN, ShuffleNToM | Names for multiple output collections |
| `WithOutValueName(name)` | Collect, MapValue | Name for the output value |
| `WithOutValueNames(names...)` | MapValueNToM | Names for multiple output values |
| `WithOutBatchSize(Optional[int])` | Most | Override batch size for output collections |
| `WithOutChannelCapacity(Optional[int])` | Most | Override channel capacity for output collections |
| `WithParallelism(Optional[int])` | Map, Collect | Number of parallel workers |
| `WithUserOperatorContext(any)` | All | Per-operator context accessible via `ctx.UserOperatorContext` |
