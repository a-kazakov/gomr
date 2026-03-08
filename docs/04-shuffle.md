# Shuffle Deep Dive

## Overview

Shuffle is the most powerful operator in gomr. It performs disk-based key grouping: all values sharing the same key are brought together and passed to a user-defined reducer. This is the MapReduce "reduce" step.

The shuffle has two phases:

1. **Scatter** — Parallel workers read from input collections, serialize each value with its key, hash the key to determine a shard, and write key-value pairs to sharded files on disk.
2. **Gather** — Parallel workers read sharded files, merge-sort by key, and call the user's reducer for each group of values sharing a key.

## Basic Usage

```go
output := gomr.Shuffle[*mySerializer, *myReducer](input,
    gomr.WithOperationName("Group by category"),
    gomr.WithOutCollectionNames("Grouped"),
)
```

Shuffle requires two type parameters:
- A pointer to a `ShuffleSerializer` implementation
- A pointer to a `Reducer` implementation

## Implementing ShuffleSerializer

The serializer controls how values are mapped to keys and serialized to disk.

```go
type mySerializer struct{}

func (s mySerializer) MarshalKeyToBytes(value *InputType, dest []byte, cursor int64) (int, int64) {
    // Write the key bytes into dest.
    // Return (bytesWritten, nextCursor).
    // Return gomr.StopEmitting as nextCursor when done.
    //
    // Key size is limited to 1KB.

    key := value.CategoryID
    binary.BigEndian.PutUint64(dest, uint64(key))
    return 8, gomr.StopEmitting
}

func (s mySerializer) MarshalValueToBytes(value *InputType, dest []byte) int {
    // Serialize the value into dest. Return bytes written.
    // Value size is limited to 16MB.

    n, _ := binary.Encode(dest, binary.LittleEndian, value.Payload)
    return n
}

func (s mySerializer) UnmarshalValueFromBytes(key []byte, data []byte, dest *IntermediateType) {
    // Deserialize data into dest.
    // key is available if you need it for the intermediate type.

    binary.Decode(data, binary.LittleEndian, &dest.Payload)
    dest.CategoryID = int64(binary.BigEndian.Uint64(key))
}
```

### The Cursor Pattern

A single input value can emit **multiple** keys. The `cursor` parameter tracks position:

- First call: `cursor = 0`
- Return `(bytesWritten, nextCursor)` where `nextCursor` is passed to the next call
- Return `gomr.StopEmitting` as `nextCursor` to finish

```go
func (s mySerializer) MarshalKeyToBytes(value *Edge, dest []byte, cursor int64) (int, int64) {
    switch cursor {
    case 0:
        // First key: source node
        binary.BigEndian.PutUint64(dest, uint64(value.Source))
        return 8, 1
    case 1:
        // Second key: destination node
        binary.BigEndian.PutUint64(dest, uint64(value.Dest))
        return 8, gomr.StopEmitting
    }
    panic("unexpected cursor")
}
```

### Serializer Setup

If your serializer needs initialization, implement the optional `Setup` method:

```go
func (s *mySerializer) Setup(ctx gomr.OperatorContext) {
    // Initialize resources, load lookup tables, etc.
}
```

For shuffles with a side value, implement `Setup(ctx gomr.OperatorContext, sideValue TSideValue)` instead.

## Implementing Reducer

The reducer processes all values for a given key and emits output values.

```go
type myReducer struct{}

func (r myReducer) Setup(ctx gomr.OperatorContext) {
    // Called once per gather worker before processing starts.
}

func (r myReducer) Reduce(key []byte, receiver gomr.ShuffleReceiver[IntermediateType], emitter gomr.Emitter[OutputType]) {
    // Process all values for this key.
    var count int
    for v := range receiver.IterValues() {
        count++
    }
    *emitter.GetEmitPointer() = OutputType{
        Key:   binary.BigEndian.Uint64(key),
        Count: count,
    }
}

func (r myReducer) Teardown(emitter gomr.Emitter[OutputType]) {
    // Called once per gather worker after all keys are processed.
    // Can emit final values if needed.
}
```

The `Reduce` method receives:
- `key` — the raw key bytes (as written by `MarshalKeyToBytes`)
- `receiver` — iterator over all values with this key (provides `IterValues()`)
- `emitter` — write output values here

## Complete Example

```go
// Serializer: group int64 values by their last 3 digits
type lastDigitsSerializer struct{}

func (s lastDigitsSerializer) MarshalKeyToBytes(value *int64, dest []byte, cursor int64) (int, int64) {
    key := *value % 1000
    binary.BigEndian.PutUint64(dest, uint64(key))
    return 8, gomr.StopEmitting
}

func (s lastDigitsSerializer) MarshalValueToBytes(value *int64, dest []byte) int {
    binary.LittleEndian.PutUint64(dest, uint64(*value))
    return 8
}

func (s lastDigitsSerializer) UnmarshalValueFromBytes(key []byte, data []byte, dest *int64) {
    *dest = int64(binary.LittleEndian.Uint64(data))
}

// Reducer: sum values per key, emit "key: sum" strings
type sumReducer struct{}

func (r sumReducer) Setup(ctx gomr.OperatorContext)       {}
func (r sumReducer) Teardown(emitter gomr.Emitter[string]) {}

func (r sumReducer) Reduce(key []byte, receiver gomr.ShuffleReceiver[int64], emitter gomr.Emitter[string]) {
    var sum int64
    for v := range receiver.IterValues() {
        sum += *v
    }
    keyValue := binary.BigEndian.Uint64(key)
    *emitter.GetEmitPointer() = fmt.Sprintf("%d: %d", keyValue, sum)
}

// Usage
output := gomr.Shuffle[*lastDigitsSerializer, *sumReducer](input)
```

## Multi-Input / Multi-Output Shuffle

Shuffle supports 1–5 input collections and 1–5 output collections.

### Multi-Output

When the reducer emits to multiple output collections:

```go
type multiReducer struct{}

func (r multiReducer) Setup(ctx gomr.OperatorContext) {}
func (r multiReducer) Teardown(emitter0 gomr.Emitter[string], emitter1 gomr.Emitter[int]) {}

func (r multiReducer) Reduce(key []byte, receiver gomr.ShuffleReceiver[int64], emitter0 gomr.Emitter[string], emitter1 gomr.Emitter[int]) {
    var count int
    for v := range receiver.IterValues() {
        *emitter0.GetEmitPointer() = fmt.Sprintf("key=%x val=%d", key, *v)
        count++
    }
    *emitter1.GetEmitPointer() = count
}

strings, counts := gomr.Shuffle1To2[*mySerializer, *multiReducer](input)
```

### Multi-Input

When shuffling multiple collections together, each input needs its own serializer. The reducer receives separate receivers, one per input:

```go
out := gomr.Shuffle2To1[*serializerA, *serializerB, *myReducer](collectionA, collectionB)
```

Naming: `ShuffleNToM` where N = input count, M = output count. Also available: `Shuffle` (alias for `Shuffle1To1`).

## Side Values

Shuffle operators support a side value dependency:

```go
output := gomr.ShuffleWithSideValue[*mySerializer, *myReducer](input, mySideValue)
```

When a side value is provided:
- The serializer can implement `Setup(ctx gomr.OperatorContext, sideValue TSideValue)` to access it
- The reducer can implement `Setup(ctx gomr.OperatorContext, sideValue TSideValue)` to access it

## Configuration Options

| Option | Description | Default |
|---|---|---|
| `WithNumShards(Optional[int32])` | Number of shards for key distribution | 4 × GOMAXPROCS |
| `WithScatterParallelism(Optional[int])` | Workers for scatter phase | GOMAXPROCS + 8 |
| `WithGatherParallelism(Optional[int])` | Workers for gather phase | GOMAXPROCS |
| `WithLocalShuffleBufferSize(Optional[int64])` | Per-worker write buffer size | 512 MiB |
| `WithLocalShuffleBufferSizeJitter(Optional[float64])` | Random jitter factor for buffer size | 0.2 |
| `WithShuffleReadBufferSize(Optional[int])` | I/O read buffer | 1 MiB |
| `WithShuffleWriteBufferSize(Optional[int])` | I/O write buffer | 8 MiB |
| `WithFileMergeThreshold(Optional[int])` | Files per shard before merging | 100 |
| `WithScratchSpacePaths(Optional[string])` | Comma-separated disk paths | OS temp dir |
| `WithTargetWriteLatency(Optional[time.Duration])` | Disk write throttling target | 50ms |
| `WithOperationName(name)` | Operation name for metrics | "Shuffle" |
| `WithOutCollectionNames(names...)` | Output collection names | — |
| `WithUserOperatorContext(any)` | Per-operator context | — |
