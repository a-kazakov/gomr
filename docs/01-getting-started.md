# Getting Started

## Installation

```bash
go get github.com/a-kazakov/gomr
```

## Minimal Example

Here's the simplest possible pipeline — seed some numbers, double them, and collect the sum:

```go
package main

import (
    "fmt"

    "github.com/a-kazakov/gomr"
)

func main() {
    // 1. Create a pipeline
    p := gomr.NewPipeline()

    // 2. Seed data into the pipeline
    numbers := gomr.NewSeedCollection(p, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
        for i := 1; i <= 100; i++ {
            *emitter.GetEmitPointer() = i
        }
    })

    // 3. Transform: double each number
    doubled := gomr.Map(numbers, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
        for v := range receiver.IterValues() {
            *emitter.GetEmitPointer() = *v * 2
        }
    })

    // 4. Collect into a single value (sum)
    total := gomr.Collect(
        doubled,
        // Pre-collect: runs in parallel across workers
        func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int64 {
            var sum int64
            for v := range receiver.IterValues() {
                sum += int64(*v)
            }
            return sum
        },
        // Post-collect: runs once, merges intermediate results
        func(ctx gomr.OperatorContext, intermediates []int64) int64 {
            var sum int64
            for _, v := range intermediates {
                sum += v
            }
            return sum
        },
    )

    // 5. Wait for completion and read the result
    p.WaitForCompletion()
    fmt.Println("Sum:", total.Wait()) // Sum: 10100
}
```

## Core Concepts at a Glance

**Pipeline** — The top-level container. Create one, wire up operators, call `WaitForCompletion()`.

**Collection[T]** — A typed stream of values flowing through the pipeline. Produced by one operator and consumed by exactly one downstream operator. Think of it as a typed channel of batches.

**Value[T]** — A future/promise that resolves to a single computed value. Call `.Wait()` to block until the value is ready. Produced by `Collect` and `MapValue` operators.

**Emitter[T]** — The write interface for operators. Call `*emitter.GetEmitPointer() = value` to emit a value. The emitter handles batching and sending automatically.

## Running a Pipeline

```go
// With default parameters
p := gomr.NewPipeline()

// With custom parameters
params := parameters.NewParameters()
params.Processing.DefaultParallelism.Set(8)
p := gomr.NewPipelineWithParameters(params)

// ... wire up operators ...

// Block until all operators complete
p.WaitForCompletion()
```

`WaitForCompletion` will panic if any collection in the pipeline is not consumed by a downstream operator. Every collection must be consumed by exactly one operator (or by `Ignore`).

## User Context

You can attach arbitrary data to a pipeline that will be available in every operator via the `OperatorContext`:

```go
p := gomr.NewPipeline()
p.UserContext = &MyAppState{DB: db, Config: cfg}

// Later, inside any operator:
func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
    state := ctx.UserContext.(*MyAppState)
    // use state.DB, state.Config, etc.
}
```

## What's Next

- [Core Concepts](02-core-concepts.md) — Deeper dive into Pipeline, Collection, Value, Emitter, and OperatorContext
- [Operators Reference](03-operators.md) — Complete guide to all operators
- [Shuffle Deep Dive](04-shuffle.md) — Disk-based key grouping and reduction
- [Configuration](05-configuration.md) — Parameters, flags, and per-operator overrides
- [Metrics & Visualization](06-metrics.md) — Monitoring your pipeline
- [Performance Guide](07-performance.md) — Tuning for throughput and resource usage
