# gomr

A typed MapReduce pipeline framework for Go. Build concurrent data processing pipelines with type-safe operators, automatic backpressure, disk-spilling shuffles, and real-time visualization.

## Features

- **Type-safe pipelines** — Generics throughout: `Collection[T]`, `Value[T]`, `Emitter[T]`
- **Rich operator set** — Seed, Map, Fork, Merge, Shuffle, Collect, SpillBuffer, MapValue, and more
- **Disk-spilling shuffle** — Sort-by-key with automatic spill to disk when memory is constrained
- **Automatic backpressure** — Channel-based flow control between operators
- **Parallel execution** — Configurable parallelism per operator with goroutine management
- **Real-time metrics** — Built-in metrics collection with push-based reporting
- **Visualization dashboard** — React-based real-time pipeline monitoring UI
- **Hierarchical configuration** — Global defaults with per-operator overrides via flags, env vars, or code

## Installation

```bash
go get github.com/a-kazakov/gomr
```

## Quick Start

```go
package main

import (
    "fmt"

    "github.com/a-kazakov/gomr"
)

func main() {
    p := gomr.NewPipeline()

    // Seed data
    numbers := gomr.NewSeedCollection(p, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
        for i := 1; i <= 100; i++ {
            *emitter.GetEmitPointer() = i
        }
    })

    // Transform: double each number
    doubled := gomr.Map(numbers, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
        for v := range receiver.IterValues() {
            *emitter.GetEmitPointer() = *v * 2
        }
    })

    // Collect into a single value (sum)
    total := gomr.Collect(
        doubled,
        func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int64 {
            var sum int64
            for v := range receiver.IterValues() {
                sum += int64(*v)
            }
            return sum
        },
        func(ctx gomr.OperatorContext, partialSums []int64) int64 {
            var total int64
            for _, s := range partialSums {
                total += s
            }
            return total
        },
    )

    p.WaitForCompletion()
    fmt.Println("Total:", total.Wait())
}
```

## Documentation

Full documentation is in the [docs/](docs/) directory:

1. [Getting Started](docs/01-getting-started.md) — Installation, minimal example, and core concept overview
2. [Core Concepts](docs/02-core-concepts.md) — Pipeline, Collection, Value, Emitter, Receiver, and OperatorContext
3. [Operators Reference](docs/03-operators.md) — All operators with signatures and examples
4. [Shuffle Deep Dive](docs/04-shuffle.md) — Disk-based key grouping, serializers, reducers, cursor pattern
5. [Configuration & Parameters](docs/05-configuration.md) — All parameters with defaults, per-operator overrides, flags, and environment loading
6. [Metrics & Visualization](docs/06-metrics.md) — Metrics push, JSON format, user counters, dashboard
7. [Performance Guide](docs/07-performance.md) — Batch size, parallelism, shuffle tuning, compression, memory

## Project Structure

```
gomr/
├── *.go                    # Core framework (package gomr)
├── internal/               # Implementation details
├── metrics/                # Metrics collection and push reporting
├── parameters/             # Hierarchical configuration system
├── docs/                   # Documentation
├── extensions/
│   ├── fileio/             # File I/O with compression and S3 support
│   └── marshal/            # High-performance binary serialization
├── test/                   # Integration tests
├── examples/               # Example pipelines
├── vis/                    # Real-time visualization server (Node.js)
└── vis-cloudflare/         # Visualization on Cloudflare Pages
```

## Extensions

### fileio

File I/O abstraction with pluggable backends (local filesystem, S3), compression (gzip, zstd), and gomr pipeline integration for reading/writing files.

```bash
go get github.com/a-kazakov/gomr/extensions/fileio
```

### marshal

Zero-allocation binary serialization for Go primitive types. Useful for implementing shuffle serializers.

```bash
go get github.com/a-kazakov/gomr/extensions/marshal
```

## Operators Overview

| Operator | Description |
|---|---|
| `NewSeedCollection` | Data source — generates initial data |
| `Map` | Parallel N-to-M transformation |
| `MapWithSideValue` | Map with access to a resolved `Value[T]` |
| `Fork` | Replicate a collection to multiple outputs |
| `Merge` | Combine multiple collections into one |
| `Shuffle` | Disk-spilling sort-by-key with reduce |
| `Collect` | Aggregate a collection into a single `Value[T]` |
| `MapValue` | Transform `Value[T]` types |
| `SpillBuffer` | Pass-through with disk spill on backpressure |
| `ToCollection` | Convert `Value[T]` to `Collection[T]` |
| `Ignore` | Terminal operator — drain without processing |

Multi-arity variants (e.g., `MapTo2`, `MapTo5`, `ForkTo3`, `Shuffle2To3`) are available via code generation.

## Visualization

The `vis/` directory contains a React-based real-time dashboard for monitoring running pipelines:

```bash
cd vis
npm install
npm start
# Dashboard runs on http://localhost:3000
```

Configure your pipeline to push metrics:

```go
params := parameters.NewParameters()
params.Metrics.PushURL.Set("http://localhost:3000")
p := gomr.NewPipelineWithParameters(params)
```

## Development

### Building

```bash
go build ./...
```

### Running Tests

```bash
# Core framework tests
go test ./...

# Integration tests
cd test && go test ./...
```

### Code Generation

Multi-arity operator variants are generated from templates:

```bash
go generate ./...
```

## License

MIT License. See [LICENSE](LICENSE) for details.
