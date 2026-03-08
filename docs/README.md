# gomr Documentation

User guide for the gomr typed MapReduce pipeline framework.

## Table of Contents

1. [Getting Started](01-getting-started.md) — Installation, minimal example, and core concept overview
2. [Core Concepts](02-core-concepts.md) — Pipeline, Collection, Value, Emitter, Receiver, and OperatorContext
3. [Operators Reference](03-operators.md) — NewSeedCollection, Map, Fork, Merge, Collect, MapValue, ToCollection, Ignore, SpillBuffer
4. [Shuffle Deep Dive](04-shuffle.md) — Disk-based key grouping, ShuffleSerializer, Reducer, multi-input/output, cursor pattern
5. [Configuration & Parameters](05-configuration.md) — All parameters with defaults, per-operator overrides, flags, and environment loading
6. [Metrics & Visualization](06-metrics.md) — Metrics push, JSON snapshot format, user counters, gomr-vis dashboard
7. [Performance Guide](07-performance.md) — Batch size, parallelism, shuffle tuning, compression, SpillBuffer, memory
