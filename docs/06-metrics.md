# Metrics & Visualization

## Enabling Metrics Push

Configure the pipeline to push metrics snapshots to an HTTP endpoint:

```go
params := parameters.NewParameters()
params.Metrics.PushURL.Set("http://localhost:3000")
params.Metrics.PushInterval.Set(2 * time.Second) // default: 5s

p := gomr.NewPipelineWithParameters(params)
```

Or via flags:

```bash
./mypipeline -metrics.push_url=http://localhost:3000 -metrics.push_interval=2s
```

When configured, the pipeline POSTs JSON snapshots to `{push_url}/sink/{job_id}` at the specified interval. A final snapshot is pushed when `WaitForCompletion()` returns.

## What Gets Tracked

### Pipeline Level

- **Operations**: All registered operators with their kind, phase, and parallelism
- **Collections**: All collections with batch size, channel pressure/capacity, completion status
- **Values**: All values with resolution status
- **User Counters**: Pipeline-scoped counters (see below)

### Operation Metrics

Each operator tracks:
- **Kind**: `seed`, `map`, `shuffle`, `collect`, `fork`, `merge`, `map_value`, `to_collection`, `spill_buffer`
- **Phase**: Current execution phase with timestamps for each transition
- **Parallelism**: Number of active workers
- **Input/Output Collections**: Elements and batches consumed/produced
- **Input/Output Values**: Consumption and production status
- **Shuffle Metrics** (shuffle only): Spills count, disk usage, elements gathered, groups gathered
- **User Counters**: Operation-scoped counters

### Collection Metrics

- **Pressure**: Current number of batches in the channel (backpressure indicator)
- **Capacity**: Channel capacity
- **Batch Size**: Configured batch size
- **Completed**: Whether the collection has been closed
- **User Counters**: Collection-scoped counters

### Value Metrics

- **Is Resolved**: Whether the value has been computed
- **User Counters**: Value-scoped counters

### Phases

Operations transition through phases:

| Phase | Description |
|---|---|
| `pending` | Operation registered but not started |
| `running` | Active processing |
| `scattering` | Shuffle: writing sharded files |
| `flushing` | Shuffle: flushing write buffers |
| `gathering` | Shuffle: reading and reducing |
| `pre-collect` | Collect: parallel pre-collection phase |
| `aggregate` | Collect: single-threaded aggregation |
| `completed` | All work finished |

Phase transitions are recorded with timestamps.

## JSON Snapshot Format

```json
{
  "operations": {
    "op-id": {
      "id": "op-id",
      "name": "My Operation",
      "kind": "map",
      "parallelism": 8,
      "phase": "running",
      "phases": [
        {"phase_name": "pending", "switch_timestamp": "2024-01-01T00:00:00Z"},
        {"phase_name": "running", "switch_timestamp": "2024-01-01T00:00:01Z"}
      ],
      "shuffle": null,
      "input_collections": [
        {"id": "col-1", "elements_consumed": 50000, "batches_consumed": 49}
      ],
      "output_collections": [
        {"id": "col-2", "elements_produced": 50000, "batches_produced": 49}
      ],
      "input_values": [],
      "output_values": [],
      "user_counters": {"errors": 3}
    }
  },
  "collections": {
    "col-id": {
      "id": "col-id",
      "name": "My Collection",
      "batch_size": 1024,
      "pressure": 15,
      "capacity": 1000,
      "completed": false,
      "user_counters": {}
    }
  },
  "values": {
    "val-id": {
      "id": "val-id",
      "name": "My Value",
      "is_resolved": false,
      "user_counters": {}
    }
  },
  "user_counters": {"total_records": 1000000}
}
```

## User Counters

User counters are `*atomic.Int64` values that you can create and increment from within operators. They appear in metrics snapshots.

### Accessing Counters

```go
gomr.Map(input, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[Record], emitter gomr.Emitter[Record]) {
    for v := range receiver.IterValues() {
        if v.IsValid() {
            *emitter.GetEmitPointer() = *v
        } else {
            // Increment operation-level counter
            ctx.OperationUserCounters.GetCounter("invalid_records").Add(1)
        }
    }
})
```

### Counter Scopes

| Scope | Access | Use Case |
|---|---|---|
| `ctx.PipelineUserCounters` | Pipeline-wide | Global aggregates across all operators |
| `ctx.OperationUserCounters` | Per-operator | Operator-specific metrics |
| `ctx.OutCollectionsUserCounters[i]` | Per-output-collection | Track per-collection stats |
| `ctx.OutValuesUserCounters[i]` | Per-output-value | Track per-value stats |

`GetCounter(name)` creates the counter on first access and returns the same `*atomic.Int64` on subsequent calls with the same name. Counters are safe for concurrent use.

## Using gomr-vis

The `gomr-vis` visualization server provides a real-time React dashboard for monitoring pipelines.

### Starting the Server

```bash
cd vis
npm install
npm start
# Server runs on port 3000
```

### Connecting

Configure your pipeline to push metrics to the server:

```go
params.Metrics.PushURL.Set("http://localhost:3000")
```

### Job ID

Each pipeline run has a unique job ID (a ULID by default). You can set a custom one:

```go
params.Pipeline.JobID.Set("my-custom-job-id")
```

The visualization URL is `http://localhost:3000/job/{job_id}`. The job ID is available at runtime via `p.GetJobID()`.
