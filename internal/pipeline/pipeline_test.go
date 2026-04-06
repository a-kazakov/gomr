package pipeline

// Tests use package-internal access for: startMetricsPush, stopMetricsPush, consumed field, metricsPusher field, UserContext field on OperatorContext.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/metrics"
	"github.com/a-kazakov/gomr/parameters"
)

// =============================================================================
// Pipeline
// =============================================================================

func TestPipeline(t *testing.T) {
	t.Run("new pipeline", func(t *testing.T) {
		p := NewPipeline()
		if p == nil {
			t.Fatal("NewPipeline returned nil")
		}
		if p.Parameters == nil {
			t.Error("Parameters should not be nil")
		}
		if p.GoroutineDispatcher == nil {
			t.Error("GoroutineDispatcher should not be nil")
		}
		if p.Metrics == nil {
			t.Error("Metrics should not be nil")
		}
	})

	t.Run("new pipeline with parameters", func(t *testing.T) {
		params := parameters.NewParameters()
		p := NewPipelineWithParameters(params)
		if p.Parameters != params {
			t.Error("Parameters should be the passed-in params")
		}
	})

	t.Run("get metrics", func(t *testing.T) {
		p := NewPipeline()
		m := p.GetMetrics()
		if m != p.Metrics {
			t.Error("GetMetrics should return p.Metrics")
		}
	})

	t.Run("get job id", func(t *testing.T) {
		p := NewPipeline()
		// Default job ID comes from parameters
		id := p.GetJobID()
		if id == "" {
			t.Error("default JobID should not be empty")
		}
	})

	t.Run("unconsumed collection panics on wait", func(t *testing.T) {
		p := NewPipeline()
		NewDerivedCollection[int]("test", 10, 5, &Collection[int]{Pipeline: p, Metrics: p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }), outChannel: make(chan primitives.Batch[int])})
		defer func() {
			r := recover()
			if r == nil {
				t.Error("expected panic for unconsumed collection")
			}
		}()
		p.WaitForCompletion()
	})

	t.Run("wait for completion all consumed", func(t *testing.T) {
		p := NewPipeline()
		coll, _ := NewDerivedCollection[int]("test", 10, 5, &Collection[int]{Pipeline: p, Metrics: p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }), outChannel: make(chan primitives.Batch[int])})
		// Mark as consumed
		coll.consumed = true
		// Close the channel so it's ready
		close(coll.outChannel)
		// Should not panic
		p.WaitForCompletion()
	})

	t.Run("register collection", func(t *testing.T) {
		p := NewPipeline()
		if len(p.collections) != 0 {
			t.Errorf("initial collections = %d, want 0", len(p.collections))
		}
		coll, _ := NewDerivedCollection[int]("test", 10, 5, &Collection[int]{Pipeline: p, Metrics: p.Metrics.AddCollection("dummy", 10, func() (int, int) { return 0, 0 }), outChannel: make(chan primitives.Batch[int])})
		_ = coll
		// NewDerivedCollection calls RegisterCollection internally
		if len(p.collections) < 1 {
			t.Error("collection should have been registered")
		}
	})

	t.Run("build operator contexts", func(t *testing.T) {
		p := NewPipeline()
		opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, "op")
		collMetrics := p.Metrics.AddCollection("c", 10, func() (int, int) { return 0, 0 })
		valMetrics := p.Metrics.AddValue("v", func() bool { return false })

		ctxs := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{collMetrics}, []*metrics.ValueMetrics{valMetrics}, 3, "user_ctx")
		if len(ctxs) != 3 {
			t.Fatalf("contexts = %d, want 3", len(ctxs))
		}
		for i, ctx := range ctxs {
			if ctx.WorkerIndex != i {
				t.Errorf("ctx[%d].WorkerIndex = %d", i, ctx.WorkerIndex)
			}
			if ctx.TotalWorkers != 3 {
				t.Errorf("ctx[%d].TotalWorkers = %d, want 3", i, ctx.TotalWorkers)
			}
			if ctx.OperatorId != opMetrics.Id {
				t.Errorf("ctx[%d].OperatorId mismatch", i)
			}
			if ctx.UserOperatorContext != "user_ctx" {
				t.Errorf("ctx[%d].UserOperatorContext = %v", i, ctx.UserOperatorContext)
			}
			if len(ctx.OutCollectionsUserCounters) != 1 {
				t.Errorf("ctx[%d].OutCollectionsUserCounters = %d, want 1", i, len(ctx.OutCollectionsUserCounters))
			}
			if len(ctx.OutValuesUserCounters) != 1 {
				t.Errorf("ctx[%d].OutValuesUserCounters = %d, want 1", i, len(ctx.OutValuesUserCounters))
			}
		}
		// All contexts should share the same mutex
		if ctxs[0].Mutex != ctxs[1].Mutex || ctxs[1].Mutex != ctxs[2].Mutex {
			t.Error("contexts should share the same mutex")
		}
	})

	t.Run("build operator contexts user context", func(t *testing.T) {
		p := NewPipeline()
		p.UserContext = "pipeline_user_ctx"
		opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, "op")
		ctxs := p.BuildOperatorContexts(opMetrics, nil, nil, 1, nil)
		if ctxs[0].UserContext != "pipeline_user_ctx" {
			t.Errorf("UserContext = %v, want pipeline_user_ctx", ctxs[0].UserContext)
		}
	})
}

// =============================================================================
// Collection
// =============================================================================

func TestCollection(t *testing.T) {
	t.Run("derived collection", func(t *testing.T) {
		p := NewPipeline()
		parent := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		coll, ch := NewDerivedCollection[string]("derived", 20, 8, parent)
		if coll == nil {
			t.Fatal("NewDerivedCollection returned nil")
		}
		if ch == nil {
			t.Fatal("channel is nil")
		}
		if cap(ch) != 8 {
			t.Errorf("channel capacity = %d, want 8", cap(ch))
		}
		if coll.Metrics.Name != "derived" {
			t.Errorf("Name = %q, want derived", coll.Metrics.Name)
		}
		if coll.Metrics.BatchSize != 20 {
			t.Errorf("BatchSize = %d, want 20", coll.Metrics.BatchSize)
		}
		if coll.Pipeline != p {
			t.Error("Pipeline mismatch")
		}
		if coll.isConsumed() {
			t.Error("should not be consumed initially")
		}
	})

	t.Run("multiple parents same pipeline", func(t *testing.T) {
		p := NewPipeline()
		parent1 := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("p1", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		parent2 := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("p2", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		coll, ch := NewDerivedCollection[int]("derived_multi", 10, 5, parent1, parent2)
		if coll == nil || ch == nil {
			t.Fatal("NewDerivedCollection returned nil")
		}
		if coll.Pipeline != p {
			t.Error("Pipeline mismatch")
		}
	})

	t.Run("double consume panics via get receiver", func(t *testing.T) {
		p := NewPipeline()
		parent := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		coll, _ := NewDerivedCollection[int]("test", 10, 5, parent)
		var elemCount, batchCount atomic.Int64
		coll.GetReceiver(&elemCount, &batchCount)
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for double consume")
			}
		}()
		coll.GetReceiver(&elemCount, &batchCount)
	})

	t.Run("double consume panics via get raw channel", func(t *testing.T) {
		p := NewPipeline()
		parent := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		coll, _ := NewDerivedCollection[int]("test", 10, 5, parent)
		coll.GetRawChannel()
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for double consume")
			}
		}()
		coll.GetRawChannel()
	})

	t.Run("pipeline mismatch panics", func(t *testing.T) {
		p1 := NewPipeline()
		p2 := NewPipeline()
		parent1 := &Collection[int]{
			Pipeline:   p1,
			Metrics:    p1.Metrics.AddCollection("p1", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		parent2 := &Collection[int]{
			Pipeline:   p2,
			Metrics:    p2.Metrics.AddCollection("p2", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for different pipelines")
			}
		}()
		NewDerivedCollection[int]("bad", 10, 5, parent1, parent2)
	})

	t.Run("get raw channel", func(t *testing.T) {
		p := NewPipeline()
		parent := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		coll, expectedCh := NewDerivedCollection[int]("test", 10, 5, parent)
		ch := coll.GetRawChannel()
		if ch != expectedCh {
			t.Error("GetRawChannel should return the same channel")
		}
		if !coll.isConsumed() {
			t.Error("should be consumed after GetRawChannel")
		}
	})

	t.Run("get receiver marks consumed", func(t *testing.T) {
		p := NewPipeline()
		parent := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		coll, _ := NewDerivedCollection[int]("test", 10, 5, parent)
		var elemCount, batchCount atomic.Int64
		receiver := coll.GetReceiver(&elemCount, &batchCount)
		if receiver == nil {
			t.Fatal("GetReceiver returned nil")
		}
		if !coll.isConsumed() {
			t.Error("should be consumed after GetReceiver")
		}
	})

	t.Run("close", func(t *testing.T) {
		p := NewPipeline()
		parent := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		coll, ch := NewDerivedCollection[int]("test", 10, 5, parent)
		coll.Close()
		if !coll.Metrics.Completed {
			t.Error("Completed should be true after Close")
		}
		// Channel should be closed
		_, ok := <-ch
		if ok {
			t.Error("channel should be closed")
		}
	})

	t.Run("collections to parents", func(t *testing.T) {
		p := NewPipeline()
		c1 := &Collection[int]{Pipeline: p, Metrics: p.Metrics.AddCollection("c1", 10, func() (int, int) { return 0, 0 }), outChannel: make(chan primitives.Batch[int])}
		c2 := &Collection[int]{Pipeline: p, Metrics: p.Metrics.AddCollection("c2", 10, func() (int, int) { return 0, 0 }), outChannel: make(chan primitives.Batch[int])}
		parents := CollectionsToParents([]*Collection[int]{c1, c2})
		if len(parents) != 2 {
			t.Errorf("parents = %d, want 2", len(parents))
		}
		if parents[0].GetPipeline() != p {
			t.Error("parent[0] pipeline mismatch")
		}
	})

	t.Run("get metrics", func(t *testing.T) {
		p := NewPipeline()
		m := p.Metrics.AddCollection("test", 10, func() (int, int) { return 0, 0 })
		c := &Collection[int]{Pipeline: p, Metrics: m, outChannel: make(chan primitives.Batch[int])}
		if c.GetMetrics() != m {
			t.Error("GetMetrics should return the collection's metrics")
		}
	})

	t.Run("get pipeline", func(t *testing.T) {
		p := NewPipeline()
		c := &Collection[int]{Pipeline: p, Metrics: p.Metrics.AddCollection("test", 10, func() (int, int) { return 0, 0 }), outChannel: make(chan primitives.Batch[int])}
		if c.GetPipeline() != p {
			t.Error("GetPipeline should return the pipeline")
		}
	})

	t.Run("channel info closure", func(t *testing.T) {
		p := NewPipeline()
		parent := &Collection[int]{
			Pipeline:   p,
			Metrics:    p.Metrics.AddCollection("parent", 10, func() (int, int) { return 0, 0 }),
			outChannel: make(chan primitives.Batch[int]),
		}
		coll, ch := NewDerivedCollection[int]("derived", 10, 8, parent)
		// Invoke the GetChannelInfo closure which captures nextOutChannel
		pressure, capacity := coll.Metrics.GetChannelInfo()
		if capacity != 8 {
			t.Errorf("capacity = %d, want 8", capacity)
		}
		if pressure != 0 {
			t.Errorf("pressure = %d, want 0 (empty channel)", pressure)
		}
		// Send a batch to the channel to change pressure
		ch <- primitives.Batch[int]{Values: make([]int, 1)}
		pressure2, _ := coll.Metrics.GetChannelInfo()
		if pressure2 != 1 {
			t.Errorf("pressure after send = %d, want 1", pressure2)
		}
		// Drain
		<-ch
	})
}

// =============================================================================
// Value
// =============================================================================

func TestValue(t *testing.T) {
	t.Run("new value", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[int](p, "test_val")
		if v == nil {
			t.Fatal("NewValue returned nil")
		}
		if v.Pipeline != p {
			t.Error("Pipeline mismatch")
		}
		if v.Metrics == nil {
			t.Error("Metrics should not be nil")
		}
		if v.Metrics.Name != "test_val" {
			t.Errorf("Name = %q, want test_val", v.Metrics.Name)
		}
	})

	t.Run("resolve and wait", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[int](p, "v")
		v.Resolve(42)
		result := v.Wait()
		if result != 42 {
			t.Errorf("Wait() = %d, want 42", result)
		}
	})

	t.Run("wait already resolved", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[string](p, "v")
		v.Resolve("hello")
		// Second Wait should return immediately via fast path
		r1 := v.Wait()
		r2 := v.Wait()
		if r1 != "hello" || r2 != "hello" {
			t.Error("Wait should return resolved value")
		}
	})

	t.Run("double resolve panics ignored", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[int](p, "v")
		v.Resolve(1)
		v.Resolve(2) // should be ignored
		if v.Wait() != 1 {
			t.Errorf("Wait() = %d, want 1 (first resolve wins)", v.Wait())
		}
	})

	t.Run("ready channel", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[int](p, "v")
		ch := v.Ready()
		select {
		case <-ch:
			t.Error("Ready should not be signaled before Resolve")
		default:
		}
		v.Resolve(10)
		select {
		case <-ch:
			// ok
		default:
			t.Error("Ready should be signaled after Resolve")
		}
	})

	t.Run("get pipeline", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[int](p, "v")
		if v.GetPipeline() != p {
			t.Error("GetPipeline should return the pipeline")
		}
	})

	t.Run("metrics is resolved", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[int](p, "v")
		if v.Metrics.GetIsResolved() {
			t.Error("should not be resolved initially")
		}
		v.Resolve(5)
		if !v.Metrics.GetIsResolved() {
			t.Error("should be resolved after Resolve")
		}
	})

	t.Run("wait blocks until resolve", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[int](p, "v")
		done := make(chan int, 1)
		go func() {
			done <- v.Wait()
		}()
		// Value should block, so done should be empty
		select {
		case <-done:
			t.Error("Wait should block until Resolve")
		default:
		}
		v.Resolve(99)
		result := <-done
		if result != 99 {
			t.Errorf("result = %d, want 99", result)
		}
	})

	t.Run("wait slow path", func(t *testing.T) {
		p := NewPipeline()
		v := NewValue[int](p, "v")
		// Ensure done is false - this will take the slow path through the channel
		if v.done.Load() {
			t.Fatal("value should not be done initially")
		}
		// Resolve in a goroutine so we can test the blocking Wait path
		go func() {
			time.Sleep(10 * time.Millisecond)
			v.Resolve(77)
		}()
		result := v.Wait()
		if result != 77 {
			t.Errorf("Wait() = %d, want 77", result)
		}
	})
}

// =============================================================================
// Seed
// =============================================================================

func TestSeed(t *testing.T) {
	t.Run("data flows through", func(t *testing.T) {
		p := NewPipeline()
		opts := &options.SeedOptions{}
		coll := NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			for i := 0; i < 5; i++ {
				*emitter.GetEmitPointer() = i
			}
		}, opts)
		if coll == nil {
			t.Fatal("NewSeedCollection returned nil")
		}
		if coll.Pipeline != p {
			t.Error("Pipeline mismatch")
		}
		// Consume the collection
		var elemCount, batchCount atomic.Int64
		receiver := coll.GetReceiver(&elemCount, &batchCount)
		var values []int
		for v := range receiver.IterValues() {
			values = append(values, *v)
		}
		if len(values) != 5 {
			t.Errorf("values = %d, want 5", len(values))
		}
		for i, v := range values {
			if v != i {
				t.Errorf("values[%d] = %d, want %d", i, v, i)
			}
		}
		p.WaitForCompletion()
	})

	t.Run("empty seed", func(t *testing.T) {
		p := NewPipeline()
		opts := &options.SeedOptions{}
		coll := NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			// emit nothing
		}, opts)
		var ec, bc atomic.Int64
		receiver := coll.GetReceiver(&ec, &bc)
		var values []int
		for v := range receiver.IterValues() {
			values = append(values, *v)
		}
		if len(values) != 0 {
			t.Errorf("values = %d, want 0", len(values))
		}
		p.WaitForCompletion()
	})

	t.Run("custom options", func(t *testing.T) {
		p := NewPipeline()
		opts := &options.SeedOptions{
			OperationName:      parameters.Some("MySeed"),
			OutCollectionName:  parameters.Some("MySeeded"),
			OutBatchSize:       parameters.Some(4),
			OutChannelCapacity: parameters.Some(2),
		}
		coll := NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			*emitter.GetEmitPointer() = 100
		}, opts)
		if coll.Metrics.Name != "MySeeded" {
			t.Errorf("collection name = %q, want MySeeded", coll.Metrics.Name)
		}
		if coll.Metrics.BatchSize != 4 {
			t.Errorf("batch size = %d, want 4", coll.Metrics.BatchSize)
		}
		// Consume
		var ec, bc atomic.Int64
		receiver := coll.GetReceiver(&ec, &bc)
		var values []int
		for v := range receiver.IterValues() {
			values = append(values, *v)
		}
		if len(values) != 1 || values[0] != 100 {
			t.Errorf("values = %v, want [100]", values)
		}
		p.WaitForCompletion()
	})

	t.Run("user operator context", func(t *testing.T) {
		p := NewPipeline()
		var capturedCtx any
		opts := &options.SeedOptions{
			UserOperatorContext: parameters.Some[any]("my_op_ctx"),
		}
		coll := NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			capturedCtx = ctx.UserOperatorContext
			*emitter.GetEmitPointer() = 1
		}, opts)
		var ec, bc atomic.Int64
		receiver := coll.GetReceiver(&ec, &bc)
		for range receiver.IterValues() {
		}
		p.WaitForCompletion()
		if capturedCtx != "my_op_ctx" {
			t.Errorf("UserOperatorContext = %v, want my_op_ctx", capturedCtx)
		}
	})

	t.Run("channel info closure", func(t *testing.T) {
		p := NewPipeline()
		opts := &options.SeedOptions{
			OutChannelCapacity: parameters.Some(4),
		}
		coll := NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			// emit nothing
		}, opts)
		// Call GetChannelInfo to exercise the closure that captures outChannel in seed.go
		pressure, capacity := coll.Metrics.GetChannelInfo()
		if capacity != 4 {
			t.Errorf("capacity = %d, want 4", capacity)
		}
		// Pressure may be 0 or non-zero depending on timing; just ensure it doesn't panic
		_ = pressure
		// Consume
		var ec, bc atomic.Int64
		receiver := coll.GetReceiver(&ec, &bc)
		for range receiver.IterValues() {
		}
		p.WaitForCompletion()
	})
}

// =============================================================================
// Metrics Push
// =============================================================================

func TestMetricsPush(t *testing.T) {
	t.Run("start with url", func(t *testing.T) {
		p := NewPipeline()
		// Use a fake URL -- the pusher goroutine will fail silently on push attempts
		p.startMetricsPush("http://127.0.0.1:1/metrics", 1*time.Second, "test-job", "")
		if p.metricsPusher == nil {
			t.Fatal("metricsPusher should not be nil after startMetricsPush with valid URL")
		}
		p.stopMetricsPush()
		if p.metricsPusher != nil {
			t.Error("metricsPusher should be nil after stopMetricsPush")
		}
	})

	t.Run("stop", func(t *testing.T) {
		p := NewPipeline()
		// startMetricsPush with empty URL should be a no-op
		p.startMetricsPush("", 0, "", "")
		if p.metricsPusher != nil {
			t.Error("metricsPusher should be nil for empty URL")
		}
		// stopMetricsPush with nil pusher should not panic
		p.stopMetricsPush()
	})

	t.Run("empty url", func(t *testing.T) {
		p := NewPipeline()
		p.startMetricsPush("", 0, "", "")
		if p.metricsPusher != nil {
			t.Error("metricsPusher should be nil for empty URL")
		}
	})

	t.Run("new pipeline with parameters metrics push", func(t *testing.T) {
		params := parameters.NewParameters()
		source := func(key string) (string, bool) {
			switch key {
			case "metrics.push_url":
				return "http://127.0.0.1:1/metrics", true
			case "metrics.push_interval":
				return "1s", true
			default:
				return "", false
			}
		}
		if err := params.Metrics.LoadFromSource(source); err != nil {
			t.Fatalf("LoadFromSource failed: %v", err)
		}

		p := NewPipelineWithParameters(params)
		if p.metricsPusher == nil {
			t.Fatal("metricsPusher should not be nil when push URL is configured")
		}
		// Clean up: stop the pusher
		p.stopMetricsPush()
	})

	t.Run("negative interval", func(t *testing.T) {
		p := NewPipeline()
		p.startMetricsPush("http://example.com", -1*time.Second, "test-job", "")
		if p.metricsPusher != nil {
			t.Error("metricsPusher should be nil for negative interval")
		}
	})
}

func TestValueConcurrentWait(t *testing.T) {
	p := NewPipeline()
	v := NewValue[int](p, "concurrent")

	const numGoroutines = 100
	results := make([]int, numGoroutines)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Start many goroutines waiting for the value
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx] = v.Wait()
		}(i)
	}

	// Give goroutines time to start waiting
	time.Sleep(10 * time.Millisecond)

	// Resolve the value
	v.Resolve(42)

	wg.Wait()

	for i, r := range results {
		if r != 42 {
			t.Errorf("goroutine %d got %d, want 42", i, r)
		}
	}
}
