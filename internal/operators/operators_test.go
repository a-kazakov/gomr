package operators

// Tests use package-internal access for: seedInts()/collectSlice() helpers, extractShuffleParameters(), getShufflePath(), getEmptyReceiver(), intSerializer struct.

import (
	"sync/atomic"
	"testing"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
)

// seedInts creates a pipeline with a seed collection emitting values 0..n-1.
func seedInts(n int) (*pipeline.Pipeline, *pipeline.Collection[int]) {
	p := pipeline.NewPipeline()
	opts := &options.SeedOptions{}
	coll := pipeline.NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
		for i := 0; i < n; i++ {
			*emitter.GetEmitPointer() = i
		}
	}, opts)
	return p, coll
}

// collectSlice consumes a collection into a slice.
func collectSlice[T any](coll *pipeline.Collection[T]) []T {
	var ec, bc atomic.Int64
	receiver := coll.GetReceiver(&ec, &bc)
	var result []T
	for v := range receiver.IterValues() {
		result = append(result, *v)
	}
	return result
}

// intSerializer implements core.ElementSerializer[int] for testing SpillBuffer.
type intSerializer struct{}

func (s intSerializer) MarshalElementToBytes(value *int, dest []byte) int {
	v := *value
	dest[0] = byte(v)
	dest[1] = byte(v >> 8)
	dest[2] = byte(v >> 16)
	dest[3] = byte(v >> 24)
	dest[4] = byte(v >> 32)
	dest[5] = byte(v >> 40)
	dest[6] = byte(v >> 48)
	dest[7] = byte(v >> 56)
	return 8
}

func (s intSerializer) UnmarshalElementFromBytes(data []byte, dest *int) {
	*dest = int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24 |
		int(data[4])<<32 | int(data[5])<<40 | int(data[6])<<48 | int(data[7])<<56
}

// =============================================================================
// Map
// =============================================================================

func TestMap(t *testing.T) {
	t.Run("map to 1", func(t *testing.T) {
		p, coll := seedInts(5)
		opts := options.ApplyMapOptions(options.WithParallelism(1))
		mapped := MapTo1(
			coll,
			func(ctx *core.OperatorContext, receiver core.CollectionReceiver[int], e0 *primitives.Emitter[int]) {
				for v := range receiver.IterValues() {
					*e0.GetEmitPointer() = *v * 2
				}
			},
			opts,
		)
		result := collectSlice(mapped)
		p.WaitForCompletion()
		if len(result) != 5 {
			t.Fatalf("result = %d, want 5", len(result))
		}
		for i, v := range result {
			if v != i*2 {
				t.Errorf("result[%d] = %d, want %d", i, v, i*2)
			}
		}
	})

	t.Run("map to 1 parallel", func(t *testing.T) {
		p, coll := seedInts(100)
		opts := options.ApplyMapOptions(options.WithParallelism(4))
		mapped := MapTo1(
			coll,
			func(ctx *core.OperatorContext, receiver core.CollectionReceiver[int], e0 *primitives.Emitter[int]) {
				for v := range receiver.IterValues() {
					*e0.GetEmitPointer() = *v + 1
				}
			},
			opts,
		)
		result := collectSlice(mapped)
		p.WaitForCompletion()
		if len(result) != 100 {
			t.Fatalf("result = %d, want 100", len(result))
		}
	})

	t.Run("map to 2 with side value", func(t *testing.T) {
		p, coll := seedInts(4)
		sideValue := pipeline.NewValue[int](p, "multiplier")
		sideValue.Resolve(10)
		opts := options.ApplyMapOptions(options.WithParallelism(1))
		c0, c1 := MapTo2WithSideValue(
			coll,
			sideValue,
			func(ctx *core.OperatorContext, receiver core.CollectionReceiver[int], e0 *primitives.Emitter[int], e1 *primitives.Emitter[string], sv int) {
				for v := range receiver.IterValues() {
					*e0.GetEmitPointer() = *v * sv
					*e1.GetEmitPointer() = "ok"
				}
			},
			opts,
		)
		r0 := collectSlice(c0)
		r1 := collectSlice(c1)
		p.WaitForCompletion()
		if len(r0) != 4 {
			t.Errorf("r0 len = %d, want 4", len(r0))
		}
		if len(r1) != 4 {
			t.Errorf("r1 len = %d, want 4", len(r1))
		}
		// With multiplier=10: values should be 0,10,20,30
		seen := make(map[int]bool)
		for _, v := range r0 {
			seen[v] = true
		}
		for _, expected := range []int{0, 10, 20, 30} {
			if !seen[expected] {
				t.Errorf("missing value %d in r0", expected)
			}
		}
	})

	t.Run("map value 3 to 2", func(t *testing.T) {
		p := pipeline.NewPipeline()
		v0 := pipeline.NewValue[int](p, "a")
		v1 := pipeline.NewValue[int](p, "b")
		v2 := pipeline.NewValue[int](p, "c")
		v0.Resolve(10)
		v1.Resolve(20)
		v2.Resolve(30)
		opts := options.ApplyMapValueOptions()
		out0, out1 := MapValue3To2(v0, v1, v2,
			func(ctx *core.OperatorContext, a int, b int, c int) (int, string) {
				return a + b + c, "sum"
			},
			opts,
		)
		p.WaitForCompletion()
		if out0.Wait() != 60 {
			t.Errorf("out0 = %d, want 60", out0.Wait())
		}
		if out1.Wait() != "sum" {
			t.Errorf("out1 = %q, want %q", out1.Wait(), "sum")
		}
	})
}

// =============================================================================
// Fork
// =============================================================================

func TestFork(t *testing.T) {
	t.Run("fork to any", func(t *testing.T) {
		p, coll := seedInts(5)
		opts := options.ApplyForkOptions()
		forks := ForkToAny(coll, 3, opts)
		if len(forks) != 3 {
			t.Fatalf("forks = %d, want 3", len(forks))
		}
		// Consume all forks
		results := make([][]int, 3)
		for i, f := range forks {
			results[i] = collectSlice(f)
		}
		p.WaitForCompletion()
		for i, r := range results {
			if len(r) != 5 {
				t.Errorf("fork[%d] got %d values, want 5", i, len(r))
			}
		}
	})

	t.Run("fork to any 2-way", func(t *testing.T) {
		p, coll := seedInts(3)
		opts := options.ApplyForkOptions()
		forks := ForkToAny(coll, 2, opts)
		r0 := collectSlice(forks[0])
		r1 := collectSlice(forks[1])
		p.WaitForCompletion()
		if len(r0) != 3 || len(r1) != 3 {
			t.Errorf("r0=%d, r1=%d, want 3 each", len(r0), len(r1))
		}
	})

	t.Run("fork to 2 generated", func(t *testing.T) {
		p, coll := seedInts(5)
		opts := options.ApplyForkOptions()
		f0, f1 := ForkTo2(coll, opts)
		r0 := collectSlice(f0)
		r1 := collectSlice(f1)
		p.WaitForCompletion()
		if len(r0) != 5 || len(r1) != 5 {
			t.Errorf("r0=%d, r1=%d, want 5", len(r0), len(r1))
		}
	})
}

// =============================================================================
// Merge
// =============================================================================

func TestMerge(t *testing.T) {
	t.Run("merge 2", func(t *testing.T) {
		p, coll := seedInts(5)
		opts := options.ApplyForkOptions()
		forks := ForkToAny(coll, 2, opts)
		mergeOpts := options.ApplyMergeOptions()
		merged := mergeFanIn([]*pipeline.Collection[int]{forks[0], forks[1]}, mergeOpts)
		result := collectSlice(merged)
		p.WaitForCompletion()
		// Each value appears twice (forked then merged)
		if len(result) != 10 {
			t.Errorf("merged = %d, want 10", len(result))
		}
	})

	t.Run("merge generated", func(t *testing.T) {
		p, coll := seedInts(5)
		opts := options.ApplyForkOptions()
		f0, f1 := ForkTo2(coll, opts)
		mergeOpts := options.ApplyMergeOptions()
		merged := Merge([]*pipeline.Collection[int]{f0, f1}, mergeOpts)
		result := collectSlice(merged)
		p.WaitForCompletion()
		if len(result) != 10 {
			t.Errorf("merged = %d, want 10", len(result))
		}
	})

	t.Run("merge different batch sizes", func(t *testing.T) {
		p := pipeline.NewPipeline()
		// Create two seed collections with different batch sizes
		seedOpts1 := options.ApplySeedOptions(options.WithOutBatchSize(4))
		coll1 := pipeline.NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			for i := 0; i < 5; i++ {
				*emitter.GetEmitPointer() = i
			}
		}, seedOpts1)

		seedOpts2 := options.ApplySeedOptions(options.WithOutBatchSize(8))
		coll2 := pipeline.NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			for i := 10; i < 15; i++ {
				*emitter.GetEmitPointer() = i
			}
		}, seedOpts2)

		mergeOpts := options.ApplyMergeOptions()
		merged := mergeFanIn([]*pipeline.Collection[int]{coll1, coll2}, mergeOpts)
		result := collectSlice(merged)
		p.WaitForCompletion()
		if len(result) != 10 {
			t.Errorf("merged = %d, want 10", len(result))
		}
	})
}

// =============================================================================
// Collect
// =============================================================================

func TestCollect(t *testing.T) {
	t.Run("basic collect", func(t *testing.T) {
		p, coll := seedInts(5)
		opts := options.ApplyCollectOptions(options.WithParallelism(1))
		value := Collect[int, int, int](
			coll,
			func(ctx *core.OperatorContext, receiver core.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx *core.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
			opts,
		)
		p.WaitForCompletion()
		if value.Wait() != 10 { // 0+1+2+3+4=10
			t.Errorf("sum = %d, want 10", value.Wait())
		}
	})

	t.Run("parallel collect", func(t *testing.T) {
		p, coll := seedInts(100)
		opts := options.ApplyCollectOptions(options.WithParallelism(4))
		value := Collect[int, int, int](
			coll,
			func(ctx *core.OperatorContext, receiver core.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx *core.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
			opts,
		)
		p.WaitForCompletion()
		if value.Wait() != 4950 { // sum(0..99) = 4950
			t.Errorf("sum = %d, want 4950", value.Wait())
		}
	})

	t.Run("with side value", func(t *testing.T) {
		p, coll := seedInts(5)

		// Create a side value that resolves to a multiplier
		sideValue := pipeline.NewValue[int](p, "multiplier")
		sideValue.Resolve(10)

		opts := options.ApplyCollectOptions(options.WithParallelism(1))
		value := CollectWithSideValue[int, int, int, int](
			coll,
			sideValue,
			func(ctx *core.OperatorContext, receiver core.CollectionReceiver[int], mult int) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v * mult
				}
				return sum
			},
			func(ctx *core.OperatorContext, intermediates []int, mult int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total + mult // add mult to distinguish from Collect
			},
			opts,
		)
		p.WaitForCompletion()
		// sum(0..4)*10 = 100, plus mult 10 = 110
		if value.Wait() != 110 {
			t.Errorf("sum = %d, want 110", value.Wait())
		}
	})

	t.Run("with side value parallel", func(t *testing.T) {
		p, coll := seedInts(100)

		sideValue := pipeline.NewValue[int](p, "offset")
		sideValue.Resolve(1)

		opts := options.ApplyCollectOptions(options.WithParallelism(4))
		value := CollectWithSideValue[int, int, int, int](
			coll,
			sideValue,
			func(ctx *core.OperatorContext, receiver core.CollectionReceiver[int], offset int) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v + offset
				}
				return sum
			},
			func(ctx *core.OperatorContext, intermediates []int, offset int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
			opts,
		)
		p.WaitForCompletion()
		// sum(0..99) + 100*1 = 4950 + 100 = 5050
		if value.Wait() != 5050 {
			t.Errorf("sum = %d, want 5050", value.Wait())
		}
	})
}

// =============================================================================
// ToCollection
// =============================================================================

func TestToCollection(t *testing.T) {
	t.Run("round trip", func(t *testing.T) {
		p, coll := seedInts(5)
		collectOpts := options.ApplyCollectOptions(options.WithParallelism(1))
		value := Collect[int, int, int](
			coll,
			func(ctx *core.OperatorContext, receiver core.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx *core.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
			collectOpts,
		)
		toCollOpts := options.ApplyToCollectionOptions()
		outColl := ToCollection(value, toCollOpts)
		result := collectSlice(outColl)
		p.WaitForCompletion()
		if len(result) != 1 || result[0] != 10 {
			t.Errorf("result = %v, want [10]", result)
		}
	})
}

// =============================================================================
// SpillBuffer
// =============================================================================

func TestSpillBuffer(t *testing.T) {
	t.Run("passthrough", func(t *testing.T) {
		// Test SpillBuffer with a large enough channel so everything goes through hot path.
		// Don't set OutBatchSize or SpillDirectories to cover the fallback paths.
		p := pipeline.NewPipeline()
		seedOpts := &options.SeedOptions{}
		n := 50
		coll := pipeline.NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			for i := 0; i < n; i++ {
				*emitter.GetEmitPointer() = i
			}
		}, seedOpts)

		// No WithSpillDirectories and no WithOutBatchSize -- exercises fallback logic
		spillOpts := options.ApplySpillBufferOptions()
		spilled := SpillBuffer[intSerializer](coll, spillOpts)
		result := collectSlice(spilled)
		p.WaitForCompletion()

		if len(result) != n {
			t.Errorf("got %d values, want %d", len(result), n)
		}
	})

	t.Run("cold path", func(t *testing.T) {
		// Force cold path by filling the output channel before reading.
		// We seed data via a collection, but don't start consuming the SpillBuffer
		// output until after all data is seeded. With outChannelCapacity=0
		// and a blocked reader, the non-blocking select always falls to default (cold path).
		p := pipeline.NewPipeline()
		n := 100

		// Use a small seed batch size so we get many batches
		seedOpts := options.ApplySeedOptions(options.WithOutBatchSize(5))
		seeded := make(chan struct{})
		coll := pipeline.NewSeedCollection[int](p, func(ctx *core.OperatorContext, emitter *primitives.Emitter[int]) {
			for i := 0; i < n; i++ {
				*emitter.GetEmitPointer() = i
			}
			// Signal that seeding is done so we can start consuming after cold path is forced
			close(seeded)
		}, seedOpts)

		tmpDir := t.TempDir()
		spillOpts := options.ApplySpillBufferOptions(
			options.WithSpillDirectories(tmpDir),
			options.WithOutChannelCapacity(0), // synchronous channel -- hot path select fails when reader isn't waiting
			options.WithOutBatchSize(5),
			options.WithSpillWriteParallelism(1),
			options.WithSpillReadParallelism(1),
			options.WithMaxSpillFileSize(50), // tiny file limit to force multiple files
		)
		spilled := SpillBuffer[intSerializer](coll, spillOpts)

		// Wait for seeding to complete before starting to read.
		// This ensures the writer goroutine processes batches while the reader
		// side of outChannel has no consumer, forcing the cold path.
		<-seeded

		result := collectSlice(spilled)
		p.WaitForCompletion()

		if len(result) != n {
			t.Errorf("got %d values, want %d", len(result), n)
		}

		// Verify all values are present (order may differ due to hot/cold paths)
		seen := make(map[int]bool)
		for _, v := range result {
			seen[v] = true
		}
		for i := 0; i < n; i++ {
			if !seen[i] {
				t.Errorf("missing value %d", i)
			}
		}
	})
}

// =============================================================================
// Shuffle helpers
// =============================================================================

func TestShuffleHelpers(t *testing.T) {
	t.Run("extract parameters", func(t *testing.T) {
		p := pipeline.NewPipeline()
		opts := options.ApplyShuffleOptions()
		numShards, scatterParallelism, gatherParallelism, outBatchSize,
			localShuffleBufferSize, localShuffleBufferSizeJitter,
			fileMergeThreshold, readBufferSize, writeBufferSize,
			targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)

		// Check that defaults are reasonable (non-zero)
		if numShards <= 0 {
			t.Errorf("numShards = %d, want > 0", numShards)
		}
		if scatterParallelism <= 0 {
			t.Errorf("scatterParallelism = %d, want > 0", scatterParallelism)
		}
		if gatherParallelism <= 0 {
			t.Errorf("gatherParallelism = %d, want > 0", gatherParallelism)
		}
		if outBatchSize <= 0 {
			t.Errorf("outBatchSize = %d, want > 0", outBatchSize)
		}
		if localShuffleBufferSize <= 0 {
			t.Errorf("localShuffleBufferSize = %d, want > 0", localShuffleBufferSize)
		}
		if localShuffleBufferSizeJitter < 0 {
			t.Errorf("localShuffleBufferSizeJitter = %f, want >= 0", localShuffleBufferSizeJitter)
		}
		if fileMergeThreshold <= 0 {
			t.Errorf("fileMergeThreshold = %d, want > 0", fileMergeThreshold)
		}
		if readBufferSize <= 0 {
			t.Errorf("readBufferSize = %d, want > 0", readBufferSize)
		}
		if writeBufferSize <= 0 {
			t.Errorf("writeBufferSize = %d, want > 0", writeBufferSize)
		}
		if targetWriteLatency <= 0 {
			t.Errorf("targetWriteLatency = %v, want > 0", targetWriteLatency)
		}
		if len(scratchSpacePaths) == 0 {
			t.Error("scratchSpacePaths should not be empty")
		}
	})

	t.Run("extract with overrides", func(t *testing.T) {
		p := pipeline.NewPipeline()
		customPaths := []string{"/tmp/a", "/tmp/b"}
		opts := options.ApplyShuffleOptions(
			options.WithScratchSpacePaths(customPaths...),
		)
		_, _, _, _, _, _, _, _, _, _, scratchSpacePaths := extractShuffleParameters(p, opts)
		if len(scratchSpacePaths) != 2 || scratchSpacePaths[0] != "/tmp/a" || scratchSpacePaths[1] != "/tmp/b" {
			t.Errorf("scratchSpacePaths = %v, want [/tmp/a /tmp/b]", scratchSpacePaths)
		}
	})

	t.Run("get path", func(t *testing.T) {
		paths := []string{"/scratch1", "/scratch2", "/scratch3"}

		// goroutineIndex 0 -> paths[0]
		result0 := getShufflePath(paths, "job123", "shuffle456", 0)
		if result0 != "/scratch1/gomr/job123/shuffles/shuffle456/0000" {
			t.Errorf("getShufflePath(0) = %q", result0)
		}

		// goroutineIndex 1 -> paths[1]
		result1 := getShufflePath(paths, "job123", "shuffle456", 1)
		if result1 != "/scratch2/gomr/job123/shuffles/shuffle456/0001" {
			t.Errorf("getShufflePath(1) = %q", result1)
		}

		// goroutineIndex 3 -> paths[0] (wraps around)
		result3 := getShufflePath(paths, "job123", "shuffle456", 3)
		if result3 != "/scratch1/gomr/job123/shuffles/shuffle456/0003" {
			t.Errorf("getShufflePath(3) = %q", result3)
		}
	})

	t.Run("get empty receiver", func(t *testing.T) {
		receiver := getEmptyReceiver[int]()
		count := 0
		for range receiver.IterValues() {
			count++
		}
		if count != 0 {
			t.Errorf("empty receiver should yield 0 values, got %d", count)
		}
	})

	t.Run("ignore", func(t *testing.T) {
		p, coll := seedInts(10)
		opts := options.ApplyIgnoreOptions()
		Ignore(coll, opts)
		p.WaitForCompletion()
		// No panic means success
	})
}
