package gomr

import (
	"cmp"
	"encoding/binary"
	"os"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/parameters"
)

func sortedEqual[T cmp.Ordered](t *testing.T, got []T, want []T) {
	t.Helper()
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// intSerializer implements core.ElementSerializer[int] for SpillBuffer tests.
type intSerializer struct{}

func (s *intSerializer) MarshalElementToBytes(value *int, dest []byte) int {
	binary.NativeEndian.PutUint64(dest, uint64(*value))
	return 8
}

func (s *intSerializer) UnmarshalElementFromBytes(data []byte, dest *int) {
	*dest = int(binary.NativeEndian.Uint64(data))
}

// helper: creates a pipeline and seeds ints 0..n-1
func seedTestInts(n int) (*pipeline.Pipeline, *pipeline.Collection[int]) {
	p := NewPipeline()
	coll := NewSeedCollection(p, func(ctx OperatorContext, emitter Emitter[int]) {
		for i := 0; i < n; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	return p, coll
}

// helper: collect a collection into a slice via raw receiver
func collectTestSlice[T any](coll *pipeline.Collection[T]) []T {
	var ec, bc atomic.Int64
	receiver := coll.GetReceiver(&ec, &bc)
	var result []T
	for v := range receiver.IterValues() {
		result = append(result, *v)
	}
	return result
}

func TestPipeline(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		p := NewPipeline()
		if p == nil {
			t.Fatal("NewPipeline returned nil")
		}
	})

	t.Run("with parameters", func(t *testing.T) {
		params := parameters.NewParameters()
		p := NewPipelineWithParameters(params)
		if p == nil {
			t.Fatal("NewPipelineWithParameters returned nil")
		}
	})
}

func TestSeedAndMap(t *testing.T) {
	t.Run("seed and collect sum", func(t *testing.T) {
		p, coll := seedTestInts(5)
		val := Collect[int, int, int](
			coll,
			func(ctx OperatorContext, receiver CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)
		p.WaitForCompletion()
		if val.Wait() != 10 {
			t.Errorf("sum = %d, want 10", val.Wait())
		}
	})

	t.Run("map triples", func(t *testing.T) {
		p, coll := seedTestInts(5)
		mapped := Map(coll, func(ctx OperatorContext, receiver CollectionReceiver[int], emitter Emitter[int]) {
			for v := range receiver.IterValues() {
				*emitter.GetEmitPointer() = *v * 3
			}
		})
		result := collectTestSlice(mapped)
		p.WaitForCompletion()
		sortedEqual(t, result, []int{0, 3, 6, 9, 12})
	})

	t.Run("map with options", func(t *testing.T) {
		p, coll := seedTestInts(10)
		mapped := Map(coll,
			func(ctx OperatorContext, receiver CollectionReceiver[int], emitter Emitter[int]) {
				for v := range receiver.IterValues() {
					*emitter.GetEmitPointer() = *v + 1
				}
			},
			WithParallelism(2),
			WithOperationName("double-map"),
		)
		result := collectTestSlice(mapped)
		p.WaitForCompletion()
		sortedEqual(t, result, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	})

	t.Run("seed with options", func(t *testing.T) {
		p := NewPipeline()
		coll := NewSeedCollection(p, func(ctx OperatorContext, emitter Emitter[string]) {
			*emitter.GetEmitPointer() = "hello"
		}, WithOperationName("test-seed"), WithOutCollectionName("strings"))
		result := collectTestSlice(coll)
		p.WaitForCompletion()
		if len(result) != 1 || result[0] != "hello" {
			t.Errorf("result = %v, want [hello]", result)
		}
	})
}

func TestForkAndMerge(t *testing.T) {
	t.Run("fork to any", func(t *testing.T) {
		p, coll := seedTestInts(4)
		forks := ForkToAny(coll, 2)
		r0 := collectTestSlice(forks[0])
		r1 := collectTestSlice(forks[1])
		p.WaitForCompletion()
		sortedEqual(t, r0, []int{0, 1, 2, 3})
		sortedEqual(t, r1, []int{0, 1, 2, 3})
	})

	t.Run("fork to 2", func(t *testing.T) {
		p, coll := seedTestInts(3)
		f0, f1 := ForkTo2(coll)
		r0 := collectTestSlice(f0)
		r1 := collectTestSlice(f1)
		p.WaitForCompletion()
		sortedEqual(t, r0, []int{0, 1, 2})
		sortedEqual(t, r1, []int{0, 1, 2})
	})

	t.Run("merge", func(t *testing.T) {
		p, coll := seedTestInts(5)
		forks := ForkToAny(coll, 2)
		merged := Merge(forks)
		result := collectTestSlice(merged)
		p.WaitForCompletion()
		sortedEqual(t, result, []int{0, 0, 1, 1, 2, 2, 3, 3, 4, 4})
	})
}

func TestCollectors(t *testing.T) {
	t.Run("to collection", func(t *testing.T) {
		p, coll := seedTestInts(3)
		val := Collect[int, int, int](
			coll,
			func(ctx OperatorContext, receiver CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)
		outColl := ToCollection(val)
		result := collectTestSlice(outColl)
		p.WaitForCompletion()
		if len(result) != 1 || result[0] != 3 {
			t.Errorf("result = %v, want [3]", result)
		}
	})

	t.Run("map value", func(t *testing.T) {
		p, coll := seedTestInts(3)
		val := Collect[int, int, int](
			coll,
			func(ctx OperatorContext, receiver CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)
		doubled := MapValue(val, func(ctx OperatorContext, v int) int {
			return v * 2
		})
		p.WaitForCompletion()
		if doubled.Wait() != 6 {
			t.Errorf("doubled = %d, want 6", doubled.Wait())
		}
	})

	t.Run("collect with side value", func(t *testing.T) {
		p, coll := seedTestInts(5)
		forks := ForkToAny(coll, 2)

		multiplier := Collect[int, int, int](
			forks[0],
			func(ctx OperatorContext, receiver CollectionReceiver[int]) int {
				count := 0
				for range receiver.IterValues() {
					count++
				}
				return count
			},
			func(ctx OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)

		val := CollectWithSideValue[int, int, int, int](
			forks[1],
			multiplier,
			func(ctx OperatorContext, receiver CollectionReceiver[int], sv int) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v * sv
				}
				return sum
			},
			func(ctx OperatorContext, intermediates []int, sv int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)

		p.WaitForCompletion()
		// multiplier = count(0..4) = 5, sum = (0+1+2+3+4)*5 = 50
		if val.Wait() != 50 {
			t.Errorf("result = %d, want 50", val.Wait())
		}
	})
}

func TestAdvanced(t *testing.T) {
	t.Run("map to 2", func(t *testing.T) {
		p, coll := seedTestInts(4)
		c0, c1 := MapTo2(coll, func(ctx OperatorContext, receiver CollectionReceiver[int], e0 *primitives.Emitter[int], e1 *primitives.Emitter[int]) {
			for v := range receiver.IterValues() {
				*e0.GetEmitPointer() = *v
				*e1.GetEmitPointer() = *v * 10
			}
		})
		r0 := collectTestSlice(c0)
		r1 := collectTestSlice(c1)
		p.WaitForCompletion()
		sortedEqual(t, r0, []int{0, 1, 2, 3})
		sortedEqual(t, r1, []int{0, 10, 20, 30})
	})

	t.Run("map to 2 with side value", func(t *testing.T) {
		p, coll := seedTestInts(4)
		forks := ForkToAny(coll, 2)

		multiplier := Collect[int, int, int](
			forks[0],
			func(ctx OperatorContext, receiver CollectionReceiver[int]) int {
				count := 0
				for range receiver.IterValues() {
					count++
				}
				return count
			},
			func(ctx OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)

		c0, c1 := MapTo2WithSideValue(
			forks[1],
			multiplier,
			func(ctx OperatorContext, receiver CollectionReceiver[int], e0 *primitives.Emitter[int], e1 *primitives.Emitter[string], sv int) {
				for v := range receiver.IterValues() {
					*e0.GetEmitPointer() = *v * sv
					*e1.GetEmitPointer() = "ok"
				}
			},
		)
		r0 := collectTestSlice(c0)
		r1 := collectTestSlice(c1)
		p.WaitForCompletion()
		sortedEqual(t, r0, []int{0, 4, 8, 12})
		sortedEqual(t, r1, []string{"ok", "ok", "ok", "ok"})
	})

	t.Run("map with side value", func(t *testing.T) {
		p, coll := seedTestInts(5)
		forks := ForkToAny(coll, 2)

		multiplier := Collect[int, int, int](
			forks[0],
			func(ctx OperatorContext, receiver CollectionReceiver[int]) int {
				count := 0
				for range receiver.IterValues() {
					count++
				}
				return count
			},
			func(ctx OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)

		mapped := MapWithSideValue(
			forks[1],
			multiplier,
			func(ctx OperatorContext, receiver CollectionReceiver[int], emitter Emitter[int], sv int) {
				for v := range receiver.IterValues() {
					*emitter.GetEmitPointer() = *v * sv
				}
			},
		)
		result := collectTestSlice(mapped)
		p.WaitForCompletion()
		// multiplier = 5, each value multiplied by 5: [0,5,10,15,20]
		sortedEqual(t, result, []int{0, 5, 10, 15, 20})
	})

	t.Run("map value 3 to 2", func(t *testing.T) {
		p := NewPipeline()
		v0 := pipeline.NewValue[int](p, "a")
		v1 := pipeline.NewValue[int](p, "b")
		v2 := pipeline.NewValue[int](p, "c")
		v0.Resolve(10)
		v1.Resolve(20)
		v2.Resolve(30)
		out0, out1 := MapValue3To2(v0, v1, v2,
			func(ctx *core.OperatorContext, a int, b int, c int) (int, string) {
				return a + b + c, "sum"
			},
		)
		p.WaitForCompletion()
		if out0.Wait() != 60 {
			t.Errorf("out0 = %d, want 60", out0.Wait())
		}
		if out1.Wait() != "sum" {
			t.Errorf("out1 = %q, want %q", out1.Wait(), "sum")
		}
	})

	t.Run("spill buffer", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "gomr-spill-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(tmpDir)

		p := NewPipeline()
		coll := NewSeedCollection(p, func(ctx OperatorContext, emitter Emitter[int]) {
			for i := 0; i < 10; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		spilled := SpillBuffer[*intSerializer](coll, WithSpillDirectories(tmpDir))
		result := collectTestSlice(spilled)
		p.WaitForCompletion()
		sortedEqual(t, result, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	})

	t.Run("ignore", func(t *testing.T) {
		p, coll := seedTestInts(10)
		Ignore(coll)
		p.WaitForCompletion()
	})
}

func TestReExports(t *testing.T) {
	t.Run("Some and None", func(t *testing.T) {
		s := Some(42)
		if !s.IsSet() {
			t.Error("Some should be set")
		}
		if s.Get() != 42 {
			t.Errorf("Some.Get() = %d, want 42", s.Get())
		}
		n := None[int]()
		if n.IsSet() {
			t.Error("None should not be set")
		}
	})

	t.Run("operation kinds", func(t *testing.T) {
		// Verify that re-exported constants match core
		if OPERATION_KIND_SEED != core.OPERATION_KIND_SEED {
			t.Error("OPERATION_KIND_SEED mismatch")
		}
		if OPERATION_KIND_MAP != core.OPERATION_KIND_MAP {
			t.Error("OPERATION_KIND_MAP mismatch")
		}
		if OPERATION_KIND_SHUFFLE != core.OPERATION_KIND_SHUFFLE {
			t.Error("OPERATION_KIND_SHUFFLE mismatch")
		}
		if OPERATION_KIND_FORK != core.OPERATION_KIND_FORK {
			t.Error("OPERATION_KIND_FORK mismatch")
		}
		if OPERATION_KIND_MERGE != core.OPERATION_KIND_MERGE {
			t.Error("OPERATION_KIND_MERGE mismatch")
		}
		if OPERATION_KIND_COLLECT != core.OPERATION_KIND_COLLECT {
			t.Error("OPERATION_KIND_COLLECT mismatch")
		}
		if OPERATION_KIND_TO_COLLECTION != core.OPERATION_KIND_TO_COLLECTION {
			t.Error("OPERATION_KIND_TO_COLLECTION mismatch")
		}
		if OPERATION_KIND_SPILL_BUFFER != core.OPERATION_KIND_SPILL_BUFFER {
			t.Error("OPERATION_KIND_SPILL_BUFFER mismatch")
		}
	})

	t.Run("phases", func(t *testing.T) {
		if OPERATION_PHASE_PENDING != core.OPERATION_PHASE_PENDING {
			t.Error("OPERATION_PHASE_PENDING mismatch")
		}
		if OPERATION_PHASE_RUNNING != core.OPERATION_PHASE_RUNNING {
			t.Error("OPERATION_PHASE_RUNNING mismatch")
		}
		if OPERATION_PHASE_COMPLETED != core.OPERATION_PHASE_COMPLETED {
			t.Error("OPERATION_PHASE_COMPLETED mismatch")
		}
	})

	t.Run("StopEmitting", func(t *testing.T) {
		if StopEmitting != core.StopEmitting {
			t.Error("StopEmitting mismatch")
		}
	})
}
