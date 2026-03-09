package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func TestMapValue(t *testing.T) {
	t.Run("MapValue 1 to 1", func(t *testing.T) {
		pipeline := newTestPipeline(t)
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 100; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		sumValue := gomr.Collect[int, int, int](
			initial,
			func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx gomr.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)
		doubled := gomr.MapValue(sumValue, func(ctx gomr.OperatorContext, v int) int {
			return v * 2
		})
		pipeline.WaitForCompletion()
		if doubled.Wait() != 9900 {
			t.Errorf("doubled sum = %d, want 9900", doubled.Wait())
		}
	})

	t.Run("MapValue 2 to 1 mean computation", func(t *testing.T) {
		pipeline := newTestPipeline(t)
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 100; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		fork1, fork2 := gomr.ForkTo2(initial)

		sumValue := gomr.Collect[int, int, int](
			fork1,
			func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx gomr.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)

		countValue := gomr.Collect[int, int, int](
			fork2,
			func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
				count := 0
				for range receiver.IterValues() {
					count++
				}
				return count
			},
			func(ctx gomr.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)

		mean := gomr.MapValue2To1(sumValue, countValue,
			func(ctx gomr.OperatorContext, sum int, count int) int {
				return sum / count
			},
		)
		pipeline.WaitForCompletion()
		if mean.Wait() != 49 {
			t.Errorf("mean = %d, want 49", mean.Wait())
		}
	})

	t.Run("MapValue 1 to 2", func(t *testing.T) {
		pipeline := newTestPipeline(t)
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 10; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		sumValue := gomr.Collect[int, int, int](
			initial,
			func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx gomr.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)
		out1, out2 := gomr.MapValue1To2(sumValue,
			func(ctx gomr.OperatorContext, v int) (int, int) {
				return v * 2, v * 3
			},
		)
		pipeline.WaitForCompletion()
		if out1.Wait() != 90 {
			t.Errorf("out1 = %d, want 90", out1.Wait())
		}
		if out2.Wait() != 135 {
			t.Errorf("out2 = %d, want 135", out2.Wait())
		}
	})
}

func TestToCollection(t *testing.T) {
	t.Run("ToCollection round-trip", func(t *testing.T) {
		pipeline := newTestPipeline(t)
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 5; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		sumValue := gomr.Collect[int, int, int](
			initial,
			func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx gomr.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)
		coll := gomr.ToCollection(sumValue)
		roundTripped := gomr.Collect[int, int, int](
			coll,
			func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx gomr.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)
		pipeline.WaitForCompletion()
		if roundTripped.Wait() != 10 {
			t.Errorf("round-tripped sum = %d, want 10", roundTripped.Wait())
		}
	})

	t.Run("ToCollection then Map", func(t *testing.T) {
		pipeline := newTestPipeline(t)
		initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
			for i := 0; i < 5; i++ {
				*emitter.GetEmitPointer() = i
			}
		})
		sumValue := gomr.Collect[int, int, int](
			initial,
			func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
				sum := 0
				for v := range receiver.IterValues() {
					sum += *v
				}
				return sum
			},
			func(ctx gomr.OperatorContext, intermediates []int) int {
				total := 0
				for _, v := range intermediates {
					total += v
				}
				return total
			},
		)
		coll := gomr.ToCollection(sumValue)
		mapped := gomr.Map(coll, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
			for v := range receiver.IterValues() {
				*emitter.GetEmitPointer() = *v * 3
			}
		})
		collected := collectToSliceValue(mapped)
		pipeline.WaitForCompletion()
		verifySliceValue(t, collected, func(yield func(int) bool) {
			yield(30)
		})
	})
}
