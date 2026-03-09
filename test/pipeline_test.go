package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func TestChainedMaps(t *testing.T) {
	// Seed(0-9) → Map(×2) → Map(+1) → collectToSliceValue → verify [1,3,5,7,9,11,13,15,17,19]
	pipeline := newTestPipeline(t)
	seed := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 10; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	doubled := gomr.Map(seed, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
		for value := range receiver.IterValues() {
			*emitter.GetEmitPointer() = *value * 2
		}
	})
	plusOne := gomr.Map(doubled, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
		for value := range receiver.IterValues() {
			*emitter.GetEmitPointer() = *value + 1
		}
	})
	collected := collectToSliceValue(plusOne)
	pipeline.WaitForCompletion()
	verifySliceValue(t, collected, func(yield func(int) bool) {
		for i := 0; i < 10; i++ {
			if !yield(i*2 + 1) {
				break
			}
		}
	})
}

func TestForkMapMerge(t *testing.T) {
	// Seed(0-4) → ForkTo2
	//   fork1 → Map(×2) → [0,2,4,6,8]
	//   fork2 → Map(×3) → [0,3,6,9,12]
	// Merge → collectToSliceValue → verify [0,0,2,3,4,6,6,8,9,12]
	pipeline := newTestPipeline(t)
	seed := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 5; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	fork1, fork2 := gomr.ForkTo2(seed)
	mapped1 := gomr.Map(fork1, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
		for value := range receiver.IterValues() {
			*emitter.GetEmitPointer() = *value * 2
		}
	})
	mapped2 := gomr.Map(fork2, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
		for value := range receiver.IterValues() {
			*emitter.GetEmitPointer() = *value * 3
		}
	})
	merged := gomr.Merge([]gomr.Collection[int]{mapped1, mapped2})
	collected := collectToSliceValue(merged)
	pipeline.WaitForCompletion()
	verifySliceValue(t, collected, func(yield func(int) bool) {
		// fork1 values: 0,2,4,6,8
		for i := 0; i < 5; i++ {
			if !yield(i * 2) {
				return
			}
		}
		// fork2 values: 0,3,6,9,12
		for i := 0; i < 5; i++ {
			if !yield(i * 3) {
				return
			}
		}
	})
}

func TestValueToCollectionFlow(t *testing.T) {
	// Seed(0-9) → Map(×2) → Collect(sum=90) → ToCollection → Map(×10) → collectToSliceValue → verify [900]
	pipeline := newTestPipeline(t)
	seed := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 10; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	doubled := gomr.Map(seed, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
		for value := range receiver.IterValues() {
			*emitter.GetEmitPointer() = *value * 2
		}
	})
	sumValue := gomr.Collect(doubled,
		func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
			sum := 0
			for value := range receiver.IterValues() {
				sum += *value
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
	sumCollection := gomr.ToCollection(sumValue)
	times10 := gomr.Map(sumCollection, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int]) {
		for value := range receiver.IterValues() {
			*emitter.GetEmitPointer() = *value * 10
		}
	})
	collected := collectToSliceValue(times10)
	pipeline.WaitForCompletion()
	verifySliceValue(t, collected, func(yield func(int) bool) {
		yield(900)
	})
}

func TestSideValueThreading(t *testing.T) {
	// Seed(0-9) → ForkTo2
	//   fork1 → Collect(count=10)   [side value]
	//   fork2 → CollectWithSideValue(count, func(receiver, count) → sum/count) → verify mean == 4
	pipeline := newTestPipeline(t)
	seed := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 10; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	fork1, fork2 := gomr.ForkTo2(seed)

	// fork1: count elements
	countValue := gomr.Collect(fork1,
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

	// fork2: compute mean using side value (count)
	meanValue := gomr.CollectWithSideValue(fork2, countValue,
		func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], count int) int {
			sum := 0
			for value := range receiver.IterValues() {
				sum += *value
			}
			return sum
		},
		func(ctx gomr.OperatorContext, intermediates []int, count int) int {
			total := 0
			for _, v := range intermediates {
				total += v
			}
			return total / count
		},
	)

	pipeline.WaitForCompletion()
	result := meanValue.Wait()
	// sum of 0..9 = 45, 45/10 = 4 (integer division)
	expected := 4
	if result != expected {
		t.Errorf("Expected mean %d, got %d", expected, result)
	}
}
