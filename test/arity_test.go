package test

import (
	"testing"

	"github.com/a-kazakov/gomr"
)

func createSingleValue(pipeline gomr.Pipeline, v int) gomr.Value[int] {
	collection := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		*emitter.GetEmitPointer() = v
	})
	return gomr.Collect(collection, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) int {
		var result int
		for value := range receiver.IterValues() {
			result = *value
		}
		return result
	}, func(ctx gomr.OperatorContext, intermediates []int) int {
		sum := 0
		for _, v := range intermediates {
			sum += v
		}
		return sum
	})
}

func TestForkTo3(t *testing.T) {
	pipeline, _ := newTestPipeline(t)
	initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 5; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	f1, f2, f3 := gomr.ForkTo3(initial)
	c1 := collectToSliceValue(f1)
	c2 := collectToSliceValue(f2)
	c3 := collectToSliceValue(f3)
	pipeline.WaitForCompletion()
	expected := func(yield func(value int) bool) {
		for i := 0; i < 5; i++ {
			if !yield(i) {
				break
			}
		}
	}
	verifySliceValue(t, c1, expected)
	verifySliceValue(t, c2, expected)
	verifySliceValue(t, c3, expected)
}

func TestForkTo4(t *testing.T) {
	pipeline, _ := newTestPipeline(t)
	initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 5; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	f1, f2, f3, f4 := gomr.ForkTo4(initial)
	c1 := collectToSliceValue(f1)
	c2 := collectToSliceValue(f2)
	c3 := collectToSliceValue(f3)
	c4 := collectToSliceValue(f4)
	pipeline.WaitForCompletion()
	expected := func(yield func(value int) bool) {
		for i := 0; i < 5; i++ {
			if !yield(i) {
				break
			}
		}
	}
	verifySliceValue(t, c1, expected)
	verifySliceValue(t, c2, expected)
	verifySliceValue(t, c3, expected)
	verifySliceValue(t, c4, expected)
}

func TestForkTo5(t *testing.T) {
	pipeline, _ := newTestPipeline(t)
	initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 5; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	f1, f2, f3, f4, f5 := gomr.ForkTo5(initial)
	c1 := collectToSliceValue(f1)
	c2 := collectToSliceValue(f2)
	c3 := collectToSliceValue(f3)
	c4 := collectToSliceValue(f4)
	c5 := collectToSliceValue(f5)
	pipeline.WaitForCompletion()
	expected := func(yield func(value int) bool) {
		for i := 0; i < 5; i++ {
			if !yield(i) {
				break
			}
		}
	}
	verifySliceValue(t, c1, expected)
	verifySliceValue(t, c2, expected)
	verifySliceValue(t, c3, expected)
	verifySliceValue(t, c4, expected)
	verifySliceValue(t, c5, expected)
}

func TestMapTo4(t *testing.T) {
	pipeline, _ := newTestPipeline(t)
	initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 10; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	evens, odds, doubled, negated := gomr.MapTo4(initial,
		func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitEven gomr.Emitter[int], emitOdd gomr.Emitter[int], emitDoubled gomr.Emitter[int], emitNegated gomr.Emitter[int]) {
			for value := range receiver.IterValues() {
				if *value%2 == 0 {
					*emitEven.GetEmitPointer() = *value
				} else {
					*emitOdd.GetEmitPointer() = *value
				}
				*emitDoubled.GetEmitPointer() = *value * 2
				*emitNegated.GetEmitPointer() = -*value
			}
		},
	)
	cEvens := collectToSliceValue(evens)
	cOdds := collectToSliceValue(odds)
	cDoubled := collectToSliceValue(doubled)
	cNegated := collectToSliceValue(negated)
	pipeline.WaitForCompletion()

	verifySliceValue(t, cEvens, func(yield func(value int) bool) {
		for i := 0; i < 10; i += 2 {
			if !yield(i) {
				break
			}
		}
	})
	verifySliceValue(t, cOdds, func(yield func(value int) bool) {
		for i := 1; i < 10; i += 2 {
			if !yield(i) {
				break
			}
		}
	})
	verifySliceValue(t, cDoubled, func(yield func(value int) bool) {
		for i := 0; i < 10; i++ {
			if !yield(i * 2) {
				break
			}
		}
	})
	verifySliceValue(t, cNegated, func(yield func(value int) bool) {
		for i := 0; i < 10; i++ {
			if !yield(-i) {
				break
			}
		}
	})
}

func TestMapTo5(t *testing.T) {
	pipeline, _ := newTestPipeline(t)
	initial := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < 10; i++ {
			*emitter.GetEmitPointer() = i
		}
	})
	out1, out2, out3, out4, out5 := gomr.MapTo5(initial,
		func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[int], e1 gomr.Emitter[int], e2 gomr.Emitter[int], e3 gomr.Emitter[int], e4 gomr.Emitter[int], e5 gomr.Emitter[int]) {
			for value := range receiver.IterValues() {
				*e1.GetEmitPointer() = *value * 1
				*e2.GetEmitPointer() = *value * 2
				*e3.GetEmitPointer() = *value * 3
				*e4.GetEmitPointer() = *value * 4
				*e5.GetEmitPointer() = *value * 5
			}
		},
	)
	c1 := collectToSliceValue(out1)
	c2 := collectToSliceValue(out2)
	c3 := collectToSliceValue(out3)
	c4 := collectToSliceValue(out4)
	c5 := collectToSliceValue(out5)
	pipeline.WaitForCompletion()

	for multiplier, collected := range map[int]gomr.Value[[]int]{1: c1, 2: c2, 3: c3, 4: c4, 5: c5} {
		m := multiplier
		verifySliceValue(t, collected, func(yield func(value int) bool) {
			for i := 0; i < 10; i++ {
				if !yield(i * m) {
					break
				}
			}
		})
	}
}

func TestMapValue2To2(t *testing.T) {
	pipeline, _ := newTestPipeline(t)
	a := createSingleValue(pipeline, 10)
	b := createSingleValue(pipeline, 20)
	sum, product := gomr.MapValue2To2(a, b,
		func(ctx gomr.OperatorContext, a int, b int) (int, int) {
			return a + b, a * b
		},
	)
	pipeline.WaitForCompletion()
	if got := sum.Wait(); got != 30 {
		t.Errorf("sum = %d, want 30", got)
	}
	if got := product.Wait(); got != 200 {
		t.Errorf("product = %d, want 200", got)
	}
}
