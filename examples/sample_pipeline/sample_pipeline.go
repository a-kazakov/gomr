package sample_pipeline

import (
	"encoding/binary"
	"fmt"

	"github.com/a-kazakov/gomr"
	"github.com/cespare/xxhash/v2"
)

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

type lastDigitSumStringShuffleSerializer struct{}

func (s lastDigitSumStringShuffleSerializer) MarshalKeyToBytes(value *int64, dest []byte, cursor int64) (int, int64) {
	key := abs(*value) % 1000000
	n, err := binary.Encode(dest, binary.NativeEndian, key)
	if err != nil {
		panic(fmt.Errorf("error encoding key: %w", err))
	}
	return n, gomr.StopEmitting
}

func (s lastDigitSumStringShuffleSerializer) MarshalValueToBytes(value *int64, dest []byte) int {
	n, err := binary.Encode(dest, binary.NativeEndian, value)
	if err != nil {
		panic(fmt.Errorf("error encoding value: %w", err))
	}
	return n
}

func (s lastDigitSumStringShuffleSerializer) UnmarshalValueFromBytes(key []byte, data []byte, dest *int64) {
	_, err := binary.Decode(data, binary.NativeEndian, dest)
	if err != nil {
		panic(fmt.Errorf("error decoding value: %w", err))
	}
}

type lastDigitSumStringReducer struct{}

func (r lastDigitSumStringReducer) Setup(context gomr.OperatorContext) {
}
func (r lastDigitSumStringReducer) Teardown(emitter gomr.Emitter[string]) {
}

type intSerializer struct{}

func (r intSerializer) Setup(context gomr.OperatorContext) {
}
func (r intSerializer) MarshalElementToBytes(value *int, dest []byte) int {
	binary.NativeEndian.PutUint64(dest, uint64(*value))
	return 8
}
func (r intSerializer) UnmarshalElementFromBytes(data []byte, dest *int) {
	*dest = int(binary.NativeEndian.Uint64(data))
}

func (r lastDigitSumStringReducer) Reduce(key []byte, receiver gomr.ShuffleReceiver[int64], emitter gomr.Emitter[string]) {
	sum := 0
	for inValue := range receiver.IterValues() {
		sum += int(*inValue)
	}
	keyValue := binary.NativeEndian.Uint64(key)
	*emitter.GetEmitPointer() = fmt.Sprintf("%d: %d", keyValue, sum)
}

func Build(p gomr.Pipeline, count int) gomr.Value[string] {
	initial := gomr.NewSeedCollection(p, func(context gomr.OperatorContext, emitter gomr.Emitter[int]) {
		for i := 0; i < count; i++ {
			*emitter.GetEmitPointer() = i
		}
	},
		gomr.WithOperationName("Generate initial values"),
		gomr.WithOutCollectionName("Initial values"),
	)
	odds, evens := gomr.MapTo2(
		initial,
		func(context gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter0 gomr.Emitter[int], emitter1 gomr.Emitter[int]) {
			for value := range receiver.IterValues() {
				if *value%2 == 0 {
					*emitter0.GetEmitPointer() = *value
				} else {
					*emitter1.GetEmitPointer() = *value
				}
			}
		},
		gomr.WithOperationName("Split into odds and evens"),
		gomr.WithOutCollectionNames("Odds", "Evens"),
	)
	odds, oddsF := gomr.ForkTo2(odds)
	evens, evensF := gomr.ForkTo2(evens)
	odds = gomr.SpillBuffer[intSerializer](odds)
	evens = gomr.SpillBuffer[intSerializer](evens)
	oddsCount := gomr.Collect(oddsF, func(context gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) uint64 {
		result := uint64(0)
		for inValue := range receiver.IterBatches() {
			result += uint64(len(inValue))
		}
		return result
	}, func(context gomr.OperatorContext, intermediateValues []uint64) uint64 {
		result := uint64(0)
		for _, intermediateValue := range intermediateValues {
			result += intermediateValue
		}
		return result
	},
		gomr.WithOperationName("Count odds"),
		gomr.WithOutValueName("Odds count"),
	)
	evensCount := gomr.Collect(evensF, func(context gomr.OperatorContext, receiver gomr.CollectionReceiver[int]) uint64 {
		result := uint64(0)
		for inValue := range receiver.IterBatches() {
			result += uint64(len(inValue))
		}
		return result
	}, func(context gomr.OperatorContext, intermediateValues []uint64) uint64 {
		result := uint64(0)
		for _, intermediateValue := range intermediateValues {
			result += intermediateValue
		}
		return result
	},
		gomr.WithOperationName("Count evens"),
		gomr.WithOutValueName("Evens count"),
	)
	countsSum, countsProd := gomr.MapValue2To2(
		oddsCount,
		evensCount,
		func(context gomr.OperatorContext, oddsCount uint64, evensCount uint64) (uint64, uint64) {
			return oddsCount + evensCount, oddsCount * evensCount
		},
		gomr.WithOperationName("Sum and product of counts"),
		gomr.WithOutValueNames("Sum", "Product"),
	)
	merged := gomr.Merge(
		[]gomr.Collection[int64]{
			gomr.MapWithSideValue(odds, countsSum, func(context gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int64], sideValue uint64) {
				for inValue := range receiver.IterValues() {
					tmp := uint64(*inValue) + sideValue
					for range 10000 {
						tmp = tmp ^ ((tmp << 13) ^ (tmp >> 7) ^ uint64(*inValue*31+17))
					}
					*emitter.GetEmitPointer() = int64(tmp)
				}
			},
				gomr.WithOperationName("Transform odds"),
				gomr.WithOutCollectionNames("Transformed odds"),
			),
			gomr.Map(evens, func(context gomr.OperatorContext, receiver gomr.CollectionReceiver[int], emitter gomr.Emitter[int64]) {
				for inValue := range receiver.IterValues() {
					tmp := uint64(*inValue)
					for range 1000 {
						tmp = tmp*6364136223846793005 + 1 + (tmp*17 + 23)
						tmp ^= tmp >> 21
						tmp ^= tmp << 29
					}
					*emitter.GetEmitPointer() = int64(tmp)
				}
			},
				gomr.WithOperationName("Transform evens"),
				gomr.WithOutCollectionNames("Transformed evens"),
			),
		},
		gomr.WithOperationName("Merge odds and evens"),
		gomr.WithOutCollectionName("Merged odds and evens"),
	)
	shuffled := gomr.Shuffle[*lastDigitSumStringShuffleSerializer, *lastDigitSumStringReducer](
		merged,
		gomr.WithOperationName("Shuffle by last digits"),
		gomr.WithOutCollectionNames("Shuffled by last digits"),
	)
	allValuesString := gomr.MapValue4(
		oddsCount,
		evensCount,
		countsSum,
		countsProd,
		func(context gomr.OperatorContext, oddsCount uint64, evensCount uint64, countsSum uint64, countsProd uint64) string {
			return fmt.Sprintf("Odds count: %d, Evens count: %d, Sum: %d, Product: %d", oddsCount, evensCount, countsSum, countsProd)
		},
		gomr.WithOperationName("To string"),
		gomr.WithOutValueNames("All values string"),
	)
	return gomr.CollectWithSideValue(
		gomr.Merge([]gomr.Collection[string]{shuffled, gomr.ToCollection(allValuesString)}, gomr.WithOperationName("Final merge")),
		countsProd,
		func(context gomr.OperatorContext, receiver gomr.CollectionReceiver[string], sideValue uint64) uint64 {
			descriptor := uint64(0)
			for inValue := range receiver.IterValues() {
				descriptor ^= (xxhash.Sum64([]byte(*inValue)) + sideValue)
			}
			return descriptor
		},
		func(context gomr.OperatorContext, intermediateValues []uint64, sideValue uint64) string {
			descriptor := uint64(0)
			for _, intermediateValue := range intermediateValues {
				descriptor ^= intermediateValue
			}
			descriptor ^= sideValue
			return fmt.Sprintf("Result: %d", descriptor)
		},
		gomr.WithOperationName("Collect result"),
		gomr.WithOutValueName("Result"),
	)
}
