package parameters

import (
	"fmt"
	"time"
)

type Validator[T any] func(T) error

func NoValidation[T any](v T) error {
	return nil
}

func Positive[T ~int | ~int32 | ~int64 | ~float32 | ~float64](v T) error {
	if v <= 0 {
		return fmt.Errorf("value must be positive, got %v", v)
	}
	return nil
}

func PositiveDuration(v time.Duration) error {
	if v <= 0 {
		return fmt.Errorf("duration must be positive, got %v", v)
	}
	return nil
}

func NonNegative[T ~int | ~int32 | ~int64 | ~float32 | ~float64](v T) error {
	if v < 0 {
		return fmt.Errorf("value must be non-negative, got %v", v)
	}
	return nil
}

func NonNegativeDuration(v time.Duration) error {
	if v < 0 {
		return fmt.Errorf("duration must be non-negative, got %v", v)
	}
	return nil
}

func InRange[T ~int | ~int32 | ~int64 | ~float32 | ~float64](min, max T) Validator[T] {
	return func(v T) error {
		if v < min || v > max {
			return fmt.Errorf("value must be in range [%v, %v], got %v", min, max, v)
		}
		return nil
	}
}

func InRangeExclusive[T ~int | ~int32 | ~int64 | ~float32 | ~float64](min, max T) Validator[T] {
	return func(v T) error {
		if v <= min || v >= max {
			return fmt.Errorf("value must be in range (%v, %v), got %v", min, max, v)
		}
		return nil
	}
}

func OneOf[T comparable](allowed ...T) Validator[T] {
	return func(v T) error {
		for _, a := range allowed {
			if v == a {
				return nil
			}
		}
		return fmt.Errorf("value must be one of %v, got %v", allowed, v)
	}
}

func NotEmpty(v string) error {
	if v == "" {
		return fmt.Errorf("value must not be empty")
	}
	return nil
}

func And[T any](validators ...Validator[T]) Validator[T] {
	return func(v T) error {
		for _, validator := range validators {
			if err := validator(v); err != nil {
				return err
			}
		}
		return nil
	}
}
