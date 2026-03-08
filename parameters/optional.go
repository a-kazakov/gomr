package parameters

// Optional represents a value that may or may not be set.
// Unlike pointers, it distinguishes "explicitly set to zero" from "not set".
type Optional[T any] struct {
	value T
	set   bool
}

func Some[T any](v T) Optional[T] {
	return Optional[T]{value: v, set: true}
}

func None[T any]() Optional[T] {
	return Optional[T]{}
}

func (o Optional[T]) IsSet() bool {
	return o.set
}

// Get returns the value. Panics if not set.
func (o Optional[T]) Get() T {
	if !o.set {
		panic("Optional.Get() called on empty Optional")
	}
	return o.value
}

func (o Optional[T]) GetOr(def T) T {
	if o.set {
		return o.value
	}
	return def
}

func (o Optional[T]) GetOrZero() T {
	return o.value
}

func (o Optional[T]) Ptr() *T {
	if o.set {
		return &o.value
	}
	return nil
}

func OptionalFromPtr[T any](ptr *T) Optional[T] {
	if ptr == nil {
		return None[T]()
	}
	return Some(*ptr)
}
