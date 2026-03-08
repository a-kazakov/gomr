package must

import "fmt"

// BeTrue panics with msg if cond is false. Args are passed to fmt.Sprintf.
func BeTrue(cond bool, msg string, args ...any) {
	if !cond {
		if len(args) == 0 {
			panic(msg)
		}
		panic(fmt.Sprintf(msg, args...))
	}
}

// Result holds a value and an associated error for fluent checking.
type Result[T any] struct {
	val T
	err error
}

// NoError captures a (value, error) pair for fluent error checking.
// Call .Else(msg) to get the value or panic if err != nil.
// Accepts multi-return function calls directly: must.NoError(os.Open(path)).Else("msg")
func NoError[T any](val T, err error) Result[T] {
	return Result[T]{val: val, err: err}
}

// Else returns the value if no error, or panics with "msg: err".
// Args are passed to fmt.Sprintf for the message; the error is appended with %w.
func (r Result[T]) Else(msg string, args ...any) T {
	if r.err != nil {
		if len(args) == 0 {
			panic(fmt.Errorf("%s: %w", msg, r.err))
		}
		panic(fmt.Errorf("%s: %w", fmt.Sprintf(msg, args...), r.err))
	}
	return r.val
}

// ErrCheck holds an error for fluent checking of error-only functions.
type ErrCheck struct {
	err error
}

// OK captures an error for fluent checking.
// Call .Else(msg) to ensure no error, or panic.
func OK(err error) ErrCheck {
	return ErrCheck{err: err}
}

// Else does nothing if err is nil, or panics with "msg: err".
// Args are passed to fmt.Sprintf for the message; the error is appended with %w.
func (r ErrCheck) Else(msg string, args ...any) {
	if r.err != nil {
		if len(args) == 0 {
			panic(fmt.Errorf("%s: %w", msg, r.err))
		}
		panic(fmt.Errorf("%s: %w", fmt.Sprintf(msg, args...), r.err))
	}
}
