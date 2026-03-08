package core

// Parameter is a generic interface for configuration values with defaults.
// Get returns the current value. Resolve and ResolvePtr return the provided
// value if meaningful, otherwise the default.
type Parameter[T any] interface {
	Get() T
	Resolve(providedValue T) T
	ResolvePtr(providedValue *T) T
}
