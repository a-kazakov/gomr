package parameters

import (
	"flag"
	"reflect"
)

// Flagger is a generic interface for registering flags.
// The method signatures are designed to be compatible with the standard library's flag.FlagSet.
//
// To use with standard library flag:
//
//	import "flag"
//	params.RegisterFlags(flag.CommandLine)
//
// To use with a custom flag.FlagSet:
//
//	fs := flag.NewFlagSet("myapp", flag.ExitOnError)
//	params.RegisterFlags(fs)
//
// To use with other libraries (cobra, kingpin, etc.), create a simple adapter.
type Flagger interface {
	// Var registers a flag with a custom flag.Value implementation.
	// This is the primary method used for custom types.
	Var(value flag.Value, name string, usage string)

	// Legacy methods for backwards compatibility with simple adapters.
	// New code should use Var() which supports all types.
	IntVar(p *int, name string, value int, usage string)
	StringVar(p *string, name string, value string, usage string)
	BoolVar(p *bool, name string, value bool, usage string)
	Int64Var(p *int64, name string, value int64, usage string)
}

// Source is a function that looks up a string value by key.
// Returns the value and whether it was found.
//
// Examples:
//   - os.LookupEnv for environment variables
//   - A map lookup for config files
//   - A function that transforms keys (e.g., dots to underscores for env vars)
type Source func(key string) (string, bool)

type ParameterRegistrar interface {
	RegisterFlag(f Flagger)
	FullName() string
}

type ParameterLoader interface {
	LoadFromSource(lookup Source) error
	FullName() string
}

// RegisterAllFlags walks a struct and registers all *Parameter[T] fields with the Flagger.
// The struct fields must be exported and be of type *Parameter[T].
func RegisterAllFlags(group any, f Flagger) {
	v := reflect.ValueOf(group)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic("RegisterAllFlags: expected struct or pointer to struct")
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.CanInterface() {
			continue
		}

		// Check if the field implements ParameterRegistrar
		if registrar, ok := field.Interface().(ParameterRegistrar); ok {
			registrar.RegisterFlag(f)
			continue
		}

		// If it's a pointer to a struct (nested parameter group), recurse
		if field.Kind() == reflect.Ptr && !field.IsNil() {
			elem := field.Elem()
			if elem.Kind() == reflect.Struct {
				// Check if the nested struct has a RegisterFlags method
				if method := field.MethodByName("RegisterFlags"); method.IsValid() {
					method.Call([]reflect.Value{reflect.ValueOf(f)})
				} else {
					RegisterAllFlags(field.Interface(), f)
				}
			}
		}

		_ = t.Field(i) // suppress unused variable warning
	}
}

// LoadAllFromSource walks a struct and loads all *Parameter[T] fields from the Source.
func LoadAllFromSource(group any, lookup Source) error {
	v := reflect.ValueOf(group)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic("LoadAllFromSource: expected struct or pointer to struct")
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.CanInterface() {
			continue
		}

		// Check if the field implements ParameterLoader
		if loader, ok := field.Interface().(ParameterLoader); ok {
			if err := loader.LoadFromSource(lookup); err != nil {
				return err
			}
			continue
		}

		// If it's a pointer to a struct (nested parameter group), recurse
		if field.Kind() == reflect.Ptr && !field.IsNil() {
			elem := field.Elem()
			if elem.Kind() == reflect.Struct {
				// Check if the nested struct has a LoadFromSource method
				if method := field.MethodByName("LoadFromSource"); method.IsValid() {
					results := method.Call([]reflect.Value{reflect.ValueOf(lookup)})
					if len(results) > 0 && !results[0].IsNil() {
						return results[0].Interface().(error)
					}
				} else {
					if err := LoadAllFromSource(field.Interface(), lookup); err != nil {
						return err
					}
				}
			}
		}

		_ = t.Field(i) // suppress unused variable warning
	}

	return nil
}
