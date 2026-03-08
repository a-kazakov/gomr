package parameters

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/a-kazakov/gomr/internal/core"
)

// Parameter holds a single configurable value with support for:
// - Loading from flags, environment variables, or other sources
// - Custom parsing (including suffix support)
// - Validation
// - Default values (static or dynamic)
// - Operator-level overrides via Resolve
type Parameter[T any] struct {
	prefix      string
	name        string
	description string
	parser      Parser[T]
	validateFn  func(T) error
	defaultFn   func() T

	// Runtime state
	cachedValue *T
	cacheOnce   sync.Once
	flagValue   *flagValue[T]
	source      string // "default", "flag", "source" (for debugging)
}

type ParameterConfig[T any] struct {
	Prefix      string
	Name        string
	Description string
	Default     T
	DefaultFn   func() T // Takes precedence over Default if set
	Parser      Parser[T]
	Validate    func(T) error
}

func NewParameter[T any](cfg ParameterConfig[T]) *Parameter[T] {
	if cfg.Name == "" {
		panic("parameter name is required")
	}
	if cfg.Description == "" {
		panic(fmt.Sprintf("parameter %s.%s: description is required", cfg.Prefix, cfg.Name))
	}
	if cfg.Parser == nil {
		panic(fmt.Sprintf("parameter %s.%s: parser is required", cfg.Prefix, cfg.Name))
	}

	defaultFn := cfg.DefaultFn
	if defaultFn == nil {
		defaultVal := cfg.Default
		defaultFn = func() T { return defaultVal }
	}

	validateFn := cfg.Validate
	if validateFn == nil {
		validateFn = func(T) error { return nil }
	}

	return &Parameter[T]{
		prefix:      cfg.Prefix,
		name:        cfg.Name,
		description: cfg.Description,
		parser:      cfg.Parser,
		validateFn:  validateFn,
		defaultFn:   defaultFn,
		source:      "default",
	}
}

func (p *Parameter[T]) FullName() string {
	if p.prefix == "" {
		return p.name
	}
	return fmt.Sprintf("%s.%s", p.prefix, p.name)
}

func (p *Parameter[T]) Description() string {
	return p.description
}

// Get returns the resolved value of the parameter.
// Priority: flag > source > default
func (p *Parameter[T]) Get() T {
	p.cacheOnce.Do(func() {
		if p.cachedValue != nil {
			return // already set by LoadFromSource
		}
		value := p.retrieveValue()
		if err := p.validateFn(value); err != nil {
			panic(fmt.Sprintf("parameter %s: invalid value %v: %s", p.FullName(), value, err))
		}
		p.cachedValue = &value
	})
	return *p.cachedValue
}

func (p *Parameter[T]) retrieveValue() T {
	// 1. Check flag value (highest priority)
	if p.flagValue != nil && p.flagValue.isSet {
		p.source = "flag"
		return p.flagValue.value
	}
	// 2. Return default
	p.source = "default"
	return p.defaultFn()
}

// Resolve returns the override value if set, otherwise the parameter's resolved value.
// This is the primary API for operator-level overrides.
func (p *Parameter[T]) Resolve(override Optional[T]) T {
	if override.IsSet() {
		return override.Get()
	}
	return p.Get()
}

// ResolveZero returns the override if non-zero, otherwise the parameter's resolved value.
// Deprecated: Use Resolve with Optional for explicit zero handling.
func (p *Parameter[T]) ResolveZero(override T) T {
	var zero T
	if any(override) != any(zero) {
		return override
	}
	return p.Get()
}

func (p *Parameter[T]) ResolvePtr(override *T) T {
	if override != nil {
		return *override
	}
	return p.Get()
}

func (p *Parameter[T]) RegisterFlag(f Flagger) {
	p.flagValue = &flagValue[T]{
		parser: p.parser,
		defVal: p.defaultFn(),
	}
	f.Var(p.flagValue, p.FullName(), p.description)
}

func (p *Parameter[T]) LoadFromSource(lookup Source) error {
	fullName := p.FullName()
	strValue, found := lookup(fullName)
	if !found {
		return nil
	}

	value, err := p.parser(strValue)
	if err != nil {
		return fmt.Errorf("parameter %s: failed to parse %q: %w", fullName, strValue, err)
	}

	if err := p.validateFn(value); err != nil {
		return fmt.Errorf("parameter %s: invalid value %v: %w", fullName, value, err)
	}

	p.cachedValue = &value
	p.source = "source"
	return nil
}

// flagValue implements flag.Value for custom parsing.
type flagValue[T any] struct {
	value  T
	defVal T
	parser Parser[T]
	isSet  bool
}

func (f *flagValue[T]) String() string {
	if f == nil {
		return ""
	}
	return fmt.Sprintf("%v", f.defVal)
}

func (f *flagValue[T]) Set(s string) error {
	value, err := f.parser(s)
	if err != nil {
		return err
	}
	f.value = value
	f.isSet = true
	return nil
}

// Ensure flagValue implements flag.Value
var _ flag.Value = (*flagValue[int])(nil)

// =============================================================================
// Convenience constructors
// =============================================================================

func NewIntParam(prefix, name, description string, defaultVal int, validate func(int) error) *Parameter[int] {
	return NewParameter(ParameterConfig[int]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseInt,
		Validate:    validate,
	})
}

func NewIntParamDynamic(prefix, name, description string, defaultFn func() int, validate func(int) error) *Parameter[int] {
	return NewParameter(ParameterConfig[int]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		DefaultFn:   defaultFn,
		Parser:      ParseInt,
		Validate:    validate,
	})
}

func NewInt32Param(prefix, name, description string, defaultVal int32, validate func(int32) error) *Parameter[int32] {
	return NewParameter(ParameterConfig[int32]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseInt32,
		Validate:    validate,
	})
}

func NewInt32ParamDynamic(prefix, name, description string, defaultFn func() int32, validate func(int32) error) *Parameter[int32] {
	return NewParameter(ParameterConfig[int32]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		DefaultFn:   defaultFn,
		Parser:      ParseInt32,
		Validate:    validate,
	})
}

func NewInt64Param(prefix, name, description string, defaultVal int64, validate func(int64) error) *Parameter[int64] {
	return NewParameter(ParameterConfig[int64]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseInt64,
		Validate:    validate,
	})
}

func NewByteSizeParam(prefix, name, description string, defaultVal int64, validate func(int64) error) *Parameter[int64] {
	return NewParameter(ParameterConfig[int64]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseByteSize,
		Validate:    validate,
	})
}

func NewByteSizeParamDynamic(prefix, name, description string, defaultFn func() int64, validate func(int64) error) *Parameter[int64] {
	return NewParameter(ParameterConfig[int64]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		DefaultFn:   defaultFn,
		Parser:      ParseByteSize,
		Validate:    validate,
	})
}

func NewFloat64Param(prefix, name, description string, defaultVal float64, validate func(float64) error) *Parameter[float64] {
	return NewParameter(ParameterConfig[float64]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseFloat64,
		Validate:    validate,
	})
}

func NewStringParam(prefix, name, description string, defaultVal string, validate func(string) error) *Parameter[string] {
	return NewParameter(ParameterConfig[string]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseString,
		Validate:    validate,
	})
}

func NewStringParamDynamic(prefix, name, description string, defaultFn func() string, validate func(string) error) *Parameter[string] {
	return NewParameter(ParameterConfig[string]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		DefaultFn:   defaultFn,
		Parser:      ParseString,
		Validate:    validate,
	})
}

func NewBoolParam(prefix, name, description string, defaultVal bool) *Parameter[bool] {
	return NewParameter(ParameterConfig[bool]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseBool,
		Validate:    nil,
	})
}

func NewDurationParam(prefix, name, description string, defaultVal time.Duration, validate func(time.Duration) error) *Parameter[time.Duration] {
	return NewParameter(ParameterConfig[time.Duration]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseDuration,
		Validate:    validate,
	})
}

func NewByteSizeParamAsInt(prefix, name, description string, defaultVal int, validate func(int) error) *Parameter[int] {
	return NewParameter(ParameterConfig[int]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      func(s string) (int, error) { v, err := ParseByteSize(s); return int(v), err },
		Validate:    validate,
	})
}

func NewCompressionAlgorithmParam(prefix, name, description string, defaultVal core.CompressionAlgorithm) *Parameter[core.CompressionAlgorithm] {
	return NewParameter(ParameterConfig[core.CompressionAlgorithm]{
		Prefix:      prefix,
		Name:        name,
		Description: description,
		Default:     defaultVal,
		Parser:      ParseCompressionAlgorithm,
		Validate:    nil,
	})
}
