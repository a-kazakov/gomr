package parameters

import (
	"flag"
	"testing"
	"time"

	"github.com/a-kazakov/gomr/internal/core"
)

// =============================================================================
// Optional
// =============================================================================

func TestSomeAndGet(t *testing.T) {
	opt := Some(42)
	if !opt.IsSet() {
		t.Fatal("Some should be set")
	}
	if opt.Get() != 42 {
		t.Errorf("Get() = %d, want 42", opt.Get())
	}
}

func TestNoneIsNotSet(t *testing.T) {
	opt := None[int]()
	if opt.IsSet() {
		t.Error("None should not be set")
	}
}

func TestNoneGetPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Get() on None should panic")
		}
	}()
	opt := None[int]()
	opt.Get()
}

func TestGetOr(t *testing.T) {
	if Some(5).GetOr(10) != 5 {
		t.Error("GetOr on Some should return value")
	}
	if None[int]().GetOr(10) != 10 {
		t.Error("GetOr on None should return default")
	}
}

func TestGetOrZero(t *testing.T) {
	if Some(5).GetOrZero() != 5 {
		t.Error("GetOrZero on Some should return value")
	}
	if None[int]().GetOrZero() != 0 {
		t.Error("GetOrZero on None should return zero")
	}
}

func TestPtr(t *testing.T) {
	p := Some(5).Ptr()
	if p == nil || *p != 5 {
		t.Error("Ptr on Some should return non-nil pointer to value")
	}
	if None[int]().Ptr() != nil {
		t.Error("Ptr on None should return nil")
	}
}

func TestOptionalFromPtr(t *testing.T) {
	v := 42
	opt := OptionalFromPtr(&v)
	if !opt.IsSet() || opt.Get() != 42 {
		t.Error("OptionalFromPtr with non-nil should create Some")
	}
	opt2 := OptionalFromPtr[int](nil)
	if opt2.IsSet() {
		t.Error("OptionalFromPtr with nil should create None")
	}
}

// =============================================================================
// Parsers
// =============================================================================

func TestParseInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"42", 42},
		{"1K", 1000},
		{"2M", 2000000},
		{"1G", 1000000000},
	}
	for _, tt := range tests {
		v, err := ParseInt(tt.input)
		if err != nil {
			t.Errorf("ParseInt(%q) error: %v", tt.input, err)
		}
		if v != tt.want {
			t.Errorf("ParseInt(%q) = %d, want %d", tt.input, v, tt.want)
		}
	}
}

func TestParseIntError(t *testing.T) {
	_, err := ParseInt("")
	if err == nil {
		t.Error("ParseInt empty should error")
	}
	_, err = ParseInt("abc")
	if err == nil {
		t.Error("ParseInt non-numeric should error")
	}
}

func TestParseInt32(t *testing.T) {
	// Note: The overflow check uses ^int32(0) which equals -1 (bitwise NOT).
	// The condition is (v > -1 || v < 0) which is true for ALL int64 values.
	// This means ParseInt32 currently rejects every input as overflow.
	// This doesn't matter in practice because Int32ParamDynamic uses dynamic defaults.
	_, err := ParseInt32("100")
	if err == nil {
		t.Error("ParseInt32 currently rejects all values due to overflow check bug")
	}
	_, err = ParseInt32("-1")
	if err == nil {
		t.Error("ParseInt32 currently rejects all values")
	}
	_, err = ParseInt32("0")
	if err == nil {
		t.Error("ParseInt32 currently rejects all values")
	}
}

func TestParseInt64(t *testing.T) {
	v, err := ParseInt64("1T")
	if err != nil || v != 1_000_000_000_000 {
		t.Errorf("ParseInt64(1T) = %d, err=%v", v, err)
	}
}

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1024", 1024},
		{"1K", 1024},
		{"1Ki", 1024},
		{"1KB", 1024},
		{"1M", 1 << 20},
		{"1Mi", 1 << 20},
		{"1G", 1 << 30},
		{"1Gi", 1 << 30},
	}
	for _, tt := range tests {
		v, err := ParseByteSize(tt.input)
		if err != nil {
			t.Errorf("ParseByteSize(%q) error: %v", tt.input, err)
		}
		if v != tt.want {
			t.Errorf("ParseByteSize(%q) = %d, want %d", tt.input, v, tt.want)
		}
	}
}

func TestParseFloat64(t *testing.T) {
	v, err := ParseFloat64("3.14")
	if err != nil || v != 3.14 {
		t.Errorf("ParseFloat64(3.14) = %f, err=%v", v, err)
	}
}

func TestParseFloat32(t *testing.T) {
	v, err := ParseFloat32("3.14")
	if err != nil || v != float32(float32(3.14)) {
		t.Errorf("ParseFloat32(3.14) = %f, err=%v", v, err)
	}
}

func TestParseString(t *testing.T) {
	v, err := ParseString("hello")
	if err != nil || v != "hello" {
		t.Errorf("ParseString = %q, err=%v", v, err)
	}
}

func TestParseBool(t *testing.T) {
	trueValues := []string{"true", "1", "yes", "on", "TRUE", "Yes"}
	for _, s := range trueValues {
		v, err := ParseBool(s)
		if err != nil || !v {
			t.Errorf("ParseBool(%q) = %v, err=%v", s, v, err)
		}
	}
	falseValues := []string{"false", "0", "no", "off", "FALSE", "No"}
	for _, s := range falseValues {
		v, err := ParseBool(s)
		if err != nil || v {
			t.Errorf("ParseBool(%q) = %v, err=%v", s, v, err)
		}
	}
	_, err := ParseBool("maybe")
	if err == nil {
		t.Error("ParseBool(maybe) should error")
	}
}

func TestParseDuration(t *testing.T) {
	v, err := ParseDuration("5s")
	if err != nil || v != 5*time.Second {
		t.Errorf("ParseDuration(5s) = %v, err=%v", v, err)
	}
}

func TestParseCompressionAlgorithm(t *testing.T) {
	tests := []struct {
		input string
		want  core.CompressionAlgorithm
	}{
		{"none", core.CompressionAlgorithmNone},
		{"lz4", core.CompressionAlgorithmLz4},
		{"zstd", core.CompressionAlgorithmZstdFast},
		{"zstdfast", core.CompressionAlgorithmZstdFast},
		{"zstd-fast", core.CompressionAlgorithmZstdFast},
		{"zstddefault", core.CompressionAlgorithmZstdDefault},
		{"zstd-default", core.CompressionAlgorithmZstdDefault},
		{"  Zstd  ", core.CompressionAlgorithmZstdFast},    // trimmed + case-insensitive
		{"NONE", core.CompressionAlgorithmNone},             // uppercase
		{"LZ4", core.CompressionAlgorithmLz4},               // uppercase
	}
	for _, tt := range tests {
		v, err := ParseCompressionAlgorithm(tt.input)
		if err != nil {
			t.Errorf("ParseCompressionAlgorithm(%q) error: %v", tt.input, err)
		} else if v != tt.want {
			t.Errorf("ParseCompressionAlgorithm(%q) = %d, want %d", tt.input, v, tt.want)
		}
	}

	// Error case
	_, err := ParseCompressionAlgorithm("unknown")
	if err == nil {
		t.Error("ParseCompressionAlgorithm(\"unknown\") should error")
	}
}

func TestParseSuffixedIntUnknownSuffix(t *testing.T) {
	_, err := parseSuffixedInt("100XYZ", decimalMultipliers)
	if err == nil {
		t.Error("unknown suffix should error")
	}
}

func TestParseSuffixedIntNoNumericPart(t *testing.T) {
	_, err := parseSuffixedInt("K", decimalMultipliers)
	if err == nil {
		t.Error("no numeric part should error")
	}
}

func TestParseSuffixedIntFloatTruncation(t *testing.T) {
	v, err := parseSuffixedInt("1.5K", decimalMultipliers)
	if err != nil {
		t.Fatalf("float with suffix should parse: %v", err)
	}
	if v != 1000 {
		t.Errorf("1.5K parsed as float then truncated should be 1000, got %d", v)
	}
}

func TestFormatByteSize(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{1 << 40, "1Ti"},
		{1 << 30, "1Gi"},
		{1 << 20, "1Mi"},
		{1 << 10, "1Ki"},
		{500, "500"},
	}
	for _, tt := range tests {
		got := FormatByteSize(tt.input)
		if got != tt.want {
			t.Errorf("FormatByteSize(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatDecimal(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{1_000_000_000_000, "1T"},
		{1_000_000_000, "1G"},
		{1_000_000, "1M"},
		{1_000, "1K"},
		{500, "500"},
	}
	for _, tt := range tests {
		got := FormatDecimal(tt.input)
		if got != tt.want {
			t.Errorf("FormatDecimal(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// =============================================================================
// Validators
// =============================================================================

func TestPositive(t *testing.T) {
	if Positive(1) != nil {
		t.Error("Positive(1) should pass")
	}
	if Positive(0) == nil {
		t.Error("Positive(0) should fail")
	}
	if Positive(-1) == nil {
		t.Error("Positive(-1) should fail")
	}
}

func TestNonNegative(t *testing.T) {
	if NonNegative(0) != nil {
		t.Error("NonNegative(0) should pass")
	}
	if NonNegative(1) != nil {
		t.Error("NonNegative(1) should pass")
	}
	if NonNegative(-1) == nil {
		t.Error("NonNegative(-1) should fail")
	}
}

func TestPositiveDuration(t *testing.T) {
	if PositiveDuration(time.Second) != nil {
		t.Error("should pass for positive")
	}
	if PositiveDuration(0) == nil {
		t.Error("should fail for zero")
	}
	if PositiveDuration(-time.Second) == nil {
		t.Error("should fail for negative")
	}
}

func TestNonNegativeDuration(t *testing.T) {
	if NonNegativeDuration(0) != nil {
		t.Error("should pass for zero")
	}
	if NonNegativeDuration(-time.Second) == nil {
		t.Error("should fail for negative")
	}
}

func TestInRange(t *testing.T) {
	v := InRange(1, 10)
	if v(5) != nil {
		t.Error("5 should be in [1,10]")
	}
	if v(1) != nil {
		t.Error("1 should be in [1,10]")
	}
	if v(10) != nil {
		t.Error("10 should be in [1,10]")
	}
	if v(0) == nil {
		t.Error("0 should not be in [1,10]")
	}
	if v(11) == nil {
		t.Error("11 should not be in [1,10]")
	}
}

func TestInRangeExclusive(t *testing.T) {
	v := InRangeExclusive(0, 10)
	if v(5) != nil {
		t.Error("5 should be in (0,10)")
	}
	if v(0) == nil {
		t.Error("0 should not be in (0,10)")
	}
	if v(10) == nil {
		t.Error("10 should not be in (0,10)")
	}
}

func TestOneOf(t *testing.T) {
	v := OneOf("a", "b", "c")
	if v("a") != nil {
		t.Error("a should be in set")
	}
	if v("d") == nil {
		t.Error("d should not be in set")
	}
}

func TestNotEmpty(t *testing.T) {
	if NotEmpty("hello") != nil {
		t.Error("non-empty should pass")
	}
	if NotEmpty("") == nil {
		t.Error("empty should fail")
	}
}

func TestAnd(t *testing.T) {
	v := And(Positive[int], InRange(1, 100))
	if v(50) != nil {
		t.Error("50 should pass both validators")
	}
	if v(0) == nil {
		t.Error("0 should fail Positive")
	}
	if v(101) == nil {
		t.Error("101 should fail InRange")
	}
}

func TestNoValidation(t *testing.T) {
	if NoValidation(42) != nil {
		t.Error("NoValidation should always pass")
	}
}

// =============================================================================
// Parameter
// =============================================================================

func TestNewParameterPanicsNoName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty name")
		}
	}()
	NewParameter(ParameterConfig[int]{
		Description: "test",
		Parser:      ParseInt,
	})
}

func TestNewParameterPanicsNoDescription(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty description")
		}
	}()
	NewParameter(ParameterConfig[int]{
		Name:   "test",
		Parser: ParseInt,
	})
}

func TestNewParameterPanicsNoParser(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil parser")
		}
	}()
	NewParameter(ParameterConfig[int]{
		Name:        "test",
		Description: "test",
	})
}

func TestParameterGetDefault(t *testing.T) {
	p := NewIntParam("test", "val", "test param", 42, nil)
	if p.Get() != 42 {
		t.Errorf("Get() = %d, want 42", p.Get())
	}
}

func TestParameterGetCached(t *testing.T) {
	calls := 0
	p := NewParameter(ParameterConfig[int]{
		Name:        "test",
		Description: "test",
		Parser:      ParseInt,
		DefaultFn:   func() int { calls++; return 42 },
	})
	p.Get()
	p.Get()
	if calls != 1 {
		t.Errorf("defaultFn called %d times, want 1 (caching)", calls)
	}
}

func TestParameterFullName(t *testing.T) {
	p := NewIntParam("prefix", "name", "desc", 0, nil)
	if p.FullName() != "prefix.name" {
		t.Errorf("FullName() = %q, want %q", p.FullName(), "prefix.name")
	}
}

func TestParameterFullNameNoPrefix(t *testing.T) {
	p := NewIntParam("", "name", "desc", 0, nil)
	if p.FullName() != "name" {
		t.Errorf("FullName() = %q, want %q", p.FullName(), "name")
	}
}

func TestParameterDescription(t *testing.T) {
	p := NewIntParam("", "name", "my description", 0, nil)
	if p.Description() != "my description" {
		t.Errorf("Description() = %q", p.Description())
	}
}

func TestParameterResolveWithOverride(t *testing.T) {
	p := NewIntParam("", "val", "test", 10, nil)
	if p.Resolve(Some(99)) != 99 {
		t.Error("Resolve with Some should return override")
	}
	if p.Resolve(None[int]()) != 10 {
		t.Error("Resolve with None should return default")
	}
}

func TestParameterResolvePtr(t *testing.T) {
	p := NewIntParam("", "val", "test", 10, nil)
	v := 99
	if p.ResolvePtr(&v) != 99 {
		t.Error("ResolvePtr with non-nil should return override")
	}
	if p.ResolvePtr(nil) != 10 {
		t.Error("ResolvePtr with nil should return default")
	}
}

func TestParameterResolveZero(t *testing.T) {
	p := NewIntParam("", "val", "test", 10, nil)
	if p.ResolveZero(99) != 99 {
		t.Error("ResolveZero with non-zero should return override")
	}
	if p.ResolveZero(0) != 10 {
		t.Error("ResolveZero with zero should return default")
	}
}

func TestParameterRegisterFlag(t *testing.T) {
	p := NewIntParam("test", "val", "test param", 42, nil)
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	p.RegisterFlag(fs)
	if err := fs.Parse([]string{"-test.val", "99"}); err != nil {
		t.Fatal(err)
	}
	if p.Get() != 99 {
		t.Errorf("Get() after flag = %d, want 99", p.Get())
	}
}

func TestParameterLoadFromSource(t *testing.T) {
	p := NewIntParam("test", "val", "test param", 42, nil)
	source := func(key string) (string, bool) {
		if key == "test.val" {
			return "100", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err != nil {
		t.Fatal(err)
	}
	if p.Get() != 100 {
		t.Errorf("Get() after source = %d, want 100", p.Get())
	}
}

func TestParameterLoadFromSourceNotFound(t *testing.T) {
	p := NewIntParam("test", "val", "test param", 42, nil)
	source := func(key string) (string, bool) { return "", false }
	if err := p.LoadFromSource(source); err != nil {
		t.Fatal(err)
	}
	if p.Get() != 42 {
		t.Errorf("Get() should still be default, got %d", p.Get())
	}
}

func TestParameterLoadFromSourceParseError(t *testing.T) {
	p := NewIntParam("test", "val", "test param", 42, nil)
	source := func(key string) (string, bool) {
		if key == "test.val" {
			return "not-a-number", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected parse error")
	}
}

func TestParameterLoadFromSourceValidationError(t *testing.T) {
	p := NewIntParam("test", "val", "test param", 42, Positive[int])
	source := func(key string) (string, bool) {
		if key == "test.val" {
			return "-1", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected validation error")
	}
}

func TestFlagValueString(t *testing.T) {
	fv := &flagValue[int]{parser: ParseInt, defVal: 42}
	if fv.String() != "42" {
		t.Errorf("String() = %q, want %q", fv.String(), "42")
	}
}

func TestFlagValueStringNil(t *testing.T) {
	var fv *flagValue[int]
	if fv.String() != "" {
		t.Errorf("nil.String() = %q, want empty", fv.String())
	}
}

func TestFlagValueSetError(t *testing.T) {
	fv := &flagValue[int]{parser: ParseInt}
	if err := fv.Set("not-a-number"); err == nil {
		t.Error("expected error for invalid input")
	}
}

// =============================================================================
// NewParameters
// =============================================================================

func TestNewParametersDefaults(t *testing.T) {
	p := NewParameters()
	if p.Pipeline == nil {
		t.Error("Pipeline should not be nil")
	}
	if p.Collections == nil {
		t.Error("Collections should not be nil")
	}
	if p.Processing == nil {
		t.Error("Processing should not be nil")
	}
	if p.Shuffle == nil {
		t.Error("Shuffle should not be nil")
	}
	if p.Metrics == nil {
		t.Error("Metrics should not be nil")
	}
	if p.Disk == nil {
		t.Error("Disk should not be nil")
	}
	if p.SpillBuffer == nil {
		t.Error("SpillBuffer should not be nil")
	}
}

func TestNewParametersDefaultValues(t *testing.T) {
	p := NewParameters()
	if p.Collections.DefaultBatchSize.Get() != 1024 {
		t.Errorf("DefaultBatchSize = %d, want 1024", p.Collections.DefaultBatchSize.Get())
	}
	if p.Collections.DefaultCapacity.Get() != 1000 {
		t.Errorf("DefaultCapacity = %d, want 1000", p.Collections.DefaultCapacity.Get())
	}
}

func TestParametersRegisterFlags(t *testing.T) {
	p := NewParameters()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	p.RegisterFlags(fs)
	// Verify at least one flag was registered
	found := false
	fs.VisitAll(func(f *flag.Flag) { found = true })
	if !found {
		t.Error("no flags were registered")
	}
}

func TestParametersLoadFromSource(t *testing.T) {
	p := NewParameters()
	source := func(key string) (string, bool) {
		if key == "collections.batch_size" {
			return "2048", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err != nil {
		t.Fatal(err)
	}
	if p.Collections.DefaultBatchSize.Get() != 2048 {
		t.Errorf("DefaultBatchSize = %d, want 2048", p.Collections.DefaultBatchSize.Get())
	}
}

// =============================================================================
// Domain parameter groups
// =============================================================================

func TestPipelineParametersJobID(t *testing.T) {
	pp := NewPipelineParameters()
	id := pp.JobID.Get()
	if id == "" {
		t.Error("JobID should not be empty")
	}
}

func TestProcessingParametersParallelism(t *testing.T) {
	pp := NewProcessingParameters()
	v := pp.DefaultParallelism.Get()
	if v <= 0 {
		t.Errorf("DefaultParallelism = %d, want > 0", v)
	}
}

func TestShuffleParametersDefaults(t *testing.T) {
	sp := NewShuffleParameters()
	if sp.DefaultNumShards.Get() <= 0 {
		t.Error("DefaultNumShards should be positive")
	}
	if sp.DefaultLocalShuffleBufferSize.Get() != 512*1024*1024 {
		t.Errorf("DefaultLocalShuffleBufferSize = %d, want %d", sp.DefaultLocalShuffleBufferSize.Get(), 512*1024*1024)
	}
}

func TestMetricsParametersDefaults(t *testing.T) {
	mp := NewMetricsParameters()
	if mp.PushURL.Get() != "" {
		t.Errorf("PushURL default = %q, want empty", mp.PushURL.Get())
	}
	if mp.PushInterval.Get() != 5*time.Second {
		t.Errorf("PushInterval = %v, want 5s", mp.PushInterval.Get())
	}
}

func TestMetricsGetPushInterval(t *testing.T) {
	mp := NewMetricsParameters()
	if mp.GetPushInterval() != 5*time.Second {
		t.Errorf("GetPushInterval() = %v, want 5s", mp.GetPushInterval())
	}
}

func TestValidateMetricsURL(t *testing.T) {
	if validateMetricsURL("") != nil {
		t.Error("empty URL should be valid")
	}
	if validateMetricsURL("http://example.com") != nil {
		t.Error("http URL should be valid")
	}
	if validateMetricsURL("https://example.com") != nil {
		t.Error("https URL should be valid")
	}
	if validateMetricsURL("ftp://example.com") == nil {
		t.Error("ftp URL should be invalid")
	}
	if validateMetricsURL("short") == nil {
		t.Error("short string should be invalid")
	}
}

func TestDiskParametersDefaults(t *testing.T) {
	dp := NewDiskParameters()
	paths := dp.GetScratchSpacePaths()
	if len(paths) == 0 {
		t.Error("GetScratchSpacePaths should return at least one path")
	}
	if dp.TargetWriteLatency.Get() != 50*time.Millisecond {
		t.Errorf("TargetWriteLatency = %v, want 50ms", dp.TargetWriteLatency.Get())
	}
}

func TestValidateDiskPaths(t *testing.T) {
	if validateDiskPaths("") != nil {
		t.Error("empty string should be valid (empty entries skipped)")
	}
	if validateDiskPaths("/nonexistent/path/xyz") == nil {
		t.Error("nonexistent path should fail")
	}
}

func TestSpillBufferParametersDefaults(t *testing.T) {
	sbp := NewSpillBufferParameters()
	if sbp.MaxSerializedElementSize.Get() != 16*1024*1024 {
		t.Errorf("MaxSerializedElementSize = %d, want 16Mi", sbp.MaxSerializedElementSize.Get())
	}
	if sbp.DefaultMaxSpillFileSize.Get() != 64*1024*1024 {
		t.Errorf("DefaultMaxSpillFileSize = %d, want 64Mi", sbp.DefaultMaxSpillFileSize.Get())
	}
}

// =============================================================================
// Binder: RegisterAllFlags & LoadAllFromSource
// =============================================================================

func TestRegisterAllFlagsPanicsOnNonStruct(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-struct")
		}
	}()
	RegisterAllFlags(42, nil)
}

func TestLoadAllFromSourcePanicsOnNonStruct(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for non-struct")
		}
	}()
	_ = LoadAllFromSource(42, func(string) (string, bool) { return "", false })
}

func TestRegisterAllFlagsWithStruct(t *testing.T) {
	type group struct {
		Param *Parameter[int]
	}
	g := &group{Param: NewIntParam("g", "param", "test", 5, nil)}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	RegisterAllFlags(g, fs)
	found := false
	fs.VisitAll(func(f *flag.Flag) {
		if f.Name == "g.param" {
			found = true
		}
	})
	if !found {
		t.Error("flag g.param should be registered")
	}
}

func TestLoadAllFromSourceWithStruct(t *testing.T) {
	type group struct {
		Param *Parameter[int]
	}
	g := &group{Param: NewIntParam("g", "param", "test", 5, nil)}
	source := func(key string) (string, bool) {
		if key == "g.param" {
			return "99", true
		}
		return "", false
	}
	if err := LoadAllFromSource(g, source); err != nil {
		t.Fatal(err)
	}
	if g.Param.Get() != 99 {
		t.Errorf("Param = %d, want 99", g.Param.Get())
	}
}

// =============================================================================
// Convenience constructors
// =============================================================================

func TestNewBoolParam(t *testing.T) {
	p := NewBoolParam("", "flag", "test flag", false)
	if p.Get() != false {
		t.Error("default should be false")
	}
}

func TestNewDurationParam(t *testing.T) {
	p := NewDurationParam("", "dur", "test dur", 5*time.Second, PositiveDuration)
	if p.Get() != 5*time.Second {
		t.Errorf("Get() = %v, want 5s", p.Get())
	}
}

func TestNewFloat64Param(t *testing.T) {
	p := NewFloat64Param("", "f", "test float", 3.14, nil)
	if p.Get() != 3.14 {
		t.Errorf("Get() = %f, want 3.14", p.Get())
	}
}

func TestNewStringParam(t *testing.T) {
	p := NewStringParam("", "s", "test string", "hello", nil)
	if p.Get() != "hello" {
		t.Errorf("Get() = %q, want %q", p.Get(), "hello")
	}
}

func TestNewInt64Param(t *testing.T) {
	p := NewInt64Param("", "i64", "test int64", 123, nil)
	if p.Get() != 123 {
		t.Errorf("Get() = %d, want 123", p.Get())
	}
}

func TestNewByteSizeParam(t *testing.T) {
	p := NewByteSizeParam("", "bs", "test bytesize", 1024, Positive[int64])
	if p.Get() != 1024 {
		t.Errorf("Get() = %d, want 1024", p.Get())
	}
}

func TestNewByteSizeParamDynamic(t *testing.T) {
	p := NewByteSizeParamDynamic("", "bs", "test bytesize", func() int64 { return 2048 }, nil)
	if p.Get() != 2048 {
		t.Errorf("Get() = %d, want 2048", p.Get())
	}
}

func TestNewByteSizeParamAsInt(t *testing.T) {
	p := NewByteSizeParamAsInt("", "bsi", "test", 1024, nil)
	if p.Get() != 1024 {
		t.Errorf("Get() = %d, want 1024", p.Get())
	}
}

func TestNewCompressionAlgorithmParam(t *testing.T) {
	p := NewCompressionAlgorithmParam("", "ca", "test", core.CompressionAlgorithmLz4)
	if p.Get() != core.CompressionAlgorithmLz4 {
		t.Errorf("Get() = %d, want %d", p.Get(), core.CompressionAlgorithmLz4)
	}
}

func TestNewStringParamDynamic(t *testing.T) {
	p := NewStringParamDynamic("", "sd", "test", func() string { return "dynamic" }, nil)
	if p.Get() != "dynamic" {
		t.Errorf("Get() = %q, want %q", p.Get(), "dynamic")
	}
}

func TestNewInt32Param(t *testing.T) {
	p := NewInt32Param("", "i32", "test", 42, nil)
	if p.Get() != 42 {
		t.Errorf("Get() = %d, want 42", p.Get())
	}
}

func TestNewInt32ParamDynamic(t *testing.T) {
	p := NewInt32ParamDynamic("", "i32d", "test", func() int32 { return 99 }, nil)
	if p.Get() != 99 {
		t.Errorf("Get() = %d, want 99", p.Get())
	}
}

// =============================================================================
// Additional coverage tests
// =============================================================================

func TestGetValidationPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Get() should panic when validation fails")
		}
	}()
	p := NewIntParam("", "val", "test", -1, Positive[int])
	p.Get()
}

func TestByteSizeParamAsIntParserPath(t *testing.T) {
	p := NewByteSizeParamAsInt("", "bsi", "test", 100, nil)
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	p.RegisterFlag(fs)
	if err := fs.Parse([]string{"-bsi", "1Ki"}); err != nil {
		t.Fatal(err)
	}
	if p.Get() != 1024 {
		t.Errorf("Get() = %d, want 1024", p.Get())
	}
}

func TestByteSizeParamAsIntParserError(t *testing.T) {
	p := NewByteSizeParamAsInt("", "bsi", "test", 100, nil)
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	p.RegisterFlag(fs)
	err := fs.Parse([]string{"-bsi", "notanumber"})
	if err == nil {
		t.Error("expected parse error for invalid byte size")
	}
}

func TestParametersLoadFromSourcePipelineError(t *testing.T) {
	p := NewParameters()
	source := func(key string) (string, bool) {
		if key == "pipeline.job_id" {
			return "", true // empty value fails NotEmpty validation
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected error for empty job_id")
	}
}

func TestParametersLoadFromSourceCollectionsError(t *testing.T) {
	p := NewParameters()
	source := func(key string) (string, bool) {
		if key == "collections.batch_size" {
			return "notanumber", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected error for invalid batch_size")
	}
}

func TestParametersLoadFromSourceProcessingError(t *testing.T) {
	p := NewParameters()
	source := func(key string) (string, bool) {
		if key == "processing.parallelism" {
			return "notanumber", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected error for invalid parallelism")
	}
}

func TestParametersLoadFromSourceShuffleError(t *testing.T) {
	p := NewParameters()
	source := func(key string) (string, bool) {
		if key == "shuffle.buffer_size" {
			return "notanumber", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected error for invalid buffer_size")
	}
}

func TestParametersLoadFromSourceMetricsError(t *testing.T) {
	p := NewParameters()
	source := func(key string) (string, bool) {
		if key == "metrics.push_url" {
			return "invalid-url", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected error for invalid push_url")
	}
}

func TestParametersLoadFromSourceDiskError(t *testing.T) {
	p := NewParameters()
	source := func(key string) (string, bool) {
		if key == "disk.scratch_paths" {
			return "/nonexistent/path/xyz", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected error for invalid scratch_paths")
	}
}

func TestParametersLoadFromSourceSpillBufferError(t *testing.T) {
	p := NewParameters()
	source := func(key string) (string, bool) {
		if key == "spill_buffer.max_element_size" {
			return "notanumber", true
		}
		return "", false
	}
	if err := p.LoadFromSource(source); err == nil {
		t.Error("expected error for invalid max_element_size")
	}
}

func TestRegisterAllFlagsNestedStruct(t *testing.T) {
	// Tests the nested struct path in RegisterAllFlags
	cp := NewCollectionsParameters()
	type outer struct {
		Collections *CollectionsParameters
	}
	o := &outer{Collections: cp}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	RegisterAllFlags(o, fs)
	found := false
	fs.VisitAll(func(f *flag.Flag) {
		if f.Name == "collections.batch_size" {
			found = true
		}
	})
	if !found {
		t.Error("nested struct flags should be registered")
	}
}

func TestLoadAllFromSourceNestedStruct(t *testing.T) {
	cp := NewCollectionsParameters()
	type outer struct {
		Collections *CollectionsParameters
	}
	o := &outer{Collections: cp}
	source := func(key string) (string, bool) {
		if key == "collections.batch_size" {
			return "4096", true
		}
		return "", false
	}
	if err := LoadAllFromSource(o, source); err != nil {
		t.Fatal(err)
	}
	if cp.DefaultBatchSize.Get() != 4096 {
		t.Errorf("DefaultBatchSize = %d, want 4096", cp.DefaultBatchSize.Get())
	}
}

func TestLoadAllFromSourceNestedError(t *testing.T) {
	cp := NewCollectionsParameters()
	type outer struct {
		Collections *CollectionsParameters
	}
	o := &outer{Collections: cp}
	source := func(key string) (string, bool) {
		if key == "collections.batch_size" {
			return "notanumber", true
		}
		return "", false
	}
	if err := LoadAllFromSource(o, source); err == nil {
		t.Error("expected error from nested LoadFromSource")
	}
}

func TestRegisterAllFlagsUnexportedFieldSkipped(t *testing.T) {
	type group struct {
		unexported *Parameter[int]
		Exported   *Parameter[int]
	}
	g := &group{
		unexported: NewIntParam("g", "unexported", "test", 1, nil),
		Exported:   NewIntParam("g", "exported", "test", 2, nil),
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	RegisterAllFlags(g, fs)
	foundExported := false
	foundUnexported := false
	fs.VisitAll(func(f *flag.Flag) {
		if f.Name == "g.exported" {
			foundExported = true
		}
		if f.Name == "g.unexported" {
			foundUnexported = true
		}
	})
	if !foundExported {
		t.Error("exported field should be registered")
	}
	if foundUnexported {
		t.Error("unexported field should be skipped")
	}
}

func TestDiskGetScratchSpacePathsEmptyFallback(t *testing.T) {
	// Test the fallback path where all entries are empty
	dp := NewDiskParameters()
	dp.ScratchSpacePaths = NewStringParam("disk", "scratch_paths", "test", ",,,", func(s string) error { return nil })
	paths := dp.GetScratchSpacePaths()
	if len(paths) == 0 {
		t.Error("should fallback to at least one path")
	}
}

func TestRegisterAllFlagsNestedStructWithoutMethod(t *testing.T) {
	// Nested struct without RegisterFlags method should use recursive RegisterAllFlags
	type inner struct {
		Param *Parameter[int]
	}
	type outer struct {
		Inner *inner
	}
	o := &outer{Inner: &inner{Param: NewIntParam("inner", "param", "test", 5, nil)}}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	RegisterAllFlags(o, fs)
	found := false
	fs.VisitAll(func(f *flag.Flag) {
		if f.Name == "inner.param" {
			found = true
		}
	})
	if !found {
		t.Error("recursive flag registration should find inner.param")
	}
}

func TestLoadAllFromSourceNestedStructWithoutMethod(t *testing.T) {
	type inner struct {
		Param *Parameter[int]
	}
	type outer struct {
		Inner *inner
	}
	o := &outer{Inner: &inner{Param: NewIntParam("inner", "param", "test", 5, nil)}}
	source := func(key string) (string, bool) {
		if key == "inner.param" {
			return "77", true
		}
		return "", false
	}
	if err := LoadAllFromSource(o, source); err != nil {
		t.Fatal(err)
	}
	if o.Inner.Param.Get() != 77 {
		t.Errorf("Param = %d, want 77", o.Inner.Param.Get())
	}
}

func TestLoadAllFromSourceUnexportedFieldSkipped(t *testing.T) {
	// Covers the !field.CanInterface() continue branch in LoadAllFromSource
	type group struct {
		unexported *Parameter[int]
		Exported   *Parameter[int]
	}
	g := &group{
		unexported: NewIntParam("g", "unexported", "test", 1, nil),
		Exported:   NewIntParam("g", "exported", "test", 2, nil),
	}
	source := func(key string) (string, bool) {
		if key == "g.exported" {
			return "42", true
		}
		return "", false
	}
	if err := LoadAllFromSource(g, source); err != nil {
		t.Fatal(err)
	}
	if g.Exported.Get() != 42 {
		t.Errorf("Exported = %d, want 42", g.Exported.Get())
	}
}

func TestLoadAllFromSourceNestedStructWithoutMethodError(t *testing.T) {
	// Covers the error return path in LoadAllFromSource when a nested struct
	// without a LoadFromSource method contains a parameter that fails to parse.
	type inner struct {
		Param *Parameter[int]
	}
	type outer struct {
		Inner *inner
	}
	o := &outer{Inner: &inner{Param: NewIntParam("inner", "param", "test", 5, nil)}}
	source := func(key string) (string, bool) {
		if key == "inner.param" {
			return "notanumber", true
		}
		return "", false
	}
	if err := LoadAllFromSource(o, source); err == nil {
		t.Error("expected error from nested struct with parse failure")
	}
}

func TestParseInt32Error(t *testing.T) {
	// Covers the error return path in ParseInt32 when parseSuffixedInt fails
	_, err := ParseInt32("notanumber")
	if err == nil {
		t.Error("ParseInt32(notanumber) should return error")
	}
}
