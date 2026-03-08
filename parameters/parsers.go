package parameters

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/a-kazakov/gomr/internal/core"
)

type Parser[T any] func(s string) (T, error)

// parseSuffixedInt parses an integer with an optional suffix.
// multipliers maps suffix strings to their multiplier values.
func parseSuffixedInt(s string, multipliers map[string]int64) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}

	// Find where digits end and suffix begins
	suffixStart := len(s)
	for i := len(s) - 1; i >= 0; i-- {
		if unicode.IsDigit(rune(s[i])) || s[i] == '.' || s[i] == '-' || s[i] == '+' {
			suffixStart = i + 1
			break
		}
	}

	numPart := strings.TrimSpace(s[:suffixStart])
	suffixPart := strings.TrimSpace(s[suffixStart:])

	if numPart == "" {
		return 0, fmt.Errorf("no numeric part in %q", s)
	}

	// Parse the numeric part
	value, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil {
		// Try parsing as float and truncate
		fvalue, ferr := strconv.ParseFloat(numPart, 64)
		if ferr != nil {
			return 0, fmt.Errorf("invalid number %q: %w", numPart, err)
		}
		value = int64(fvalue)
	}

	// Apply suffix multiplier
	if suffixPart != "" {
		suffixUpper := strings.ToUpper(suffixPart)
		multiplier, ok := multipliers[suffixUpper]
		if !ok {
			return 0, fmt.Errorf("unknown suffix %q", suffixPart)
		}
		value *= multiplier
	}

	return value, nil
}

// Decimal multipliers (base 10): K=1e3, M=1e6, G=1e9, T=1e12
var decimalMultipliers = map[string]int64{
	"K":  1_000,
	"M":  1_000_000,
	"G":  1_000_000_000,
	"T":  1_000_000_000_000,
	"KI": 1_000,     // Allow Ki but treat as decimal
	"MI": 1_000_000, // Allow Mi but treat as decimal
	"GI": 1_000_000_000,
	"TI": 1_000_000_000_000,
}

// Binary multipliers (base 2): K/Ki=2^10, M/Mi=2^20, G/Gi=2^30, T/Ti=2^40
var binaryMultipliers = map[string]int64{
	"K":   1 << 10, // 1,024
	"KI":  1 << 10,
	"KB":  1 << 10,
	"KIB": 1 << 10,
	"M":   1 << 20, // 1,048,576
	"MI":  1 << 20,
	"MB":  1 << 20,
	"MIB": 1 << 20,
	"G":   1 << 30, // 1,073,741,824
	"GI":  1 << 30,
	"GB":  1 << 30,
	"GIB": 1 << 30,
	"T":   1 << 40,
	"TI":  1 << 40,
	"TB":  1 << 40,
	"TIB": 1 << 40,
}

// ParseInt parses an integer with optional decimal suffixes (K, M, G, T).
// Examples: "1000", "1K" (1000), "5M" (5000000)
func ParseInt(s string) (int, error) {
	v, err := parseSuffixedInt(s, decimalMultipliers)
	if err != nil {
		return 0, err
	}
	return int(v), nil
}

func ParseInt32(s string) (int32, error) {
	v, err := parseSuffixedInt(s, decimalMultipliers)
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt32 || v < math.MinInt32 {
		return 0, fmt.Errorf("value %d overflows int32", v)
	}
	return int32(v), nil
}

func ParseInt64(s string) (int64, error) {
	return parseSuffixedInt(s, decimalMultipliers)
}

// ParseByteSize parses a byte size with binary suffixes (K, Ki, M, Mi, G, Gi, T, Ti).
// Examples: "1024", "1K" (1024), "512Mi" (536870912)
func ParseByteSize(s string) (int64, error) {
	return parseSuffixedInt(s, binaryMultipliers)
}

func ParseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(s), 64)
}

func ParseFloat32(s string) (float32, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 32)
	return float32(v), err
}

func ParseString(s string) (string, error) {
	return s, nil
}

// ParseBool parses a boolean. Accepts: true, false, 1, 0, yes, no, on, off.
func ParseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value %q", s)
	}
}

// ParseDuration parses a time.Duration using Go's standard format.
// Examples: "1h30m", "500ms", "2s"
func ParseDuration(s string) (time.Duration, error) {
	return time.ParseDuration(strings.TrimSpace(s))
}

func FormatByteSize(v int64) string {
	const (
		ki = 1 << 10
		mi = 1 << 20
		gi = 1 << 30
		ti = 1 << 40
	)
	switch {
	case v >= ti && v%ti == 0:
		return fmt.Sprintf("%dTi", v/ti)
	case v >= gi && v%gi == 0:
		return fmt.Sprintf("%dGi", v/gi)
	case v >= mi && v%mi == 0:
		return fmt.Sprintf("%dMi", v/mi)
	case v >= ki && v%ki == 0:
		return fmt.Sprintf("%dKi", v/ki)
	default:
		return fmt.Sprintf("%d", v)
	}
}

func FormatDecimal(v int64) string {
	const (
		k = 1_000
		m = 1_000_000
		g = 1_000_000_000
		t = 1_000_000_000_000
	)
	switch {
	case v >= t && v%t == 0:
		return fmt.Sprintf("%dT", v/t)
	case v >= g && v%g == 0:
		return fmt.Sprintf("%dG", v/g)
	case v >= m && v%m == 0:
		return fmt.Sprintf("%dM", v/m)
	case v >= k && v%k == 0:
		return fmt.Sprintf("%dK", v/k)
	default:
		return fmt.Sprintf("%d", v)
	}
}

func ParseCompressionAlgorithm(s string) (core.CompressionAlgorithm, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "none":
		return core.CompressionAlgorithmNone, nil
	case "lz4":
		return core.CompressionAlgorithmLz4, nil
	case "zstdfast":
	case "zstd-fast":
	case "zstd":
		return core.CompressionAlgorithmZstdFast, nil
	case "zstd-default":
	case "zstddefault":
		return core.CompressionAlgorithmZstdDefault, nil
	default:
		return 0, fmt.Errorf("invalid compression algorithm %q", s)
	}
	return 0, nil
}
