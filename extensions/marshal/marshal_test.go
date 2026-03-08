package marshal

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// Primitive round-trip tests (table-driven)
// ---------------------------------------------------------------------------

func TestBoolRoundTrip(t *testing.T) {
	for _, v := range []bool{true, false} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 1)
			n := MarshalBool(v, buf)
			if n != 1 {
				t.Fatalf("MarshalBool returned %d, want 1", n)
			}
			var got bool
			n = UnmarshalBool(buf, &got)
			if n != 1 {
				t.Fatalf("UnmarshalBool returned %d, want 1", n)
			}
			if got != v {
				t.Fatalf("got %v, want %v", got, v)
			}
		})
	}
}

func TestByteRoundTrip(t *testing.T) {
	for _, v := range []byte{0, 1, 127, 255} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 1)
			n := MarshalByte(v, buf)
			if n != 1 {
				t.Fatalf("MarshalByte returned %d, want 1", n)
			}
			var got byte
			n = UnmarshalByte(buf, &got)
			if n != 1 {
				t.Fatalf("UnmarshalByte returned %d, want 1", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestInt16RoundTrip(t *testing.T) {
	for _, v := range []int16{0, 1, -1, math.MaxInt16, math.MinInt16} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 2)
			n := MarshalInt16(v, buf)
			if n != 2 {
				t.Fatalf("MarshalInt16 returned %d, want 2", n)
			}
			var got int16
			n = UnmarshalInt16(buf, &got)
			if n != 2 {
				t.Fatalf("UnmarshalInt16 returned %d, want 2", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestInt32RoundTrip(t *testing.T) {
	for _, v := range []int32{0, 1, -1, math.MaxInt32, math.MinInt32} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 4)
			n := MarshalInt32(v, buf)
			if n != 4 {
				t.Fatalf("MarshalInt32 returned %d, want 4", n)
			}
			var got int32
			n = UnmarshalInt32(buf, &got)
			if n != 4 {
				t.Fatalf("UnmarshalInt32 returned %d, want 4", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestInt64RoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 8)
			n := MarshalInt64(v, buf)
			if n != 8 {
				t.Fatalf("MarshalInt64 returned %d, want 8", n)
			}
			var got int64
			n = UnmarshalInt64(buf, &got)
			if n != 8 {
				t.Fatalf("UnmarshalInt64 returned %d, want 8", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestUint8RoundTrip(t *testing.T) {
	for _, v := range []uint8{0, 1, 127, math.MaxUint8} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 1)
			n := MarshalUint8(v, buf)
			if n != 1 {
				t.Fatalf("MarshalUint8 returned %d, want 1", n)
			}
			var got uint8
			n = UnmarshalUint8(buf, &got)
			if n != 1 {
				t.Fatalf("UnmarshalUint8 returned %d, want 1", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestUint16RoundTrip(t *testing.T) {
	for _, v := range []uint16{0, 1, math.MaxUint16} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 2)
			n := MarshalUint16(v, buf)
			if n != 2 {
				t.Fatalf("MarshalUint16 returned %d, want 2", n)
			}
			var got uint16
			n = UnmarshalUint16(buf, &got)
			if n != 2 {
				t.Fatalf("UnmarshalUint16 returned %d, want 2", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestUint32RoundTrip(t *testing.T) {
	for _, v := range []uint32{0, 1, math.MaxUint32} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 4)
			n := MarshalUint32(v, buf)
			if n != 4 {
				t.Fatalf("MarshalUint32 returned %d, want 4", n)
			}
			var got uint32
			n = UnmarshalUint32(buf, &got)
			if n != 4 {
				t.Fatalf("UnmarshalUint32 returned %d, want 4", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestUint64RoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, math.MaxUint64} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 8)
			n := MarshalUint64(v, buf)
			if n != 8 {
				t.Fatalf("MarshalUint64 returned %d, want 8", n)
			}
			var got uint64
			n = UnmarshalUint64(buf, &got)
			if n != 8 {
				t.Fatalf("UnmarshalUint64 returned %d, want 8", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestFloat32RoundTrip(t *testing.T) {
	cases := []float32{0, 1.5, -1.5, math.MaxFloat32, math.SmallestNonzeroFloat32, float32(math.Inf(1)), float32(math.Inf(-1))}
	for _, v := range cases {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 4)
			n := MarshalFloat32(v, buf)
			if n != 4 {
				t.Fatalf("MarshalFloat32 returned %d, want 4", n)
			}
			var got float32
			n = UnmarshalFloat32(buf, &got)
			if n != 4 {
				t.Fatalf("UnmarshalFloat32 returned %d, want 4", n)
			}
			if got != v {
				t.Fatalf("got %v, want %v", got, v)
			}
		})
	}
}

func TestFloat32NaN(t *testing.T) {
	buf := make([]byte, 4)
	MarshalFloat32(float32(math.NaN()), buf)
	var got float32
	UnmarshalFloat32(buf, &got)
	if !math.IsNaN(float64(got)) {
		t.Fatalf("expected NaN, got %v", got)
	}
}

func TestFloat64RoundTrip(t *testing.T) {
	cases := []float64{0, 1.5, -1.5, math.MaxFloat64, math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1)}
	for _, v := range cases {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 8)
			n := MarshalFloat64(v, buf)
			if n != 8 {
				t.Fatalf("MarshalFloat64 returned %d, want 8", n)
			}
			var got float64
			n = UnmarshalFloat64(buf, &got)
			if n != 8 {
				t.Fatalf("UnmarshalFloat64 returned %d, want 8", n)
			}
			if got != v {
				t.Fatalf("got %v, want %v", got, v)
			}
		})
	}
}

func TestFloat64NaN(t *testing.T) {
	buf := make([]byte, 8)
	MarshalFloat64(math.NaN(), buf)
	var got float64
	UnmarshalFloat64(buf, &got)
	if !math.IsNaN(got) {
		t.Fatalf("expected NaN, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// IntAs variants
// ---------------------------------------------------------------------------

func TestIntAs64RoundTrip(t *testing.T) {
	for _, v := range []int{0, 1, -1, math.MaxInt64, math.MinInt64} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 8)
			n := MarshalIntAs64(v, buf)
			if n != 8 {
				t.Fatalf("returned %d", n)
			}
			var got int
			n = UnmarshalIntAs64(buf, &got)
			if n != 8 {
				t.Fatalf("returned %d", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestIntAs32RoundTrip(t *testing.T) {
	for _, v := range []int{0, 1, 42, math.MaxInt32, math.MaxUint32} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 4)
			n := MarshalIntAs32(v, buf)
			if n != 4 {
				t.Fatalf("returned %d", n)
			}
			var got int
			n = UnmarshalIntAs32(buf, &got)
			if n != 4 {
				t.Fatalf("returned %d", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestIntAs16RoundTrip(t *testing.T) {
	for _, v := range []int{0, 1, 255, math.MaxInt16, 0} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 2)
			n := MarshalIntAs16(v, buf)
			if n != 2 {
				t.Fatalf("returned %d", n)
			}
			var got int
			n = UnmarshalIntAs16(buf, &got)
			if n != 2 {
				t.Fatalf("returned %d", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

func TestIntAs8RoundTrip(t *testing.T) {
	for _, v := range []int{0, 1, 127, 255} {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 1)
			n := MarshalIntAs8(v, buf)
			if n != 1 {
				t.Fatalf("returned %d", n)
			}
			var got int
			n = UnmarshalIntAs8(buf, &got)
			if n != 1 {
				t.Fatalf("returned %d", n)
			}
			if got != v {
				t.Fatalf("got %d, want %d", got, v)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// String marshal/unmarshal
// ---------------------------------------------------------------------------

func TestStringRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		val  string
	}{
		{"empty", ""},
		{"ascii", "hello world"},
		{"utf8_multibyte", "\u00e9\u00e8\u00ea\u4e16\u754c\U0001f600"},
		{"large", string(make([]byte, 4096))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 4+len(tc.val))
			n := MarshalString(tc.val, buf)
			if n != 4+len(tc.val) {
				t.Fatalf("MarshalString returned %d, want %d", n, 4+len(tc.val))
			}
			var got string
			n = UnmarshalString(buf, &got)
			if n != 4+len(tc.val) {
				t.Fatalf("UnmarshalString returned %d, want %d", n, 4+len(tc.val))
			}
			if got != tc.val {
				t.Fatalf("got %q, want %q", got, tc.val)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Byte-slice marshal/unmarshal
// ---------------------------------------------------------------------------

func TestBytesRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		val  []byte
	}{
		{"empty", []byte{}},
		{"small", []byte{1, 2, 3, 4, 5}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 4+len(tc.val))
			n := MarshalBytes(tc.val, buf)
			if n != 4+len(tc.val) {
				t.Fatalf("MarshalBytes returned %d, want %d", n, 4+len(tc.val))
			}
			dest := make([]byte, len(tc.val))
			n = UnmarshalBytes(buf, dest)
			if n != 4+len(tc.val) {
				t.Fatalf("UnmarshalBytes returned %d, want %d", n, 4+len(tc.val))
			}
			for i := range tc.val {
				if dest[i] != tc.val[i] {
					t.Fatalf("mismatch at index %d: got %d, want %d", i, dest[i], tc.val[i])
				}
			}
		})
	}
}

func TestUnmarshalBytesAlloc(t *testing.T) {
	cases := []struct {
		name string
		val  []byte
	}{
		{"empty", []byte{}},
		{"data", []byte{10, 20, 30}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, 4+len(tc.val))
			MarshalBytes(tc.val, buf)

			got, n := UnmarshalBytesAlloc(buf)
			if n != 4+len(tc.val) {
				t.Fatalf("returned %d, want %d", n, 4+len(tc.val))
			}
			if len(got) != len(tc.val) {
				t.Fatalf("length %d, want %d", len(got), len(tc.val))
			}
			for i := range tc.val {
				if got[i] != tc.val[i] {
					t.Fatalf("mismatch at %d", i)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Unsafe struct marshal
// ---------------------------------------------------------------------------

type podStruct struct {
	A int32
	B float64
	C uint16
}

func TestStructUnsafeRoundTrip(t *testing.T) {
	original := podStruct{A: 42, B: 3.14, C: 999}
	buf := make([]byte, 64) // comfortably large
	n := MarshalStructUnsafe(&original, buf)
	if n == 0 {
		t.Fatal("MarshalStructUnsafe returned 0")
	}

	var got podStruct
	m := UnmarshalStructUnsafe(buf, &got)
	if m != n {
		t.Fatalf("size mismatch: marshal %d, unmarshal %d", n, m)
	}
	if got != original {
		t.Fatalf("got %+v, want %+v", got, original)
	}
}

// ---------------------------------------------------------------------------
// Panic on insufficient data
// ---------------------------------------------------------------------------

func TestPanicNotEnoughDataString(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic, got none")
		}
	}()
	// Encode a string claiming length 100 but provide only 5 bytes total.
	buf := make([]byte, 5)
	MarshalIntAs32(100, buf) // write length prefix = 100
	var s string
	UnmarshalString(buf, &s)
}

func TestPanicNotEnoughDataBytes(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic, got none")
		}
	}()
	buf := make([]byte, 5)
	MarshalIntAs32(100, buf)
	dest := make([]byte, 100)
	UnmarshalBytes(buf, dest)
}

func TestPanicNotEnoughDataBytesAlloc(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic, got none")
		}
	}()
	buf := make([]byte, 5)
	MarshalIntAs32(100, buf)
	UnmarshalBytesAlloc(buf)
}
