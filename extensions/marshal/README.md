# marshal

High-performance binary serialization for Go primitive types. Stdlib only, zero allocations on the marshal path.

All functions write to / read from a caller-provided `[]byte` buffer and return the number of bytes consumed, making them composable for building binary formats.

## Install

```
go get github.com/a-kazakov/gomr/extensions/marshal
```

## Usage

```go
import "github.com/a-kazakov/gomr/extensions/marshal"

// Marshal
buf := make([]byte, 256)
n := marshal.MarshalString("hello", buf)
n += marshal.MarshalInt64(42, buf[n:])
n += marshal.MarshalFloat64(3.14, buf[n:])
n += marshal.MarshalBool(true, buf[n:])

// Unmarshal
var s string
var i int64
var f float64
var b bool
m := marshal.UnmarshalString(buf, &s)
m += marshal.UnmarshalInt64(buf[m:], &i)
m += marshal.UnmarshalFloat64(buf[m:], &f)
m += marshal.UnmarshalBool(buf[m:], &b)
```

## API

### Primitives

Fixed-size types using native byte order:

| Type | Functions | Size |
|------|-----------|------|
| `bool` | `MarshalBool` / `UnmarshalBool` | 1 byte |
| `byte` | `MarshalByte` / `UnmarshalByte` | 1 byte |
| `int` | `MarshalIntAs8` / `MarshalIntAs16` / `MarshalIntAs32` / `MarshalIntAs64` | 1/2/4/8 bytes |
| `int16` | `MarshalInt16` / `UnmarshalInt16` | 2 bytes |
| `int32` | `MarshalInt32` / `UnmarshalInt32` | 4 bytes |
| `int64` | `MarshalInt64` / `UnmarshalInt64` | 8 bytes |
| `uint8` | `MarshalUint8` / `UnmarshalUint8` | 1 byte |
| `uint16` | `MarshalUint16` / `UnmarshalUint16` | 2 bytes |
| `uint32` | `MarshalUint32` / `UnmarshalUint32` | 4 bytes |
| `uint64` | `MarshalUint64` / `UnmarshalUint64` | 8 bytes |
| `float32` | `MarshalFloat32` / `UnmarshalFloat32` | 4 bytes |
| `float64` | `MarshalFloat64` / `UnmarshalFloat64` | 8 bytes |

### Strings

4-byte length prefix + data:

```go
marshal.MarshalString(s, buf)      // write
marshal.UnmarshalString(buf, &s)   // read (copies bytes into new string)
```

### Byte slices

4-byte length prefix + data:

```go
marshal.MarshalBytes(src, buf)           // write
marshal.UnmarshalBytes(buf, dest)        // read into pre-allocated slice
result, n := marshal.UnmarshalBytesAlloc(buf)  // read with allocation
```

### Unsafe struct copy

Direct memory copy for POD types (no pointers, no padding surprises):

```go
marshal.MarshalStructUnsafe(&myStruct, buf)
marshal.UnmarshalStructUnsafe(buf, &myStruct)
```
